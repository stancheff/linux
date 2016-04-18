/*
 * Kernel Device Mapper for abstracting ZAC/ZBC devices as normal
 * block devices for linux file systems.
 *
 * Copyright (C) 2015 Seagate Technology PLC
 *
 * Written by:
 * Shaun Tancheff <shaun.tancheff@seagate.com>
 *
 * This file is licensed under  the terms of the GNU General Public
 * License version 2. This program is licensed "as is" without any
 * warranty of any kind, whether express or implied.
 */

#include "dm.h"
#include <linux/dm-io.h>
#include <linux/init.h>
#include <linux/mempool.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/vmalloc.h>
#include <linux/random.h>
#include <linux/crc32c.h>
#include <linux/crc16.h>
#include <linux/sort.h>
#include <linux/ctype.h>
#include <linux/types.h>
#include <linux/blkzoned_api.h>
#include <linux/timer.h>
#include <linux/delay.h>
#include <linux/kthread.h>
#include <linux/freezer.h>
#include <linux/proc_fs.h>
#include <linux/seq_file.h>
#include <linux/kfifo.h>
#include <linux/bsearch.h>
#include "dm-zdm.h"

#define PRIu64 "llu"
#define PRIx64 "llx"
#define PRId32 "d"
#define PRIx32 "x"
#define PRIu32 "u"

#define BIOSET_RESV 4

/**
 * _zdisk() - Return a pretty ZDM name.
 * @znd: ZDM Instance
 *
 * Return: ZDM/backing device pretty name.
 */
static inline char *_zdisk(struct zdm *znd)
{
	return znd->bdev_name;
}

#define Z_ERR(znd, fmt, arg...) \
	pr_err("dm-zdm(%s): " fmt "\n", _zdisk(znd), ## arg)

#define Z_INFO(znd, fmt, arg...) \
	pr_info("dm-zdm(%s): " fmt "\n", _zdisk(znd), ## arg)

#define Z_DBG(znd, fmt, arg...) \
	pr_debug("dm-zdm(%s): " fmt "\n", _zdisk(znd), ## arg)

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
static void do_io_work(struct work_struct *work);
static int block_io(struct zdm *, enum dm_io_mem_type, void *, sector_t,
		    unsigned int, u8 op, unsigned int op_flgs, int);
static int znd_async_io(struct zdm *znd,
			enum dm_io_mem_type dtype,
			void *data, sector_t block, unsigned int nDMsect,
			u8 bi_op, unsigned int bi_flags, int queue,
			io_notify_fn callback, void *context);
static int zoned_bio(struct zdm *znd, struct bio *bio);
static int zoned_map_write(struct zdm *znd, struct bio*, u64 s_zdm);
static sector_t get_dev_size(struct dm_target *ti);
static int dmz_reset_wp(struct zdm *znd, u64 z_id);
static int dmz_open_zone(struct zdm *znd, u64 z_id);
static int dmz_close_zone(struct zdm *znd, u64 z_id);
static int dmz_report_zones(struct zdm *znd, u64 z_id,
			    struct page *pgs, size_t bufsz);
static void activity_timeout(unsigned long data);
static void zoned_destroy(struct zdm *);
static int gc_can_cherrypick(struct zdm *znd, u32 sid, int delay, gfp_t gfp);
static void bg_work_task(struct work_struct *work);
static void on_timeout_activity(struct zdm *znd, int delay);
static int zdm_create_proc_entries(struct zdm *znd);
static void zdm_remove_proc_entries(struct zdm *znd);

/**
 * get_bdev_bd_inode() - Get primary backing device inode
 * @znd: ZDM Instance
 *
 * Return: backing device inode
 */
static inline struct inode *get_bdev_bd_inode(struct zdm *znd)
{
	return znd->dev->bdev->bd_inode;
}

#include "libzdm.c"

#define BIO_CACHE_SECTORS (IO_VCACHE_PAGES * Z_BLOCKS_PER_DM_SECTOR)

/**
 * bio_stream() - Decode stream id from BIO.
 * @znd: ZDM Instance
 *
 * Return: stream_id
 */
static inline u32 bio_stream(struct bio *bio)
{
	u32 stream_id = 0x40;

	/*
	 * Since adding stream id to a BIO is not yet in mainline we just
	 * use this heuristic to try to skip unnecessary co-mingling of data.
	 */

	if (bio->bi_rw & REQ_META) {
		stream_id = 0xff;
	} else {
		unsigned int id = bio_get_streamid(bio);

		/* high 8 bits is hash of PID, low 8 bits is hash of inode# */
		stream_id = id >> 8;
		if (stream_id == 0)
			stream_id++;
		if (stream_id == 0xff)
			stream_id--;
	}

	return stream_id;
}

/**
 * zoned_map_discard() - Return a pretty ZDM name.
 * @znd: ZDM Instance
 * @bio: struct bio hold discard information
 * @s_zdm: tlba being discarded.
 *
 * Return: 0 on success, otherwise error code.
 */
static int zoned_map_discard(struct zdm *znd, struct bio *bio, u64 s_zdm)
{
	int rcode   = DM_MAPIO_SUBMITTED;
	u64 blks    = bio->bi_iter.bi_size / Z_C4K;
	int err;

	if (znd->is_empty)
		goto cleanup_out;

	err = z_mapped_discard(znd, s_zdm, blks, CRIT);
	if (err < 0) {
		rcode = err;
		goto out;
	}

cleanup_out:
	bio->bi_iter.bi_sector = 8;
	bio_endio(bio);
out:
	return rcode;
}

/**
 * is_non_wp_zone() - Test zone # to see if it flagged as conventional.
 * @znd: ZDM Instance
 * @z_id: Zone #
 *
 * Return: 1 if conventional zone. 0 if sequentional write zone.
 */
static int is_non_wp_zone(struct zdm *znd, u64 z_id)
{
	u32 gzoff = z_id % 1024;
	struct meta_pg *wpg = &znd->wp[z_id >> 10];
	u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]);

	return (wp & Z_WP_NON_SEQ) ? 1 : 0;
}

/**
 */
struct zone_action {
	struct work_struct work;
	struct zdm *znd;
	u64 s_addr;
	unsigned long bi_rw;
	int wp_err;
};

/**
 * do_zone_action_work() - Issue a 'zone action' to the backing device.
 * @work: Work to do.
 */
static void do_zone_action_work(struct work_struct *work)
{
	struct zone_action *za = container_of(work, struct zone_action, work);
	struct zdm *znd = za->znd;
	struct block_device *bdev = znd->dev->bdev;
	const gfp_t gfp = GFP_KERNEL;

	/* Explicitly work on device lbas not partition offsets. */
	if (bdev != bdev->bd_contains)
		bdev = bdev->bd_contains;

	za->wp_err = blkdev_issue_zone_action(bdev, za->bi_rw, za->s_addr, gfp);
}

/**
 * dmz_zone_action() - Issue a 'zone action' to the backing device (via worker).
 * @znd: ZDM Instance
 * @z_id: Zone # to open.
 * @rw: One of REQ_OPEN_ZONE, REQ_CLOSE_ZONE, or REQ_RESET_ZONE.
 *
 * Return: 0 on success, otherwise error.
 */
static int dmz_zone_action(struct zdm *znd, u64 z_id, unsigned long rw)
{
	int wp_err = 0;

	if (is_non_wp_zone(znd, z_id))
		return wp_err;

	if (znd->bdev_is_zoned) {
		u64 z_offset = zone_to_sector(z_id + znd->zdstart);
		struct zone_action za = {
			.znd = znd,
			.bi_rw = rw,
			.s_addr = z_offset,
			.wp_err = 0,
		};
		if (znd->ata_passthrough)
			za.bi_rw |= REQ_META;
		/*
		 * Issue the synchronous I/O from a different thread
		 * to avoid generic_make_request recursion.
		 */
		INIT_WORK_ONSTACK(&za.work, do_zone_action_work);
		queue_work(znd->zone_action_wq, &za.work);
		flush_workqueue(znd->zone_action_wq);
		destroy_work_on_stack(&za.work);
		wp_err = za.wp_err;

		if (wp_err) {
			Z_ERR(znd, "Zone Cmd: LBA: %" PRIx64
			      " [Z:%" PRIu64 "] -> %d failed.",
			      za.s_addr, z_id, wp_err);
			Z_ERR(znd, "ZAC/ZBC support disabled.");
			znd->bdev_is_zoned = 0;
			wp_err = -ENOTSUPP;
		}
	}
	return wp_err;
}

/**
 * dmz_reset_wp() - Reset write pointer for zone z_id.
 * @znd: ZDM Instance
 * @z_id: Zone # to reset.
 *
 * Return: 0 on success, otherwise error.
 */
static int dmz_reset_wp(struct zdm *znd, u64 z_id)
{
	return dmz_zone_action(znd, z_id, REQ_RESET_ZONE);
}

/**
 * dmz_open_zone() - Open zone for writing.
 * @znd: ZDM Instance
 * @z_id: Zone # to open.
 *
 * Return: 0 on success, otherwise error.
 */
static int dmz_open_zone(struct zdm *znd, u64 z_id)
{
	if (!znd->issue_open_zone)
		return 0;
	return dmz_zone_action(znd, z_id, REQ_OPEN_ZONE);

}

/**
 * dmz_close_zone() - Close zone to writing.
 * @znd: ZDM Instance
 * @z_id: Zone # to close.
 *
 * Return: 0 on success, otherwise error.
 */
static int dmz_close_zone(struct zdm *znd, u64 z_id)
{
	if (!znd->issue_close_zone)
		return 0;
	return dmz_zone_action(znd, z_id, REQ_CLOSE_ZONE);
}

/**
 * dmz_report_zones() - issue report zones from z_id zones after zdstart
 * @znd: ZDM Instance
 * @z_id: Zone past zdstart
 * @report: structure filled
 * @bufsz: kmalloc()'d space reserved for report
 *
 * Return: -ENOTSUPP or 0 on success
 */
static int dmz_report_zones(struct zdm *znd, u64 z_id,
			    struct page *pgs, size_t bufsz)
{
	int wp_err = -ENOTSUPP;

	if (znd->bdev_is_zoned) {
		u8  opt = ZOPT_NON_SEQ_AND_RESET;
		unsigned long bi_rw = 0;
		struct block_device *bdev = znd->dev->bdev;
		u64 s_addr = zone_to_sector(z_id + znd->zdstart);

		if (bdev != bdev->bd_contains)
			s_addr -= znd->start_sect << Z_SHFT4K;

		if (znd->ata_passthrough)
			bi_rw = REQ_META;

		wp_err = blkdev_issue_zone_report(bdev, bi_rw, s_addr, opt,
						  pgs, bufsz, GFP_KERNEL);
		if (wp_err) {
			Z_ERR(znd, "Report Zones: LBA: %" PRIx64
			      " [Z:%" PRIu64 " -> %d failed.",
			      s_addr, z_id + znd->zdstart, wp_err);
			Z_ERR(znd, "ZAC/ZBC support disabled.");
			znd->bdev_is_zoned = 0;
			wp_err = -ENOTSUPP;
		}
	}
	return wp_err;
}

#if USE_KTHREAD

static inline int stop_or_data(struct zdm *znd)
{
	if (kthread_should_stop())
		return 1;
	return kfifo_is_empty(&znd->bio_fifo) ? 0 : 1;
}

static int znd_bio_kthread(void *arg)
{
	int err = 0;
	struct zdm *znd = (struct zdm *)arg;

	Z_ERR(znd, "znd_bio_kthread [started]");

	while (!kthread_should_stop()) {
		struct bio *bio = NULL;
		int rcode;

		if (kfifo_is_empty(&znd->bio_fifo)) {
			wake_up(&znd->wait_fifo);
			wait_event_freezable(znd->wait_bio, stop_or_data(znd));
			continue;
		}
		if (!kfifo_get(&znd->bio_fifo, &bio)) {
			wake_up(&znd->wait_fifo);
			continue;
		}
		if (kfifo_avail(&znd->bio_fifo) > 5)
			wake_up(&znd->wait_fifo);

		rcode = zoned_bio(znd, bio);
		if (rcode == DM_MAPIO_REMAPPED)
			rcode = DM_MAPIO_SUBMITTED;

		if (rcode < 0) {
			znd->meta_result = err = rcode;
			goto out;
		}
	}
	err = 0;
	Z_ERR(znd, "znd_bio_kthread [stopped]");

out:
	return err;
}

/**
 * zoned_map() - Handle an incoming BIO
 * @ti: Device Mapper Target Instance
 * @bio: The BIO to disposition.
 *
 * Return: 0 on success, otherwise error.
 */
static int zoned_map(struct dm_target *ti, struct bio *bio)
{
	struct zdm *znd = ti->private;
	int rcode = DM_MAPIO_REQUEUE;

	while (kfifo_put(&znd->bio_fifo, bio) == 0) {
		wake_up_process(znd->bio_kthread);
		wake_up(&znd->wait_bio);
		wait_event_freezable(znd->wait_fifo,
			kfifo_avail(&znd->bio_fifo) > 0);
	}
	rcode = DM_MAPIO_SUBMITTED;
	wake_up_process(znd->bio_kthread);
	wake_up(&znd->wait_bio);

	return rcode;
}

#else

/**
 * zoned_map() - Handle an incoming BIO
 * @ti: Device Mapper Target Instance
 * @bio: The BIO to disposition.
 *
 * Return: 0 on success, otherwise error.
 */
static int zoned_map(struct dm_target *ti, struct bio *bio)
{
	struct zdm *znd = ti->private;
	int err = zoned_bio(znd, bio);

	if (err < 0) {
		znd->meta_result = err;
		err = 0;
	}

	return err;
}

#endif

/**
 * zoned_actual_size() - Set number of 4k blocks available on block device.
 * @ti: Device Mapper Target Instance
 * @znd: ZDM Instance
 *
 * Return: 0 on success, otherwise error.
 */
static void zoned_actual_size(struct dm_target *ti, struct zdm *znd)
{
	znd->nr_blocks = i_size_read(get_bdev_bd_inode(znd)) / Z_C4K;
}

/**
 * zoned_ctr() - Create a ZDM Instance from DM Target Instance and args.
 * @ti: Device Mapper Target Instance
 * @argc: Number of args to handle.
 * @argv: args to handle.
 *
 * Return: 0 on success, otherwise error.
 */
static int zoned_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	const int reset_non_empty = false;
	int create = 0;
	int force = 0;
	int zbc_probe = 1;
	int zac_probe = 1;
	int r;
	struct zdm *znd;
	u64 first_data_zone = 0;
	u64 mz_md_provision = MZ_METADATA_ZONES;

	BUILD_BUG_ON(Z_C4K != (sizeof(struct map_sect_to_lba) * Z_UNSORTED));
	BUILD_BUG_ON(Z_C4K != (sizeof(struct io_4k_block)));
	BUILD_BUG_ON(Z_C4K != (sizeof(struct mz_superkey)));

	znd = ZDM_ALLOC(NULL, sizeof(*znd), KM_00, NORMAL);
	if (!znd) {
		ti->error = "Error allocating zdm structure";
		return -ENOMEM;
	}

	znd->trim = 1;
	znd->raid5_trim = 0;

	if (argc < 1) {
		ti->error = "Invalid argument count";
		return -EINVAL;
	}

	for (r = 1; r < argc; r++) {
		if (isdigit(*argv[r])) {
			int krc = kstrtoll(argv[r], 0, &first_data_zone);

			if (krc != 0) {
				DMERR("Failed to parse %s: %d", argv[r], krc);
				first_data_zone = 0;
			}
		}
		if (!strcasecmp("create", argv[r]))
			create = 1;
		if (!strcasecmp("load", argv[r]))
			create = 0;
		if (!strcasecmp("force", argv[r]))
			force = 1;
		if (!strcasecmp("nozbc", argv[r]))
			zbc_probe = 0;
		if (!strcasecmp("nozac", argv[r]))
			zac_probe = 0;
		if (!strcasecmp("discard", argv[r]))
			znd->trim = 1;
		if (!strcasecmp("nodiscard", argv[r]))
			znd->trim = 0;
		if (!strcasecmp("raid5_trim", argv[r]))
			znd->raid5_trim = 1;

		if (!strncasecmp("reserve=", argv[r], 8)) {
			u64 mz_resv;
			int krc = kstrtoll(argv[r] + 8, 0, &mz_resv);

			if (krc == 0) {
				if (mz_resv > mz_md_provision)
					mz_md_provision = mz_resv;
			} else {
				DMERR("Reserved 'FAILED TO PARSE.' %s: %d",
					argv[r]+8, krc);
				mz_resv = 0;
			}
		}
	}

	znd->ti = ti;
	ti->private = znd;
	znd->zdstart = first_data_zone;
	znd->mz_provision = mz_md_provision;

	r = dm_get_device(ti, argv[0], FMODE_READ | FMODE_WRITE, &znd->dev);
	if (r) {
		ti->error = "Error opening backing device";
		zoned_destroy(znd);
		return -EINVAL;
	}

	if (znd->dev->bdev) {
		bdevname(znd->dev->bdev, znd->bdev_name);
		znd->start_sect = get_start_sect(znd->dev->bdev) >> 3;
	}

	/*
	 * Set if this target needs to receive flushes regardless of
	 * whether or not its underlying devices have support.
	 */
	ti->flush_supported = true;

	/*
	 * Set if this target needs to receive discards regardless of
	 * whether or not its underlying devices have support.
	 */
	ti->discards_supported = true;

	/*
	 * Set if the target required discard bios to be split
	 * on max_io_len boundary.
	 */
	ti->split_discard_bios = false;

	/*
	 * Set if this target does not return zeroes on discarded blocks.
	 */
	ti->discard_zeroes_data_unsupported = false;

	/*
	 * Set if this target wants discard bios to be sent.
	 */
	ti->num_discard_bios = 1;

	if (!znd->trim) {
		ti->discards_supported = false;
		ti->num_discard_bios = 0;
	}

	zoned_actual_size(ti, znd);

	r = do_init_zoned(ti, znd);
	if (r) {
		ti->error = "Error in zdm init";
		zoned_destroy(znd);
		return -EINVAL;
	}

	znd->issue_open_zone = 1;
	znd->issue_close_zone = 1;
	znd->filled_zone = NOZONE;

	if (zac_probe)
		znd->bdev_is_zoned = znd->ata_passthrough = 1;
	if (zbc_probe)
		znd->bdev_is_zoned = 1;

	r = zoned_init_disk(ti, znd, create, force);
	if (r) {
		ti->error = "Error in zdm init from disk";
		zoned_destroy(znd);
		return -EINVAL;
	}
	r = zoned_wp_sync(znd, reset_non_empty);
	if (r) {
		ti->error = "Error in zdm re-sync WP";
		zoned_destroy(znd);
		return -EINVAL;
	}
	update_all_stale_ratio(znd);

	znd->bio_set = bioset_create(BIOSET_RESV, 0);
	if (!znd->bio_set)
		return -ENOMEM;

#if USE_KTHREAD
	r = kfifo_alloc(&znd->bio_fifo, KFIFO_SIZE, GFP_KERNEL);
	if (r)
		return r;

	znd->bio_kthread = kthread_run(znd_bio_kthread, znd, "zdm-io-%s",
		znd->bdev_name);
	if (IS_ERR(znd->bio_kthread)) {
		r = PTR_ERR(znd->bio_kthread);
		ti->error = "Couldn't alloc kthread";
		zoned_destroy(znd);
		return r;
	}
#endif

	r = zdm_create_proc_entries(znd);
	if (r) {
		ti->error = "Failed to create /proc entries";
		zoned_destroy(znd);
		return -EINVAL;
	}

	mod_timer(&znd->timer, jiffies + msecs_to_jiffies(5000));

	return 0;
}

/**
 * zoned_dtr() - Deconstruct a ZDM Instance from DM Target Instance.
 * @ti: Device Mapper Target Instance
 *
 * Return: 0 on success, otherwise error.
 */
static void zoned_dtr(struct dm_target *ti)
{
	struct zdm *znd = ti->private;

	if (znd->z_sballoc) {
		struct mz_superkey *key_blk = znd->z_sballoc;
		struct zdm_superblock *sblock = &key_blk->sblock;

		sblock->flags = cpu_to_le32(0);
		sblock->csum = sb_crc32(sblock);
	}

#if USE_KTHREAD
	wake_up_process(znd->bio_kthread);
	wait_event(znd->wait_bio, kfifo_is_empty(&znd->bio_fifo));
	kthread_stop(znd->bio_kthread);
	kfifo_free(&znd->bio_fifo);
#endif
	if (znd->bio_set)
		bioset_free(znd->bio_set);
	zdm_remove_proc_entries(znd);
	zoned_destroy(znd);
}


/**
 * do_io_work() - Read or write a data from a block device.
 * @work: Work to be done.
 */
static void do_io_work(struct work_struct *work)
{
	struct z_io_req_t *req = container_of(work, struct z_io_req_t, work);
	struct dm_io_request *io_req = req->io_req;
	unsigned long error_bits = 0;

	req->result = dm_io(io_req, 1, req->where, &error_bits);
	if (error_bits)
		DMERR("ERROR: dm_io error: %lx", error_bits);
}

/**
 * znd_async_io() - Issue I/O via dm_io async or sync (using worker thread).
 * @znd: ZDM Instance
 * @dtype: Type of memory in data
 * @data: Data for I/O
 * @block: bLBA for I/O
 * @nDMsect: Number of 512 byte blocks to read/write.
 * @rw: READ or WRITE
 * @queue: if true then use worker thread for I/O and wait.
 * @callback: callback to use on I/O complete.
 * context: context to be passed to callback.
 *
 * Return 0 on success, otherwise error.
 */
static int znd_async_io(struct zdm *znd,
			enum dm_io_mem_type dtype,
			void *data, sector_t block, unsigned int nDMsect,
			u8 bi_op, unsigned int bi_flags, int queue,
			io_notify_fn callback, void *context)
{
	unsigned long error_bits = 0;
	int rcode;
	struct dm_io_region where = {
		.bdev = znd->dev->bdev,
		.sector = block,
		.count = nDMsect,
	};

	struct dm_io_request io_req = {
		.bi_op = bi_op,
		.bi_op_flags = bi_flags,
		.mem.type = dtype,
		.mem.offset = 0,
		.mem.ptr.vma = data,
		.client = znd->io_client,
		.notify.fn = callback,
		.notify.context = context,
	};

	switch (dtype) {
	case DM_IO_KMEM:
		io_req.mem.ptr.addr = data;
		break;
	case DM_IO_BIO:
		io_req.mem.ptr.bio = data;
		where.count = nDMsect;
		break;
	case DM_IO_VMA:
		io_req.mem.ptr.vma = data;
		break;
	default:
		Z_ERR(znd, "page list not handled here ..  see dm-io.");
		break;
	}

	if (queue) {
		struct z_io_req_t req;

		/*
		 * Issue the synchronous I/O from a different thread
		 * to avoid generic_make_request recursion.
		 */
		INIT_WORK_ONSTACK(&req.work, do_io_work);
		req.where = &where;
		req.io_req = &io_req;
		queue_work(znd->io_wq, &req.work);
		flush_workqueue(znd->io_wq);
		destroy_work_on_stack(&req.work);

		rcode = req.result;
		if (rcode < 0)
			Z_ERR(znd, "ERROR: dm_io error: %d", rcode);
		goto done;
	}
	rcode = dm_io(&io_req, 1, &where, &error_bits);
	if (error_bits || rcode < 0)
		Z_ERR(znd, "ERROR: dm_io error: %d -- %lx", rcode, error_bits);

done:
	return rcode;
}

/**
 * block_io() - Issue sync I/O maybe using using a worker thread.
 * @znd: ZDM Instance
 * @dtype: Type of memory in data
 * @data: Data for I/O
 * @sector: bLBA for I/O [512 byte resolution]
 * @nblks: Number of 512 byte blocks to read/write.
 * @op: bi_op (Read/Write/Discard ... )
 * @op_flags: bi_op_flags (Sync, Flush, FUA, ...)
 * @queue: if true then use worker thread for I/O and wait.
 *
 * Return 0 on success, otherwise error.
 */
static int block_io(struct zdm *znd,
		    enum dm_io_mem_type dtype, void *data, sector_t sector,
		    unsigned int nblks, u8 op, unsigned int op_flags, int queue)
{
	return znd_async_io(znd, dtype, data, sector, nblks, op,
			    op_flags, queue, NULL, NULL);
}

/**
 * read_block() - Issue sync read maybe using using a worker thread.
 * @ti: Device Mapper Target Instance
 * @dtype: Type of memory in data
 * @data: Data for I/O
 * @lba: bLBA for I/O [4k resolution]
 * @count: Number of 4k blocks to read/write.
 * @queue: if true then use worker thread for I/O and wait.
 *
 * Return 0 on success, otherwise error.
 */
static int read_block(struct dm_target *ti, enum dm_io_mem_type dtype,
		      void *data, u64 lba, unsigned int count, int queue)
{
	struct zdm *znd = ti->private;
	sector_t block = lba << Z_SHFT4K;
	unsigned int nDMsect = count << Z_SHFT4K;
	int rc;

	if (lba >= znd->nr_blocks) {
		Z_ERR(znd, "Error reading past end of media: %llx.", lba);
		rc = -EIO;
		return rc;
	}

	rc = block_io(znd, dtype, data, block, nDMsect, REQ_OP_READ, 0, queue);
	if (rc) {
		Z_ERR(znd, "read error: %d -- R: %llx [%u dm sect] (Q:%d)",
			rc, lba, nDMsect, queue);
		dump_stack();
	}

	return rc;
}

/**
 * writef_block() - Issue sync write maybe using using a worker thread.
 * @ti: Device Mapper Target Instance
 * @dtype: Type of memory in data
 * @data: Data for I/O
 * @lba: bLBA for I/O [4k resolution]
 * @op_flags: bi_op_flags for bio (Sync/Flush/FUA)
 * @count: Number of 4k blocks to read/write.
 * @queue: if true then use worker thread for I/O and wait.
 *
 * Return 0 on success, otherwise error.
 */
static int writef_block(struct dm_target *ti, enum dm_io_mem_type dtype,
			void *data, u64 lba, unsigned int op_flags,
			unsigned int count, int queue)
{
	struct zdm *znd = ti->private;
	sector_t block = lba << Z_SHFT4K;
	unsigned int nDMsect = count << Z_SHFT4K;
	int rc;

	rc = block_io(znd, dtype, data, block, nDMsect, REQ_OP_WRITE,
		      op_flags, queue);
	if (rc) {
		Z_ERR(znd, "write error: %d W: %llx [%u dm sect] (Q:%d)",
			rc, lba, nDMsect, queue);
		dump_stack();
	}

	return rc;
}

/**
 * write_block() - Issue sync write maybe using using a worker thread.
 * @ti: Device Mapper Target Instance
 * @dtype: Type of memory in data
 * @data: Data for I/O
 * @lba: bLBA for I/O [4k resolution]
 * @count: Number of 4k blocks to read/write.
 * @queue: if true then use worker thread for I/O and wait.
 *
 * Return 0 on success, otherwise error.
 */
static int write_block(struct dm_target *ti, enum dm_io_mem_type dtype,
		       void *data, u64 lba, unsigned int count, int queue)
{
	unsigned int op_flags = 0;

	return writef_block(ti, dtype, data, lba, op_flags, count, queue);
}

/**
 * struct zsplit_hook - Extra data attached to a hooked bio
 * @znd: ZDM Instance to update on BIO completion.
 * @endio: BIO's original bi_end_io handler
 * @private: BIO's original bi_private data.
 */
struct zsplit_hook {
	struct zdm *znd;
	bio_end_io_t *endio;
	void *private;
};

/**
 * hook_bio() - Wrapper for hooking bio's endio function.
 * @znd: ZDM Instance
 * @bio: Bio to clone and hook
 * @endiofn: End IO Function to hook with.
 */
static int hook_bio(struct zdm *znd, struct bio *split, bio_end_io_t *endiofn)
{
	struct zsplit_hook *hook = kmalloc(sizeof(*hook), GFP_NOIO);

	if (!hook)
		return -ENOMEM;

	/*
	 * On endio report back to ZDM Instance and restore
	 * original the bi_private and bi_end_io.
	 * Since all of our splits are also chain'd we also
	 * 'know' that bi_private will be the bio we sharded
	 * and that bi_end_io is the bio_chain_endio helper.
	 */
	hook->znd = znd;
	hook->private = split->bi_private; /* = bio */
	hook->endio = split->bi_end_io; /* = bio_chain_endio */

	/*
	 * Now on complete the bio will call endiofn which is 'zsplit_endio'
	 * and we can record the update WP location and restore the
	 * original bi_private and bi_end_io
	 */
	split->bi_private = hook;
	split->bi_end_io = endiofn;

	return 0;
}


/**
 * TODO: On write error such as this we can incr wp-used but we need
 * to re-queue/re-map the write to a new location on disk?
 *
 * sd 0:0:1:0: [sdb] tag#1 FAILED Result: hostbyte=DID_SOFT_ERROR
 *	driverbyte=DRIVER_OK
 * sd 0:0:1:0: [sdb] tag#1
 *	CDB: Write(16) 8a 00 00 00 00 00 06 d4 92 a0 00 00 00 08 00 00
 *	blk_update_request: I/O error, dev sdb, sector 114594464
 * exec scsi cmd failed,opcode:133
 * sdb: command 1 failed
 * sd 0:0:1:0: [sdb] tag#1
 *	CDB: Write(16) 8a 00 00 00 00 00 00 01 0a 70 00 00 00 18 00 00
 * mpt3sas_cm0:    sas_address(0x4433221105000000), phy(5)
 * mpt3sas_cm0:    enclosure_logical_id(0x500605b0074854d0),slot(6)
 * mpt3sas_cm0:    enclosure level(0x0000), connector name(     )
 * mpt3sas_cm0:    handle(0x000a), ioc_status(success)(0x0000), smid(17)
 * mpt3sas_cm0:    request_len(12288), underflow(12288), resid(-1036288)
 * mpt3sas_cm0:    tag(65535), transfer_count(1048576), sc->result(0x00000000)
 * mpt3sas_cm0:    scsi_status(check condition)(0x02),
 *	scsi_state(autosense valid )(0x01)
 * mpt3sas_cm0:    [sense_key,asc,ascq]: [0x06,0x29,0x00], count(18)
 * Aborting journal on device dm-0-8.
 * EXT4-fs error (device dm-0):
 *	ext4_journal_check_start:56: Detected aborted journal
 * EXT4-fs (dm-0): Remounting filesystem read-only
 */

/**
 * _common_endio() - Bio endio tracking for update internal WP.
 * @bio: Bio being completed.
 *
 * Bios that are split for writing are usually split to land on a zone
 * boundary. Forward the bio along the endio path and update the WP.
 */
static void _common_endio(struct zdm *znd, struct bio *bio)
{
	u64 lba = bio->bi_iter.bi_sector >> Z_SHFT4K;
	u32 blks = bio->bi_iter.bi_size / Z_C4K;

	if (bio_data_dir(bio) == WRITE && lba > znd->start_sect) {
		lba -= znd->start_sect;
		if (lba > 0)
			increment_used_blks(znd, lba - 1, blks + 1);
	}
}

/**
 * zoned_endio() - DM bio completion notification.
 * @ti: DM Target instance.
 * @bio: Bio being completed.
 * @err: Error associated with bio.
 *
 * Non-split and non-dm_io bios end notification is here.
 * Update the WP location for WRITE bios.
 */
static int zoned_endio(struct dm_target *ti, struct bio *bio, int err)
{
	struct zdm *znd = ti->private;

	_common_endio(znd, bio);
	return 0;
}

/**
 * zsplit_endio() - Bio endio tracking for update internal WP.
 * @bio: Bio being completed.
 *
 * Bios that are split for writing are usually split to land on a zone
 * boundary. Forward the bio along the endio path and update the WP.
 */
static void zsplit_endio(struct bio *bio)
{
	struct zsplit_hook *hook = bio->bi_private;
	struct bio *parent = hook->private;
	struct zdm *znd = hook->znd;

	_common_endio(znd, bio);

	bio->bi_private = hook->private;
	bio->bi_end_io = hook->endio;

	/* On split bio's we are responsible for de-ref'ing and freeing */
	bio_put(bio);
	if (parent)
		bio_endio(parent);

	/* release our temporary private data */
	kfree(hook);
}

/**
 * zsplit_bio() - Split and chain a bio.
 * @znd: ZDM Instance
 * @bio: Bio to split
 * @sectors: Number of sectors.
 *
 * Return: split bio.
 */
static struct bio *zsplit_bio(struct zdm *znd, struct bio *bio, int sectors)
{
	struct bio *split = bio;

	if (bio_sectors(bio) > sectors) {
		split = bio_split(bio, sectors, GFP_NOIO, znd->bio_set);
		if (!split)
			goto out;
		bio_chain(split, bio);
		if (bio_data_dir(bio) == WRITE)
			hook_bio(znd, split, zsplit_endio);
	}
out:
	return split;
}

/**
 * zm_cow() - Read Modify Write to write less than 4k size blocks.
 * @znd: ZDM Instance
 * @bio: Bio to write
 * @s_zdm: tLBA
 * @blks: number of blocks to RMW (should be 1).
 * @origin: Current bLBA
 *
 * Return: 0 on success, otherwise error.
 */
static int zm_cow(struct zdm *znd, struct bio *bio, u64 s_zdm, u32 blks,
		  u64 origin)
{
	struct dm_target *ti = znd->ti;
	int count = 1;
	int use_wq = 1;
	unsigned int bytes = bio_cur_bytes(bio);
	u8 *data = bio_data(bio);
	u8 *io = NULL;
	u16 ua_off = bio->bi_iter.bi_sector & 0x0007;
	u16 ua_size = bio->bi_iter.bi_size & 0x0FFF;	/* in bytes */
	u32 mapped = 0;
	u64 disk_lba = 0;

	if (!znd->cow_block)
		znd->cow_block = ZDM_ALLOC(znd, Z_C4K, PG_02, CRIT);

	io = znd->cow_block;
	if (!io)
		return -EIO;

	disk_lba = z_acquire(znd, Z_AQ_STREAM_ID, blks, &mapped);
	if (!disk_lba || !mapped)
		return -ENOSPC;

	while (bytes) {
		int ioer;
		unsigned int iobytes = Z_C4K;

		/* ---------------------------------------------------------- */
		if (origin) {
			if (s_zdm != znd->cow_addr) {
				Z_ERR(znd, "Copy block from %llx <= %llx",
				      origin, s_zdm);
				ioer = read_block(ti, DM_IO_KMEM, io, origin,
						count, use_wq);
				if (ioer)
					return -EIO;

				znd->cow_addr = s_zdm;
			} else {
				Z_ERR(znd, "Cached block from %llx <= %llx",
				      origin, s_zdm);
			}
		} else {
			memset(io, 0, Z_C4K);
		}

		if (ua_off)
			iobytes -= ua_off * 512;

		if (bytes < iobytes)
			iobytes = bytes;

		Z_ERR(znd, "Moving %u bytes from origin [offset:%u]",
		      iobytes, ua_off * 512);

		memcpy(io + (ua_off * 512), data, iobytes);

		/* ---------------------------------------------------------- */

		ioer = write_block(ti, DM_IO_KMEM, io, disk_lba, count, use_wq);
		if (ioer)
			return -EIO;

		MutexLock(&znd->mz_io_mutex);
		ioer = z_mapped_addmany(znd, s_zdm, disk_lba, mapped, CRIT);
		mutex_unlock(&znd->mz_io_mutex);
		if (ioer) {
			Z_ERR(znd, "%s: Map MANY failed.", __func__);
			return -EIO;
		}
		increment_used_blks(znd, disk_lba, mapped);

		data += iobytes;
		bytes -= iobytes;
		ua_size -= (ua_size > iobytes) ? iobytes : ua_size;
		ua_off = 0;
		disk_lba++;

		if (bytes && (ua_size || ua_off)) {
			s_zdm++;
			origin = current_mapping(znd, s_zdm, CRIT);
		}
	}
	bio_endio(bio);

	return DM_MAPIO_SUBMITTED;
}

/**
 * Write 4k blocks from cache to lba.
 * Move any remaining 512 byte blocks to the start of cache and update
 * the @_blen count is updated
 */
static int zm_write_cache(struct zdm *znd, struct io_dm_block *dm_vbuf,
			  u64 lba, u32 *_blen)
{
	int use_wq    = 1;
	int cached    = *_blen;
	int blks      = cached >> 3;
	int sectors   = blks << 3;
	int remainder = cached - sectors;
	int err;

	err = write_block(znd->ti, DM_IO_VMA, dm_vbuf, lba, blks, use_wq);
	if (!err) {
		if (remainder)
			memcpy(dm_vbuf[0].data,
			       dm_vbuf[sectors].data, remainder * 512);
		*_blen = remainder;
	}
	return err;
}

/**
 * zm_write_bios() - Map and write bios.
 * @znd: ZDM Instance
 * @bio: Bio to be written.
 * @s_zdm: tLBA for mapping.
 *
 * Return: DM_MAPIO_SUBMITTED or negative on error.
 */
static int zm_write_bios(struct zdm *znd, struct bio *bio, u64 s_zdm)
{
	struct bio *split = NULL;
	u32 acqflgs = Z_AQ_STREAM_ID | bio_stream(bio);
	u64 lba     = 0;
	u32 mapped  = 0;
	int err     = -EIO;
	int done    = 0;
	int sectors;
	u32 blks;

	do {
		blks = dm_div_up(bio->bi_iter.bi_size, Z_C4K);
		lba = z_acquire(znd, acqflgs, blks, &mapped);
		if (!lba && mapped)
			lba = z_acquire(znd, acqflgs, mapped, &mapped);

		if (!lba) {
			if (atomic_read(&znd->gc_throttle) == 0) {
				err = -ENOSPC;
				goto out;
			}

			Z_ERR(znd, "Throttle input ... Mandatory GC.");
			if (delayed_work_pending(&znd->gc_work)) {
				mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
				flush_delayed_work(&znd->gc_work);
			}
			continue;
		}

		sectors = mapped << Z_SHFT4K;
		split = zsplit_bio(znd, bio, sectors);
		if (split == bio)
			done = 1;

		if (!split) {
			err = -ENOMEM;
			goto out;
		}
		split->bi_iter.bi_sector = lba << Z_SHFT4K;
		generic_make_request(split);
		MutexLock(&znd->mz_io_mutex);
		err = z_mapped_addmany(znd, s_zdm, lba, mapped, CRIT);
		mutex_unlock(&znd->mz_io_mutex);
		if (err) {
			Z_ERR(znd, "%s: Map MANY failed.", __func__);
			err = DM_MAPIO_REQUEUE;
			goto out;
		}
		s_zdm += mapped;
	} while (!done);
	err = DM_MAPIO_SUBMITTED;

out:
	return err;
}

/**
 * zm_write_pages() - Copy bio pages to 4k aligned buffer. Write and map buffer.
 * @znd: ZDM Instance
 * @bio: Bio to be written.
 * @s_zdm: tLBA for mapping.
 *
 * Return: DM_MAPIO_SUBMITTED or negative on error.
 */
static int zm_write_pages(struct zdm *znd, struct bio *bio, u64 s_zdm)
{
	u32 blks     = dm_div_up(bio->bi_iter.bi_size, Z_C4K);
	u64 lba      = 0;
	u32 blen     = 0; /* total: IO_VCACHE_PAGES * 8 */
	u32 written  = 0;
	int avail    = 0;
	u32 acqflgs  = Z_AQ_STREAM_ID | bio_stream(bio);
	int err;
	struct bvec_iter start;
	struct bvec_iter iter;
	struct bio_vec bv;
	struct io_4k_block *io_vcache;
	struct io_dm_block *dm_vbuf = NULL;

	MutexLock(&znd->vcio_lock);
	io_vcache = get_io_vcache(znd, CRIT);
	if (!io_vcache) {
		Z_ERR(znd, "%s: FAILED to get SYNC CACHE.", __func__);
		err = -ENOMEM;
		goto out;
	}

	dm_vbuf = (struct io_dm_block *)io_vcache;

	/* USE: dm_vbuf for dumping bio pages to disk ... */
	start = bio->bi_iter; /* struct implicit copy */
	do {
		u64 alloc_ori = 0;
		u32 mcount = 0;
		u32 mapped = 0;

reacquire:
		/*
		 * When lba is zero no blocks were not allocated.
		 * Retry with the smaller request
		 */
		lba = z_acquire(znd, acqflgs, blks - written, &mapped);
		if (!lba && mapped)
			lba = z_acquire(znd, acqflgs, mapped, &mapped);

		if (!lba) {
			if (atomic_read(&znd->gc_throttle) == 0) {
				err = -ENOSPC;
				goto out;
			}

			Z_ERR(znd, "Throttle input ... Mandatory GC.");
			if (delayed_work_pending(&znd->gc_work)) {
				mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
				flush_delayed_work(&znd->gc_work);
			}
			goto reacquire;
		}

		/* this may be redundant .. if we have lba we have mapped > 0 */
		if (lba && mapped)
			avail += mapped * 8; /* claimed pages in dm blocks */

		alloc_ori = lba;

		/* copy [upto mapped] pages to buffer */
		__bio_for_each_segment(bv, bio, iter, start) {
			int issue_write = 0;
			unsigned int boff;
			void *src;

			if (avail <= 0) {
				Z_ERR(znd, "%s: TBD: Close Z# %llu",
					__func__, alloc_ori >> 16);
				start = iter;
				break;
			}

			src = kmap_atomic(bv.bv_page);
			boff = bv.bv_offset;
			memcpy(dm_vbuf[blen].data, src + boff, bv.bv_len);
			kunmap_atomic(src);
			blen   += bv.bv_len / 512;
			avail  -= bv.bv_len / 512;

			if ((blen >= (mapped * 8)) ||
			    (blen >= (BIO_CACHE_SECTORS - 8)))
				issue_write = 1;

			/*
			 * If there is less than 1 4k block in out cache,
			 * send the available blocks to disk
			 */
			if (issue_write) {
				int blks = blen / 8;

				err = zm_write_cache(znd, dm_vbuf, lba, &blen);
				if (err) {
					Z_ERR(znd, "%s: bio-> %" PRIx64
					      " [%d of %d blks] -> %d",
					      __func__, lba, blen, blks, err);
					bio->bi_error = err;
					bio_endio(bio);
					goto out;
				}
				lba     += blks;
				written += blks;
				mcount  += blks;
				mapped  -= blks;

				if (mapped == 0) {
					bio_advance_iter(bio, &iter, bv.bv_len);
					start = iter;
					break;
				}
				if (mapped < 0) {
					Z_ERR(znd, "ERROR: Bad write %"
					      PRId32 " beyond alloc'd space",
					      mapped);
				}
			}
		} /* end: __bio_for_each_segment */
		if ((mapped > 0) && ((blen / 8) > 0)) {
			int blks = blen / 8;

			err = zm_write_cache(znd, dm_vbuf, lba, &blen);
			if (err) {
				Z_ERR(znd, "%s: bio-> %" PRIx64
				      " [%d of %d blks] -> %d",
				      __func__, lba, blen, blks, err);
				bio->bi_error = err;
				bio_endio(bio);
				goto out;
			}
			lba     += blks;
			written += blks;
			mcount  += blks;
			mapped  -= blks;

			if (mapped < 0) {
				Z_ERR(znd, "ERROR: [2] Bad write %"
				      PRId32 " beyond alloc'd space",
				      mapped);
			}
		}
		MutexLock(&znd->mz_io_mutex);
		err = z_mapped_addmany(znd, s_zdm, alloc_ori, mcount, CRIT);
		mutex_unlock(&znd->mz_io_mutex);
		if (err) {
			Z_ERR(znd, "%s: Map MANY failed.", __func__);
			err = DM_MAPIO_REQUEUE;
			/*
			 * FIXME:
			 * Ending the BIO here is causing a GFP:
			 -       DEBUG_PAGEALLOC
			 -    in Workqueue:
			 -        writeback bdi_writeback_workfn (flush-252:0)
			 -    backtrace:
			 -      __map_bio+0x7a/0x280
			 -      __split_and_process_bio+0x2e3/0x4e0
			 -      ? __split_and_process_bio+0x22/0x4e0
			 -      ? generic_start_io_acct+0x5/0x210
			 -      dm_make_request+0x6b/0x100
			 -      generic_make_request+0xc0/0x110
			 -      ....
			 -
			 - bio->bi_error = err;
			 - bio_endio(bio);
			 */
			goto out;
		}
		increment_used_blks(znd, alloc_ori, mcount);

		if (written < blks)
			s_zdm += written;

		if (written == blks && blen > 0)
			Z_ERR(znd, "%s: blen: %d un-written blocks!!",
			      __func__, blen);
	} while (written < blks);
	bio_endio(bio);
	err = DM_MAPIO_SUBMITTED;

out:
	put_io_vcache(znd, io_vcache);
	mutex_unlock(&znd->vcio_lock);

	return err;
}

/**
 * is_empty_page() - Scan memory range for any set bits.
 * @pg: The start of memory to be scanned.
 * @len: Number of bytes to check (should be long aligned)
 * Return: 0 if any bits are set, 1 if all bits are 0.
 */
static int is_empty_page(void *pg, size_t len)
{
	unsigned long *chk = pg;
	size_t count = len / sizeof(*chk);
	size_t entry;

	for (entry = 0; entry < count; entry++) {
		if (chk[entry])
			return 0;
	}
	return 1;
}

/**
 * is_zero_bio() - Scan bio to see if all bytes are 0.
 * @bio: The bio to be scanned.
 * Return: 1 if all bits are 0. 0 if any bits in bio are set.
 */
static int is_zero_bio(struct bio *bio)
{
	int is_empty = 0;
	struct bvec_iter iter;
	struct bio_vec bv;

	/* Scan bio to determine if it is zero'd */
	bio_for_each_segment(bv, bio, iter) {
		unsigned int boff;
		void *src;

		src = kmap_atomic(bv.bv_page);
		boff = bv.bv_offset;
		is_empty = is_empty_page(src + boff, bv.bv_len);
		kunmap_atomic(src);

		if (!is_empty)
			break;
	} /* end: __bio_for_each_segment */

	return is_empty;
}

/**
 * is_bio_aligned() - Test bio and bio_vec for 4k aligned pages.
 * @bio: Bio to be tested.
 * Return: 1 if bio is 4k aligned, 0 if not.
 */
static int is_bio_aligned(struct bio *bio)
{
	int aligned = 1;
	struct bvec_iter iter;
	struct bio_vec bv;

	bio_for_each_segment(bv, bio, iter) {
		if ((bv.bv_offset & 0x0FFF) || (bv.bv_len & 0x0FFF)) {
			aligned = 0;
			break;
		}
	}
	return aligned;
}

/**
 * zoned_map_write() - Write a bio by the fastest safe method.
 * @znd: ZDM Instance
 * @bio: Bio to be written
 * @s_zdm: tLBA for mapping.
 *
 * Bios that are less than 4k need RMW.
 * Bios that are single pages are deduped and written or discarded.
 * Bios that are multiple pages with 4k aligned bvecs are written as bio(s).
 * Biso that are multiple pages and mis-algined are copied to an algined buffer
 * and submitted and new I/O.
 */
static int zoned_map_write(struct zdm *znd, struct bio *bio, u64 s_zdm)
{
	u32 blks     = dm_div_up(bio->bi_iter.bi_size, Z_C4K);
	u16 ua_off   = bio->bi_iter.bi_sector & 0x0007;
	u16 ua_size  = bio->bi_iter.bi_size & 0x0FFF;	/* in bytes */
	int rcode    = -EIO;

	if (ua_size || ua_off) {
		u64 origin;

		origin = current_mapping(znd, s_zdm, CRIT);
		if (origin) {
			znd->is_empty = 0;
			rcode = zm_cow(znd, bio, s_zdm, blks, origin);
		}
		return rcode;
	}

	/* on RAID 4/5/6 all writes are 4k */
	if (blks == 1) {
		if (is_zero_bio(bio)) {
			rcode = zoned_map_discard(znd, bio, s_zdm);
		} else {
			znd->is_empty = 0;
			rcode = zm_write_bios(znd, bio, s_zdm);
		}
		return rcode;
	}

	/*
	 * For larger bios test for 4k alignment.
	 * When bios are mis-algined we must copy out the
	 * the mis-algined pages into a new bio and submit.
	 * [The 4k alignment requests on our queue may be ignored
	 *  by mis-behaving layers that are not 4k safe].
	 */
	znd->is_empty = 0;
	if (is_bio_aligned(bio))
		rcode = zm_write_bios(znd, bio, s_zdm);
	else
		rcode = zm_write_pages(znd, bio, s_zdm);

	return rcode;
}

/**
 * zm_read_bios() - Read bios from device
 * @znd: ZDM Instance
 * @bio: Bio to read
 * @s_zdm: tLBA to read from.
 *
 * Return DM_MAPIO_SUBMITTED or negative on error.
 */
static int zm_read_bios(struct zdm *znd, struct bio *bio, u64 s_zdm)
{
	struct bio *split = NULL;
	int rcode = DM_MAPIO_SUBMITTED;
	u64 lba;
	u64 blba;
	u32 blks;
	int sectors;
	int count;
	u16 ua_off;
	u16 ua_size;

	do {
		blks = dm_div_up(bio->bi_iter.bi_size, Z_C4K);
		ua_off = bio->bi_iter.bi_sector & 0x0007;
		ua_size = bio->bi_iter.bi_size & 0x0FFF;	/* in bytes */

		lba = blba = current_mapping(znd, s_zdm, CRIT);
		if (blba) {
			for (count = 1; count < blks; count++) {
				lba = current_mapping(znd, s_zdm + count, CRIT);
				if (lba != (blba + count))
					break;
			}
		} else {
			for (count = 1; count < blks; count++) {
				lba = current_mapping(znd, s_zdm + count, CRIT);
				if (lba != 0ul)
					break;
			}
		}
		s_zdm += count;
		sectors = (count << Z_SHFT4K);
		if (ua_size)
			sectors += (ua_size >> SECTOR_SHIFT) - 8;

		split = zsplit_bio(znd, bio, sectors);
		if (!split) {
			rcode = -ENOMEM;
			goto out;
		}
		if (blba) {
			split->bi_iter.bi_sector = (blba << Z_SHFT4K) + ua_off;
			generic_make_request(split);
		} else {
			zero_fill_bio(split);
			bio_endio(split);
		}
	} while (split != bio);

out:
	return rcode;
}

/**
 * zoned_bio() - Handle and incoming BIO.
 * @znd: ZDM Instance
 */
static int zoned_bio(struct zdm *znd, struct bio *bio)
{
	bool is_write = (bio_data_dir(bio) == WRITE);
	u64 s_zdm = (bio->bi_iter.bi_sector >> Z_SHFT4K) + znd->md_end;
	int rcode = DM_MAPIO_SUBMITTED;
	struct request_queue *q;
	int force_sync_now = 0;

	/* map to backing device ... NOT dm-zdm device */
	bio->bi_bdev = znd->dev->bdev;

	q = bdev_get_queue(bio->bi_bdev);
	q->queue_flags |= QUEUE_FLAG_NOMERGES;

	if (is_write && znd->meta_result) {
		if (!(bio->bi_op & REQ_OP_DISCARD)) {
			rcode = znd->meta_result;
			Z_ERR(znd, "MAP ERR (meta): %d", rcode);
			goto out;
		}
	}

	/* check for REQ_FLUSH flag */
	if (bio->bi_rw & (REQ_PREFLUSH | REQ_FUA)) {
		bio->bi_rw &= ~(REQ_PREFLUSH | REQ_FUA);
		set_bit(DO_FLUSH, &znd->flags);
		force_sync_now = 1;
	}
	if (bio->bi_rw & REQ_SYNC) {
		set_bit(DO_SYNC, &znd->flags);
		force_sync_now = 1;
	}

	Z_DBG(znd, "%s: s:%"PRIx64" sz:%u -> %s", __func__, s_zdm,
	      bio->bi_iter.bi_size, is_write ? "W" : "R");

	if (bio->bi_iter.bi_size) {
		if (bio->bi_op & REQ_OP_DISCARD) {
			rcode = zoned_map_discard(znd, bio, s_zdm);
		} else if (is_write) {
			const int wait = 1;
			u32 blks = bio->bi_iter.bi_size >> 12;

			rcode = zoned_map_write(znd, bio, s_zdm);
			if (rcode == DM_MAPIO_SUBMITTED) {
				z_discard_partial(znd, blks, CRIT);
				if (znd->z_gc_free < 7)
					gc_immediate(znd, !wait, CRIT);
			}
		} else {
			rcode = zm_read_bios(znd, bio, s_zdm);
		}
		znd->age = jiffies;
	}

	if (test_bit(DO_FLUSH, &znd->flags) ||
	    test_bit(DO_SYNC, &znd->flags) ||
	    test_bit(DO_MAPCACHE_MOVE, &znd->flags) ||
	    test_bit(DO_MEMPOOL, &znd->flags)) {
		if (!test_bit(DO_METAWORK_QD, &znd->flags) &&
		    !work_pending(&znd->meta_work)) {
			set_bit(DO_METAWORK_QD, &znd->flags);
			queue_work(znd->meta_wq, &znd->meta_work);
		}
	}

	if (force_sync_now && work_pending(&znd->meta_work))
		flush_workqueue(znd->meta_wq);

out:
	if (rcode == DM_MAPIO_REMAPPED || rcode == DM_MAPIO_SUBMITTED)
		goto done;

	if (rcode < 0 || rcode == DM_MAPIO_REQUEUE) {
		Z_ERR(znd, "MAP ERR: %d", rcode);
		Z_ERR(znd, "%s: s:%"PRIx64" sz:%u -> %s", __func__, s_zdm,
		      bio->bi_iter.bi_size, is_write ? "W" : "R");
		dump_stack();
	} else {
		Z_ERR(znd, "MAP ERR: %d", rcode);
		Z_ERR(znd, "%s: s:%"PRIx64" sz:%u -> %s", __func__, s_zdm,
		      bio->bi_iter.bi_size, is_write ? "W" : "R");
		dump_stack();
		rcode = -EIO;
	}
done:
	return rcode;
}

/**
 * _do_mem_purge() - conditionally trigger a reduction of cache memory
 * @znd: ZDM Instance
 */
static inline int _do_mem_purge(struct zdm *znd)
{
	int do_work = 0;

	if (atomic_read(&znd->incore) > 3) {
		set_bit(DO_MEMPOOL, &znd->flags);
		if (!work_pending(&znd->meta_work))
			do_work = 1;
	}
	return do_work;
}

/**
 * on_timeout_activity() - Periodic background task execution.
 * @znd: ZDM Instance
 * @mempurge: If memory purge should be scheduled.
 * @delay: Delay metric for periodic GC
 *
 * NOTE: Executed as a worker task queued froma timer.
 */
static void on_timeout_activity(struct zdm *znd, int delay)
{
	int max_tries = 1;

	if (test_bit(ZF_FREEZE, &znd->flags))
		return;

	if (is_expired_msecs(znd->age, DISCARD_IDLE_MSECS))
		max_tries = 20;

	do {
		int count;

		count = z_discard_partial(znd, Z_BLKSZ, NORMAL);
		if (count != 1 || --max_tries < 0)
			break;

		if (test_bit(ZF_FREEZE, &znd->flags))
			return;

	} while (is_expired_msecs(znd->age, DISCARD_IDLE_MSECS));

	gc_queue_with_delay(znd, delay, NORMAL);

	if (!test_bit(DO_GC_NO_PURGE, &znd->flags) && _do_mem_purge(znd))
		queue_work(znd->meta_wq, &znd->meta_work);
}

/**
 * bg_work_task() - periodic background worker
 * @work: context for worker thread
 */
static void bg_work_task(struct work_struct *work)
{
	struct zdm *znd;
	const int delay = 1;

	if (!work)
		return;

	znd = container_of(work, struct zdm, bg_work);
	on_timeout_activity(znd, delay);
}

/**
 * activity_timeout() - Handler for timer used to trigger background worker.
 * @data: context for timer.
 */
static void activity_timeout(unsigned long data)
{
	struct zdm *znd = (struct zdm *) data;

	if (!work_pending(&znd->bg_work))
		queue_work(znd->bg_wq, &znd->bg_work);

	if (!test_bit(ZF_FREEZE, &znd->flags))
		mod_timer(&znd->timer, jiffies + msecs_to_jiffies(2500));
}

/**
 * get_dev_size() - Report accessible size of device to upper layer.
 * @ti: DM Target
 *
 * Return: Size in 512 byte sectors
 */
static sector_t get_dev_size(struct dm_target *ti)
{
	struct zdm *znd = ti->private;
	u64 sz = i_size_read(get_bdev_bd_inode(znd));	/* size in bytes. */
	u64 lut_resv = znd->gz_count * znd->mz_provision;

	/*
	 * NOTE: `sz` should match `ti->len` when the dm_table
	 *       is setup correctly
	 */
	sz -= (lut_resv * Z_SMR_SZ_BYTES);

	return to_sector(sz);
}

/**
 * zoned_iterate_devices() - Iterate over devices call fn() at each.
 * @ti: DM Target
 * @fn: Function for each callout
 * @data: Context for fn().
 */
static int zoned_iterate_devices(struct dm_target *ti,
				 iterate_devices_callout_fn fn, void *data)
{
	struct zdm *znd = ti->private;
	int rc;

	rc = fn(ti, znd->dev, 0, get_dev_size(ti), data);
	return rc;
}

/**
 * zoned_io_hints() - The place to tweek queue limits for DM targets
 * @ti: DM Target
 * @limits: queue_limits for this DM target
 */
static void zoned_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	struct zdm *znd = ti->private;
	u64 io_opt_sectors = limits->io_opt >> SECTOR_SHIFT;

	/*
	 * If the system-determined stacked limits are compatible with the
	 * zdm device's blocksize (io_opt is a factor) do not override them.
	 */
	if (io_opt_sectors < 8 || do_div(io_opt_sectors, 8)) {
		blk_limits_io_min(limits, 0);
		blk_limits_io_opt(limits, 8 << SECTOR_SHIFT);
	}

	limits->logical_block_size =
		limits->physical_block_size =
		limits->io_min = Z_C4K;
	if (znd->trim) {
		limits->discard_alignment = Z_C4K;
		limits->discard_granularity = Z_C4K;
		limits->max_discard_sectors = 1 << 30;
		limits->max_hw_discard_sectors = 1 << 30;
		limits->discard_zeroes_data = 1;
		limits->raid_discard_safe = znd->raid5_trim;
	}
}

/**
 * zoned_status() - Report status of DM Target
 * @ti: DM Target
 * @type: Type of status to report.
 * @status_flags: Flags
 * @result: Fill in with status.
 * @maxlen: Maximum number of bytes for result.
 */
static void zoned_status(struct dm_target *ti, status_type_t type,
			 unsigned int status_flags, char *result,
			 unsigned int maxlen)
{
	struct zdm *znd = (struct zdm *) ti->private;

	switch (type) {
	case STATUSTYPE_INFO:
		result[0] = '\0';
		break;

	case STATUSTYPE_TABLE:
		scnprintf(result, maxlen, "%s Z#%u", znd->dev->name,
			 znd->zdstart);
		break;
	}
}

/* -------------------------------------------------------------------------- */
/* --ProcFS Support Routines------------------------------------------------- */
/* -------------------------------------------------------------------------- */

#if defined(CONFIG_PROC_FS)

/**
 * struct zone_info_entry - Proc zone entry.
 * @zone: Zone Index
 * @info: Info (WP/Used).
 */
struct zone_info_entry {
	u32 zone;
	u32 info;
};

/**
 * Startup writing to our proc entry
 */
static void *proc_wp_start(struct seq_file *seqf, loff_t *pos)
{
	struct zdm *znd = seqf->private;

	if (*pos == 0)
		znd->wp_proc_at = *pos;
	return &znd->wp_proc_at;
}

/**
 * Increment to our next 'grand zone' 4k page.
 */
static void *proc_wp_next(struct seq_file *seqf, void *v, loff_t *pos)
{
	struct zdm *znd = seqf->private;
	u32 zone = ++znd->wp_proc_at;

	return zone < znd->data_zones ? &znd->wp_proc_at : NULL;
}

/**
 * Stop ... a place to free resources that we don't hold .. [noop].
 */
static void proc_wp_stop(struct seq_file *seqf, void *v)
{
}

/**
 * Write as many entries as possbile ....
 */
static int proc_wp_show(struct seq_file *seqf, void *v)
{
	int err = 0;
	struct zdm *znd = seqf->private;
	u32 zone = znd->wp_proc_at;
	u32 out = 0;

	while (zone < znd->data_zones) {
		u32 gzno  = zone >> GZ_BITS;
		u32 gzoff = zone & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		struct zone_info_entry entry;

		entry.zone = zone;
		entry.info = le32_to_cpu(wpg->wp_alloc[gzoff]);

		err = seq_write(seqf, &entry, sizeof(entry));
		if (err) {
			/*
			 * write failure is temporary ..
			 * just return and try again
			 */
			err = 0;
			goto out;
		}
		out++;
		zone = ++znd->wp_proc_at;
	}

out:
	if (err)
		Z_ERR(znd, "%s: %llu -> %d", __func__, znd->wp_proc_at, err);

	return err;
}

/**
 * zdm_wp_ops() - Seq_file operations for retrieving WP via proc fs
 */
static const struct seq_operations zdm_wp_ops = {
	.start	= proc_wp_start,
	.next	= proc_wp_next,
	.stop	= proc_wp_stop,
	.show	= proc_wp_show
};

/**
 * zdm_wp_open() - Need to migrate our private data to the seq_file
 */
static int zdm_wp_open(struct inode *inode, struct file *file)
{
	/* seq_open will populate file->private_data with a seq_file */
	int err = seq_open(file, &zdm_wp_ops);

	if (!err) {
		struct zdm *znd = PDE_DATA(inode);
		struct seq_file *seqf = file->private_data;

		seqf->private = znd;
	}
	return err;
}

/**
 * zdm_wp_fops() - File operations for retrieving WP via proc fs
 */
static const struct file_operations zdm_wp_fops = {
	.open		= zdm_wp_open,
	.read		= seq_read,
	.llseek		= seq_lseek,
	.release	= seq_release,
};


/**
 * Startup writing to our proc entry
 */
static void *proc_used_start(struct seq_file *seqf, loff_t *pos)
{
	struct zdm *znd = seqf->private;

	if (*pos == 0)
		znd->wp_proc_at = *pos;
	return &znd->wp_proc_at;
}

/**
 * Increment to our next zone
 */
static void *proc_used_next(struct seq_file *seqf, void *v, loff_t *pos)
{
	struct zdm *znd = seqf->private;
	u32 zone = ++znd->wp_proc_at;

	return zone < znd->data_zones ? &znd->wp_proc_at : NULL;
}

/**
 * Stop ... a place to free resources that we don't hold .. [noop].
 */
static void proc_used_stop(struct seq_file *seqf, void *v)
{
}

/**
 * proc_used_show() - Write as many 'used' entries as possbile.
 * @seqf: seq_file I/O handler
 * @v: An unused parameter.
 */
static int proc_used_show(struct seq_file *seqf, void *v)
{
	int err = 0;
	struct zdm *znd = seqf->private;
	u32 zone = znd->wp_proc_at;
	u32 out = 0;

	while (zone < znd->data_zones) {
		u32 gzno  = zone >> GZ_BITS;
		u32 gzoff = zone & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		struct zone_info_entry entry;

		entry.zone = zone;
		entry.info = le32_to_cpu(wpg->zf_est[gzoff]);

		err = seq_write(seqf, &entry, sizeof(entry));
		if (err) {
			/*
			 * write failure is temporary ..
			 * just return and try again
			 */
			err = 0;
			goto out;
		}
		out++;
		zone = ++znd->wp_proc_at;
	}

out:
	if (err)
		Z_ERR(znd, "%s: %llu -> %d", __func__, znd->wp_proc_at, err);

	return err;
}

/**
 * zdm_used_ops() - Seq_file Ops for retrieving 'used' state via proc fs
 */
static const struct seq_operations zdm_used_ops = {
	.start	= proc_used_start,
	.next	= proc_used_next,
	.stop	= proc_used_stop,
	.show	= proc_used_show
};

/**
 * zdm_used_open() - Need to migrate our private data to the seq_file
 */
static int zdm_used_open(struct inode *inode, struct file *file)
{
	/* seq_open will populate file->private_data with a seq_file */
	int err = seq_open(file, &zdm_used_ops);

	if (!err) {
		struct zdm *znd = PDE_DATA(inode);
		struct seq_file *seqf = file->private_data;

		seqf->private = znd;
	}
	return err;
}

/**
 * zdm_used_fops() - File operations for retrieving 'used' state via proc fs
 */
static const struct file_operations zdm_used_fops = {
	.open		= zdm_used_open,
	.read		= seq_read,
	.llseek		= seq_lseek,
	.release	= seq_release,
};

/**
 * zdm_status_show() - Dump the status structure via proc fs
 */
static int zdm_status_show(struct seq_file *seqf, void *unused)
{
	struct zdm *znd = seqf->private;
	struct zdm_ioc_status status;
	u32 zone;

	memset(&status, 0, sizeof(status));
	for (zone = 0; zone < znd->data_zones; zone++) {
		u32 gzno  = zone >> GZ_BITS;
		u32 gzoff = zone & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp_at = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_VALUE_MASK;

		status.b_used += wp_at;
		status.b_available += Z_BLKSZ - wp_at;
	}
	status.map_cache_entries = znd->mc_entries;
	status.discard_cache_entries = znd->dc_entries;
	status.b_discard = znd->discard_count;
	status.journal_pages = znd->journal_entries;
	status.journal_entries = znd->jrnl.in_use;

	/*  fixed array of ->fwd_tm and ->rev_tm */
	status.m_zones = znd->data_zones;

	status.memstat = znd->memstat;
	memcpy(status.bins, znd->bins, sizeof(status.bins));
	status.mlut_blocks = atomic_read(&znd->incore);

	return seq_write(seqf, &status, sizeof(status));
}

/**
 * zdm_status_open() - Open seq_file from file.
 * @inode: Our data is stuffed here, Retrieve it.
 * @file: file objected used by seq_file.
 */
static int zdm_status_open(struct inode *inode, struct file *file)
{
	return single_open(file, zdm_status_show, PDE_DATA(inode));
}

/**
 * zdm_used_fops() - File operations to chain to zdm_status_open.
 */
static const struct file_operations zdm_status_fops = {
	.open = zdm_status_open,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
};

/**
 * zdm_info_show() - Report some information as text.
 * @seqf: Sequence file for writing
 * @unused: Not used.
 */
static int zdm_info_show(struct seq_file *seqf, void *unused)
{
	struct zdm *znd = seqf->private;

	seq_printf(seqf, "On device:     %s\n", _zdisk(znd));
	seq_printf(seqf, "Data Zones:    %u\n", znd->data_zones);
	seq_printf(seqf, "Empty Zones:   %u\n", znd->z_gc_free);
	seq_printf(seqf, "Cached Pages:  %u\n", znd->mc_entries);
	seq_printf(seqf, "Discard Pages: %u\n", znd->dc_entries);
	seq_printf(seqf, "Journal Pages: %u\n", znd->journal_entries);
	seq_printf(seqf, "Journal InUse: %u / %u\n",
		znd->jrnl.in_use, znd->jrnl.size);
	seq_printf(seqf, "ZTL Pages:     %d\n", atomic_read(&znd->incore));
	seq_printf(seqf, "   in ZTL:     %d\n", znd->in_zlt);
	seq_printf(seqf, "   in LZY:     %d\n", znd->in_lzy);
	seq_printf(seqf, "RAM in Use:    %lu\n", znd->memstat);
	seq_printf(seqf, "Zones GC'd:    %u\n", znd->gc_events);
	seq_printf(seqf, "GC Throttle:   %d\n", atomic_read(&znd->gc_throttle));
#if ALLOC_DEBUG
	seq_printf(seqf, "Max Allocs:    %u\n", znd->hw_allocs);
#endif

	return 0;
}

/**
 * zdm_info_open() - Open seq_file from file.
 * @inode: Our data is stuffed here, Retrieve it.
 * @file: file objected used by seq_file.
 */
static int zdm_info_open(struct inode *inode, struct file *file)
{
	return single_open(file, zdm_info_show, PDE_DATA(inode));
}

/**
 * zdm_used_fops() - File operations to chain to zdm_info_open.
 */
static const struct file_operations zdm_info_fops = {
	.open = zdm_info_open,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
};

/**
 * zdm_create_proc_entries() - Create proc entries for ZDM utilities
 * @znd: ZDM Instance
 */
static int zdm_create_proc_entries(struct zdm *znd)
{
	snprintf(znd->proc_name, sizeof(znd->proc_name), "zdm_%s", _zdisk(znd));

	znd->proc_fs = proc_mkdir(znd->proc_name, NULL);
	if (!znd->proc_fs)
		return -ENOMEM;

	proc_create_data(PROC_WP, 0, znd->proc_fs, &zdm_wp_fops, znd);
	proc_create_data(PROC_FREE, 0, znd->proc_fs, &zdm_used_fops, znd);
	proc_create_data(PROC_DATA, 0, znd->proc_fs, &zdm_status_fops, znd);
	proc_create_data(PROC_STATUS, 0, znd->proc_fs, &zdm_info_fops, znd);

	return 0;
}

/**
 * zdm_remove_proc_entries() - Remove proc entries
 * @znd: ZDM Instance
 */
static void zdm_remove_proc_entries(struct zdm *znd)
{
	remove_proc_subtree(znd->proc_name, NULL);
}

#else /* !CONFIG_PROC_FS */

static int zdm_create_proc_entries(struct zdm *znd)
{
	(void)znd;
	return 0;
}
static void zdm_remove_proc_entries(struct zdm *znd)
{
	(void)znd;
}
#endif /* CONFIG_PROC_FS */


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void start_worker(struct zdm *znd)
{
	clear_bit(ZF_FREEZE, &znd->flags);
	atomic_set(&znd->suspended, 0);
	mod_timer(&znd->timer, jiffies + msecs_to_jiffies(5000));
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void stop_worker(struct zdm *znd)
{
	set_bit(ZF_FREEZE, &znd->flags);
	atomic_set(&znd->suspended, 1);
	zoned_io_flush(znd);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void zoned_postsuspend(struct dm_target *ti)
{
	struct zdm *znd = ti->private;

	stop_worker(znd);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void zoned_resume(struct dm_target *ti)
{
	/* TODO */
}

/*
 *
 */
static int zoned_message(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct zdm *znd = ti->private;
	int iter;

	for (iter = 0; iter < argc; iter++)
		Z_ERR(znd, "Message: %s not handled.", argv[argc]);

	return 0;
}


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_preresume(struct dm_target *ti)
{
	struct zdm *znd = ti->private;

	start_worker(znd);
	return 0;
}
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static struct target_type zoned_target = {
	.name = "zdm",
	.module = THIS_MODULE,
	.version = {1, 0, 0},
	.ctr = zoned_ctr,
	.dtr = zoned_dtr,
	.map = zoned_map,
	.end_io = zoned_endio,
	.postsuspend = zoned_postsuspend,
	.preresume = zoned_preresume,
	.resume = zoned_resume,
	.status = zoned_status,
	.message = zoned_message,
	.iterate_devices = zoned_iterate_devices,
	.io_hints = zoned_io_hints
};

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int __init dm_zoned_init(void)
{
	int rcode = dm_register_target(&zoned_target);

	if (rcode)
		DMERR("zdm target registration failed: %d", rcode);

	return rcode;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
static void __exit dm_zoned_exit(void)
{
	dm_unregister_target(&zoned_target);
}

module_init(dm_zoned_init);
module_exit(dm_zoned_exit);

MODULE_DESCRIPTION(DM_NAME " zdm target for Host Aware/Managed drives.");
MODULE_AUTHOR("Shaun Tancheff <shaun.tancheff@seagate.com>");
MODULE_LICENSE("GPL");
