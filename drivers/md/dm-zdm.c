/*
 * Kernel Device Mapper for abstracting ZAC/ZBC devices as normal
 * block devices for linux file systems.
 *
 * Copyright (C) 2015,2016 Seagate Technology PLC
 *
 * Written by:
 * Shaun Tancheff <shaun.tancheff@seagate.com>
 *
 * Bio queue support and metadata relocation by:
 * Vineet Agarwal <vineet.agarwal@seagate.com>
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

#define BIOSET_RESV		256
#define READ_PRIO_DEPTH		256

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
			unsigned int op, unsigned int opf, int queue,
			io_notify_fn callback, void *context);
static int zoned_bio(struct zdm *znd, struct bio *bio);
static int zoned_map_write(struct zdm *znd, struct bio*, u64 s_zdm);
static sector_t get_dev_size(struct dm_target *ti);
static int dmz_reset_wp(struct zdm *znd, u64 z_id);
static int dmz_open_zone(struct zdm *znd, u64 z_id);
static int dmz_close_zone(struct zdm *znd, u64 z_id);
static int dmz_report_zones(struct zdm *znd, u64 z_id, struct blk_zone *zones,
			    unsigned int *nz, gfp_t gfp);
static void activity_timeout(unsigned long data);
static void zoned_destroy(struct zdm *);
static int gc_can_cherrypick(struct zdm *znd, u32 sid, int delay, gfp_t gfp);
static void bg_work_task(struct work_struct *work);
static void on_timeout_activity(struct zdm *znd, int delay);
static int zdm_create_proc_entries(struct zdm *znd);
static void zdm_remove_proc_entries(struct zdm *znd);

#if ENABLE_SEC_METADATA
/**
 * znd_get_backing_dev() - Get the backing device.
 * @znd: ZDM Instance
 * @block: Logical sector / tlba
 *
 * Return: Backing device
 */
static struct block_device *znd_get_backing_dev(struct zdm *znd,
						sector_t *block)
{
	struct block_device *bdev = NULL;

	switch (znd->meta_dst_flag) {

	case DST_TO_PRI_DEVICE:
		bdev = znd->dev->bdev;
		break;

	case DST_TO_SEC_DEVICE:
		if (*block < (znd->data_lba << Z_SHFT4K)) {
			bdev = znd->meta_dev->bdev;
		} else {
			bdev = znd->dev->bdev;
			/*
			 * Align data drive starting LBA to zone boundary
			 * to ensure wp sync.
			 */
			*block = *block  - (znd->data_lba << Z_SHFT4K)
						+ znd->sec_zone_align;
		}
		break;

	case DST_TO_BOTH_DEVICE:
		if (*block < (znd->data_lba << Z_SHFT4K))
			bdev = znd->meta_dev->bdev;
		else
			bdev = znd->dev->bdev;
		break;
	}
	return bdev;
}
#endif
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
	if (bio->bi_opf & REQ_META) {
		stream_id = 0xfe; /* upper level metadata */
	} else if (bio->bi_iter.bi_size < (Z_C4K * 4)) {
		/* avoid XFS meta/data churn in extent maps */
		stream_id = 0xfd; /* 'hot' upper level data */

#if 0 /* bio_get_streamid() is available */
	} else {
		unsigned int id = bio_get_streamid(bio);

		/* high 8 bits is hash of PID, low 8 bits is hash of inode# */
		stream_id = id >> 8;
		if (stream_id == 0)
			stream_id++;
		if (stream_id >= 0xfc)
			stream_id--;
#endif
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
	int rcode = DM_MAPIO_SUBMITTED;
	u32 blks = bio->bi_iter.bi_size >> PAGE_SHIFT;
	unsigned long flags;
	int redundant = 0;
	const gfp_t gfp = GFP_ATOMIC;
	int err;

	spin_lock_irqsave(&znd->stats_lock, flags);
	if (znd->is_empty)
		redundant = 1;
	else if ((s_zdm + blks) >= znd->htlba)
		redundant = 1;
	spin_unlock_irqrestore(&znd->stats_lock, flags);

	if (!redundant) {
		err = z_mapped_discard(znd, s_zdm, blks, gfp);
		if (err < 0) {
			bio->bi_error = err;
			rcode = err;
		}
	}
	bio->bi_iter.bi_sector = 8;
	bio_endio(bio);
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
	unsigned int op;
	unsigned int op_f;
	int wait;
	int wp_err;
};

/**
 * zsplit_endio() - Bio endio tracking for update internal WP.
 * @bio: Bio being completed.
 *
 * Bios that are split for writing are usually split to land on a zone
 * boundary. Forward the bio along the endio path and update the WP.
 */
static void za_endio(struct bio *bio)
{
	struct zone_action *za = bio->bi_private;

	switch (bio_op(bio)) {
	case REQ_OP_ZONE_RESET:
		/* find the zone and reset the wp on it */
		break;
	default:
		pr_err("%s: unexpected op: %d\n", __func__, bio_op(bio));
		break;
	}

	if (bio->bi_error) {
		struct zdm *znd = za->znd;

		Z_ERR(znd, "Zone Cmd: LBA: %" PRIx64 " -> %d failed.",
		      za->s_addr, bio->bi_error);
		Z_ERR(znd, "ZAC/ZBC support disabled.");

		znd->bdev_is_zoned = 0;
	}

	if (!za->wait)
		kfree(za);

	bio_put(bio);
}


/**
 * do_zone_action_work() - Issue a 'zone action' to the backing device.
 * @work: Work to do.
 */
static void do_zone_action_work(struct work_struct *work)
{
	struct zone_action *za = container_of(work, struct zone_action, work);
	struct zdm *znd = za->znd;
	struct block_device *bdev = znd->dev->bdev;
	const gfp_t gfp = GFP_ATOMIC;
	struct bio *bio = bio_alloc_bioset(gfp, 1, znd->bio_set);

	if (bio) {
		bio->bi_iter.bi_sector = za->s_addr;
		bio->bi_bdev = bdev;
		bio->bi_vcnt = 0;
		bio->bi_iter.bi_size = 0;
		bio_set_op_attrs(bio, za->op, za->op_f);
		if (!za->wait) {
			bio->bi_private = za;
			bio->bi_end_io = za_endio;
			submit_bio(bio);
			return;
		}
		za->wp_err = submit_bio_wait(bio);
		bio_put(bio);
	} else {
		za->wp_err = -ENOMEM;
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
	}
}

/**
 * dmz_zone_action() - Issue a 'zone action' to the backing device (via worker).
 * @znd: ZDM Instance
 * @z_id: Zone # to open.
 * @rw: One of REQ_OPEN_ZONE, REQ_CLOSE_ZONE, or REQ_RESET_ZONE.
 *
 * Return: 0 on success, otherwise error.
 */
static int dmz_zone_action(struct zdm *znd, u64 z_id, unsigned int op,
			   unsigned int op_f, int wait)
{
	int wp_err = 0;
	u64 z_offset = zone_to_sector(z_id);
	struct zone_action za = {
		.znd = znd,
		.s_addr = z_offset,
		.op = op,
		.op_f = op_f,
		.wait = wait,
		.wp_err = 0,
	};

#if ENABLE_SEC_METADATA
	if (znd->meta_dst_flag == DST_TO_SEC_DEVICE) {
		z_offset = zone_to_sector(z_id) + znd->sec_zone_align
				 + (znd->sec_dev_start_sect << Z_SHFT4K);
		za.s_addr = z_offset;
	}
#endif

	if (is_non_wp_zone(znd, z_id))
		return wp_err;

	if (!znd->bdev_is_zoned)
		return wp_err;

	if (!wait) {
		struct zone_action *zact = kzalloc(sizeof(*zact), GFP_ATOMIC);

		if (!zact)
			return -ENOMEM;

		memcpy(zact, &za, sizeof(za));
		INIT_WORK(&zact->work, do_zone_action_work);
		queue_work(znd->zone_action_wq, &zact->work);
		return 0;
	}

	/*
	 * Issue the synchronous I/O from a different thread
	 * to avoid generic_make_request recursion.
	 */
	INIT_WORK_ONSTACK(&za.work, do_zone_action_work);
	queue_work(znd->zone_action_wq, &za.work);
	flush_workqueue(znd->zone_action_wq);
	destroy_work_on_stack(&za.work);
	wp_err = za.wp_err;

	if (wait && wp_err) {
		struct hd_struct *p = znd->dev->bdev->bd_part;

		Z_ERR(znd, "Zone Cmd: LBA: %" PRIx64 " (%" PRIx64
			   " [Z:%" PRIu64 "] -> %d failed.",
		      za.s_addr, za.s_addr + p->start_sect, z_id, wp_err);

		Z_ERR(znd, "ZAC/ZBC support disabled.");
		znd->bdev_is_zoned = 0;
		wp_err = -ENOTSUPP;
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
	return dmz_zone_action(znd, z_id, REQ_OP_ZONE_RESET, 0, 1);
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
	return 0;
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
	return 0;
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
static int dmz_report_zones(struct zdm *znd, u64 z_id, struct blk_zone *zones,
			    unsigned int *nz, gfp_t gfp)
{
	int wp_err = -ENOTSUPP;

	if (znd->bdev_is_zoned) {
		struct block_device *bdev = znd->dev->bdev;
		u64 s_addr = zone_to_sector(z_id);

#if ENABLE_SEC_METADATA
		if (znd->meta_dst_flag == DST_TO_SEC_DEVICE)
			s_addr = zone_to_sector(z_id) + znd->sec_zone_align;
#endif
		wp_err = blkdev_report_zones(bdev, s_addr, zones, nz, gfp);
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

/**
 * _zoned_map() - kthread handling
 * @znd: ZDM Instance
 * @bio: bio to be mapped.
 *
 * Return: 0 on success, otherwise error.
 */
static int _zoned_map(struct zdm *znd, struct bio *bio)
{
	int err;

	err = zoned_bio(znd, bio);
	if (err < 0) {
		znd->meta_result = err;
		err = 0;
	}

	return err;

}

/**
 * znd_bio_merge_dispatch() - kthread handling
 * @arg: Argument
 *
 * Return: 0 on success, otherwise error.
 */
static int znd_bio_merge_dispatch(void *arg)
{
	struct zdm *znd = (struct zdm *)arg;
	struct list_head *jlst = &znd->bio_srt_jif_lst_head;
	struct zdm_q_node *node;
	struct bio *bio = NULL;
	unsigned long flags;
	long timeout;

	Z_INFO(znd, "znd_bio_merge_dispatch kthread [started]");
	while (!kthread_should_stop() || atomic_read(&znd->enqueued)) {
		node = NULL;
		timeout = znd->queue_delay;
		spin_lock_irqsave(&znd->zdm_bio_q_lck, flags);
		node = list_first_entry_or_null(jlst, struct zdm_q_node, jlist);
		if (node) {
			bio = node->bio;
			list_del(&node->jlist);
			spin_unlock_irqrestore(&znd->zdm_bio_q_lck, flags);
			ZDM_FREE(znd, node, sizeof(struct zdm_q_node), KM_14);
			atomic_dec(&znd->enqueued);
			if (bio) {
				_zoned_map(znd, bio);
				timeout = 0;
			}
		} else {
			spin_unlock_irqrestore(&znd->zdm_bio_q_lck, flags);
			if (znd->queue_depth <= atomic_read(&znd->enqueued))
				timeout *= 100;
		}
		if (timeout)
			schedule_timeout_interruptible(timeout);
	}
	z_flush_bdev(znd, GFP_KERNEL);
	Z_INFO(znd, "znd_bio_merge_dispatch kthread [stopped]");
	return 0;
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
	struct zdm_q_node *q_node;
	unsigned long flags;
	u32 op;

	if (znd->queue_depth == 0 && atomic_read(&znd->enqueued) == 0)
		return _zoned_map(znd, bio);

	op = bio_op(bio);
	if (op == REQ_OP_READ || op == REQ_OP_DISCARD)
		return _zoned_map(znd, bio);

	q_node = ZDM_ALLOC(znd, sizeof(struct zdm_q_node), KM_14, GFP_ATOMIC);
	if (unlikely(!q_node)) {
		Z_INFO(znd, "Bio Q allocation failed");
		return _zoned_map(znd, bio);
	}

	q_node->bio = bio;
	q_node->jiffies = jiffies;
	q_node->bi_sector = bio->bi_iter.bi_sector;
	INIT_LIST_HEAD(&(q_node->jlist));

	spin_lock_irqsave(&znd->zdm_bio_q_lck, flags);
	list_add_tail(&(q_node->jlist), &(znd->bio_srt_jif_lst_head));
	atomic_inc(&znd->enqueued);
	spin_unlock_irqrestore(&znd->zdm_bio_q_lck, flags);
	wake_up_process(znd->bio_kthread);

	return DM_MAPIO_SUBMITTED;
}

/**
 * zoned_actual_size() - Set number of 4k blocks available on block device.
 * @ti: Device Mapper Target Instance
 * @znd: ZDM Instance
 *
 * Return: 0 on success, otherwise error.
 */
static void zoned_actual_size(struct dm_target *ti, struct zdm *znd)
{
#if ENABLE_SEC_METADATA
	if (znd->meta_dst_flag == DST_TO_SEC_DEVICE) {
		znd->nr_blocks = (i_size_read(get_bdev_bd_inode(znd))
				 - (znd->sec_zone_align << Z_SHFT_SEC)) / Z_C4K;
	} else
		znd->nr_blocks = i_size_read(get_bdev_bd_inode(znd)) / Z_C4K;
#else
	znd->nr_blocks = i_size_read(get_bdev_bd_inode(znd)) / Z_C4K;
#endif
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
#if ENABLE_SEC_METADATA
	char *meta_dev = NULL;
#endif
	u64 first_data_zone = 0;
	u64 mz_md_provision = MZ_METADATA_ZONES;

	BUILD_BUG_ON(Z_C4K != (sizeof(struct map_cache_page)));
	BUILD_BUG_ON(Z_C4K != (sizeof(struct io_4k_block)));
	BUILD_BUG_ON(Z_C4K != (sizeof(struct mz_superkey)));
	BUILD_BUG_ON(PAGE_SIZE != sizeof(struct map_pool));

	znd = ZDM_ALLOC(NULL, sizeof(*znd), KM_00, GFP_KERNEL);
	if (!znd) {
		ti->error = "Error allocating zdm structure";
		return -ENOMEM;
	}

	znd->enable_trim = 0;
	znd->queue_depth = 0;
	znd->gc_prio_def = 0xff00;
	znd->gc_prio_low = 0x7fff;
	znd->gc_prio_high = 0x0400;
	znd->gc_prio_crit = 0x0040;
	znd->gc_wm_crit = 7;
	znd->gc_wm_high = 5;
	znd->gc_wm_low = 25;
	znd->gc_status = 1;
	znd->cache_ageout_ms = 9000;
	znd->cache_size = 4096;
	znd->cache_to_pagecache = 0;
	znd->cache_reada = 64;
	znd->journal_age = 3;
	znd->queue_delay = msecs_to_jiffies(WB_DELAY_MS);

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
			znd->enable_trim = 1;
		if (!strcasecmp("nodiscard", argv[r]))
			znd->enable_trim = 0;
		if (!strcasecmp("bio-queue", argv[r]))
			znd->queue_depth = 1;
		if (!strcasecmp("no-bio-queue", argv[r]))
			znd->queue_depth = 0;

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
#if ENABLE_SEC_METADATA
		if (!strncasecmp("meta=", argv[r], 5))
			meta_dev = argv[r]+5;
		if (!strcasecmp("mirror-md", argv[r]))
			znd->meta_dst_flag = DST_TO_BOTH_DEVICE;
#endif
	}

	znd->ti = ti;
	ti->private = znd;
	znd->zdstart = first_data_zone; /* IN ABSOLUTE COORDs */
	znd->mz_provision = mz_md_provision;

	r = dm_get_device(ti, argv[0], FMODE_READ | FMODE_WRITE, &znd->dev);
	if (r) {
		ti->error = "Error opening backing device";
		zoned_destroy(znd);
		return -EINVAL;
	}

#if ENABLE_SEC_METADATA
	if (znd->meta_dst_flag != DST_TO_BOTH_DEVICE) {
		if (meta_dev)
			znd->meta_dst_flag = DST_TO_SEC_DEVICE;
		else
			znd->meta_dst_flag = DST_TO_PRI_DEVICE;
	}
#endif

	if (znd->dev->bdev) {
		u64 sect = get_start_sect(znd->dev->bdev) >> Z_SHFT4K;

		bdevname(znd->dev->bdev, znd->bdev_name);
#if ENABLE_SEC_METADATA
		if (znd->meta_dst_flag == DST_TO_SEC_DEVICE) {
			u64 zone = dm_round_up(sect, Z_BLKSZ) - sect;

			znd->sec_dev_start_sect = sect;
			znd->sec_zone_align = zone << Z_SHFT4K;
		} else
			znd->start_sect = sect;
#else
		znd->start_sect = sect;
#endif
	}

#if ENABLE_SEC_METADATA
	if (meta_dev) {
		u64 sect;

		r = dm_get_device(ti, meta_dev, FMODE_READ | FMODE_WRITE,
				&znd->meta_dev);
		if (r) {
			ti->error = "Error opening metadata device";
			zoned_destroy(znd);
			return -EINVAL;
		}
		bdevname(znd->meta_dev->bdev, znd->bdev_metaname);
		sect = get_start_sect(znd->meta_dev->bdev) >> Z_SHFT4K;
		if (znd->meta_dst_flag == DST_TO_SEC_DEVICE)
			znd->start_sect = sect;
		else
			znd->sec_dev_start_sect = sect;
	}
#endif

	/*
	 * Set if this target needs to receive flushes regardless of
	 * whether or not its underlying devices have support.
	 */
	ti->num_flush_bios = 1;
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

	if (!znd->enable_trim) {
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
	znd->filled_zone = NOZONE;

	if (zac_probe || zbc_probe)
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

	INIT_LIST_HEAD(&(znd->bio_srt_jif_lst_head));
	spin_lock_init(&znd->zdm_bio_q_lck);
	znd->bio_kthread = kthread_run(znd_bio_merge_dispatch, znd, "zdm-io-%s",
			znd->bdev_name);
	if (IS_ERR(znd->bio_kthread)) {
		r = PTR_ERR(znd->bio_kthread);
		ti->error = "Couldn't alloc kthread";
		zoned_destroy(znd);
		return r;
	}

	r = zdm_create_proc_entries(znd);
	if (r) {
		ti->error = "Failed to create /proc entries";
		zoned_destroy(znd);
		return -EINVAL;
	}

	/* Restore any ZDM SB 'config' changes here */
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

	wake_up_process(znd->bio_kthread);
	wait_event(znd->wait_bio, atomic_read(&znd->enqueued) == 0);
	kthread_stop(znd->bio_kthread);
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
		DMERR("ERROR: dm_io_work error: %lx", error_bits);
}

/**
 * _znd_async_io() - Issue I/O via dm_io async or sync (using worker thread).
 * @znd: ZDM Instance
 * @io_req: I/O request
 * @data: Data for I/O
 * @where: I/O region
 * @dtype: I/O data type
 * @queue: Use worker when true
 *
 * Return 0 on success, otherwise error.
 */
static int _znd_async_io(struct zdm *znd, struct dm_io_request *io_req,
			void *data, struct dm_io_region *where,
			enum dm_io_mem_type dtype, int queue)
{
	int rcode;
	unsigned long error_bits = 0;

	switch (dtype) {
	case DM_IO_KMEM:
		io_req->mem.ptr.addr = data;
		break;
	case DM_IO_BIO:
		io_req->mem.ptr.bio = data;
		break;
	case DM_IO_VMA:
		io_req->mem.ptr.vma = data;
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
		req.where = where;
		req.io_req = io_req;
		queue_work(znd->io_wq, &req.work);

		Z_DBG(znd, "%s: wait for %s io (%lx)",
			__func__,
			io_req->bi_op == REQ_OP_READ ? "R" : "W",
			where->sector >> 3);

		flush_workqueue(znd->io_wq);

		Z_DBG(znd, "%s: cmplted %s io (%lx)",
			__func__,
			io_req->bi_op == REQ_OP_READ ? "R" : "W",
			where->sector >> 3);
		destroy_work_on_stack(&req.work);

		rcode = req.result;
		if (rcode < 0)
			Z_ERR(znd, "ERROR: dm_io error: %d", rcode);
		goto done;
	}
	rcode = dm_io(io_req, 1, where, &error_bits);
	if (error_bits || rcode < 0)
		Z_ERR(znd, "ERROR: dm_io error: %d -- %lx", rcode, error_bits);

done:
	return rcode;

}

/**
 * znd_async_io() - Issue I/O via dm_io async or sync (using worker thread).
 * @znd: ZDM Instance
 * @dtype: Type of memory in data
 * @data: Data for I/O
 * @block: bLBA for I/O
 * @nDMsect: Number of 512 byte blocks to read/write.
 * @rw: REQ_OP_READ or REQ_OP_WRITE
 * @queue: if true then use worker thread for I/O and wait.
 * @callback: callback to use on I/O complete.
 * context: context to be passed to callback.
 *
 * Return 0 on success, otherwise error.
 */
static int znd_async_io(struct zdm *znd,
			enum dm_io_mem_type dtype,
			void *data, sector_t block, unsigned int nDMsect,
			unsigned int op, unsigned int opf, int queue,
			io_notify_fn callback, void *context)
{
	int rcode;
#if ENABLE_SEC_METADATA
	struct dm_io_region where;
#else
	struct dm_io_region where = {
		.bdev = znd->dev->bdev,
		.sector = block,
		.count = nDMsect,
	};

#endif
	struct dm_io_request io_req = {
		.bi_op = op,
		.bi_op_flags = opf,
		.mem.type = dtype,
		.mem.offset = 0,
		.mem.ptr.vma = data,
		.client = znd->io_client,
		.notify.fn = callback,
		.notify.context = context,
	};

#if ENABLE_SEC_METADATA
	where.bdev  = znd_get_backing_dev(znd, &block);
	where.count = nDMsect;
	where.sector = block;
	if (op == REQ_OP_WRITE &&
	    znd->meta_dst_flag == DST_TO_BOTH_DEVICE &&
	    block < (znd->data_lba << Z_SHFT4K)) {
		where.bdev = znd->meta_dev->bdev;
		rcode = _znd_async_io(znd, &io_req, data, &where, dtype, queue);
		where.bdev = znd->dev->bdev;
		rcode = _znd_async_io(znd, &io_req, data, &where, dtype, queue);
	} else

/*
 * do we need to check for OP_READ && DST_TO_SEC_DEVICE,
 * also wouldn't DST_TO_BOTH_DEVICE prefer to read from DST_TO_SEC_DEVICE ?
 */

#endif
		rcode = _znd_async_io(znd, &io_req, data, &where, dtype, queue);

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
static int read_block(struct zdm *znd, enum dm_io_mem_type dtype,
		      void *data, u64 lba, unsigned int count, int queue)
{
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
static int writef_block(struct zdm *znd, enum dm_io_mem_type dtype,
			void *data, u64 lba, unsigned int op_flags,
			unsigned int count, int queue)
{
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
static int write_block(struct zdm *znd, enum dm_io_mem_type dtype,
		       void *data, u64 lba, unsigned int count, int queue)
{
	unsigned int op_flags = 0;

	return writef_block(znd, dtype, data, lba, op_flags, count, queue);
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

	if (!hook) {
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
		return -ENOMEM;
	}

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

#if ENABLE_SEC_METADATA
	struct block_device *bdev = znd->dev->bdev;

	if (bio->bi_bdev != bdev->bd_contains && bio->bi_bdev != bdev)
		return;

	switch (znd->meta_dst_flag) {
	case DST_TO_PRI_DEVICE:
	case DST_TO_BOTH_DEVICE:
		if (bio_op(bio) == REQ_OP_WRITE && lba > znd->start_sect) {
			lba -= znd->start_sect;
			if (lba > 0)
				increment_used_blks(znd, lba - 1, blks + 1);
		}
		break;
	case DST_TO_SEC_DEVICE:
		if (bio_op(bio) == REQ_OP_WRITE &&
		    lba > znd->sec_dev_start_sect) {

			lba = lba - znd->sec_dev_start_sect
				 - (znd->sec_zone_align >> Z_SHFT4K);
			if (lba > 0)
				increment_used_blks(znd, lba - 1, blks + 1);
		}
		break;
	}
#else
	if (bio_op(bio) == REQ_OP_WRITE && lba > znd->start_sect) {
		lba -= znd->start_sect;
		if (lba > 0)
			increment_used_blks(znd, lba - 1, blks + 1);
	}
#endif
}

/**
 * zoned_endio() - DM bio completion notification.
 * @ti: DM Target instance.
 * @bio: Bio being completed.
 * @err: Error associated with bio.
 *
 * Non-split and non-dm_io bios end notification is here.
 * Update the WP location for REQ_OP_WRITE bios.
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
		if (bio_data_dir(bio) == REQ_OP_WRITE)
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
	int count = 1;
	int use_wq = 1;
	unsigned int bytes = bio_cur_bytes(bio);
	u8 *data = bio_data(bio);
	u8 *io = NULL;
	u16 ua_off = bio->bi_iter.bi_sector & 0x0007;
	u16 ua_size = bio->bi_iter.bi_size & 0x0FFF;	/* in bytes */
	u32 mapped = 0;
	u64 disk_lba = 0;

	znd->is_empty = 0;
	if (!znd->cow_block)
		znd->cow_block = ZDM_ALLOC(znd, Z_C4K, PG_02, GFP_ATOMIC);

	io = znd->cow_block;
	if (!io)
		return -EIO;

	disk_lba = z_acquire(znd, Z_AQ_STREAM_ID, blks, &mapped);
	if (!disk_lba || !mapped)
		return -ENOSPC;

	while (bytes) {
		int ioer;
		unsigned int iobytes = Z_C4K;
		gfp_t gfp = GFP_ATOMIC;

		/* ---------------------------------------------------------- */
		if (origin) {
			if (s_zdm != znd->cow_addr) {
				Z_ERR(znd, "Copy block from %llx <= %llx",
				      origin, s_zdm);
				ioer = read_block(znd, DM_IO_KMEM, io, origin,
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

		ioer = write_block(znd, DM_IO_KMEM, io, disk_lba, count, use_wq);
		if (ioer)
			return -EIO;

		ioer = z_mapped_addmany(znd, s_zdm, disk_lba, mapped, gfp);
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
			origin = current_mapping(znd, s_zdm, gfp);
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

	err = write_block(znd, DM_IO_VMA, dm_vbuf, lba, blks, use_wq);
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
#if ENABLE_SEC_METADATA
	sector_t sector;
#endif
	znd->is_empty = 0;
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
			Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
			goto out;
		}

#if ENABLE_SEC_METADATA
		sector = lba << Z_SHFT4K;
		split->bi_bdev = znd_get_backing_dev(znd, &sector);
		split->bi_iter.bi_sector = sector;
#else
		split->bi_iter.bi_sector = lba << Z_SHFT4K;
#endif
		submit_bio(split);
		err = z_mapped_addmany(znd, s_zdm, lba, mapped, GFP_ATOMIC);
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
	gfp_t gfp = GFP_ATOMIC;
	struct bvec_iter start;
	struct bvec_iter iter;
	struct bio_vec bv;
	struct io_4k_block *io_vcache;
	struct io_dm_block *dm_vbuf = NULL;

	znd->is_empty = 0;
	MutexLock(&znd->vcio_lock);
	io_vcache = get_io_vcache(znd, gfp);
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

				if (mapped < blks) {
					Z_ERR(znd, "ERROR: Bad write %"
					      PRId32 " beyond alloc'd space",
					      mapped);
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

			if (mapped < blks) {
				Z_ERR(znd, "ERROR: [2] Bad write %"
				      PRId32 " beyond alloc'd space",
				      mapped);
			}

			lba     += blks;
			written += blks;
			mcount  += blks;
			mapped  -= blks;

		}
		err = z_mapped_addmany(znd, s_zdm, alloc_ori, mcount, gfp);
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
	unsigned long flags;

	if (ua_size || ua_off) {
		u64 origin;

		origin = current_mapping(znd, s_zdm, GFP_ATOMIC);
		if (origin) {
			rcode = zm_cow(znd, bio, s_zdm, blks, origin);
			spin_lock_irqsave(&znd->stats_lock, flags);
			if (znd->htlba < (s_zdm + blks))
				znd->htlba = s_zdm + blks;
			spin_unlock_irqrestore(&znd->stats_lock, flags);
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
	if (is_bio_aligned(bio)) {
		if (is_zero_bio(bio)) {
			rcode = zoned_map_discard(znd, bio, s_zdm);
		} else {
			rcode = zm_write_bios(znd, bio, s_zdm);
			spin_lock_irqsave(&znd->stats_lock, flags);
			if (znd->htlba < (s_zdm + blks))
				znd->htlba = s_zdm + blks;
			spin_unlock_irqrestore(&znd->stats_lock, flags);
		}
	} else {
		rcode = zm_write_pages(znd, bio, s_zdm);
		spin_lock_irqsave(&znd->stats_lock, flags);
		if (znd->htlba < (s_zdm + blks))
			znd->htlba = s_zdm + blks;
		spin_unlock_irqrestore(&znd->stats_lock, flags);
	}

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
	u64 blba;
	u32 blks;
	int sectors;
	int count;
	u16 ua_off;
	u16 ua_size;
	gfp_t gfp = GFP_ATOMIC;
#if ENABLE_SEC_METADATA
	sector_t sector;
#endif

	do {
		count = blks = dm_div_up(bio->bi_iter.bi_size, Z_C4K);
		ua_off = bio->bi_iter.bi_sector & 0x0007;
		ua_size = bio->bi_iter.bi_size & 0x0FFF;
		blba = current_map_range(znd, s_zdm, &count, gfp);
		s_zdm += count;
		sectors = (count << Z_SHFT4K);
		if (ua_size)
			sectors += (ua_size >> SECTOR_SHIFT) - 8;

		split = zsplit_bio(znd, bio, sectors);
		if (!split) {
			rcode = -ENOMEM;
			Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
			goto out;
		}
		if (blba) {
#if ENABLE_SEC_METADATA
			sector = (blba << Z_SHFT4K) + ua_off;
			split->bi_bdev = znd_get_backing_dev(znd, &sector);
			split->bi_iter.bi_sector = sector;
#else
			split->bi_iter.bi_sector = (blba << Z_SHFT4K) + ua_off;
#endif
			submit_bio(split);
		} else {
			zero_fill_bio(split);
			bio_endio(split);
		}
	} while (split != bio);

out:
	return rcode;
}

#define REQ_CHECKPOINT (REQ_FLUSH_SEQ | REQ_PREFLUSH | REQ_FUA)

/**
 * zoned_bio() - Handle and incoming BIO.
 * @znd: ZDM Instance
 */
static int zoned_bio(struct zdm *znd, struct bio *bio)
{
	bool is_write = op_is_write(bio_op(bio));
	u64 s_zdm = (bio->bi_iter.bi_sector >> Z_SHFT4K) + znd->md_end;
	int rcode = DM_MAPIO_SUBMITTED;
	struct request_queue *q;
	bool op_is_flush = false;
	bool do_flush_workqueue = false;
	bool do_end_bio = false;

	/* map to backing device ... NOT dm-zdm device */
	bio->bi_bdev = znd->dev->bdev;

	q = bdev_get_queue(bio->bi_bdev);
	q->queue_flags |= QUEUE_FLAG_NOMERGES;

	if (is_write && znd->meta_result) {
		if (!(bio_op(bio) == REQ_OP_DISCARD)) {
			rcode = znd->meta_result;
			Z_ERR(znd, "MAP ERR (meta): %d", rcode);
			goto out;
		}
	}

	if (is_write) {
		if ((bio->bi_opf & REQ_CHECKPOINT) ||
		     bio_op(bio) == REQ_OP_FLUSH) {

#if ENABLE_SEC_METADATA
			if (znd->meta_dst_flag != DST_TO_SEC_DEVICE)
				bio->bi_opf &= ~REQ_CHECKPOINT;
#else
			bio->bi_opf &= ~REQ_CHECKPOINT;
#endif
			set_bit(DO_SYNC, &znd->flags);
			set_bit(DO_FLUSH, &znd->flags);
			op_is_flush = true;
			do_flush_workqueue = true;
		}
	}

	if (znd->last_op_is_flush && op_is_flush && bio->bi_iter.bi_size == 0) {
		do_end_bio = true;
		goto out;
	}

	Z_DBG(znd, "%s: U:%lx sz:%u (%lu) -> s:%"PRIx64"-> %s%s", __func__,
	      bio->bi_iter.bi_sector, bio->bi_iter.bi_size,
	      bio->bi_iter.bi_size / PAGE_SIZE, s_zdm,
	      op_is_flush ? "F" : (is_write ? "W" : "R"),
	      bio_op(bio) == REQ_OP_DISCARD ? "+D" : "");

	if (bio->bi_iter.bi_size) {
		if (bio_op(bio) == REQ_OP_DISCARD) {
			rcode = zoned_map_discard(znd, bio, s_zdm);
		} else if (is_write) {
			const gfp_t gfp = GFP_ATOMIC;
			const int gc_wait = 0;

			rcode = zoned_map_write(znd, bio, s_zdm);
			if (znd->z_gc_free < (znd->gc_wm_crit + 2))
				gc_immediate(znd, gc_wait, gfp);

		} else {
			rcode = zm_read_bios(znd, bio, s_zdm);
		}
		znd->age = jiffies;
	} else {
		do_end_bio = true;
	}

	if (znd->memstat > 25 << 20)
		set_bit(DO_MEMPOOL, &znd->flags);

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

	if (znd->trim->count > MC_HIGH_WM ||
	    znd->unused->count > MC_HIGH_WM ||
	    znd->wbjrnl->count > MC_HIGH_WM ||
	    znd->ingress->count > MC_HIGH_WM)
		do_flush_workqueue = true;

	if (do_flush_workqueue && work_pending(&znd->meta_work))
		flush_workqueue(znd->meta_wq);



out:
	if (do_end_bio) {
		if (znd->meta_result) {
			bio->bi_error = znd->meta_result;
			znd->meta_result = 0;
		}
		bio_endio(bio);
	}

	Z_DBG(znd, "%s: ..... -> s:%"PRIx64"-> rc: %d", __func__, s_zdm, rcode);

	znd->last_op_is_flush = op_is_flush;

	return rcode;
}

/**
 * _do_mem_purge() - conditionally trigger a reduction of cache memory
 * @znd: ZDM Instance
 */
static inline int _do_mem_purge(struct zdm *znd)
{
	const int pool_size = znd->cache_size >> 1;
	int do_work = 0;

	if (atomic_read(&znd->incore) > pool_size) {
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

	if (is_expired_msecs(znd->flush_age, 30000)) {
		if (!test_bit(DO_METAWORK_QD, &znd->flags) &&
		    !work_pending(&znd->meta_work)) {
			Z_DBG(znd, "Periodic FLUSH");
			set_bit(DO_SYNC, &znd->flags);
			set_bit(DO_FLUSH, &znd->flags);
			set_bit(DO_METAWORK_QD, &znd->flags);
			queue_work(znd->meta_wq, &znd->meta_work);
			znd->flush_age = jiffies_64;
		}
	}

	if (is_expired_msecs(znd->age, DISCARD_IDLE_MSECS))
		max_tries = 20;

	do {
		int count;

		count = unmap_deref_chunk(znd, 2048, 0, GFP_KERNEL);
		if (count == -EAGAIN) {
			if (!work_pending(&znd->meta_work)) {
				set_bit(DO_METAWORK_QD, &znd->flags);
				queue_work(znd->meta_wq, &znd->meta_work);
			}
			break;
		}
		if (count != 1 || --max_tries < 0)
			break;

		if (test_bit(ZF_FREEZE, &znd->flags))
			return;

	} while (is_expired_msecs(znd->age, DISCARD_IDLE_MSECS));

	gc_queue_with_delay(znd, delay, GFP_KERNEL);

	if (_do_mem_purge(znd))
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
	if (znd->enable_trim) {
		limits->discard_alignment = Z_C4K;
		limits->discard_granularity = Z_C4K;
		limits->max_discard_sectors = 1 << 20;
		limits->max_hw_discard_sectors = 1 << 20;
		limits->discard_zeroes_data = 1;
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

	return zone < znd->zone_count ? &znd->wp_proc_at : NULL;
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

	while (zone < znd->zone_count) {
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

	return zone < znd->zone_count ? &znd->wp_proc_at : NULL;
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

	while (zone < znd->zone_count) {
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
	for (zone = 0; zone < znd->zone_count; zone++) {
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
	status.journal_pages = znd->wbjrnl->size / PAGE_SIZE;
	status.journal_entries = znd->wbjrnl->count;

	/*  fixed array of ->fwd_tm and ->rev_tm */
	status.m_zones = znd->zone_count;

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
	int bin;

	seq_printf(seqf, "On device:     %s\n", _zdisk(znd));
	seq_printf(seqf, "Data Zones:    %u\n", znd->data_zones);
	seq_printf(seqf, "Empty Zones:   %u\n", znd->z_gc_free);
	seq_printf(seqf, "Cached Pages:  %u\n", znd->mc_entries);
	seq_printf(seqf, "Discard Pages: %u\n", znd->dc_entries);
	seq_printf(seqf, "ZTL Pages:     %d\n", atomic_read(&znd->incore));
	seq_printf(seqf, "   in ZTL:     %d\n", znd->in_zlt);
	seq_printf(seqf, "   in LZY:     %d\n", znd->in_lzy);
	seq_printf(seqf, "RAM in Use:    %lu [%lu MiB]\n",
		   znd->memstat, dm_div_up(znd->memstat, 1 << 20));
	seq_printf(seqf, "Zones GC'd:    %u\n", znd->gc_events);
	seq_printf(seqf, "GC Throttle:   %d\n", atomic_read(&znd->gc_throttle));
	seq_printf(seqf, "Ingress InUse: %u / %u\n",
		znd->ingress->count, znd->ingress->size);
	seq_printf(seqf, "Unused InUse:  %u / %u\n",
		znd->unused->count, znd->unused->size);
	seq_printf(seqf, "Discard InUse: %u / %u\n",
		znd->trim->count, znd->trim->size);
	seq_printf(seqf, "WB Jrnl InUse: %u / %u\n",
		znd->wbjrnl->count, znd->wbjrnl->size);

	seq_printf(seqf, "queue-depth=%u\n", znd->queue_depth);
	seq_printf(seqf, "gc-prio-def=%u\n", znd->gc_prio_def);
	seq_printf(seqf, "gc-prio-low=%u\n", znd->gc_prio_low);
	seq_printf(seqf, "gc-prio-high=%u\n", znd->gc_prio_high);
	seq_printf(seqf, "gc-prio-crit=%u\n", znd->gc_prio_crit);
	seq_printf(seqf, "gc-wm-crit=%u\n", znd->gc_wm_crit);
	seq_printf(seqf, "gc-wm-high=%u\n", znd->gc_wm_high);
	seq_printf(seqf, "gc-wm-low=%u\n", znd->gc_wm_low);
	seq_printf(seqf, "gc-status=%u\n", znd->gc_status);
	seq_printf(seqf, "cache-ageout-ms=%u\n", znd->cache_ageout_ms);
	seq_printf(seqf, "cache-size=%u\n", znd->cache_size);
	seq_printf(seqf, "cache-to-pagecache=%u\n", znd->cache_to_pagecache);
	seq_printf(seqf, "cache-reada=%u\n", znd->cache_reada);
	seq_printf(seqf, "journal-age=%u\n", znd->journal_age);

	for (bin = 0; bin < ARRAY_SIZE(znd->bins); bin++) {
		if (znd->max_bins[bin])
			seq_printf(seqf, "#%d: %d/%d\n",
				bin, znd->bins[bin], znd->max_bins[bin]);
	}

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
 * zdm_info_write() - Handle writes to /proc/zdm_sdXn/status
 * @file: In memory file structure attached to ../status
 * @buffer: User space buffer being written to status
 * @count: Number of bytes in write
 * @ppos: pseudo position within stream that buffer is starting at.
 */
static ssize_t zdm_info_write(struct file *file, const char __user *buffer,
			  size_t count, loff_t *ppos)
{
	struct zdm *znd = PDE_DATA(file_inode(file));
	char *user_data = NULL;

	if (count > 32768)
		return -EINVAL;

	user_data = vmalloc(count+1);
	if (!user_data) {
		Z_ERR(znd, "Out of space for user buffer .. %ld", count + 1);
		return -ENOMEM;
	}
	if (copy_from_user(user_data, buffer, count)) {
		vfree(user_data);
		return -EFAULT;
	}
	user_data[count] = 0;

	/* do stuff */

	/* echo a (possibly truncated) copy of user's input to kenrel log */
	if (count > 30)
		user_data[30] = 0;
	Z_ERR(znd, "User sent %ld bytes at offset %lld ... %s",
	      count, *ppos, user_data);

	return count;
}

/**
 * zdm_used_fops() - File operations to chain to zdm_info_open.
 */
static const struct file_operations zdm_info_fops = {
	.open = zdm_info_open,
	.read = seq_read,
	.write = zdm_info_write,
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

struct marg {
	const char *prefix;
	u32 *value;
};

/**
 * zoned_message() - dmsetup message sent to target.
 * @ti: Target Instance
 * @argc: Number of arguments
 * @argv: Array of arguments
 */
static int zoned_message(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct zdm *znd = ti->private;
	int iter;
	struct marg margs[] = {
		{ "queue-depth=",       &znd->queue_depth        },
		{ "gc-prio-def=",       &znd->gc_prio_def        },
		{ "gc-prio-low=",       &znd->gc_prio_low        },
		{ "gc-prio-high=",      &znd->gc_prio_high       },
		{ "gc-prio-crit=",      &znd->gc_prio_crit       },
		{ "gc-wm-crit=",        &znd->gc_wm_crit         },
		{ "gc-wm-high=",        &znd->gc_wm_high         },
		{ "gc-wm-low=",         &znd->gc_wm_low          },
		{ "gc-status=",         &znd->gc_status          },
		{ "cache-ageout-ms=",   &znd->cache_ageout_ms    },
		{ "cache-size=",        &znd->cache_size         },
		{ "cache-to-pagecache=",&znd->cache_to_pagecache },
		{ "cache-reada=",       &znd->cache_reada        },
		{ "journal-age=",       &znd->journal_age        },
	};

	for (iter = 0; iter < argc; iter++) {
		u64 tmp;
		int ii, err;
		bool handled = false;

		for (ii = 0; ii < ARRAY_SIZE(margs); ii++) {
			const char *opt = margs[ii].prefix;
			int len = strlen(opt);

			if (!strncasecmp(argv[iter], opt, len)) {
				err = kstrtoll(argv[iter] + len, 0, &tmp);
				if (err) {
					Z_ERR(znd, "Invalid arg %s\n",
						argv[iter]);
					continue;
				}
				*margs[ii].value = tmp;
				handled = true;
				break;
			}
		}
		if (!handled)
			Z_ERR(znd, "Message: %s not handled.", argv[iter]);
	}

	if (znd->queue_depth == 0 && atomic_read(&znd->enqueued) > 0)
		wake_up_process(znd->bio_kthread);

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
