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
#include <linux/blk-zoned-ctrl.h>
#include <linux/timer.h>
#include <linux/delay.h>
#include <linux/kthread.h>
#include <linux/freezer.h>
#include <linux/proc_fs.h>
#include <linux/seq_file.h>
#include "dm-zoned.h"

/*
 * FUTURE FIXME:
 * Current sd.c does not swizzle on report zones and no
 * scsi native drives exists so ... therefore all results are
 * little endian ...
 * When sd.c translates the output of report zones
 * then remove the 'everything is little endian' assumption.
 */
#define REPORT_ZONES_LE_ONLY 1

#define PRIu64 "llu"
#define PRIx64 "llx"
#define PRId32 "d"
#define PRIx32 "x"
#define PRIu32 "u"

static inline char *_zdisk(struct zoned *znd)
{
	return znd->bdev_name;
}

#define Z_ERR(znd, fmt, arg...) \
	pr_err("dm-zoned(%s): " fmt "\n", _zdisk(znd), ## arg)

#define Z_INFO(znd, fmt, arg...) \
	pr_info("dm-zoned(%s): " fmt "\n", _zdisk(znd), ## arg)

#define Z_DBG(znd, fmt, arg...) \
	pr_debug("dm-zoned(%s): " fmt "\n", _zdisk(znd), ## arg)

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
static int dev_is_congested(struct dm_dev *dev, int bdi_bits);
static int zoned_is_congested(struct dm_target_callbacks *cb, int bdi_bits);
static int zoned_ctr(struct dm_target *ti, unsigned argc, char **argv);
static void do_io_work(struct work_struct *work);
static int block_io(struct zoned *, enum dm_io_mem_type, void *, sector_t,
		    unsigned int, int, int);
static int znd_async_io(struct zoned *znd,
			enum dm_io_mem_type dtype,
			void *data,
			sector_t block, unsigned int nDMsect, int rw, int queue,
			io_notify_fn callback, void *context);
static int zoned_bio(struct zoned *znd, struct bio *bio);
static int zoned_map_write(struct zoned *znd, struct bio*, u64 s_zdm);
static int zoned_map_read(struct zoned *znd, struct bio *bio);
static int zoned_map(struct dm_target *ti, struct bio *bio);
static sector_t get_dev_size(struct dm_target *ti);
static int zoned_iterate_devices(struct dm_target *ti,
				 iterate_devices_callout_fn fn, void *data);
static void zoned_io_hints(struct dm_target *ti, struct queue_limits *limits);
static int is_zoned_inquiry(struct zoned *znd, int trim, int ata);
static int dmz_reset_wp(struct zoned *znd, u64 z_id);
static int dmz_open_zone(struct zoned *znd, u64 z_id);
static int dmz_close_zone(struct zoned *znd, u64 z_id);
static u32 dmz_report_count(struct zoned *znd, void *report, size_t bufsz);
static int dmz_report_zones(struct zoned *znd, u64 z_id,
			    struct bdev_zone_report *report, size_t bufsz);
static void activity_timeout(unsigned long data);
static void zoned_destroy(struct zoned *);
static int gc_can_cherrypick(struct zoned *znd, u32 sid, int delay, int gfp);
static void bg_work_task(struct work_struct *work);
static void on_timeout_activity(struct zoned *znd, int mempurge, int delay);
static int zdm_create_proc_entries(struct zoned *znd);
static void zdm_remove_proc_entries(struct zoned *znd);

/**
 * bio_stream() - Decode stream id from BIO.
 * @znd: ZDM Instance
 *
 * Return: stream_id
 */
static inline u32 bio_stream(struct bio *bio)
{
	/*
	 * Since adding stream id to a BIO is not yet in mainline we just
	 * assign some defaults: use stream_id 0xff for upper level meta data
	 * and 0x40 for everything else ...
	 */
	return (bio->bi_rw & REQ_META) ? 0xff : 0x40;
}

/**
 * get_bdev_bd_inode() - Get primary backing device inode
 * @znd: ZDM Instance
 *
 * Return: backing device inode
 */
static inline struct inode *get_bdev_bd_inode(struct zoned *znd)
{
	return znd->dev->bdev->bd_inode;
}

#include "libzoned.c"

static int dev_is_congested(struct dm_dev *dev, int bdi_bits)
{
	struct request_queue *q = bdev_get_queue(dev->bdev);

	return bdi_congested(&q->backing_dev_info, bdi_bits);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_is_congested(struct dm_target_callbacks *cb, int bdi_bits)
{
	struct zoned *znd = container_of(cb, struct zoned, callbacks);
	int backing = dev_is_congested(znd->dev, bdi_bits);

	if (znd->gc_backlog > 1) {
		/*
		 * Was BDI_async_congested;
		 * Was BDI_sync_congested;
		 */
		backing |= 1 << WB_async_congested;
		backing |= 1 << WB_sync_congested;
	}
	return backing;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void set_discard_support(struct zoned *znd, int trim)
{
	struct mapped_device *md = dm_table_get_md(znd->ti->table);
	struct queue_limits *limits = dm_get_queue_limits(md);
	struct gendisk *disk = znd->dev->bdev->bd_disk;

	Z_INFO(znd, "Discard Support: %s", trim ? "on" : "off");
	if (limits) {
		limits->logical_block_size =
			limits->physical_block_size =
			limits->io_min = Z_C4K;
		if (trim) {
			limits->discard_alignment = Z_C4K;
			limits->discard_granularity = Z_C4K;
			limits->max_discard_sectors = 1 << 16;
			limits->max_hw_discard_sectors = 1 << 16;
			limits->discard_zeroes_data = 1;
		}
	}
	/* fixup stacked queue limits so discard zero's data is honored */
	if (trim && disk->queue) {
		limits = &disk->queue->limits;
		limits->discard_alignment = Z_C4K;
		limits->discard_granularity = Z_C4K;
		limits->max_discard_sectors = 1 << 16;
		limits->max_hw_discard_sectors = 1 << 16;
		limits->discard_zeroes_data = 1;
	}
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int is_zoned_inquiry(struct zoned *znd, int trim, int ata)
{
	struct gendisk *disk = znd->dev->bdev->bd_disk;

	if (disk->queue) {
		u8 extended = 1;
		u8 page_op = 0xb1;
		u8 *buf = NULL;
		u16 sz = 64;
		int wp_err;

		set_discard_support(znd, trim);

#ifdef CONFIG_BLK_ZONED_CTRL
		if (ata) {
			struct zoned_identity ident;

			wp_err = blk_zoned_identify_ata(disk, &ident);
			if (!wp_err) {
				if (ident.type_id == HOST_AWARE) {
					znd->zinqtype = Z_TYPE_SMR_HA;
					znd->ata_passthrough = 1;
				}
			}
			return 0;
		}

		buf = ZDM_ALLOC(znd, Z_C4K, PG_01, NORMAL); /* zoned inq */
		if (!buf)
			return -ENOMEM;

		wp_err = blk_zoned_inquiry(disk, extended, page_op, sz, buf);
		if (!wp_err) {
			znd->zinqtype = buf[Z_VPD_INFO_BYTE] >> 4 & 0x03;
			if (znd->zinqtype != Z_TYPE_SMR_HA &&
			    buf[4] == 0x17 && buf[5] == 0x5c) {
				Z_ERR(znd, "Forcing ResetWP capability ... ");
				znd->zinqtype = Z_TYPE_SMR_HA;
				znd->ata_passthrough = 0;
			}
		}
#else
	#warning "CONFIG_BLK_ZONED_CTRL required."
#endif
		if (buf)
			ZDM_FREE(znd, buf, Z_C4K, PG_01);
	}

	return 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_map_discard(struct zoned *znd, struct bio *bio)
{
	u64 lba     = 0;
	int rcode   = DM_MAPIO_SUBMITTED;
	u64 s_zdm   = (bio->bi_iter.bi_sector >> Z_SHFT4K) + znd->md_end;
	u64 blks    = bio->bi_iter.bi_size / Z_C4K;
	u64 count;

	if (znd->is_empty)
		goto cleanup_out;

	for (count = 0; count < blks; count++) {
		int err = 0;
		u64 addr = s_zdm + count;

		MutexLock(&znd->mz_io_mutex);
		lba = current_mapping(znd, addr, CRIT);
		if (lba)
			err = z_mapped_discard(znd, addr, lba);
		mutex_unlock(&znd->mz_io_mutex);
		if (err) {
			rcode = err;
			goto out;
		}
		if ((count & 0xFFFFF) == 0 && blks > 0x80000) {
			if (test_bit(DO_JOURNAL_MOVE, &znd->flags) ||
			    test_bit(DO_MEMPOOL, &znd->flags)) {
				if (!test_bit(DO_METAWORK_QD, &znd->flags) &&
				    !work_pending(&znd->meta_work)) {

					Z_ERR(znd, "Large discard @ %"
					      PRIx64 " %" PRIu64 " blocks  ...",
					      s_zdm, blks);

					set_bit(DO_METAWORK_QD, &znd->flags);
					queue_work(znd->meta_wq,
						  &znd->meta_work);
					flush_workqueue(znd->meta_wq);
				}
			}
		}
	}

cleanup_out:
	bio_endio(bio);
out:
	return rcode;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int is_non_wp_zone(struct zoned *znd, u64 z_id)
{
	u32 gzoff = z_id % 1024;
	struct meta_pg *wpg = &znd->wp[z_id >> 10];
	u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]);

	return (wp & Z_WP_NON_SEQ) ? 1 : 0;
}


static int dmz_reset_wp(struct zoned *znd, u64 z_id)
{
	int wp_err = 0;

	if (is_non_wp_zone(znd, z_id))
		return wp_err;

#ifdef CONFIG_BLK_ZONED_CTRL
	if (znd->zinqtype == Z_TYPE_SMR_HA) {
		struct gendisk *disk = znd->dev->bdev->bd_disk;
		u64 z_offset = z_id + znd->zdstart;
		u64 s_addr = zone_to_sector(z_offset);
		int retry = 5;

		wp_err = -1;
		while (wp_err && --retry > 0) {
			if (znd->ata_passthrough)
				wp_err = blk_zoned_reset_wp_ata(disk, s_addr);
			else
				wp_err = blk_zoned_reset_wp(disk, s_addr);
		}

		if (wp_err) {
			Z_ERR(znd, "Reset WP: %" PRIx64 " [Z:%" PRIu64
			      "] -> %d failed.", s_addr, z_id, wp_err);
			Z_ERR(znd, "ZAC/ZBC support disabled.");
			znd->zinqtype = 0;
			wp_err = -ENOTSUPP;
		}
	}
#endif /* CONFIG_BLK_ZONED_CTRL */
	return wp_err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int dmz_open_zone(struct zoned *znd, u64 z_id)
{
	int wp_err = 0;

	if (is_non_wp_zone(znd, z_id))
		return wp_err;

#ifdef CONFIG_BLK_ZONED_CTRL
	if (znd->zinqtype == Z_TYPE_SMR_HA) {
		struct gendisk *disk = znd->dev->bdev->bd_disk;
		u64 z_offset = z_id + znd->zdstart;
		u64 s_addr = zone_to_sector(z_offset);
		int retry = 5;

		wp_err = -1;
		while (wp_err && --retry > 0) {
			if (znd->ata_passthrough)
				wp_err = blk_zoned_open_ata(disk, s_addr);
			else
				wp_err = blk_zoned_open(disk, s_addr);
		}

		if (wp_err) {
			Z_ERR(znd, "Open Zone: LBA: %" PRIx64
			      " [Z:%" PRIu64 "] -> %d failed.",
			      s_addr, z_id, wp_err);
			Z_ERR(znd, "ZAC/ZBC support disabled.");
			znd->zinqtype = 0;
			wp_err = -ENOTSUPP;
		}
	}
#endif /* CONFIG_BLK_ZONED_CTRL */

	return wp_err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int dmz_close_zone(struct zoned *znd, u64 z_id)
{
	int wp_err = 0;

	if (is_non_wp_zone(znd, z_id))
		return wp_err;

#ifdef CONFIG_BLK_ZONED_CTRL
	if (znd->zinqtype == Z_TYPE_SMR_HA) {
		struct gendisk *disk = znd->dev->bdev->bd_disk;
		u64 z_offset = z_id + znd->zdstart;
		u64 s_addr = zone_to_sector(z_offset);
		int retry = 5;

		wp_err = -1;
		while (wp_err && --retry > 0) {
			if (znd->ata_passthrough)
				wp_err = blk_zoned_close_ata(disk, s_addr);
			else
				wp_err = blk_zoned_close(disk, s_addr);
		}

		if (wp_err) {
			Z_ERR(znd, "Close Zone: LBA: %" PRIx64
			      " [Z:%" PRIu64 "] -> %d failed.",
			      s_addr, z_id, wp_err);
			Z_ERR(znd, "ZAC/ZBC support disabled.");
			znd->zinqtype = 0;
			wp_err = -ENOTSUPP;
		}
	}
#endif /* CONFIG_BLK_ZONED_CTRL */
	return wp_err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u32 dmz_report_count(struct zoned *znd, void *rpt_in, size_t bufsz)
{
	u32 count;
	u32 max_count = (bufsz - sizeof(struct bdev_zone_report))
		      /	 sizeof(struct bdev_zone_descriptor);

	if (REPORT_ZONES_LE_ONLY || znd->ata_passthrough) {
		struct bdev_zone_report_le *report = rpt_in;

		/* ZAC: ata results are little endian */
		if (max_count > le32_to_cpu(report->descriptor_count))
			report->descriptor_count = cpu_to_le32(max_count);
		count = le32_to_cpu(report->descriptor_count);
	} else {
		struct bdev_zone_report *report = rpt_in;

		/* ZBC: scsi results are big endian */
		if (max_count > be32_to_cpu(report->descriptor_count))
			report->descriptor_count = cpu_to_be32(max_count);
		count = be32_to_cpu(report->descriptor_count);
	}
	return count;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/**
 * dmz_report_zones() - issue report zones from z_id zones after zdstart
 * @znd: ZDM Target
 * @z_id: Zone past zdstart
 * @report: structure filled
 * @bufsz: kmalloc()'d space reserved for report
 *
 * Return: -ENOTSUPP or 0 on success
 */
static int dmz_report_zones(struct zoned *znd, u64 z_id,
			    struct bdev_zone_report *report, size_t bufsz)
{
	int wp_err = -ENOTSUPP;

#ifdef CONFIG_BLK_ZONED_CTRL
	if (znd->zinqtype == Z_TYPE_SMR_HA) {
		struct gendisk *disk = znd->dev->bdev->bd_disk;
		u64 s_addr = (z_id + znd->zdstart) << 19;
		u8  opt = ZOPT_NON_SEQ_AND_RESET;
		int retry = 5;

		wp_err = -1;
		while (wp_err && --retry > 0) {
			if (znd->ata_passthrough)
				wp_err = blk_zoned_report_ata(disk, s_addr, opt,
							      report, bufsz);
			else
				wp_err = blk_zoned_report(disk, s_addr, opt,
							  report, bufsz);
		}

		if (wp_err) {
			Z_ERR(znd, "Report Zones: LBA: %" PRIx64
			      " [Z:%" PRIu64 " -> %d failed.",
			      s_addr, z_id + znd->zdstart, wp_err);
			Z_ERR(znd, "ZAC/ZBC support disabled.");
			znd->zinqtype = 0;
			wp_err = -ENOTSUPP;
		}
	}
#endif /* CONFIG_BLK_ZONED_CTRL */
	return wp_err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline int is_zone_reset(struct bdev_zone_descriptor *dentry)
{
	u8 type = dentry->type & 0x0F;
	u8 cond = (dentry->flags & 0xF0) >> 4;

	return (ZCOND_ZC1_EMPTY == cond || ZTYP_CONVENTIONAL == type) ? 1 : 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline u32 get_wp_from_descriptor(struct zoned *znd, void *dentry_in)
{
	u32 wp = 0;

	/*
	 * If ATA passthrough was used then ZAC results are little endian.
	 * otherwise ZBC results are big endian.
	 */

	if (REPORT_ZONES_LE_ONLY || znd->ata_passthrough) {
		struct bdev_zone_descriptor_le *lil = dentry_in;

		wp = le64_to_cpu(lil->lba_wptr) - le64_to_cpu(lil->lba_start);
	} else {
		struct bdev_zone_descriptor *big = dentry_in;

		wp = be64_to_cpu(big->lba_wptr) - be64_to_cpu(big->lba_start);
	}
	return wp;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline int is_conventional(struct bdev_zone_descriptor *dentry)
{
	return (ZTYP_CONVENTIONAL == (dentry->type & 0x0F)) ? 1 : 0;
}

static inline void _inc_wp_free(struct meta_pg *wpg, u32 gzoff, u32 lost)
{
	wpg->zf_est[gzoff] = cpu_to_le32(le32_to_cpu(wpg->zf_est[gzoff])+lost);
}

static int zoned_wp_sync(struct zoned *znd, int reset_non_empty)
{
	int rcode = 0;
	u32 rcount = 0;
	u32 iter;
	size_t bufsz = REPORT_BUFFER * Z_C4K;
	struct bdev_zone_report *report = kmalloc(bufsz, GFP_KERNEL);

	if (!report) {
		rcode = -ENOMEM;
		goto out;
	}

	for (iter = 0; iter < znd->data_zones; iter++) {
		u32 entry = iter % 4096;
		u32 gzno  = iter >> 10;
		u32 gzoff = iter & ((1 << 10) - 1);
		struct meta_pg *wpg = &znd->wp[gzno];
		struct bdev_zone_descriptor *dentry;
		u32 wp_flgs;
		u32 wp_at;
		u32 wp;

		if (entry == 0) {
			int err = dmz_report_zones(znd, iter, report, bufsz);

			if (err) {
				Z_ERR(znd, "report zones-> %d", err);
				if (err != -ENOTSUPP)
					rcode = err;
				goto out;
			}
			rcount = dmz_report_count(znd, report, bufsz);
		}

		dentry = &report->descriptors[entry];
		if (reset_non_empty && !is_conventional(dentry)) {
			int err = 0;

			if (!is_zone_reset(dentry))
				err = dmz_reset_wp(znd, gzoff);

			if (err) {
				Z_ERR(znd, "reset wp-> %d", err);
				if (err != -ENOTSUPP)
					rcode = err;
				goto out;
			}
			wp = wp_at = 0;
			wpg->wp_alloc[gzoff] = cpu_to_le32(0);
			wpg->zf_est[gzoff] = cpu_to_le32(Z_BLKSZ);
			continue;
		}

		wp = get_wp_from_descriptor(znd, dentry);
		wp >>= Z_SHFT4K; /* 512 sectors to 4k sectors */
		wp_at = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_VALUE_MASK;
		wp_flgs = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_FLAGS_MASK;

		if (is_conventional(dentry)) {
			wp = wp_at;
			wp_flgs |= Z_WP_NON_SEQ;
		} else {
			wp_flgs &= ~Z_WP_NON_SEQ;
		}

		if (wp > wp_at) {
			u32 lost = wp - wp_at;

			wp_at = wp;
			_inc_wp_free(wpg, gzoff, lost);

			Z_ERR(znd, "Z#%u z:%x [wp:%x rz:%x] lost %u blocks.",
			      iter, gzoff, wp_at, wp, lost);
		}
		wpg->wp_alloc[gzoff] = cpu_to_le32(wp_at|wp_flgs);
	}

out:
	kfree(report);

	return rcode;
}

#if USE_KTHREAD

static int bio_queue_empty(struct zoned *znd)
{
	int empty;
	unsigned long flags;

	spin_lock_irqsave(&znd->bio_qlck, flags);
	empty = (znd->bio_in == znd->bio_out) ? 1 : 0;
	spin_unlock_irqrestore(&znd->bio_qlck, flags);

	return empty;
}

static inline int bio_queue_next(u32 io)
{
	return ((io + 1) & 0x1FF);
}

static int bio_queue_full(struct zoned *znd)
{
	int full;

	spin_lock(&znd->bio_qlck);
	full = (znd->bio_out == bio_queue_next(znd->bio_in)) ? 1 : 0;
	spin_unlock(&znd->bio_qlck);

	return full;
}

static void bio_queue_add(struct zoned *znd, struct bio *bio)
{
	int retry = 0;

	do {
		retry = 0;
		if (bio_queue_full(znd))
			wait_event_interruptible(znd->bio_wait,
				!bio_queue_full(znd));

		spin_lock(&znd->bio_qlck);
		if (znd->bio_in == bio_queue_next(znd->bio_out)) {
			spin_unlock(&znd->bio_qlck);
			retry = 1;
		}
	} while (retry);

	znd->bio_queue[znd->bio_in] = bio;
	znd->bio_in = bio_queue_next(znd->bio_in);
	spin_unlock(&znd->bio_qlck);
}

static struct bio *bio_queue_get(struct zoned *znd)
{
	struct bio *bio = NULL;

	spin_lock(&znd->bio_qlck);
	if (znd->bio_in != znd->bio_out) {
		bio = znd->bio_queue[znd->bio_out];
		znd->bio_out = bio_queue_next(znd->bio_out);
	}
	spin_unlock(&znd->bio_qlck);

	if (bio)
		wake_up_process(znd->bio_kthread); /* add may be waiting */

	return bio;
}

static int znd_bio_kthread(void *arg)
{
	int err = 0;
	struct zoned *znd = (struct zoned *)arg;

	Z_ERR(znd, "znd_bio_kthread [started]");

	while (!kthread_should_stop()) {
		struct bio *bio;

		bio = bio_queue_get(znd);
		if (!bio) {
			wait_event_interruptible(znd->bio_wait,
				!bio_queue_empty(znd));
			continue;
		}

		err = zoned_bio(znd, bio);
		if (err < 0) {
			znd->meta_result = err;
			goto out;
		}
	}
	err = 0;

	Z_ERR(znd, "znd_bio_kthread [stopped]");

out:
	return err;
}

static int zoned_map(struct dm_target *ti, struct bio *bio)
{
	struct zoned *znd = ti->private;

	bio_queue_add(znd, bio);
	wake_up_process(znd->bio_kthread);

	return DM_MAPIO_SUBMITTED;
}

#else

static int zoned_map(struct dm_target *ti, struct bio *bio)
{
	struct zoned *znd = ti->private;
	int err = zoned_bio(znd, bio);

	if (err < 0) {
		znd->meta_result = err;
		err = 0;
	}

	return err;
}

#endif

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void zoned_actual_size(struct dm_target *ti, struct zoned *znd)
{
	znd->nr_blocks = i_size_read(get_bdev_bd_inode(znd)) / Z_C4K;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/**
 * <data dev> <format|check|force>
 */
static int zoned_ctr(struct dm_target *ti, unsigned argc, char **argv)
{
	const int reset_non_empty = 0;
	int create = 0;
	int force = 0;
	int zbc_probe = 1;
	int zac_probe = 1;
	int trim = 1;
	int r;
	struct zoned *znd;
	long long first_data_zone = 0;
	long long mz_md_provision = MZ_METADATA_ZONES;

	BUILD_BUG_ON(Z_C4K != (sizeof(struct map_sect_to_lba) * Z_UNSORTED));
	BUILD_BUG_ON(Z_C4K != (sizeof(struct io_4k_block)));
	BUILD_BUG_ON(Z_C4K != (sizeof(struct mz_superkey)));

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
			trim = 1;
		if (!strcasecmp("nodiscard", argv[r]))
			trim = 0;

		if (!strncasecmp("reserve=", argv[r], 8)) {
			long long mz_resv;
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

	znd = ZDM_ALLOC(NULL, sizeof(*znd), KM_00, NORMAL);
	if (!znd) {
		ti->error = "Error allocating zoned structure";
		return -ENOMEM;
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

	if (!trim) {
		ti->discards_supported = false;
		ti->num_discard_bios = 0;
	}

	zoned_actual_size(ti, znd);
	znd->callbacks.congested_fn = zoned_is_congested;
	dm_table_add_target_callbacks(ti->table, &znd->callbacks);

	r = do_init_zoned(ti, znd);
	if (r) {
		ti->error = "Error in zoned init";
		zoned_destroy(znd);
		return -EINVAL;
	}
	if (zbc_probe) {
		Z_ERR(znd, "Checking for ZONED support %s",
			trim ? "with trim" : "");
		is_zoned_inquiry(znd, trim, 0);
	} else if (zac_probe) {
		Z_ERR(znd, "Checking for ZONED [ATA PASSTHROUGH] support %s",
			trim ? "with trim" : "");
		is_zoned_inquiry(znd, trim, 1);
	} else {
		Z_ERR(znd, "No PROBE");
		set_discard_support(znd, trim);
	}
	r = zoned_init_disk(ti, znd, create, force);
	if (r) {
		ti->error = "Error in zoned init from disk";
		zoned_destroy(znd);
		return -EINVAL;
	}
	r = zoned_wp_sync(znd, reset_non_empty);
	if (r) {
		ti->error = "Error in zoned re-sync WP";
		zoned_destroy(znd);
		return -EINVAL;
	}

#if USE_KTHREAD
	znd->bio_kthread = kthread_run(znd_bio_kthread, znd, "zdm-bio-%s",
		znd->bdev_name);
	if (!znd->bio_kthread) {
		ti->error = "Couldn't alloc kthread";
		zoned_destroy(znd);
		return -EINVAL;
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void zoned_dtr(struct dm_target *ti)
{
	struct zoned *znd = ti->private;

	if (znd->z_sballoc) {
		struct mz_superkey *key_blk = znd->z_sballoc;
		struct zdm_superblock *sblock = &key_blk->sblock;

		sblock->flags = cpu_to_le32(0);
		sblock->csum = sb_crc32(sblock);
	}

#if USE_KTHREAD
	wake_up_process(znd->bio_kthread);
	wait_event(znd->bio_wait, bio_queue_empty(znd));
	kthread_stop(znd->bio_kthread);
#endif

	zdm_remove_proc_entries(znd);

	zoned_destroy(znd);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 * Read or write a chunk aligned and sized block of data from a device.
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int znd_async_io(struct zoned *znd,
			enum dm_io_mem_type dtype,
			void *data,
			sector_t block, unsigned int nDMsect, int rw, int queue,
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
		.bi_rw = rw,
		.mem.type = dtype,
		.mem.offset = 0,
		.mem.ptr.vma = data,
		.client = znd->io_client,
		.notify.fn = callback,
		.notify.context = context,
	};

	MutexLock(&znd->io_lock);
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
		goto out;
	}

	rcode = dm_io(&io_req, 1, &where, &error_bits);
	if (error_bits || rcode < 0)
		Z_ERR(znd, "ERROR: dm_io error: %d -- %lx", rcode, error_bits);

out:
	mutex_unlock(&znd->io_lock);

	return rcode;
}


static int block_io(struct zoned *znd,
		    enum dm_io_mem_type dtype,
		    void *data, sector_t s, unsigned int n, int rw, int queue)
{
	return znd_async_io(znd, dtype, data, s, n, rw, queue, NULL, NULL);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 * count -> count in 4k sectors.
 */
static int read_block(struct dm_target *ti, enum dm_io_mem_type dtype,
		      void *data, u64 lba, unsigned int count, int queue)
{
	struct zoned *znd = ti->private;
	sector_t block = lba << Z_SHFT4K;
	unsigned int nDMsect = count << Z_SHFT4K;
	int rc;

	if (lba >= znd->nr_blocks) {
		Z_ERR(znd, "Error reading past end of media: %llx.", lba);
		rc = -EIO;
		return rc;
	}

	rc = block_io(znd, dtype, data, block, nDMsect, READ, queue);
	if (rc) {
		Z_ERR(znd, "read error: %d -- R: %llx [%u dm sect] (Q:%d)",
			rc, lba, nDMsect, queue);
		dump_stack();
	}

	return rc;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 * count -> count in 4k sectors.
 */
static int write_block(struct dm_target *ti, enum dm_io_mem_type dtype,
		       void *data, u64 lba, unsigned int count, int queue)
{
	struct zoned *znd = ti->private;
	sector_t block = lba << Z_SHFT4K;
	unsigned int nDMsect = count << Z_SHFT4K;
	int rc;

	rc = block_io(znd, dtype, data, block, nDMsect, WRITE, queue);
	if (rc) {
		Z_ERR(znd, "write error: %d W: %llx [%u dm sect] (Q:%d)",
			rc, lba, nDMsect, queue);
		dump_stack();
	}

	return rc;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zm_cow(struct zoned *znd, struct bio *bio, u64 s_zdm, u32 blks,
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

		ioer = z_mapped_addmany(znd, s_zdm, disk_lba, mapped, CRIT);
		if (ioer) {
			Z_ERR(znd, "%s: Journal MANY failed.", __func__);
			return -EIO;
		}

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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

#define BIO_CACHE_SECTORS (IO_VCACHE_PAGES * Z_BLOCKS_PER_DM_SECTOR)

/**
 * Write 4k blocks from cache to lba.
 * Move any remaining 512 byte blocks to the start of cache and update
 * the @_blen count is updated
 */
static int zm_write_cache(struct zoned *znd, struct io_dm_block *dm_vbuf,
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zm_write_one_page(struct zoned *znd, struct bio *bio, u64 s_zdm)
{
	u32 acqflgs = Z_AQ_STREAM_ID | bio_stream(bio);
	u64 lba     = 0;
	u32 mapped  = 0;
	int err     = -EIO;

reacquire:
	/*
	 * When lba is zero no blocks were not allocated.
	 * Retry with the smaller request
	 */
	lba = z_acquire(znd, acqflgs, 1, &mapped);
	if (!lba && mapped)
		lba = z_acquire(znd, acqflgs, mapped, &mapped);

	if (!lba) {
		if (znd->gc_throttle.counter == 0) {
			err = -ENOSPC;
			goto out;
		}

		Z_ERR(znd, "Throttle input ... Mandatory GC.");
		if (delayed_work_pending(&znd->gc_work)) {
			mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
			mutex_unlock(&znd->mz_io_mutex);
			flush_delayed_work(&znd->gc_work);
			MutexLock(&znd->mz_io_mutex);
		}
		goto reacquire;
	}

	bio->bi_iter.bi_sector = lba << Z_SHFT4K;
	err = z_mapped_addmany(znd, s_zdm, lba, mapped, CRIT);
	if (err) {
		Z_ERR(znd, "%s: Journal MANY failed.", __func__);
		err = DM_MAPIO_REQUEUE;
		goto out;
	}
	err = DM_MAPIO_REMAPPED;

out:
	return err;

}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zm_write_pages(struct zoned *znd, struct bio *bio, u64 s_zdm)
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
			if (znd->gc_throttle.counter == 0) {
				err = -ENOSPC;
				goto out;
			}

			Z_ERR(znd, "Throttle input ... Mandatory GC.");
			if (delayed_work_pending(&znd->gc_work)) {
				mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
				mutex_unlock(&znd->mz_io_mutex);
				flush_delayed_work(&znd->gc_work);
				MutexLock(&znd->mz_io_mutex);
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
		err = z_mapped_addmany(znd, s_zdm, alloc_ori, mcount, CRIT);
		if (err) {
			Z_ERR(znd, "%s: Journal MANY failed.", __func__);
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int is_empty_page(struct zoned *znd, void *pg, size_t len)
{
	u64 *chk = pg;
	size_t count = len / sizeof(*chk);
	size_t entry;

	for (entry = 0; entry < count; entry++) {
		if (chk[entry])
			return 0;
	}
	return 1;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int is_zero_bio(struct zoned *znd, struct bio *bio)
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
		is_empty = is_empty_page(znd, src + boff, bv.bv_len);
		kunmap_atomic(src);

		if (!is_empty)
			break;
	} /* end: __bio_for_each_segment */

	return is_empty;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_map_write(struct zoned *znd, struct bio *bio, u64 s_zdm)
{
	u32 blks     = dm_div_up(bio->bi_iter.bi_size, Z_C4K);
	u16 ua_off   = bio->bi_iter.bi_sector & 0x0007;
	u16 ua_size  = bio->bi_iter.bi_size & 0x0FFF;	/* in bytes */
	int rcode    = -EIO;

	if (ua_size || ua_off) {
		u64 origin;

		MutexLock(&znd->mz_io_mutex);
		origin = current_mapping(znd, s_zdm, CRIT);
		if (origin)
			rcode = zm_cow(znd, bio, s_zdm, blks, origin);
		mutex_unlock(&znd->mz_io_mutex);
		return rcode;
	}

	/* on RAID 4/5/6 all writes are 4k */
	if (blks == 1) {
		if (is_zero_bio(znd, bio)) {
			rcode = zoned_map_discard(znd, bio);
		} else {
			MutexLock(&znd->mz_io_mutex);
			rcode = zm_write_one_page(znd, bio, s_zdm);
			mutex_unlock(&znd->mz_io_mutex);
		}
		return rcode;
	}

	MutexLock(&znd->mz_io_mutex);
	rcode = zm_write_pages(znd, bio, s_zdm);
	mutex_unlock(&znd->mz_io_mutex);
	return rcode;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_map_read(struct zoned *znd, struct bio *bio)
{
	int rcode = DM_MAPIO_SUBMITTED;
	u64 ua_off = bio->bi_iter.bi_sector & 0x0007;
	u64 ua_size = bio->bi_iter.bi_size & 0x0FFF;	/* in bytes */
	u64 s_zdm = (bio->bi_iter.bi_sector >> Z_SHFT4K) + znd->md_end;
	u64 blks = dm_div_up(bio->bi_iter.bi_size, Z_C4K);
	u64 start_lba;

	start_lba = current_mapping(znd, s_zdm, CRIT);
	if (start_lba) {
		u64 sz;

		bio->bi_iter.bi_sector = start_lba << Z_SHFT4K;
		if (ua_off)
			bio->bi_iter.bi_sector += ua_off;

		for (sz = 1; sz < blks; sz++) {
			u64 next_lba;

			next_lba = current_mapping(znd, s_zdm + sz, CRIT);
			if (next_lba != (start_lba + sz)) {
				unsigned nsect = sz * 8;

				if (ua_size) {
					unsigned ua_blocks = ua_size / 512;

					nsect -= 8;
					nsect += ua_blocks;
				}
				Z_DBG(znd,
					"NON SEQ @ %llx + %llu [%llx] [%llx]",
					 s_zdm + sz, sz, start_lba, next_lba);

				dm_accept_partial_bio(bio, nsect);
				break;
			}
		}

		if (ua_off || ua_size)
			Z_ERR(znd, "(R): bio: sector: %lx bytes: %u",
			      bio->bi_iter.bi_sector, bio->bi_iter.bi_size);

		generic_make_request(bio);
	} else {
		zero_fill_bio(bio);
		bio_endio(bio);
	}

	return rcode;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_bio(struct zoned *znd, struct bio *bio)
{
	bool is_write = (bio_data_dir(bio) == WRITE);
	u64 s_zdm = (bio->bi_iter.bi_sector >> Z_SHFT4K) + znd->md_end;
	int rcode = DM_MAPIO_SUBMITTED;
	struct request_queue *q;
	int force_sync_now = 0;

	/* map to backing device ... NOT dm-zoned device */
	bio->bi_bdev = znd->dev->bdev;

	q = bdev_get_queue(bio->bi_bdev);
	q->queue_flags |= QUEUE_FLAG_NOMERGES;

	if (is_write && znd->meta_result) {
		if (!(bio->bi_rw & REQ_DISCARD)) {
			rcode = znd->meta_result;
			Z_ERR(znd, "MAP ERR (meta): %d", rcode);
			goto out;
		}
	}

	/* check for SYNC flag */
	if (bio->bi_rw & REQ_SYNC) {
		set_bit(DO_SYNC, &znd->flags);
		force_sync_now = 1;
	}

	Z_DBG(znd, "%s: s:%"PRIx64" sz:%u -> %s", __func__, s_zdm,
	      bio->bi_iter.bi_size, is_write ? "W" : "R");

	if (bio->bi_iter.bi_size) {
		if (bio->bi_rw & REQ_DISCARD) {
			rcode = zoned_map_discard(znd, bio);
		} else if (is_write) {
			znd->is_empty = 0;
			rcode = zoned_map_write(znd, bio, s_zdm);
		} else {
			MutexLock(&znd->mz_io_mutex);
			rcode = zoned_map_read(znd, bio);
			mutex_unlock(&znd->mz_io_mutex);
		}
		znd->age = jiffies;
	}

	if (test_bit(DO_SYNC, &znd->flags) ||
	    test_bit(DO_JOURNAL_MOVE, &znd->flags) ||
	    test_bit(DO_MEMPOOL, &znd->flags)) {
		if (!test_bit(DO_METAWORK_QD, &znd->flags) &&
		    !work_pending(&znd->meta_work)) {
			set_bit(DO_METAWORK_QD, &znd->flags);
			queue_work(znd->meta_wq, &znd->meta_work);
		}
	}

	if (znd->z_gc_free < 5) {
		Z_ERR(znd, "... issue gc low on free space");
		MutexLock(&znd->mz_io_mutex);
		gc_immediate(znd, CRIT);
		mutex_unlock(&znd->mz_io_mutex);
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline int _do_mem_purge(struct zoned *znd)
{
	int do_work = 0;

	if (znd->incore_count > 3) {
		set_bit(DO_MEMPOOL, &znd->flags);
		if (!work_pending(&znd->meta_work))
			do_work = 1;
	}
	return do_work;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void on_timeout_activity(struct zoned *znd, int mempurge, int delay)
{
	if (test_bit(ZF_FREEZE, &znd->flags))
		return;

	gc_queue_with_delay(znd, delay, CRIT);

	if (mempurge && _do_mem_purge(znd))
		queue_work(znd->meta_wq, &znd->meta_work);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void bg_work_task(struct work_struct *work)
{
	struct zoned *znd;

	if (!work)
		return;

	znd = container_of(work, struct zoned, bg_work);
	on_timeout_activity(znd, 1, 1);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void activity_timeout(unsigned long data)
{
	struct zoned *znd = (struct zoned *) data;

	if (!work_pending(&znd->bg_work))
		queue_work(znd->bg_wq, &znd->bg_work);

	if (!test_bit(ZF_FREEZE, &znd->flags))
		mod_timer(&znd->timer, jiffies + msecs_to_jiffies(2500));
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static sector_t get_dev_size(struct dm_target *ti)
{
	struct zoned *znd = ti->private;
	u64 sz = i_size_read(get_bdev_bd_inode(znd));	/* size in bytes. */
	u64 lut_resv;

	lut_resv = (znd->gz_count * znd->mz_provision);

	Z_DBG(znd, "%s size: %llu (/8) -> %llu blks -> zones -> %llu mz: %llu",
		 __func__, sz, sz / 4096, (sz / 4096) / 65536,
		 ((sz / 4096) / 65536) / 1024);

	sz -= (lut_resv * Z_SMR_SZ_BYTES);

	Z_DBG(znd, "%s backing device size: %llu (4k blocks)", __func__, sz);

	/*
	 * NOTE: `sz` should match `ti->len` when the dm_table
	 *       is setup correctly
	 */

	return to_sector(sz);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_iterate_devices(struct dm_target *ti,
				 iterate_devices_callout_fn fn, void *data)
{
	struct zoned *znd = ti->private;
	int rc = fn(ti, znd->dev, 0, get_dev_size(ti), data);

	return rc;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void zoned_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	u64 io_opt_sectors = limits->io_opt >> SECTOR_SHIFT;

	/*
	 * If the system-determined stacked limits are compatible with the
	 * zoned device's blocksize (io_opt is a factor) do not override them.
	 */
	if (io_opt_sectors < 8 || do_div(io_opt_sectors, 8)) {
		blk_limits_io_min(limits, 0);
		blk_limits_io_opt(limits, 8 << SECTOR_SHIFT);
	}

	/* NOTE: set discard stuff here */
	limits->raid_discard_safe = 1;

}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void zoned_status(struct dm_target *ti, status_type_t type,
			 unsigned status_flags, char *result, unsigned maxlen)
{
	struct zoned *znd = (struct zoned *) ti->private;

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
	struct zoned *znd = seqf->private;

	if (*pos == 0)
		znd->wp_proc_at = *pos;
	return &znd->wp_proc_at;
}

/**
 * Increment to our next 'grand zone' 4k page.
 */
static void *proc_wp_next(struct seq_file *seqf, void *v, loff_t *pos)
{
	struct zoned *znd = seqf->private;
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
	struct zoned *znd = seqf->private;
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
		struct zoned *znd = PDE_DATA(inode);
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
	struct zoned *znd = seqf->private;

	if (*pos == 0)
		znd->wp_proc_at = *pos;
	return &znd->wp_proc_at;
}

/**
 * Increment to our next zone
 */
static void *proc_used_next(struct seq_file *seqf, void *v, loff_t *pos)
{
	struct zoned *znd = seqf->private;
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
	struct zoned *znd = seqf->private;
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
		struct zoned *znd = PDE_DATA(inode);
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
	struct zoned *znd = seqf->private;
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
	status.mc_entries = znd->mc_entries;
	status.b_discard = znd->discard_count;

	/*  fixed array of ->fwd_tm and ->rev_tm */
	status.m_zones = znd->data_zones;

	status.memstat = znd->memstat;
	memcpy(status.bins, znd->bins, sizeof(status.bins));
	status.mlut_blocks = znd->incore_count;

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
	struct zoned *znd = seqf->private;

	seq_printf(seqf, "Data Zones:   %u\n", znd->data_zones);
	seq_printf(seqf, "Empty Zones:  %u\n", znd->z_gc_free);
	seq_printf(seqf, "Cached Pages: %u\n", znd->mc_entries);
	seq_printf(seqf, "ZTL Pages:    %u\n", znd->incore_count);
	seq_printf(seqf, "RAM in Use:   %lu\n", znd->memstat);

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
static int zdm_create_proc_entries(struct zoned *znd)
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
static void zdm_remove_proc_entries(struct zoned *znd)
{
	remove_proc_subtree(znd->proc_name, NULL);
}

#else /* !CONFIG_PROC_FS */

static int zdm_create_proc_entries(struct zoned *znd)
{
	(void)znd;
	return 0;
}
static void zdm_remove_proc_entries(struct zoned *znd)
{
	(void)znd;
}
#endif /* CONFIG_PROC_FS */


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_ioctl_fwd(struct dm_dev *dev, unsigned int cmd,
			   unsigned long arg)
{
	int r = scsi_verify_blk_ioctl(NULL, cmd);

	if (r == 0)
		r = __blkdev_driver_ioctl(dev->bdev, dev->mode, cmd, arg);

	return r;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int do_ioc_wpstat(struct zoned *znd, unsigned long arg, int what)
{
	void __user *parg = (void __user *)arg;
	int error = -EFAULT;
	struct zdm_ioc_request *req;

	req = kzalloc(sizeof(*req), GFP_KERNEL);
	if (!req) {
		error = -ENOMEM;
		goto out;
	}

	if (copy_from_user(req, parg, sizeof(*req)))
		goto out;

	if (req->megazone_nr < znd->gz_count) {
		struct meta_pg *wpg = &znd->wp[req->megazone_nr];
		size_t rs = req->result_size < Z_C4K ? req->result_size : Z_C4K;
		void *send_what = what ? wpg->wp_alloc : wpg->zf_est;

		if (copy_to_user(parg, send_what, rs))
			goto out;

		error = 0;
	}
out:
	kfree(req);

	return error;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
static void fill_ioc_status(struct zoned *znd, struct zdm_ioc_status *status,
			    u32 mz_no)
{
	int entry;
	struct meta_pg *wpg = &znd->wp[mz_no];

	for (entry = (mz_no << GZ_BITS); entry < znd->data_zones; entry++) {
		u32 gzoff = entry & GZ_MMSK;
		u32 wp_at = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_VALUE_MASK;

		status->b_used += wp_at;
		status->b_available += Z_BLKSZ - wp_at;
	}
	if (mz_no)
		return;

	status->mc_entries = znd->mc_entries;
	status->b_discard = znd->discard_count;

	/*  fixed array of ->fwd_tm and ->rev_tm */
	status->m_zones = znd->data_zones;

	status->memstat = znd->memstat;
	memcpy(status->bins, znd->bins, sizeof(status->bins));
	status->mlut_blocks = znd->incore_count;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int do_ioc_status(struct zoned *znd, unsigned long arg)
{
	void __user *parg = (void __user *)arg;
	int error = -EFAULT;
	struct zdm_ioc_request *req;
	struct zdm_ioc_status *stats;

	req = kzalloc(sizeof(*req), GFP_KERNEL);
	stats = kzalloc(sizeof(*stats), GFP_KERNEL);

	if (!req || !stats) {
		error = -ENOMEM;
		goto out;
	}

	if (copy_from_user(req, parg, sizeof(*req)))
		goto out;

	if (req->megazone_nr < znd->gz_count) {
		if (req->result_size < sizeof(*stats)) {
			error = -EBADTYPE;
			goto out;
		}
		fill_ioc_status(znd, stats, req->megazone_nr);
		if (copy_to_user(parg, stats, sizeof(*stats)))
			goto out;

		error = 0;
	}

out:
	kfree(req);
	kfree(stats);
	return error;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_ioctl(struct dm_target *ti, unsigned int cmd,
		       unsigned long arg)
{
	int rcode = 0;
	struct zoned *znd = (struct zoned *) ti->private;

	switch (cmd) {
	case ZDM_IOC_MZCOUNT:
		rcode = znd->gz_count;
		break;
	case ZDM_IOC_WPS:
		do_ioc_wpstat(znd, arg, 1);
		break;
	case ZDM_IOC_FREE:
		do_ioc_wpstat(znd, arg, 0);
		break;
	case ZDM_IOC_STATUS:
		do_ioc_status(znd, arg);
		break;
#if USE_KTHREAD
	case BLKFLSBUF:
		Z_ERR(znd, "Ign BLKFLSBUF (ZDM: flush backing store) %u %lu\n",
		      cmd, arg);
		rcode = 0;
		break;
#endif
	default:
		rcode = zoned_ioctl_fwd(znd->dev, cmd, arg);
		break;
	}
	return rcode;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void start_worker(struct zoned *znd)
{
	clear_bit(ZF_FREEZE, &znd->flags);
	atomic_set(&znd->suspended, 0);
	mod_timer(&znd->timer, jiffies + msecs_to_jiffies(5000));
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void stop_worker(struct zoned *znd)
{
	set_bit(ZF_FREEZE, &znd->flags);
	atomic_set(&znd->suspended, 1);
	zoned_io_flush(znd);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void zoned_postsuspend(struct dm_target *ti)
{
	struct zoned *znd = ti->private;

	stop_worker(znd);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int zoned_preresume(struct dm_target *ti)
{
	struct zoned *znd = ti->private;

	start_worker(znd);
	return 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static struct target_type zoned_target = {
	.name = "zoned",
	.module = THIS_MODULE,
	.version = {1, 0, 0},
	.ctr = zoned_ctr,
	.dtr = zoned_dtr,
	.map = zoned_map,

	.postsuspend = zoned_postsuspend,
	.preresume = zoned_preresume,
	.status = zoned_status,
		/*  .message = zoned_message, */
	.ioctl = zoned_ioctl,

	.iterate_devices = zoned_iterate_devices,
	.io_hints = zoned_io_hints
};

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int __init dm_zoned_init(void)
{
	int rcode = dm_register_target(&zoned_target);

	if (rcode)
		DMERR("zoned target registration failed: %d", rcode);

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

MODULE_DESCRIPTION(DM_NAME " zoned target for Host Aware/Managed drives.");
MODULE_AUTHOR("Shaun Tancheff <shaun.tancheff@seagate.com>");
MODULE_LICENSE("GPL");
