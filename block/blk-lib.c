/*
 * Functions related to generic helpers functions
 */
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/scatterlist.h>

#include "blk.h"

static struct bio *next_bio(struct bio *bio, unsigned int nr_pages,
		gfp_t gfp)
{
	struct bio *new = bio_alloc(gfp, nr_pages);

	if (bio) {
		bio_chain(bio, new);
		submit_bio(bio);
	}

	return new;
}

int __blkdev_issue_discard(struct block_device *bdev, sector_t sector,
		sector_t nr_sects, gfp_t gfp_mask, int flags,
		struct bio **biop)
{
	struct request_queue *q = bdev_get_queue(bdev);
	struct bio *bio = *biop;
	unsigned int granularity;
	enum req_op op;
	int alignment;

	if (!q)
		return -ENXIO;

	if (flags & BLKDEV_DISCARD_SECURE) {
		if (flags & BLKDEV_DISCARD_ZERO)
			return -EOPNOTSUPP;
		if (!blk_queue_secure_erase(q))
			return -EOPNOTSUPP;
		op = REQ_OP_SECURE_ERASE;
	} else {
		if (!blk_queue_discard(q))
			return -EOPNOTSUPP;
		if ((flags & BLKDEV_DISCARD_ZERO) &&
		    !q->limits.discard_zeroes_data)
			return -EOPNOTSUPP;
		op = REQ_OP_DISCARD;
	}

	/* Zero-sector (unknown) and one-sector granularities are the same.  */
	granularity = max(q->limits.discard_granularity >> 9, 1U);
	alignment = (bdev_discard_alignment(bdev) >> 9) % granularity;

	while (nr_sects) {
		unsigned int req_sects;
		sector_t end_sect, tmp;

		/* Make sure bi_size doesn't overflow */
		req_sects = min_t(sector_t, nr_sects, UINT_MAX >> 9);

		/**
		 * If splitting a request, and the next starting sector would be
		 * misaligned, stop the discard at the previous aligned sector.
		 */
		end_sect = sector + req_sects;
		tmp = end_sect;
		if (req_sects < nr_sects &&
		    sector_div(tmp, granularity) != alignment) {
			end_sect = end_sect - alignment;
			sector_div(end_sect, granularity);
			end_sect = end_sect * granularity + alignment;
			req_sects = end_sect - sector;
		}

		bio = next_bio(bio, 1, gfp_mask);
		bio->bi_iter.bi_sector = sector;
		bio->bi_bdev = bdev;
		bio_set_op_attrs(bio, op, 0);

		bio->bi_iter.bi_size = req_sects << 9;
		nr_sects -= req_sects;
		sector = end_sect;

		/*
		 * We can loop for a long time in here, if someone does
		 * full device discards (like mkfs). Be nice and allow
		 * us to schedule out to avoid softlocking if preempt
		 * is disabled.
		 */
		cond_resched();
	}

	*biop = bio;
	return 0;
}
EXPORT_SYMBOL(__blkdev_issue_discard);

/**
 * blkdev_issue_discard - queue a discard
 * @bdev:	blockdev to issue discard for
 * @sector:	start sector
 * @nr_sects:	number of sectors to discard
 * @gfp_mask:	memory allocation flags (for bio_alloc)
 * @flags:	BLKDEV_IFL_* flags to control behaviour
 *
 * Description:
 *    Issue a discard request for the sectors in question.
 */
int blkdev_issue_discard(struct block_device *bdev, sector_t sector,
		sector_t nr_sects, gfp_t gfp_mask, unsigned long flags)
{
	struct bio *bio = NULL;
	struct blk_plug plug;
	int ret;

	blk_start_plug(&plug);
	ret = __blkdev_issue_discard(bdev, sector, nr_sects, gfp_mask, flags,
			&bio);
	if (!ret && bio) {
		ret = submit_bio_wait(bio);
		if (ret == -EOPNOTSUPP && !(flags & BLKDEV_DISCARD_ZERO))
			ret = 0;
		bio_put(bio);
	}
	blk_finish_plug(&plug);

	return ret;
}
EXPORT_SYMBOL(blkdev_issue_discard);

/**
 * blkdev_issue_write_same - queue a write same operation
 * @bdev:	target blockdev
 * @sector:	start sector
 * @nr_sects:	number of sectors to write
 * @gfp_mask:	memory allocation flags (for bio_alloc)
 * @page:	page containing data to write
 *
 * Description:
 *    Issue a write same request for the sectors in question.
 */
int blkdev_issue_write_same(struct block_device *bdev, sector_t sector,
			    sector_t nr_sects, gfp_t gfp_mask,
			    struct page *page)
{
	struct request_queue *q = bdev_get_queue(bdev);
	unsigned int max_write_same_sectors;
	struct bio *bio = NULL;
	int ret = 0;

	if (!q)
		return -ENXIO;

	/* Ensure that max_write_same_sectors doesn't overflow bi_size */
	max_write_same_sectors = UINT_MAX >> 9;

	while (nr_sects) {
		bio = next_bio(bio, 1, gfp_mask);
		bio->bi_iter.bi_sector = sector;
		bio->bi_bdev = bdev;
		bio->bi_vcnt = 1;
		bio->bi_io_vec->bv_page = page;
		bio->bi_io_vec->bv_offset = 0;
		bio->bi_io_vec->bv_len = bdev_logical_block_size(bdev);
		bio_set_op_attrs(bio, REQ_OP_WRITE_SAME, 0);

		if (nr_sects > max_write_same_sectors) {
			bio->bi_iter.bi_size = max_write_same_sectors << 9;
			nr_sects -= max_write_same_sectors;
			sector += max_write_same_sectors;
		} else {
			bio->bi_iter.bi_size = nr_sects << 9;
			nr_sects = 0;
		}
	}

	if (bio) {
		ret = submit_bio_wait(bio);
		bio_put(bio);
	}
	return ret;
}
EXPORT_SYMBOL(blkdev_issue_write_same);

/**
 * blkdev_issue_zeroout - generate number of zero filed write bios
 * @bdev:	blockdev to issue
 * @sector:	start sector
 * @nr_sects:	number of sectors to write
 * @gfp_mask:	memory allocation flags (for bio_alloc)
 *
 * Description:
 *  Generate and issue number of bios with zerofiled pages.
 */

static int __blkdev_issue_zeroout(struct block_device *bdev, sector_t sector,
				  sector_t nr_sects, gfp_t gfp_mask)
{
	int ret;
	struct bio *bio = NULL;
	unsigned int sz;

	while (nr_sects != 0) {
		bio = next_bio(bio, min(nr_sects, (sector_t)BIO_MAX_PAGES),
				gfp_mask);
		bio->bi_iter.bi_sector = sector;
		bio->bi_bdev   = bdev;
		bio_set_op_attrs(bio, REQ_OP_WRITE, 0);

		while (nr_sects != 0) {
			sz = min((sector_t) PAGE_SIZE >> 9 , nr_sects);
			ret = bio_add_page(bio, ZERO_PAGE(0), sz << 9, 0);
			nr_sects -= ret >> 9;
			sector += ret >> 9;
			if (ret < (sz << 9))
				break;
		}
	}

	if (bio) {
		ret = submit_bio_wait(bio);
		bio_put(bio);
		return ret;
	}
	return 0;
}

/**
 * blkdev_issue_zeroout - zero-fill a block range
 * @bdev:	blockdev to write
 * @sector:	start sector
 * @nr_sects:	number of sectors to write
 * @gfp_mask:	memory allocation flags (for bio_alloc)
 * @discard:	whether to discard the block range
 *
 * Description:
 *  Zero-fill a block range.  If the discard flag is set and the block
 *  device guarantees that subsequent READ operations to the block range
 *  in question will return zeroes, the blocks will be discarded. Should
 *  the discard request fail, if the discard flag is not set, or if
 *  discard_zeroes_data is not supported, this function will resort to
 *  zeroing the blocks manually, thus provisioning (allocating,
 *  anchoring) them. If the block device supports the WRITE SAME command
 *  blkdev_issue_zeroout() will use it to optimize the process of
 *  clearing the block range. Otherwise the zeroing will be performed
 *  using regular WRITE calls.
 */

int blkdev_issue_zeroout(struct block_device *bdev, sector_t sector,
			 sector_t nr_sects, gfp_t gfp_mask, bool discard)
{
	if (discard) {
		if (!blkdev_issue_discard(bdev, sector, nr_sects, gfp_mask,
				BLKDEV_DISCARD_ZERO))
			return 0;
	}

	if (bdev_write_same(bdev) &&
	    blkdev_issue_write_same(bdev, sector, nr_sects, gfp_mask,
				    ZERO_PAGE(0)) == 0)
		return 0;

	return __blkdev_issue_zeroout(bdev, sector, nr_sects, gfp_mask);
}
EXPORT_SYMBOL(blkdev_issue_zeroout);

/**
 * fixup_zone_report() - Adjust origin and/or sector size of report
 * @bdev:	target blockdev
 * @rpt:	block zone report
 * @offmax:	maximum number of zones in report.
 *
 * Description:
 *  Rebase the zone report to the partition and/or change the sector
 *  size to block layer (order 9) sectors from device logical size.
 */
static void fixup_zone_report(struct block_device *bdev,
			      struct bdev_zone_report *rpt, u32 offmax)
{
	u64 offset = get_start_sect(bdev);
	int lborder = ilog2(bdev_logical_block_size(bdev)) - 9;

	if (offset || lborder) {
		struct bdev_zone_descriptor *bzde;
		s64 tmp;
		u32 iter;

		for (iter = 0; iter < offmax; iter++) {
			bzde = &rpt->descriptors[iter];

#if 0 /* for external */
			if (be64_to_cpu(bzde->length) == 0)
				break;

			tmp = be64_to_cpu(bzde->lba_start) << lborder;
			tmp -= offset;
			bzde->lba_start = cpu_to_be64(tmp);

			tmp = be64_to_cpu(bzde->lba_wptr) << lborder;
			tmp -= offset;
			bzde->lba_wptr  = cpu_to_be64(tmp);
#else  /* ata passthrough forced on hack */
			if (le64_to_cpu(bzde->length) == 0)
				break;

			tmp = le64_to_cpu(bzde->lba_start) << lborder;
			tmp -= offset;
			bzde->lba_start = cpu_to_le64(tmp);

			tmp = le64_to_cpu(bzde->lba_wptr) << lborder;
			tmp -= offset;
			bzde->lba_wptr  = cpu_to_le64(tmp);
#endif
		}
	}
}


/**
 * blkdev_issue_zone_report - queue a report zones operation
 * @bdev:	target blockdev
 * @op_flags:	extra bio rw flags. If unsure, use 0.
 * @sector:	starting sector (report will include this sector).
 * @opt:	See: zone_report_option, default is 0 (all zones).
 * @page:	one or more contiguous pages.
 * @pgsz:	up to size of page in bytes, size of report.
 * @gfp_mask:	memory allocation flags (for bio_alloc)
 *
 * Description:
 *    Issue a zone report request for the sectors in question.
 */
int blkdev_issue_zone_report(struct block_device *bdev, unsigned int op_flags,
			     sector_t sector, u8 opt, struct page *page,
			     size_t pgsz, gfp_t gfp_mask)
{
	struct bdev_zone_report *conv = page_address(page);
	struct bio *bio;
	unsigned int nr_iovecs = 1;
	int ret = 0;

	if (pgsz < (sizeof(struct bdev_zone_report) +
		    sizeof(struct bdev_zone_descriptor)))
		return -EINVAL;

	bio = bio_alloc(gfp_mask, nr_iovecs);
	if (!bio)
		return -ENOMEM;

	conv->descriptor_count = 0;
	bio->bi_iter.bi_sector = sector;
	bio->bi_bdev = bdev;
	bio->bi_vcnt = 0;
	bio->bi_iter.bi_size = 0;

	bio_add_page(bio, page, pgsz, 0);
	bio_set_op_attrs(bio, REQ_OP_ZONE_REPORT, op_flags);
	ret = submit_bio_wait(bio);

	/*
	 * When our request it nak'd the underlying device maybe conventional
	 * so ... report a single conventional zone the size of the device.
	 */
	if (ret == -EIO && conv->descriptor_count) {
		/* Adjust the conventional to the size of the partition ... */
		__be64 blksz = cpu_to_be64(bdev->bd_part->nr_sects);

		conv->maximum_lba = blksz;
		conv->descriptors[0].type = BLK_ZONE_TYPE_CONVENTIONAL;
		conv->descriptors[0].flags = BLK_ZONE_NO_WP << 4;
		conv->descriptors[0].length = blksz;
		conv->descriptors[0].lba_start = 0;
		conv->descriptors[0].lba_wptr = blksz;
		ret = 0;
	} else if (bio->bi_error == 0) {
		void *ptr = kmap_atomic(page);

		fixup_zone_report(bdev, ptr, max_report_entries(pgsz));
		kunmap_atomic(ptr);
	}
	bio_put(bio);
	return ret;
}
EXPORT_SYMBOL(blkdev_issue_zone_report);

/**
 * blkdev_issue_zone_action - queue a report zones operation
 * @bdev:	target blockdev
 * @op:		One of REQ_OP_ZONE_* op codes.
 * @op_flags:	extra bio rw flags. If unsure, use 0.
 * @sector:	starting lba of sector, Use ~0ul for all zones.
 * @gfp_mask:	memory allocation flags (for bio_alloc)
 *
 * Description:
 *    Issue a zone report request for the sectors in question.
 */
int blkdev_issue_zone_action(struct block_device *bdev, unsigned int op,
			     unsigned int op_flags, sector_t sector,
			     gfp_t gfp_mask)
{
	int ret;
	struct bio *bio;

	bio = bio_alloc(gfp_mask, 1);
	if (!bio)
		return -ENOMEM;

	bio->bi_iter.bi_sector = sector;
	bio->bi_bdev = bdev;
	bio->bi_vcnt = 0;
	bio->bi_iter.bi_size = 0;
	bio_set_op_attrs(bio, op, op_flags);
	ret = submit_bio_wait(bio);
	bio_put(bio);
	return ret;
}
EXPORT_SYMBOL(blkdev_issue_zone_action);
