/*
 * Copyright (C) 2001-2003 Sistina Software (UK) Limited.
 *
 * This file is released under the GPL.
 */

#include "dm.h"
#include <linux/module.h>
#include <linux/init.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/dax.h>
#include <linux/slab.h>
#include <linux/device-mapper.h>

#define DM_MSG_PREFIX "dedupe"

/*
 * dedupe: maps a dedupe range of a device.
 */
struct dedupe_c {
	struct dm_dev *dev;
	sector_t start;
	unsigned long *bitmap;
	atomic64_t dropped;
};

/*
 * Construct a dedupe mapping: <dev_path> <offset>
 */
static int dedupe_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct dedupe_c *lc;
	unsigned long long tmp;
	size_t bm_size;
	char dummy;
	int ret;

	if (argc != 2) {
		ti->error = "Invalid argument count";
		return -EINVAL;
	}

	lc = kmalloc(sizeof(*lc), GFP_KERNEL);
	if (lc == NULL) {
		ti->error = "Cannot allocate dedupe context";
		return -ENOMEM;
	}

	bm_size = BITS_TO_LONGS(ti->len >> 3) * sizeof(unsigned long);
	lc->bitmap = (unsigned long *)kvmalloc(bm_size, GFP_KERNEL);
	if (!lc->bitmap) {
		ret = -ENOMEM;
		goto bad;
	}
	memset(lc->bitmap, 0, bm_size);

	ret = -EINVAL;
	if (sscanf(argv[1], "%llu%c", &tmp, &dummy) != 1 || tmp != (sector_t)tmp) {
		ti->error = "Invalid device sector";
		goto bad;
	}
	lc->start = tmp;

	ret = dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &lc->dev);
	if (ret) {
		ti->error = "Device lookup failed";
		goto bad;
	}

	atomic64_set(&lc->dropped, 0);
	ti->num_flush_bios = 1;
	ti->num_discard_bios = 1;
	ti->num_secure_erase_bios = 1;
	ti->num_write_zeroes_bios = 1;
	ti->private = lc;
	return 0;

      bad:
	if (lc->bitmap)
		kvfree(lc->bitmap);
	kfree(lc);
	return ret;
}

static void dedupe_dtr(struct dm_target *ti)
{
	struct dedupe_c *lc = (struct dedupe_c *) ti->private;

	dm_put_device(ti, lc->dev);

	kvfree(lc->bitmap);
	kfree(lc);
}

static sector_t dedupe_map_sector(struct dm_target *ti, sector_t bi_sector)
{
	struct dedupe_c *lc = ti->private;

	return lc->start + dm_target_offset(ti, bi_sector);
}

static inline bool is_zero_page(struct page *page, size_t offset, size_t len)
{
	bool is_zero;
	int i;
	unsigned long *addr = kmap_local_page(page);

	for (is_zero = true, i = 0; is_zero && i < len / sizeof(*addr); i++)
		if (addr[i])
			is_zero = false;
	kunmap_local(addr);
	return is_zero;
}

static bool is_bvec_zero(struct bio_vec *bvec)
{
	if (bvec->bv_offset)
		return false;
	if (bvec->bv_len != PAGE_SIZE)
		return false;
	return is_zero_page(bvec->bv_page, bvec->bv_offset, bvec->bv_len);
}

static bool is_bio_zero(struct bio *bio)
{
	struct bio_vec bv;
	struct bvec_iter iter;

	bio_for_each_segment(bv, bio, iter)
		if (!is_bvec_zero(&bv))
			return false;
	return true;
}

static int dedupe_map(struct dm_target *ti, struct bio *bio)
{
	struct dedupe_c *lc = ti->private;
	sector_t start = bio->bi_iter.bi_sector >> 3;
	sector_t end = to_sector(bio->bi_iter.bi_size) >> 3;
	int n;
	int zeroed;

	switch (bio_op(bio)) {
	case REQ_OP_READ:
		zeroed = true;
		for (n = 0; n < end; n++)
			if (!test_bit(start + n, lc->bitmap))
				zeroed = false;
		if (zeroed) {
			zero_fill_bio(bio);
			bio_endio(bio);
			return DM_MAPIO_SUBMITTED;
		}
		break;
	case REQ_OP_WRITE:
		if (is_bio_zero(bio)) {
			for (n = 0; n < end; n++)
				set_bit(start + n, lc->bitmap);
			atomic64_add(end, &lc->dropped);
			bio_endio(bio);
			return DM_MAPIO_SUBMITTED;
		}
		for (n = 0; n < end; n++)
			clear_bit(start + n, lc->bitmap);
		break;
	default:
		break;
	}
	bio_set_dev(bio, lc->dev->bdev);
	bio->bi_iter.bi_sector = dedupe_map_sector(ti, bio->bi_iter.bi_sector);

	return DM_MAPIO_REMAPPED;
}

static void dedupe_status(struct dm_target *ti, status_type_t type,
			  unsigned status_flags, char *result, unsigned maxlen)
{
	struct dedupe_c *lc = (struct dedupe_c *) ti->private;
	size_t sz = 0;

	switch (type) {
	case STATUSTYPE_INFO:
		result[0] = '\0';
		break;

	case STATUSTYPE_TABLE:
		DMEMIT("%s %llu", lc->dev->name, (unsigned long long)lc->start);
		break;

	case STATUSTYPE_IMA:
		DMEMIT_TARGET_NAME_VERSION(ti->type);
		DMEMIT(",device_name=%s,start=%llu;", lc->dev->name,
		       (unsigned long long)lc->start);
		break;
	}
}

static int dedupe_prepare_ioctl(struct dm_target *ti, struct block_device **bdev)
{
	struct dedupe_c *lc = (struct dedupe_c *) ti->private;
	struct dm_dev *dev = lc->dev;

	*bdev = dev->bdev;

	/*
	 * Only pass ioctls through if the device sizes match exactly.
	 */
	if (lc->start || ti->len != bdev_nr_sectors(dev->bdev))
		return 1;
	return 0;
}

#ifdef CONFIG_BLK_DEV_ZONED
static int dedupe_report_zones(struct dm_target *ti,
		struct dm_report_zones_args *args, unsigned int nr_zones)
{
	struct dedupe_c *lc = ti->private;

	return dm_report_zones(lc->dev->bdev, lc->start,
			       dedupe_map_sector(ti, args->next_sector),
			       args, nr_zones);
}
#else
#define dedupe_report_zones NULL
#endif

static int dedupe_iterate_devices(struct dm_target *ti,
				  iterate_devices_callout_fn fn, void *data)
{
	struct dedupe_c *lc = ti->private;

	return fn(ti, lc->dev, lc->start, ti->len, data);
}

#if IS_ENABLED(CONFIG_FS_DAX)
static struct dax_device *dedupe_dax_pgoff(struct dm_target *ti, pgoff_t *pgoff)
{
	struct dedupe_c *lc = ti->private;
	sector_t sector = dedupe_map_sector(ti, *pgoff << PAGE_SECTORS_SHIFT);

	*pgoff = (get_start_sect(lc->dev->bdev) + sector) >> PAGE_SECTORS_SHIFT;
	return lc->dev->dax_dev;
}

static long dedupe_dax_direct_access(struct dm_target *ti, pgoff_t pgoff,
		long nr_pages, enum dax_access_mode mode, void **kaddr,
		pfn_t *pfn)
{
	struct dax_device *dax_dev = dedupe_dax_pgoff(ti, &pgoff);

	return dax_direct_access(dax_dev, pgoff, nr_pages, mode, kaddr, pfn);
}

static int dedupe_dax_zero_page_range(struct dm_target *ti, pgoff_t pgoff,
				      size_t nr_pages)
{
	struct dax_device *dax_dev = dedupe_dax_pgoff(ti, &pgoff);

	return dax_zero_page_range(dax_dev, pgoff, nr_pages);
}

static size_t dedupe_dax_recovery_write(struct dm_target *ti, pgoff_t pgoff,
		void *addr, size_t bytes, struct iov_iter *i)
{
	struct dax_device *dax_dev = dedupe_dax_pgoff(ti, &pgoff);

	return dax_recovery_write(dax_dev, pgoff, addr, bytes, i);
}

#else
#define dedupe_dax_direct_access NULL
#define dedupe_dax_zero_page_range NULL
#define dedupe_dax_recovery_write NULL
#endif

/**
 * dedupe_io_hints() - The place to tweek queue limits for DM targets
 * @ti: DM Target
 * @limits: queue_limits for this DM target
 */
static void dedupe_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	u64 io_opt_sectors = limits->io_opt >> SECTOR_SHIFT;

	/*
	 * If the system-determined stacked limits are compatible with the
	 * zdm device's blocksize (io_opt is a factor) do not override them.
	 */
	if (io_opt_sectors < 8 || do_div(io_opt_sectors, 8)) {
		blk_limits_io_min(limits, 0);
		blk_limits_io_opt(limits, 8 << SECTOR_SHIFT);
	}

	blk_limits_io_min(limits, 1 << 12);
	limits->logical_block_size =
		limits->physical_block_size = 1 << 12;
}

static struct target_type dedupe_target = {
	.module = THIS_MODULE,
	.name   = "dedupe",
	.version = {0, 1, 0},
	.features = DM_TARGET_PASSES_INTEGRITY | DM_TARGET_NOWAIT |
		    DM_TARGET_ZONED_HM | DM_TARGET_PASSES_CRYPTO,
	.report_zones		= dedupe_report_zones,
	.ctr			= dedupe_ctr,
	.dtr			= dedupe_dtr,
	.map			= dedupe_map,
	.status			= dedupe_status,
	.prepare_ioctl		= dedupe_prepare_ioctl,
	.iterate_devices	= dedupe_iterate_devices,
	.direct_access		= dedupe_dax_direct_access,
	.dax_zero_page_range	= dedupe_dax_zero_page_range,
	.dax_recovery_write	= dedupe_dax_recovery_write,
	.io_hints		= dedupe_io_hints
};

int __init dm_dedupe_init(void)
{
	int r = dm_register_target(&dedupe_target);

	if (r < 0)
		DMERR("register failed %d", r);

	return r;
}

void dm_dedupe_exit(void)
{
	dm_unregister_target(&dedupe_target);
}
