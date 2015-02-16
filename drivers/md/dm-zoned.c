/*
 * Copyright (C) 2015 Hannes Reinecke
 * Copyright (C) 2015 SUSE Linux GmbH
 *
 * This file is released under the GPL.
 */

#include "dm.h"
#include <linux/module.h>
#include <linux/init.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/slab.h>
#include <linux/device-mapper.h>
#include <linux/dm-io.h>
#include <linux/dm-kcopyd.h>
#include <linux/bitops.h>
#include <linux/blktrace_api.h>
#include <linux/vmalloc.h>

#define DM_MSG_PREFIX "zoned"

#define SECTOR_SHIFT		9
#define PAGE_SECTORS_SHIFT	(PAGE_SHIFT - SECTOR_SHIFT)
#define PAGE_SECTORS		(1 << PAGE_SECTORS_SHIFT)
#define DM_ZONED_POOL_SIZE 2
#define DM_ZONED_CACHE_TIMEOUT 2
#define DM_ZONED_CACHE_HIGHWAT 10
#define DM_ZONED_DEFAULT_IOS 256

enum zoned_c_flags {
	ZONED_CACHE_ATTACHED,	/* Cache attached */
	ZONED_CACHE_FAILED,	/* Flush failed, cache remains dirty */
	ZONED_CACHE_BUSY,	/* Flush in progress */
	ZONED_CACHE_UPDATE,	/* Cache fill in progress */
	ZONED_CACHE_DIRTY,	/* Cache dirty */
	ZONED_CACHE_DISCARD,	/* Context scheduled for eviction */
	ZONED_CACHE_ACTIVE,	/* I/O active on the cache */
	ZONED_CACHE_EVICT,	/* Cache selected for eviction */
};

/*
 * Zoned: caches a zone on top of a linear device.
 */
struct zoned_context {
	struct dm_dev *dev;
	struct dm_io_client *dm_io;
	struct workqueue_struct *wq;
	mempool_t *blk_zone_pool;
	mempool_t *cache_pool;
	size_t cache_size;
	int cache_min;
	int cache_lowat;
	int cache_hiwat;
	int cache_timeout;
	atomic_t num_cache;
	struct list_head cache_list;
	spinlock_t list_lock;
	struct completion evict_complete;
	/* Statistics */
	unsigned long cache_hits;
	unsigned long cache_flushes;
	unsigned long cache_loads;
	unsigned long cache_max;
	unsigned long cache_aligned_writes;
	unsigned long disk_aligned_writes;
	unsigned long disk_misaligned_writes;
};

struct blk_zone_context {
	struct kref ref;
	struct list_head node;
	struct zoned_context *zc;
	struct blk_zone *zone;
	unsigned long flags;
	struct mutex cache_mutex;
	void *cache;
	size_t cache_size;
	struct blk_zone cache_zone;
	unsigned long tstamp;
	unsigned long wq_tstamp;
	atomic_t epoch;
	atomic_t bio_done;
	struct bio_list defer;
	struct work_struct read_work;
	struct delayed_work flush_work;
	struct completion read_complete;
	struct completion *zeroout_complete;
};

struct dm_zoned_info {
	struct blk_zone_context *ctx;
};

struct zeroout_context {
	struct blk_zone_context *blk_ctx;
	struct bio *bio;
	struct work_struct zeroout_work;
	struct completion *zeroout_complete;
};

static struct kmem_cache *_zoned_cache;
static void read_zone_work(struct work_struct *work);
static void flush_zone_work(struct work_struct *work);

void *zoned_mempool_alloc_cache(gfp_t gfp_mask, void *pool_data)
{
	struct zoned_context *zc = pool_data;

	return vmalloc(zc->cache_size);
}

void zoned_mempool_free_cache(void *element, void *pool_data)
{
	vfree(element);
}

static int zoned_mempool_create(struct zoned_context *zc)
{
	mempool_t *pool;

	pool = mempool_create(zc->cache_min, zoned_mempool_alloc_cache,
			      zoned_mempool_free_cache, zc);
	if (IS_ERR(pool))
		return PTR_ERR(pool);

	zc->cache_pool = pool;

	pool = mempool_create_slab_pool(DM_ZONED_DEFAULT_IOS, _zoned_cache);
	if (IS_ERR(pool)) {
		mempool_destroy(zc->cache_pool);
		return PTR_ERR(pool);
	}
	zc->blk_zone_pool = pool;

	return 0;
}

static void zoned_init_context(struct blk_zone_context *blk_ctx)
{
	memset(blk_ctx, 0, sizeof(struct blk_zone_context));
	blk_ctx->cache_zone.start = -1;
	blk_ctx->cache_zone.wp = -1;
	kref_init(&blk_ctx->ref);
	mutex_init(&blk_ctx->cache_mutex);
	INIT_LIST_HEAD(&blk_ctx->node);
	INIT_WORK(&blk_ctx->read_work, read_zone_work);
	INIT_DELAYED_WORK(&blk_ctx->flush_work, flush_zone_work);
	init_completion(&blk_ctx->read_complete);
	atomic_set(&blk_ctx->epoch, 0);
	bio_list_init(&blk_ctx->defer);
	blk_ctx->tstamp = jiffies;
}

static void zoned_destroy_context(struct kref *ref)
{
	struct blk_zone_context *blk_ctx =
		container_of(ref, struct blk_zone_context, ref);
	struct zoned_context *zc = blk_ctx->zc;
	struct blk_zone *zone = blk_ctx->zone;
	unsigned long flags;

#if 0
	DMINFO("destroy context for %zu %lu",
	       blk_ctx->cache_zone.start, blk_ctx->epoch);
#endif
	WARN_ON(delayed_work_pending(&blk_ctx->flush_work));
	WARN_ON(test_bit(ZONED_CACHE_ATTACHED, &blk_ctx->flags));
	WARN_ON(test_bit(ZONED_CACHE_BUSY, &blk_ctx->flags));
	WARN_ON(test_bit(ZONED_CACHE_ACTIVE, &blk_ctx->flags));
	if (zone) {
		spin_lock_irqsave(&zone->lock, flags);
		WARN_ON(zone->wp < blk_ctx->cache_zone.wp);
		zone->private_data = NULL;
		blk_ctx->zone = NULL;
		spin_unlock_irqrestore(&zone->lock, flags);
	}
	spin_lock_irq(&zc->list_lock);
	list_del_init(&blk_ctx->node);
	spin_unlock_irq(&zc->list_lock);
	blk_ctx->cache_zone.start = -1;
	blk_ctx->cache_zone.wp = -1;
	WARN_ON(blk_ctx->cache);
	mempool_free(blk_ctx, zc->blk_zone_pool);
}

static bool zoned_get_context(struct blk_zone_context *blk_ctx)
{
	if (kref_get_unless_zero(&blk_ctx->ref) == 0)
		return false;
	return true;
}

static void zoned_put_context(struct blk_zone_context *blk_ctx)
{
	kref_put(&blk_ctx->ref, zoned_destroy_context);
}

static void zoned_lock_context(struct blk_zone_context *blk_ctx)
{
	mutex_lock(&blk_ctx->cache_mutex);
}

static void zoned_unlock_context(struct blk_zone_context *blk_ctx)
{
	mutex_unlock(&blk_ctx->cache_mutex);
}

static void zoned_attach_cache(struct blk_zone_context *blk_ctx)
{
	struct zoned_context *zc = blk_ctx->zc;
	void *cache;

	if (test_and_set_bit(ZONED_CACHE_ATTACHED, &blk_ctx->flags)) {
		DMWARN("zone %zu cache already attached",
		       blk_ctx->cache_zone.start);
		return;
	}
	zoned_unlock_context(blk_ctx);
	cache = mempool_alloc(blk_ctx->zc->cache_pool, GFP_KERNEL);
	if (!cache) {
		smp_mb__before_atomic();
		clear_bit(ZONED_CACHE_ATTACHED, &blk_ctx->flags);
		smp_mb__after_atomic();
		zoned_lock_context(blk_ctx);
		return;
	}
	zoned_lock_context(blk_ctx);
	/* Someone else might have attached a cache in the meantime */
	if (blk_ctx->cache) {
		DMWARN("Duplicate cache for %zu",
		       blk_ctx->cache_zone.start);
		smp_mb__before_atomic();
		clear_bit(ZONED_CACHE_ATTACHED, &blk_ctx->flags);
		smp_mb__after_atomic();
		zoned_unlock_context(blk_ctx);
		mempool_free(cache, blk_ctx->zc->cache_pool);
		zoned_lock_context(blk_ctx);
		return;
	}
	blk_ctx->cache = cache;
	zoned_get_context(blk_ctx);
	memset(blk_ctx->cache, 0x0, zc->cache_size);
	atomic_inc(&zc->num_cache);
	DMINFO("Allocate cache for %zu, total %d",
	       blk_ctx->cache_zone.start, atomic_read(&zc->num_cache));
	if (atomic_read(&zc->num_cache) > zc->cache_max)
		zc->cache_max = atomic_read(&zc->num_cache);

}

/*
 * mutex must be held
 */
static void zoned_discard_cache(struct blk_zone_context *blk_ctx,
				const char *prefix)
{
	struct zoned_context *zc = blk_ctx->zc;
	void *cache;

	DMINFO("%s discard cache for %zu flags %lx total %d", prefix,
	       blk_ctx->cache_zone.start, blk_ctx->flags,
	       atomic_read(&zc->num_cache));

	WARN_ON(test_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags));
	cache = blk_ctx->cache;
	blk_ctx->cache = NULL;
	mempool_free(cache, zc->cache_pool);
	smp_mb__before_atomic();
	clear_bit(ZONED_CACHE_ATTACHED, &blk_ctx->flags);
	smp_mb__after_atomic();
	zoned_put_context(blk_ctx);
	atomic_dec(&zc->num_cache);
}

static void flush_zone_work(struct work_struct *work)
{
	struct blk_zone_context *blk_ctx =
		container_of(work, struct blk_zone_context, flush_work.work);
	struct blk_zone *cache_zone = &blk_ctx->cache_zone;
	struct zoned_context *zc = blk_ctx->zc;
	struct block_device *bdev = zc->dev->bdev;
	struct dm_io_region to;
	int r;
	unsigned long error;
	size_t cached_size;
	sector_t cached_start = cache_zone->start;
	struct dm_io_request io_req;

	if (time_before(blk_ctx->wq_tstamp, jiffies)) {
		unsigned long tdiff = jiffies - blk_ctx->wq_tstamp;
		DMINFO("flush zone %zu delay %lu msecs",
		       cached_start, tdiff);
	}
	if (test_bit(ZONED_CACHE_ACTIVE, &blk_ctx->flags)) {
		DMINFO("flush skip active zone %zu",
		       cached_start);
		goto done_put;
	}

	if (!test_bit(ZONED_CACHE_ATTACHED, &blk_ctx->flags)) {
#if 0
		DMINFO("Skip flushing zone %zu, no cache context",
		       cached_start);
#endif
		goto done_put;
	}
	if (!test_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags)) {
		DMINFO("flush discard clean zone %zu",
		       cached_start);
		if (test_and_set_bit(ZONED_CACHE_BUSY, &blk_ctx->flags)) {
			DMWARN("flush zone %zu already busy", cached_start);
			goto done_put;
		}
		zoned_lock_context(blk_ctx);
		if (test_and_set_bit(ZONED_CACHE_DISCARD, &blk_ctx->flags)) {
			zoned_unlock_context(blk_ctx);
			goto done_put;
		}
		if (blk_ctx->cache) {
			if (test_bit(ZONED_CACHE_ACTIVE, &blk_ctx->flags) ||
			    test_bit(ZONED_CACHE_EVICT, &blk_ctx->flags))
				DMINFO("flush zone %zu flush flags %lx skip discard",
				       blk_ctx->cache_zone.start, blk_ctx->flags);
			else
				zoned_discard_cache(blk_ctx, "flush");
		}
		zoned_unlock_context(blk_ctx);
		smp_mb__before_atomic();
		clear_bit(ZONED_CACHE_DISCARD, &blk_ctx->flags);
		smp_mb__after_atomic();

		goto done_put;
	}

	zoned_lock_context(blk_ctx);
	cached_size = cache_zone->wp - cache_zone->start;
	if (test_and_set_bit(ZONED_CACHE_BUSY, &blk_ctx->flags))
		DMWARN("flush zone %zu already busy", cached_start);

	zoned_unlock_context(blk_ctx);

	DMINFO("flush zone %zu reset write pointer %zu",
	       cached_start, cached_size);
	r = blkdev_issue_discard(bdev, cached_start,
				 cache_zone->len, GFP_NOFS, 0);
	if (r < 0) {
		DMERR_LIMIT("flush zone %zu reset write pointer error %d",
			    cached_start, r);
		smp_mb__before_atomic();
		set_bit(ZONED_CACHE_FAILED, &blk_ctx->flags);
		clear_bit(ZONED_CACHE_BUSY, &blk_ctx->flags);
		smp_mb__after_atomic();
		set_device_ro(bdev, 1);
		goto done_put;
	}

	DMINFO("flush zone %zu commit %zu %d",
	       cached_start, cached_size, atomic_read(&blk_ctx->epoch));

	to.bdev = bdev;
	to.sector = cached_start;
	to.count = cached_size;

	io_req.bi_rw = WRITE | REQ_SYNC;
	io_req.client = zc->dm_io;
	io_req.notify.fn = NULL;
	io_req.notify.context = NULL;
	io_req.mem.type = DM_IO_VMA;
	io_req.mem.ptr.vma = blk_ctx->cache;

	r = dm_io(&io_req, 1, &to, &error);
	if (r) {
		DMERR_LIMIT("Failed to flush zone %zu, error %lu",
			    cached_start, error);
		smp_mb__before_atomic();
		set_bit(ZONED_CACHE_FAILED, &blk_ctx->flags);
		clear_bit(ZONED_CACHE_BUSY, &blk_ctx->flags);
		smp_mb__after_atomic();
		set_device_ro(bdev, 1);
		goto done_put;
	}
	smp_mb__before_atomic();
	clear_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags);
	smp_mb__after_atomic();
	zc->cache_flushes++;

done_put:
	smp_mb__before_atomic();
	clear_bit(ZONED_CACHE_BUSY, &blk_ctx->flags);
	smp_mb__after_atomic();

	if (test_bit(ZONED_CACHE_ATTACHED, &blk_ctx->flags) &&
	    !test_bit(ZONED_CACHE_DISCARD, &blk_ctx->flags) &&
	    !test_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags)) {
		if (mod_delayed_work(zc->wq, &blk_ctx->flush_work,
				     zc->cache_timeout * HZ))
			zoned_put_context(blk_ctx);
		else
			blk_ctx->wq_tstamp = jiffies;
	} else
		zoned_put_context(blk_ctx);
}

static void read_zone_work(struct work_struct *work)
{
	struct blk_zone_context *blk_ctx =
		container_of(work, struct blk_zone_context, read_work);
	struct zoned_context *zc = blk_ctx->zc;
	struct dm_io_region from;
	struct dm_io_request io_req;
	sector_t start_lba = blk_ctx->cache_zone.start;
	size_t num_blocks;
	unsigned long error;
	int r;

	if (test_bit(ZONED_CACHE_BUSY, &blk_ctx->flags)) {
		DMINFO("zone %zu wait for flush to finish", start_lba);
		queue_work(zc->wq, &blk_ctx->read_work);
		return;
	}
	zoned_lock_context(blk_ctx);
	blk_ctx->tstamp = jiffies;

	start_lba = blk_ctx->cache_zone.start;
	num_blocks = blk_ctx->cache_zone.wp - start_lba;

	DMINFO("Loading zone %zu %zu", start_lba, num_blocks);

	from.bdev = zc->dev->bdev;
	from.sector = start_lba;
	from.count = num_blocks;

	io_req.bi_rw = READ | REQ_SYNC;
	io_req.client = zc->dm_io;
	io_req.notify.fn = NULL;
	io_req.notify.context = NULL;
	io_req.mem.type = DM_IO_VMA;
	io_req.mem.ptr.vma = blk_ctx->cache;

	r = dm_io(&io_req, 1, &from, &error);
	if (r) {
		DMINFO("failed to load zone %zu %zu, err %lu",
		       start_lba, num_blocks, error);
		zoned_discard_cache(blk_ctx, "read");
		smp_mb__before_atomic();
		clear_bit(ZONED_CACHE_UPDATE, &blk_ctx->flags);
		smp_mb__after_atomic();
	} else {
		smp_mb__before_atomic();
		clear_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags);
		clear_bit(ZONED_CACHE_UPDATE, &blk_ctx->flags);
		smp_mb__after_atomic();
		zc->cache_loads++;
	}
	blk_ctx->tstamp = jiffies;
	zoned_unlock_context(blk_ctx);
	complete_all(&blk_ctx->read_complete);
}

/*
 * mutex must be held
 */
static int read_zone_cache(struct blk_zone_context *blk_ctx,
			   struct request_queue *q, sector_t sector)
{
	struct blk_zone *zone = blk_ctx->zone;
	int r = 0;

	if (test_and_set_bit(ZONED_CACHE_UPDATE, &blk_ctx->flags)) {
		DMINFO("zone %zu update in progress", zone->start);
		zoned_unlock_context(blk_ctx);
		wait_for_completion(&blk_ctx->read_complete);
		zoned_lock_context(blk_ctx);
		return -EAGAIN;
	}
	reinit_completion(&blk_ctx->read_complete);
	blk_add_trace_msg(q, "zoned load %zu %zu",
			  zone->start, zone->wp - zone->start);

	if (blk_zone_is_empty(zone)) {
		DMINFO("zone %zu empty", zone->start);
		smp_mb__before_atomic();
		clear_bit(ZONED_CACHE_UPDATE, &blk_ctx->flags);
		smp_mb__after_atomic();
		blk_ctx->tstamp = jiffies;
		complete_all(&blk_ctx->read_complete);
		return 0;
	}
	zoned_unlock_context(blk_ctx);
	queue_work(blk_ctx->zc->wq, &blk_ctx->read_work);
	wait_for_completion(&blk_ctx->read_complete);
	zoned_lock_context(blk_ctx);

	if (!blk_ctx->cache) {
		DMWARN("failed to load zone %zu", zone->start);
		r = -EIO;
	}
	blk_ctx->tstamp = jiffies;
	return r;
}

static void zeroout_zone_work(struct work_struct *work)
{
	struct zeroout_context *ctx =
		container_of(work, struct zeroout_context, zeroout_work);
	struct blk_zone_context *blk_ctx = ctx->blk_ctx;
	struct zoned_context *zc = blk_ctx->zc;
	struct block_device *bdev = zc->dev->bdev;
	sector_t sector = blk_ctx->cache_zone.wp;
	sector_t zeroout_sector = ctx->bio->bi_iter.bi_sector;
	sector_t nr_sects = zeroout_sector - sector;
	int ret = 0;

	if (time_before(blk_ctx->wq_tstamp, jiffies)) {
		unsigned long tdiff = jiffies - blk_ctx->wq_tstamp;
		DMINFO("zeroout zone %zu delay %lu msecs",
		       blk_ctx->cache_zone.start, tdiff);
	}

	if (nr_sects) {
		blk_ctx->cache_zone.wp += nr_sects;
		ret = blkdev_issue_zeroout(bdev, sector, nr_sects, GFP_NOFS, 0);
	}
	if (ret) {
		ctx->bio->bi_error = ret;
		bio_endio(ctx->bio);
	} else {
		blk_ctx->cache_zone.wp += bio_sectors(ctx->bio);
		generic_make_request(ctx->bio);
	}
	clear_bit_unlock(ZONED_CACHE_ACTIVE, &blk_ctx->flags);
	smp_mb__after_atomic();
	wake_up_bit(&blk_ctx->flags, ZONED_CACHE_ACTIVE);
}

static void zoned_handle_misaligned(struct blk_zone_context *blk_ctx,
				    struct bio *parent_bio, sector_t sector)
{
	struct zoned_context *zc = blk_ctx->zc;
	sector_t wp_sector = blk_ctx->cache_zone.wp;
	sector_t nr_sects = sector - wp_sector;
	struct zeroout_context ctx;

	DMINFO("zeroout zone %zu wp %zu sector %zu %zu %u",
	       blk_ctx->cache_zone.start, wp_sector, sector,
	       nr_sects, bio_sectors(parent_bio));

	parent_bio->bi_iter.bi_sector = sector;
	ctx.blk_ctx = blk_ctx;
	ctx.bio = parent_bio;
	INIT_WORK(&ctx.zeroout_work, zeroout_zone_work);
	blk_ctx->wq_tstamp = jiffies;

	queue_work(zc->wq, &ctx.zeroout_work);
	flush_work(&ctx.zeroout_work);

	DMINFO("zeroout zone %zu done wp %zu",
	       blk_ctx->cache_zone.start, blk_ctx->cache_zone.wp);
}

/*
 * LRU selection policy:
 * If below lowat, call 'evict_complete' and allocate a new cache
 * If beyond lowat, select a cache entry for eviction:
 * 1. Select oldest clean cache entry
 * 2. If not found and below hiwat, allocate a new cache
 * 3. If not found and beyond hiwat:
 *    a) select oldest BUSY cache entry and wait for flush to complete
 *    b) If not found select oldest DIRTY cache entry, schedule
 *       flushing and wait for flush to complete.
 *    c) If not found wait for 'evict_complete' and retry LRU selection
 */
void zoned_evict_cache(struct zoned_context *zc,
		       struct blk_zone_context *blk_ctx)
{
	struct blk_zone_context *found = NULL, *tmp, *oldest_ctx;
	unsigned long oldest;
	int retry = 0, retries = 0;
	bool restart;

	if (atomic_read(&zc->num_cache) <= zc->cache_lowat)
		/* 1. If below lowat, return */
		return;

retry_unlocked:
	spin_lock_irq(&zc->list_lock);
retry_locked:
	found = NULL;
	oldest = jiffies;
	oldest_ctx = NULL;
	restart = false;
	list_for_each_entry(tmp, &zc->cache_list, node) {
		if (!oldest_ctx)
			oldest_ctx = tmp;
		if (time_after(oldest_ctx->tstamp, tmp->tstamp))
			oldest_ctx = tmp;

		if (tmp == blk_ctx)
			continue;
		if (!test_bit(ZONED_CACHE_ATTACHED, &tmp->flags))
			continue;
		if (test_bit(ZONED_CACHE_ACTIVE, &tmp->flags) ||
		    test_bit(ZONED_CACHE_DISCARD, &tmp->flags) ||
		    test_bit(ZONED_CACHE_EVICT, &tmp->flags))
			continue;
		if (retry < 1 &&
		    test_bit(ZONED_CACHE_BUSY, &tmp->flags))
			continue;
		if (retry < 2 &&
		    test_bit(ZONED_CACHE_DIRTY, &tmp->flags))
			continue;

		if (time_after(tmp->tstamp, oldest))
			continue;
		oldest = tmp->tstamp;
		found = tmp;
	}
	retries++;
	if (retries > 5) {
		DMWARN("evict zone %zu flags %lx looping, found %p retries %d",
		       blk_ctx->cache_zone.start, blk_ctx->flags,
		       found, retries);
	}
	if (found) {
		if (!zoned_get_context(found))
			goto retry_locked;
		if (test_and_set_bit(ZONED_CACHE_EVICT, &found->flags)) {
			DMINFO("evict zone %zu already selected",
				found->cache_zone.start);
			zoned_put_context(found);
			goto retry_locked;
		}
	}
	spin_unlock_irq(&zc->list_lock);
	if (!found) {
		/* 2. If below hiwat, return */
		if (atomic_read(&zc->num_cache) <= zc->cache_hiwat)
			return;
		/* 3. No clean cache found */
		retry++;
		/* 5. Fallback yet to be implemented */
		WARN_ON(retry > 3);
		DMWARN("evict zone %zu retry round %d",
		       blk_ctx->cache_zone.start, retry);
		goto retry_unlocked;
	}
	if (atomic_read(&zc->num_cache) <= zc->cache_lowat)
		goto evict_done;

	if (retry && oldest_ctx != found) {
		unsigned long tdiff = jiffies - oldest_ctx->tstamp;
		DMWARN("evict oldest zone %zu %p flags %lx %u msecs",
		       oldest_ctx->cache_zone.start, oldest_ctx,
		       oldest_ctx->flags, jiffies_to_msecs(tdiff));
	}
	DMINFO("evict zone %zu epoch %d flags %lx total %d",
	       found->cache_zone.start, atomic_read(&found->epoch),
	       found->flags, atomic_read(&zc->num_cache));
	if (test_bit(ZONED_CACHE_ACTIVE, &found->flags)) {
		/* Other I/O is waiting on this cache */
		DMWARN("evict zone %zu delayed I/O pending",
		       found->cache_zone.start);
		restart = true;
		goto evict_done;
	}
	if (test_bit(ZONED_CACHE_BUSY, &found->flags)) {
		/*
		 * Wait for auto-flushing to complete.
		 */
		flush_delayed_work(&found->flush_work);
	} else if (test_bit(ZONED_CACHE_DIRTY, &found->flags)) {
		/*
		 * This should have been captured above, but status
		 * might have been changed in the meantime.
		 */
#if 0
		if (atomic_read(&zc->num_cache) <= zc->cache_hiwat) {
			DMINFO("zone %zu evict flush queue %d",
			       found->cache_zone.start,
			       atomic_read(&zc->num_cache));
			zoned_get_context(found);
			if (mod_delayed_work(zc->wq,
					     &found->flush_work, 1)) {
				zoned_put_context(found);
			} else {
				found->wq_tstamp = jiffies;
			}
			restart = true;
			goto evict_done;
		}
#endif
		DMINFO("evict zone %zu flush start %d flags %lx",
		       found->cache_zone.start, atomic_read(&found->epoch),
		       found->flags);
		zoned_get_context(found);
		if (mod_delayed_work(zc->wq, &found->flush_work, 0)) {
			DMINFO("evict zone %zu flush already started",
			       found->cache_zone.start);
			zoned_put_context(found);
		} else {
			found->wq_tstamp = jiffies;
		}
		flush_delayed_work(&found->flush_work);
	}
	WARN_ON(test_bit(ZONED_CACHE_BUSY, &found->flags));
	/* Synchronize with zoned_map() */
	zoned_lock_context(found);
	if (test_bit(ZONED_CACHE_DIRTY, &found->flags)) {
		DMWARN("evict zone %zu still dirty, retry",
		       found->cache_zone.start);
		restart = true;
	} else if (retry < 3 && test_bit(ZONED_CACHE_ACTIVE, &found->flags)) {
		/* Lost the race with zoned_map() */
		DMWARN("evict zone %zu race lost",
		       found->cache_zone.start);
		/* 4. Make sure we get a cache the next time */
		retry++;
		restart = true;
	} else if (found->cache) {
		zoned_discard_cache(found, "evict");
	}
	zoned_unlock_context(found);
evict_done:
	smp_mb__before_atomic();
	clear_bit(ZONED_CACHE_EVICT, &found->flags);
	smp_mb__after_atomic();
	wake_up_bit(&found->flags, ZONED_CACHE_EVICT);
	zoned_put_context(found);
	if (restart)
		goto retry_unlocked;
}

static void zoned_handle_bio(struct blk_zone_context *blk_ctx, sector_t sector,
			     struct bio *bio)
{
	int rw;
	struct request_queue *q = bdev_get_queue(bio->bi_bdev);
	struct bio_vec bvec;
	struct bvec_iter iter;
	struct blk_zone *cache_zone = &blk_ctx->cache_zone;
	sector_t bio_offset = sector - cache_zone->start;
	size_t sector_offset = bio_offset << SECTOR_SHIFT;
	size_t wp_lim = (cache_zone->wp - cache_zone->start) << SECTOR_SHIFT;
	size_t cache_lim = (cache_zone->len << SECTOR_SHIFT);
	size_t bio_len = 0;
	unsigned long flags;
	u8 *cache_ptr;

	rw = bio_rw(bio);
	if (rw == READA)
		rw = READ;
	else if (sector_offset == wp_lim)
		blk_ctx->zc->cache_aligned_writes++;

	bio_for_each_segment(bvec, bio, iter) {
		char *data = bvec_kmap_irq(&bvec, &flags);
		size_t len = bvec.bv_len;

		if (sector_offset > cache_lim) {
			DMINFO("bvec %c beyond boundary (len %zu off %zu lim %zu)",
			       rw == READ ? 'r' : 'w',
			       len, sector_offset, cache_lim);
			bvec_kunmap_irq(data, &flags);
			break;
		}
#if 0
		DMINFO("bio %c sector %zu offset %zu len %zu cache %zu wp %zu",
		       rw == READ ? 'r' : 'w', sector,
		       sector_offset >> SECTOR_SHIFT, len >> SECTOR_SHIFT,
		       cache_zone->start, cache_zone->wp);
#endif
		if (unlikely(len + sector_offset > cache_lim)) {
			DMWARN("bvec %c too large (len %zu off %zu lim %zu)",
			       rw == READ ? 'r' : 'w',
			       len, sector_offset, cache_lim);
			if (cache_lim > sector_offset)
				len = cache_lim - sector_offset;
			else
				len = 0;
		}
		cache_ptr = blk_ctx->cache + sector_offset;
		if (rw == READ) {
			if (unlikely(len + sector_offset > wp_lim)) {
				DMWARN("read beyond wp (len %zu off %zu wp %zu)",
				       len, sector_offset, wp_lim);
				if (wp_lim > sector_offset)
					len = wp_lim - sector_offset;
				else
					len = 0;
			}
			if (len > 0)
				memcpy(data, cache_ptr, len);
			flush_dcache_page(bvec.bv_page);
		} else {
			sector_t new_wp;

			flush_dcache_page(bvec.pv_page);
			if (len > 0) {
				memcpy(cache_ptr, data, len);
				smp_mb__before_atomic();
				set_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags);
				smp_mb__after_atomic();
			}

			new_wp = ((sector_offset + len) >> SECTOR_SHIFT);
			if (cache_zone->wp < cache_zone->start + new_wp) {
#if 0
				DMINFO("update wp %zu -> %zu",
				       blk_ctx->wp,
				       cache_zone->start + new_wp);
#endif
				cache_zone->wp = cache_zone->start + new_wp;
			}
		}
		bvec_kunmap_irq(data, &flags);
		sector_offset += bvec.bv_len;
		bio_len += (bvec.bv_len >> SECTOR_SHIFT);
	}
	blk_add_trace_msg(q, "zoned cached %zu %zu %zu", cache_zone->start,
			  bio_offset, bio_len);
	blk_ctx->tstamp = jiffies;
}

static int zoned_end_io(struct dm_target *ti, struct bio *bio, int error)
{
	struct dm_zoned_info *info =
		dm_per_bio_data(bio, ti->per_io_data_size);
	struct blk_zone_context *blk_ctx = info->ctx;
	unsigned long timeout = 1;

	if (info->ctx) {
		struct zoned_context *zc = blk_ctx->zc;

		if (error)
			DMWARN("zone %zu sector %zu error %d",
			       blk_ctx->cache_zone.start,
			       bio->bi_iter.bi_sector, error);
		info->ctx = NULL;
		timeout = zc->cache_timeout * HZ;
		if (mod_delayed_work(zc->wq, &blk_ctx->flush_work,
				     zc->cache_timeout * HZ))
			zoned_put_context(blk_ctx);
		else
			blk_ctx->wq_tstamp = jiffies;
	}

	return error;
}

static int zoned_map(struct dm_target *ti, struct bio *bio)
{
	struct zoned_context *zc = ti->private;
	struct request_queue *q = bdev_get_queue(zc->dev->bdev);
	sector_t sector;
	struct blk_zone *zone = NULL;
	struct blk_zone_context *blk_ctx = NULL;
	unsigned int num_sectors;
	unsigned long flags;
	struct dm_zoned_info *info =
		dm_per_bio_data(bio, sizeof(struct dm_zoned_info));

	bio->bi_bdev = zc->dev->bdev;
	info->ctx = NULL;
	sector = dm_target_offset(ti, bio->bi_iter.bi_sector);
	if (!bio_sectors(bio) && !(bio->bi_rw & REQ_FLUSH))
		goto remap;

	zone = blk_lookup_zone(q, sector);
	if (!zone) {
		/*
		 * No zone found.
		 * Use backing device.
		 */
		goto remap;
	}
	if (!blk_zone_is_smr(zone)) {
		/*
		 * Not an SMR zone.
		 * Use backing device.
		 */
		goto remap;
	}
	num_sectors = bio_sectors(bio);
	if (sector + bio_sectors(bio) > zone->start + zone->len) {
		DMWARN("bio %c too large (sector %zu len %u zone %zu lim %zu)",
		       bio_data_dir(bio) == READ ? 'r' : 'w', sector,
		       bio_sectors(bio), zone->start,
		       zone->start + zone->len);
		num_sectors = zone->start + zone->len - sector;
		dm_accept_partial_bio(bio, num_sectors);
	}

	spin_lock_irqsave(&zone->lock, flags);
	if (zone->private_data &&
	    zoned_get_context(zone->private_data)) {
		blk_ctx = zone->private_data;
		blk_ctx->tstamp = jiffies;
	} else {
		/* Check for FLUSH & DISCARD */
		if (unlikely(bio->bi_rw & (REQ_FLUSH | REQ_DISCARD))) {
			spin_unlock_irqrestore(&zone->lock, flags);
			goto remap;
		}
		if (bio_data_dir(bio) == READ) {
			/*
			 * Read request to non-cached zone.
			 */
			if (sector >= zone->wp) {
				/*
				 * Read beyond Write pointer.
				 * Move along, there is nothing to see.
				 */
				blk_add_trace_msg(q, "beyond wp %zu",
						  zone->wp);
				spin_unlock_irqrestore(&zone->lock, flags);
				bio_endio(bio);
				return DM_MAPIO_SUBMITTED;
			}
			/*
			 * Use backing device.
			 */
			spin_unlock_irqrestore(&zone->lock, flags);
			goto remap;
		}
		spin_unlock_irqrestore(&zone->lock, flags);
		/*
		 * Get a new private data structure from pool
		 */
		blk_ctx = mempool_alloc(zc->blk_zone_pool, GFP_KERNEL);
		if (WARN_ON(!blk_ctx)) {
			DMWARN("cannot allocate context for %zu",
			       zone->start);
			goto remap;
		}
#if 0
		DMINFO("Allocated context for %zu %p", zone->start, blk_ctx);
#endif
		spin_lock_irqsave(&zone->lock, flags);
		if (zone->private_data &&
		    zoned_get_context(zone->private_data)) {
			DMWARN("concurrent allocation for %zu", zone->start);
			mempool_free(blk_ctx, zc->blk_zone_pool);
			blk_ctx = zone->private_data;
		} else {
			zoned_init_context(blk_ctx);
			zone->private_data = blk_ctx;
			blk_ctx->zc = zc;
			blk_ctx->zone = zone;
			blk_ctx->cache_zone.start = zone->start;
			blk_ctx->cache_zone.wp = zone->wp;
			blk_ctx->cache_zone.len = zone->len;
		}
		blk_ctx->tstamp = jiffies;
	}

	if (test_bit(ZONED_CACHE_FAILED, &blk_ctx->flags) &&
	    bio_data_dir(bio) == WRITE) {
		spin_unlock_irqrestore(&zone->lock, flags);
		zoned_put_context(blk_ctx);
		bio->bi_error = -EROFS;
		bio_endio(bio);
		return DM_MAPIO_SUBMITTED;
	}
	spin_unlock_irqrestore(&zone->lock, flags);

	/* Gate against concurrent I/O to the same zone */
	atomic_inc(&blk_ctx->epoch);
	while (test_and_set_bit(ZONED_CACHE_ACTIVE, &blk_ctx->flags)) {
		blk_add_trace_msg(q, "zoned delay concurrent %zu %u",
				  sector, num_sectors);
		DMWARN("zone %zu wait for concurrent I/O sector %zu",
		       blk_ctx->cache_zone.start, sector);
		if (!wait_on_bit_timeout(&blk_ctx->flags, ZONED_CACHE_ACTIVE,
					 TASK_UNINTERRUPTIBLE,
					 zc->cache_timeout * HZ))
			break;
		DMWARN("zone %zu concurrent I/O timeout",
		       blk_ctx->cache_zone.start);
	}

	if (test_bit(ZONED_CACHE_EVICT, &blk_ctx->flags)) {
		DMINFO("zone %zu wait for eviction to complete",
		       blk_ctx->cache_zone.start);
		wait_on_bit_io(&blk_ctx->flags, ZONED_CACHE_EVICT,
			       TASK_UNINTERRUPTIBLE);
		/* Update timestamp to avoid lru eviction */
		blk_ctx->tstamp = jiffies;
	} else {
		if (cancel_delayed_work(&blk_ctx->flush_work))
			zoned_put_context(blk_ctx);
		else
			flush_delayed_work(&blk_ctx->flush_work);
	}
	if (test_bit(ZONED_CACHE_UPDATE, &blk_ctx->flags)) {
		DMINFO("zone %zu update in progress",
		       blk_ctx->cache_zone.start);
		wait_for_completion(&blk_ctx->read_complete);
	}
	zoned_lock_context(blk_ctx);
	/* Might race with LRU eviction */
	WARN_ON(test_bit(ZONED_CACHE_EVICT, &blk_ctx->flags));
	WARN_ON(test_bit(ZONED_CACHE_BUSY, &blk_ctx->flags));
	/* Check for FLUSH & DISCARD */
	if (unlikely(bio->bi_rw & (REQ_FLUSH | REQ_DISCARD))) {
		size_t len = blk_ctx->cache_zone.wp - blk_ctx->cache_zone.start;
		DMINFO("%s zone %zu %zu",
		       (bio->bi_rw & REQ_DISCARD) ? "discard" : "flush",
		       blk_ctx->cache_zone.start, len);
		blk_add_trace_msg(q, "zoned %s %zu %zu",
				  (bio->bi_rw & REQ_DISCARD) ?
				  "discard" : "flush",
				  blk_ctx->cache_zone.start, len);
		goto remap_unref;
	}
	if (sector < blk_ctx->cache_zone.wp &&
	    sector + bio_sectors(bio) > blk_ctx->cache_zone.wp) {
		DMWARN("bio %c beyond wp (sector %zu len %u wp %zu)",
		       bio_data_dir(bio) == READ ? 'r' : 'w', sector,
		       bio_sectors(bio), blk_ctx->cache_zone.wp);
		num_sectors = blk_ctx->cache_zone.wp - sector;
		dm_accept_partial_bio(bio, num_sectors);
	}
	/* list is not initialized above */
	if (list_empty(&blk_ctx->node)) {
		spin_lock_irq(&zc->list_lock);
		list_add_tail(&blk_ctx->node, &zc->cache_list);
		spin_unlock_irq(&zc->list_lock);
	}
	if (!blk_ctx->cache) {
		/*
		 * Cache miss
		 */
		if (bio_data_dir(bio) == READ) {
			/*
			 * Read request to non-cached zone.
			 * Use backing device.
			 */
#if 0
			DMINFO("bypass cache for %zu %u",
			       sector, num_sectors);
#endif
			goto remap_unref;
		} else if (sector == blk_ctx->cache_zone.wp) {
			/*
			 * Aligned write
			 */
			blk_add_trace_msg(q, "zoned aligned %zu %u",
					  sector, num_sectors);
			zc->disk_aligned_writes++;

			blk_ctx->cache_zone.wp += num_sectors;
			bio->bi_rw |= WRITE_SYNC;
			info->ctx = blk_ctx;
			zoned_get_context(blk_ctx);
			goto remap_unref;
		} else if (sector > blk_ctx->cache_zone.wp) {
			sector_t gap = sector - blk_ctx->cache_zone.wp;
			/*
			 * Write beyond WP
			 */
			blk_add_trace_msg(q, "zoned misaligned "
					  "%zu %zu %u",
					  blk_ctx->cache_zone.wp,
					  gap, num_sectors);
			zc->disk_misaligned_writes++;
			bio->bi_rw |= WRITE_SYNC;
			zoned_handle_misaligned(blk_ctx, bio, sector);
			zoned_unlock_context(blk_ctx);
			info->ctx = blk_ctx;
			return DM_MAPIO_SUBMITTED;
		}
		zoned_unlock_context(blk_ctx);
		zoned_evict_cache(zc, blk_ctx);
		zoned_lock_context(blk_ctx);
		zoned_attach_cache(blk_ctx);
		if (blk_ctx->cache == NULL) {
			DMINFO("cache pool exhausted for %zu %u",
			       sector, num_sectors);
			goto remap_unref;
		}
		if (unlikely(read_zone_cache(blk_ctx, q, sector) < 0)) {
			/*
			 * Disaster recovery:
			 * Reading zone data failed.
			 * Remap to disk and keep fingers crossed.
			 */
			goto remap_unref;
		}
	} else {
		/*
		 * Cache hit
		 */
		zc->cache_hits++;
#if 0
		DMINFO("cache hit zone %zu %zu %u",
		       zone->start, sector, num_sectors);
#endif
	}
	zoned_handle_bio(blk_ctx, sector, bio);

	blk_ctx->tstamp = jiffies;
	zoned_unlock_context(blk_ctx);

	clear_bit_unlock(ZONED_CACHE_ACTIVE, &blk_ctx->flags);
	smp_mb__after_atomic();
	wake_up_bit(&blk_ctx->flags, ZONED_CACHE_ACTIVE);

	if (unlikely(bio->bi_rw & REQ_FUA)) {
		zoned_get_context(blk_ctx);
		if (mod_delayed_work(zc->wq, &blk_ctx->flush_work, 0)) {
			DMWARN("zoned %zu FUA flush already queued",
			       blk_ctx->cache_zone.start);
			zoned_put_context(blk_ctx);
		} else
			blk_ctx->wq_tstamp = jiffies;
#if 0
		flush_delayed_work(&blk_ctx->flush_work);
#endif
	}
	bio_endio(bio);

	if (!(bio->bi_rw & REQ_FUA)) {
		zoned_get_context(blk_ctx);
		if (mod_delayed_work(zc->wq, &blk_ctx->flush_work,
				     zc->cache_timeout * HZ)) {
			DMWARN("zoned %zu flush already queued",
			       blk_ctx->cache_zone.start);
			zoned_put_context(blk_ctx);
		} else
			blk_ctx->wq_tstamp = jiffies;
	}

	zoned_put_context(blk_ctx);

	return DM_MAPIO_SUBMITTED;

remap_unref:
	blk_ctx->tstamp = jiffies;
	zoned_unlock_context(blk_ctx);

	clear_bit_unlock(ZONED_CACHE_ACTIVE, &blk_ctx->flags);
	smp_mb__after_atomic();
	wake_up_bit(&blk_ctx->flags, ZONED_CACHE_ACTIVE);
	zoned_put_context(blk_ctx);
remap:
	bio->bi_iter.bi_sector = sector;
	return DM_MAPIO_REMAPPED;
}

static void zoned_status(struct dm_target *ti, status_type_t type,
			  unsigned status_flags, char *result, unsigned maxlen)
{
	struct zoned_context *zc = ti->private;

	switch (type) {
	case STATUSTYPE_INFO:
		snprintf(result, maxlen, "%zu hits %lu aligned %lu/%lu/%lu cache %lu/%lu/%lu",
			 zc->cache_size >> SECTOR_SHIFT,
			 zc->cache_hits,
			 zc->disk_aligned_writes,
			 zc->disk_misaligned_writes,
			 zc->cache_aligned_writes,
			 zc->cache_loads, zc->cache_flushes, zc->cache_max);
		break;

	case STATUSTYPE_TABLE:
		snprintf(result, maxlen, "%s %u %u %u",
			 zc->dev->name, zc->cache_min, zc->cache_hiwat,
			 zc->cache_timeout);
		break;
	}
}

static int zoned_iterate_devices(struct dm_target *ti,
				  iterate_devices_callout_fn fn, void *data)
{
	struct zoned_context *zc = ti->private;

	return fn(ti, zc->dev, 0, ti->len, data);
}

/*
 * Construct a zoned mapping: <dev_path> <cached_dev_path>
 */
static int zoned_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct zoned_context *zc;
	struct request_queue *q;
	unsigned long long tmp;
	int logical_blksize;
	char wq_name[32], *devstr;
	int r;

	if (argc < 2) {
		ti->error = "Invalid argument count";
		return -EINVAL;
	}

	zc = kzalloc(sizeof(*zc), GFP_KERNEL);
	if (zc == NULL) {
		ti->error = "dm-zoned: Cannot allocate zoned context";
		return -ENOMEM;
	}

	if (argc > 1) {
		if (sscanf(argv[1], "%llu", &tmp) != 1) {
			ti->error = "dm-zoned: Invalid number of cache slots";
			r = -EINVAL;
			goto bad;
		}
		zc->cache_min = tmp;
	} else
		zc->cache_min = DM_ZONED_POOL_SIZE;
	if (argc > 2) {
		if (sscanf(argv[2], "%llu", &tmp) != 1) {
			ti->error = "dm-zoned: Invalid high watermark";
			r = -EINVAL;
			goto bad;
		}
		zc->cache_hiwat = tmp;
	} else
		zc->cache_hiwat = DM_ZONED_CACHE_HIGHWAT;
	zc->cache_lowat = zc->cache_hiwat - 3;

	if (argc > 3) {
		if (sscanf(argv[3], "%llu", &tmp) != 1) {
			ti->error = "dm-zoned: Invalid cache timeout";
			r = -EINVAL;
			goto bad;
		}
		zc->cache_timeout = tmp;
	} else
		zc->cache_timeout = DM_ZONED_CACHE_TIMEOUT;

	zc->dm_io = dm_io_client_create();
	if (IS_ERR(zc->dm_io)) {
		r = PTR_ERR(zc->dm_io);
		ti->error = "dm-zoned: Error creating dm_io client";
		goto bad;
	}
	if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &zc->dev)) {
		ti->error = "dm-zoned: Device lookup failed";
		r = -ENODEV;
		goto bad_dev;
	}

	q = bdev_get_queue(zc->dev->bdev);
	blk_queue_flush(q, REQ_FLUSH);
	elevator_change(q, "noop");
	logical_blksize = queue_logical_block_size(q);
	zc->cache_size = blk_max_size_offset(q, 0) * logical_blksize;
	blk_queue_io_opt(q, zc->cache_size);
	if (zoned_mempool_create(zc) < 0) {
		ti->error = "dm-zoned: cannot create context pool";
		r = -ENOMEM;
		goto bad_mempool;
	}
	devstr = strrchr(argv[0],'/');
	if (!devstr)
		devstr = argv[0];
	else
		devstr++;
	snprintf(wq_name, 32, "zoned_wq_%s", devstr);
	zc->wq = create_singlethread_workqueue(wq_name);
	if (!zc->wq) {
		ti->error = "dm-zoned: failed to create workqueue";
		r = -ENOMEM;
		goto bad_wq;
	}
	INIT_LIST_HEAD(&zc->cache_list);
	spin_lock_init(&zc->list_lock);
	atomic_set(&zc->num_cache, 0);
	init_completion(&zc->evict_complete);

	ti->num_flush_bios = 1;
	ti->num_discard_bios = 1;
	ti->flush_supported = true;
	ti->discards_supported = true;
	ti->per_io_data_size = sizeof(struct dm_zoned_info);

	ti->private = zc;
	return 0;

bad_wq:
	mempool_destroy(zc->cache_pool);
	mempool_destroy(zc->blk_zone_pool);
bad_mempool:
	dm_put_device(ti, zc->dev);
bad_dev:
	dm_io_client_destroy(zc->dm_io);
bad:
	kfree(zc);
	return r;
}

static void zoned_dtr(struct dm_target *ti)
{
	struct zoned_context *zc = ti->private;
	struct request_queue *q;
	struct rb_node *node;
	struct blk_zone_context *blk_ctx, *tmp;

	spin_lock_irq(&zc->list_lock);
	list_for_each_entry_safe(blk_ctx, tmp, &zc->cache_list, node) {
		if (!zoned_get_context(blk_ctx))
			continue;

		spin_unlock_irq(&zc->list_lock);
		wait_on_bit(&blk_ctx->flags, ZONED_CACHE_ACTIVE,
			    TASK_UNINTERRUPTIBLE);
		if (test_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags)) {
			zoned_get_context(blk_ctx);
			if (mod_delayed_work(zc->wq,
					     &blk_ctx->flush_work, 0))
				zoned_put_context(blk_ctx);
			else
				blk_ctx->wq_tstamp = jiffies;
			flush_delayed_work(&blk_ctx->flush_work);
		}
		WARN_ON(test_bit(ZONED_CACHE_BUSY, &blk_ctx->flags));
		WARN_ON(test_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags));
		zoned_put_context(blk_ctx);
		spin_lock_irq(&zc->list_lock);
	}
	spin_unlock_irq(&zc->list_lock);
	flush_workqueue(zc->wq);

	q = bdev_get_queue(zc->dev->bdev);
	for (node = rb_first(&q->zones); node; node = rb_next(node)) {
		struct blk_zone *zone;
		unsigned long flags;

		zone = rb_entry(node, struct blk_zone, node);
		spin_lock_irqsave(&zone->lock, flags);
		blk_ctx = NULL;
		if (zone->private_data) {
			blk_ctx = zone->private_data;
			zone->private_data = NULL;
			blk_ctx->zone = NULL;
		}
		spin_unlock_irqrestore(&zone->lock, flags);
		if (blk_ctx) {
			sector_t len = blk_ctx->cache_zone.wp - blk_ctx->cache_zone.start;
			DMWARN("destroy zone context %zu %zu",
			       blk_ctx->cache_zone.start, len);
			zoned_get_context(blk_ctx);
			cancel_delayed_work_sync(&blk_ctx->flush_work);
			WARN_ON(test_bit(ZONED_CACHE_DIRTY, &blk_ctx->flags));
			WARN_ON(test_bit(ZONED_CACHE_BUSY, &blk_ctx->flags));
			zoned_lock_context(blk_ctx);
			if (blk_ctx->cache)
				zoned_discard_cache(blk_ctx, "destroy");
			zoned_unlock_context(blk_ctx);
			zoned_put_context(blk_ctx);
			zoned_put_context(blk_ctx);
		}
	}
	destroy_workqueue(zc->wq);
	dm_put_device(ti, zc->dev);
	dm_io_client_destroy(zc->dm_io);

	mempool_destroy(zc->cache_pool);
	mempool_destroy(zc->blk_zone_pool);
	kfree(zc);
}

static struct target_type zoned_target = {
	.name   = "zoned",
	.version = {1, 0, 0},
	.module = THIS_MODULE,
	.ctr    = zoned_ctr,
	.dtr    = zoned_dtr,
	.map    = zoned_map,
	.end_io	= zoned_end_io,
	.status = zoned_status,
	.iterate_devices = zoned_iterate_devices,
};

int __init dm_zoned_init(void)
{
	int r;

	_zoned_cache = KMEM_CACHE(blk_zone_context, 0);
	if (!_zoned_cache) {
		DMERR("failed to create zoned workqueue");
		return -ENOMEM;
	}
	r = dm_register_target(&zoned_target);
	if (r < 0) {
		DMERR("register failed %d", r);
		kmem_cache_destroy(_zoned_cache);
	}
	return r;
}

void dm_zoned_exit(void)
{
	dm_unregister_target(&zoned_target);
	kmem_cache_destroy(_zoned_cache);
}

module_init(dm_zoned_init);
module_exit(dm_zoned_exit);

MODULE_DESCRIPTION(DM_NAME " zoned caching target");
MODULE_AUTHOR("Hannes Reinecke <hare@suse.de>");
MODULE_LICENSE("GPL");
