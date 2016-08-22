/*
 * Zoned block device handling
 *
 * Copyright (c) 2015, Hannes Reinecke
 * Copyright (c) 2015, SUSE Linux GmbH
 */

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/blkdev.h>
#include <linux/vmalloc.h>

/**
 * blk_lookup_zone() - Lookup zones
 * @q: Request Queue
 * @sector: Location to lookup
 * @start: Pointer to starting location zone (OUT)
 * @len: Pointer to length of zone (OUT)
 * @lock: Pointer to spinlock of zones in owning descriptor (OUT)
 */
struct blk_zone *blk_lookup_zone(struct request_queue *q, sector_t sector,
				 sector_t *start, sector_t *len,
				 spinlock_t **lock)
{
	int iter;
	struct blk_zone *bzone = NULL;
	struct zone_wps *zi = q->zones;

	*start = 0;
	*len = 0;
	*lock = NULL;

	if (!q->zones)
		goto out;

	for (iter = 0; iter < zi->wps_count; iter++) {
		if (sector >= zi->wps[iter]->start_lba &&
		    sector <  zi->wps[iter]->last_lba) {
			struct contiguous_wps *wp = zi->wps[iter];
			u64 index = (sector - wp->start_lba) / wp->zone_size;

			if (index >= wp->zone_count) {
				WARN(1, "Impossible index for zone\n");
				goto out;
			}

			bzone = &wp->zones[index];
			*len = wp->zone_size;
			*start = wp->start_lba + (index * wp->zone_size);
			*lock = &wp->lock;
		}
	}

out:
	return bzone;
}
EXPORT_SYMBOL_GPL(blk_lookup_zone);

/**
 * free_zone_wps() - Free up memory in use by wps
 * @zi: zone wps array(s).
 */
static void free_zone_wps(struct zone_wps *zi)
{
	/* on error free the arrays */
	if (zi && zi->wps) {
		int ca;

		for (ca = 0; ca < zi->wps_count; ca++) {
			if (zi->wps[ca]) {
				vfree(zi->wps[ca]);
				zi->wps[ca] = NULL;
			}
		}
		kfree(zi->wps);
	}
}

/**
 * blk_drop_zones() - Free zones
 * @q: Request Queue
 */
void blk_drop_zones(struct request_queue *q)
{
	if (q->zones) {
		free_zone_wps(q->zones);
		kfree(q->zones);
		q->zones = NULL;
	}
}
EXPORT_SYMBOL_GPL(blk_drop_zones);
