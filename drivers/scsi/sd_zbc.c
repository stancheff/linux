/*
 * sd_zbc.c - SCSI Zoned Block commands
 *
 * Copyright (C) 2014-2015 SUSE Linux GmbH
 * Written by: Hannes Reinecke <hare@suse.de>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License version
 * 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file COPYING.  If not, write to
 * the Free Software Foundation, 675 Mass Ave, Cambridge, MA 02139,
 * USA.
 *
 */

#include <linux/blkdev.h>
#include <linux/rbtree.h>
#include <linux/vmalloc.h>

#include <asm/unaligned.h>

#include <scsi/scsi.h>
#include <scsi/scsi_cmnd.h>
#include <scsi/scsi_dbg.h>
#include <scsi/scsi_device.h>
#include <scsi/scsi_driver.h>
#include <scsi/scsi_host.h>
#include <scsi/scsi_eh.h>

#include "sd.h"
#include "scsi_priv.h"

#define SD_ZBC_BUF_SIZE 524288

#define sd_zbc_debug(sdkp, fmt, args...)				\
	pr_debug("%s %s [%s]: " fmt,					\
		 dev_driver_string(&(sdkp)->device->sdev_gendev),	\
		 dev_name(&(sdkp)->device->sdev_gendev),		\
		 (sdkp)->disk->disk_name, ## args)

#define sd_zbc_debug_ratelimit(sdkp, fmt, args...)		\
	do {							\
		if (printk_ratelimit())				\
			sd_zbc_debug(sdkp, fmt, ## args);	\
	} while( 0 )

struct zbc_update_work {
	struct work_struct	zone_work;
	struct scsi_disk	*sdkp;
	sector_t		zone_sector;
	int			zone_buflen;
	struct bdev_zone_report zone_buf[0];
};

/**
 * get_len_from_desc() - Decode write pointer as # of blocks from start
 * @bzde: Zone descriptor entry.
 *
 * Return: Write Pointer as number of blocks from start of zone.
 */
static inline sector_t get_len_from_desc(struct scsi_disk *sdkp,
					 struct bdev_zone_descriptor *bzde)
{
	return logical_to_sectors(sdkp->device, be64_to_cpu(bzde->length));
}

/**
 * get_wp_from_desc() - Decode write pointer as # of blocks from start
 * @bzde: Zone descriptor entry.
 *
 * Return: Write Pointer as number of blocks from start of zone.
 */
static inline sector_t get_wp_from_desc(struct scsi_disk *sdkp,
					struct bdev_zone_descriptor *bzde)
{
	return logical_to_sectors(sdkp->device,
		be64_to_cpu(bzde->lba_wptr) - be64_to_cpu(bzde->lba_start));
}

/**
 * get_start_from_desc() - Decode write pointer as # of blocks from start
 * @bzde: Zone descriptor entry.
 *
 * Return: Write Pointer as number of blocks from start of zone.
 */
static inline sector_t get_start_from_desc(struct scsi_disk *sdkp,
					   struct bdev_zone_descriptor *bzde)
{
	return logical_to_sectors(sdkp->device, be64_to_cpu(bzde->lba_start));
}

static void _fill_zone(struct blk_zone *zone, struct scsi_disk *sdkp,
		       struct bdev_zone_descriptor *bzde)
{
	zone->type = bzde->type & 0x0f;
	zone->state = (bzde->flags >> 4) & 0x0f;
	zone->wp = get_wp_from_desc(sdkp, bzde);
}


static void fill_zone(struct contiguous_wps *cwps, int z_count,
		      struct scsi_disk *sdkp, struct bdev_zone_descriptor *bzde)
{
	_fill_zone(&cwps->zones[z_count], sdkp, bzde);
}

/**
 * sd_zbc_report_zones - Issue a REPORT ZONES scsi command
 * @sdkp: SCSI disk to which the command should be send
 * @buffer: response buffer
 * @bufflen: length of @buffer
 * @start_sector: logical sector for the zone information should be reported
 * @option: reporting option to be used
 */
static int sd_zbc_report_zones(struct scsi_disk *sdkp,
			       struct bdev_zone_report *buffer,
			       int bufflen, sector_t start_sector, u8 option)
{
	struct scsi_device *sdp = sdkp->device;
	const int timeout = sdp->request_queue->rq_timeout
			* SD_FLUSH_TIMEOUT_MULTIPLIER;
	struct scsi_sense_hdr sshdr;
	sector_t start_lba = sectors_to_logical(sdkp->device, start_sector);
	unsigned char cmd[16];
	int result;

	if (!scsi_device_online(sdp))
		return -ENODEV;

	sd_zbc_debug(sdkp, "REPORT ZONES lba %zu len %d\n", start_lba, bufflen);

	memset(cmd, 0, 16);
	cmd[0] = ZBC_IN;
	cmd[1] = ZI_REPORT_ZONES;
	put_unaligned_be64(start_lba, &cmd[2]);
	put_unaligned_be32(bufflen, &cmd[10]);
	cmd[14] = option;
	memset(buffer, 0, bufflen);

	result = scsi_execute_req(sdp, cmd, DMA_FROM_DEVICE,
				buffer, bufflen, &sshdr,
				timeout, SD_MAX_RETRIES, NULL);

	if (result) {
		sd_zbc_debug(sdkp,
			     "REPORT ZONES lba %zu failed with %d/%d\n",
			     start_lba, host_byte(result), driver_byte(result));
		return -EIO;
	}

	return 0;
}

static void sd_zbc_refresh_zone_work(struct work_struct *work)
{
	struct zbc_update_work *zbc_work =
		container_of(work, struct zbc_update_work, zone_work);
	struct scsi_disk *sdkp = zbc_work->sdkp;
	struct request_queue *q = sdkp->disk->queue;
	struct bdev_zone_report *rpt = zbc_work->zone_buf;
	unsigned int zone_buflen = zbc_work->zone_buflen;
	struct bdev_zone_descriptor *bzde;
	int iter;
	int offmax;
	sector_t z_at, z_start, z_len;
	spinlock_t *lock;
	struct blk_zone *zone;
	int ret;
	sector_t last_sector;
	sector_t capacity = logical_to_sectors(sdkp->device, sdkp->capacity);

	ret = sd_zbc_report_zones(sdkp, rpt, zone_buflen,
				  zbc_work->zone_sector,
				  ZBC_ZONE_REPORTING_OPTION_ALL);
	if (ret)
		goto done_free;

	offmax = max_report_entries(zone_buflen);
	for (iter = 0; iter < offmax; iter++) {
		bzde = &rpt->descriptors[iter];
		z_at = get_start_from_desc(sdkp, bzde);
		if (!z_at)
			break;
		zone = blk_lookup_zone(q, z_at, &z_start, &z_len, &lock);
		if (zone) {
			_fill_zone(zone, sdkp, bzde);
			last_sector = z_start + z_len;
		}
	}

	if (sdkp->zone_work_q && last_sector != -1 && last_sector < capacity) {
		if (test_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags)) {
			sd_zbc_debug(sdkp,
				     "zones in reset, canceling refresh\n");
			ret = -EAGAIN;
			goto done_free;
		}

		zbc_work->zone_sector = last_sector;
		queue_work(sdkp->zone_work_q, &zbc_work->zone_work);
		/* Kick request queue to be on the safe side */
		goto done_start_queue;
	}
done_free:
	kfree(zbc_work);
	if (test_and_clear_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags) && ret) {
		sd_zbc_debug(sdkp,
			     "Canceling zone initialization\n");
	}
done_start_queue:
	if (q->mq_ops)
		blk_mq_start_hw_queues(q);
	else {
		unsigned long flags;

		spin_lock_irqsave(q->queue_lock, flags);
		blk_start_queue(q);
		spin_unlock_irqrestore(q->queue_lock, flags);
	}
}

/**
 * sd_zbc_update_zones - Update zone information for @sector
 * @sdkp: SCSI disk for which the zone information needs to be updated
 * @sector: sector to be updated
 * @bufsize: buffersize to be allocated
 * @reason: non-zero if existing zones should be updated
 */
void sd_zbc_update_zones(struct scsi_disk *sdkp, sector_t sector, int bufsize,
			 int reason)
{
	struct request_queue *q = sdkp->disk->queue;
	struct zbc_update_work *zbc_work;
	int num_rec;

	if (test_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags)) {
		sd_zbc_debug(sdkp,
			     "zones in reset, not starting reason\n");
		return;
	}

	if (reason != SD_ZBC_INIT) {
		/* lookup sector, is zone pref? then ignore */
		sector_t z_start, z_len;
		spinlock_t *lck;
		struct blk_zone *zone = blk_lookup_zone(q, sector, &z_start,
							&z_len, &lck);
		/* zone actions on conventional zones are invalid */
		if (zone && reason == SD_ZBC_RESET_WP && blk_zone_is_cmr(zone))
			return;
		if (reason == SD_ZBC_RESET_WP)
			sd_zbc_debug(sdkp, "RESET WP failed %lx\n", sector);
	}

	if (!sdkp->zone_work_q)
		return;

retry:
	zbc_work = kzalloc(sizeof(struct zbc_update_work) + bufsize,
			   reason != SD_ZBC_INIT ? GFP_ATOMIC : GFP_KERNEL);
	if (!zbc_work) {
		if (bufsize > 512) {
			sd_zbc_debug(sdkp,
				     "retry with buffer size %d\n", bufsize);
			bufsize = bufsize >> 1;
			goto retry;
		}
		sd_zbc_debug(sdkp,
			     "failed to allocate %d bytes\n", bufsize);
		if (reason == SD_ZBC_INIT)
			clear_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags);
		return;
	}
	zbc_work->zone_sector = sector;
	zbc_work->zone_buflen = bufsize;
	zbc_work->sdkp = sdkp;
	INIT_WORK(&zbc_work->zone_work, sd_zbc_refresh_zone_work);
	num_rec = max_report_entries(bufsize);

	/*
	 * Mark zones under update as BUSY
	 */
	if (reason != SD_ZBC_INIT) {
		unsigned long flags;
		int iter;
		struct zone_wps *zi = q->zones;
		struct contiguous_wps *wp = NULL;
		u64 index = -1;
		int zone_busy = 0;
		int z_flgd = 0;

		for (iter = 0; iter < zi->wps_count; iter++) {
			if (sector >= zi->wps[iter]->start_lba &&
			    sector <  zi->wps[iter]->last_lba) {
				wp = zi->wps[iter];
				break;
			}
		}
		if (wp) {
			spin_lock_irqsave(&wp->lock, flags);
			index = (sector - wp->start_lba) / wp->zone_size;
			while (index < wp->zone_count && z_flgd < num_rec) {
				struct blk_zone *bzone = &wp->zones[index];

				index++;
				z_flgd++;
				if (!blk_zone_is_smr(bzone))
					continue;

				if (bzone->state == BLK_ZONE_BUSY)
					zone_busy++;
				else
					bzone->state = BLK_ZONE_BUSY;
			}
			spin_unlock_irqrestore(&wp->lock, flags);
		}
		if (z_flgd && (z_flgd == zone_busy)) {
			sd_zbc_debug(sdkp,
				     "zone update for %zu in progress\n",
				     sector);
			kfree(zbc_work);
			return;
		}
	}

	if (!queue_work(sdkp->zone_work_q, &zbc_work->zone_work)) {
		sd_zbc_debug(sdkp,
			     "zone update already queued?\n");
		kfree(zbc_work);
	}
}

/**
 * discard_or_write_same - Wrapper to setup Write Same or Reset WP for ZBC dev
 * @cmd: SCSI command / request to setup
 * @sector: Block layer sector (512 byte sector) to map to device.
 * @nr_sectors: Number of 512 byte sectors.
 * @use_write_same: When true setup WRITE_SAME_16 w/o UNMAP set.
 *                  When false setup RESET WP for zone starting at sector.
 */
static void discard_or_write_same(struct scsi_cmnd *cmd, sector_t sector,
				  unsigned int nr_sectors, bool use_write_same)
{
	struct scsi_device *sdp = cmd->device;

	/*
	 * blk_zone cache uses block layer sector units
	 * but commands use device units
	 */
	sector >>= ilog2(sdp->sector_size) - 9;
	nr_sectors >>= ilog2(sdp->sector_size) - 9;

	if (use_write_same) {
		cmd->cmd_len = 16;
		cmd->cmnd[0] = WRITE_SAME_16;
		cmd->cmnd[1] = 0; /* UNMAP (not set) */
		put_unaligned_be64(sector, &cmd->cmnd[2]);
		put_unaligned_be32(nr_sectors, &cmd->cmnd[10]);
		cmd->transfersize = sdp->sector_size;
		cmd->request->timeout = SD_WRITE_SAME_TIMEOUT;
	} else {
		cmd->cmd_len = 16;
		cmd->cmnd[0] = ZBC_OUT;
		cmd->cmnd[1] = ZO_RESET_WRITE_POINTER;
		put_unaligned_be64(sector, &cmd->cmnd[2]);
		/* Reset Write Pointer doesn't have a payload */
		cmd->transfersize = 0;
		cmd->sc_data_direction = DMA_NONE;
	}
}

/**
 * sd_zbc_setup_discard() - ZBC device hook for sd_setup_discard
 * @cmd: SCSI command/request being setup
 *
 * Handle SD_ZBC_RESET_WP provisioning mode.
 * If zone is sequential and discard matches zone size issue RESET WP
 * If zone is conventional issue WRITE_SAME_16 w/o UNMAP.
 *
 * Return:
 *  BLKPREP_OK    - Zone action not handled here (Skip futher processing)
 *  BLKPREP_DONE  - Zone action not handled here (Process as normal)
 *  BLKPREP_DEFER - Zone action should be handled here but memory
 *                  allocation failed. Retry.
 */
int sd_zbc_setup_discard(struct scsi_cmnd *cmd)
{
	struct request *rq = cmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	sector_t sector = blk_rq_pos(rq);
	unsigned int nr_sectors = blk_rq_sectors(rq);
	int ret = BLKPREP_OK;
	struct blk_zone *zone;
	unsigned long flags;
	bool use_write_same = false;
	sector_t z_start, z_len;
	spinlock_t *lck;

	zone = blk_lookup_zone(rq->q, sector, &z_start, &z_len, &lck);
	if (!zone)
		return BLKPREP_KILL;

	spin_lock_irqsave(lck, flags);
	if (zone->state == BLK_ZONE_UNKNOWN ||
	    zone->state == BLK_ZONE_BUSY) {
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding zone %zx state %x, deferring\n",
				       z_start, zone->state);
		ret = BLKPREP_DEFER;
		goto out;
	}
	if (zone->state == BLK_ZONE_OFFLINE) {
		/* let the drive fail the command */
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding offline zone %zx\n",
				       z_start);
		goto out;
	}
	if (blk_zone_is_cmr(zone)) {
		use_write_same = true;
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding CMR zone %zx\n", z_start);
		goto out;
	}
	if (z_start != sector || z_len < nr_sectors) {
		sd_printk(KERN_ERR, sdkp,
			  "Misaligned RESET WP %zx/%x on zone %zx/%zx\n",
			  sector, nr_sectors, z_start, z_len);
		ret = BLKPREP_KILL;
		goto out;
	}
	/* Protect against Reset WP when more data had been written to the
	 * zone than is being discarded.
	 */
	if (zone->wp > nr_sectors) {
		sd_printk(KERN_ERR, sdkp,
			  "Will Corrupt RESET WP %zx/%zx/%x on zone %zx/%zx/%zx\n",
			  sector, (sector_t)zone->wp, nr_sectors,
			  z_start, z_start + zone->wp, z_len);
		ret = BLKPREP_KILL;
		goto out;
	}
	if (blk_zone_is_empty(zone)) {
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding empty zone %zx [WP: %zx]\n",
				       z_start, (sector_t)zone->wp);
		ret = BLKPREP_DONE;
		goto out;
	}

out:
	/*
	 * Opportunistic setting, will be fixed up with
	 * zone update if RESET WRITE POINTER fails.
	 */
	if (ret == BLKPREP_OK && !use_write_same)
		zone->wp = 0;
	spin_unlock_irqrestore(lck, flags);

	if (ret == BLKPREP_OK)
		discard_or_write_same(cmd, sector, nr_sectors, use_write_same);

	return ret;
}


static void __set_zone_state(struct blk_zone *zone, sector_t z_len,
			     spinlock_t *lck, int op)
{
	unsigned long flags;

	spin_lock_irqsave(lck, flags);
	if (zone->type == BLK_ZONE_TYPE_CONVENTIONAL)
		goto out;

	switch (op) {
	case REQ_OP_ZONE_OPEN:
		zone->state = BLK_ZONE_OPEN_EXPLICIT;
		break;
	case REQ_OP_ZONE_FINISH:
		zone->state = BLK_ZONE_FULL;
		zone->wp = z_len;
		break;
	case REQ_OP_ZONE_CLOSE:
		zone->state = BLK_ZONE_CLOSED;
		break;
	case REQ_OP_ZONE_RESET:
		zone->wp = 0;
		break;
	default:
		WARN_ONCE(1, "%s: invalid op code: %u\n", __func__, op);
	}
out:
	spin_unlock_irqrestore(lck, flags);
}

static void update_zone_state(struct request *rq, sector_t lba, unsigned int op)
{
	struct blk_zone *zone;

	if (lba == ~0ul) {
		struct zone_wps *zi = rq->q->zones;
		struct contiguous_wps *wp;
		u32 iter, entry;

		for (iter = 0; iter < zi->wps_count; iter++) {
			wp = zi->wps[iter];
			for (entry = 0; entry < wp->zone_count; entry++) {
				zone = &wp->zones[entry];
				__set_zone_state(zone, wp->zone_size, &wp->lock,
						 op);
			}
		}
	} else {
		sector_t z_start, z_len;
		spinlock_t *lck;

		zone = blk_lookup_zone(rq->q, lba, &z_start, &z_len, &lck);
		if (zone)
			__set_zone_state(zone, z_len, lck, op);
	}
}

/**
 * sd_zbc_setup_zone_action() - ZBC device hook for sd_setup_zone_action
 * @cmd: SCSI command/request being setup
 *
 * Currently for RESET WP (REQ_OP_ZONE_RESET) if the META flag is cleared
 * the command may be translated to follow SD_ZBC_RESET_WP provisioning mode.
 *
 * Return:
 *  BLKPREP_OK    - Zone action handled here (Skip futher processing)
 *  BLKPREP_DONE  - Zone action not handled here (Caller must process)
 *  BLKPREP_DEFER - Zone action should be handled here but memory
 *                  allocation failed. Retry.
 */
int sd_zbc_setup_zone_action(struct scsi_cmnd *cmd)
{
	struct request *rq = cmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	sector_t sector = blk_rq_pos(rq);
	struct blk_zone *zone;
	spinlock_t *lck;
	sector_t z_start, z_len;
	unsigned long flags;
	unsigned int nr_sectors;
	int ret = BLKPREP_DONE;
	int op = req_op(rq);
	bool is_fua = (rq->cmd_flags & REQ_META) ? true : false;
	bool use_write_same = false;

	if (is_fua || op != REQ_OP_ZONE_RESET)
		goto out;

	zone = blk_lookup_zone(rq->q, sector, &z_start, &z_len, &lck);
	if (!zone || sdkp->provisioning_mode != SD_ZBC_RESET_WP)
		goto out;

	/* Map a Reset WP w/o FUA to a discard request */
	spin_lock_irqsave(lck, flags);
	sector = z_start;
	nr_sectors = z_len;
	if (blk_zone_is_cmr(zone))
		use_write_same = true;
	spin_unlock_irqrestore(lck, flags);

	rq->completion_data = NULL;
	if (use_write_same) {
		struct page *page;

		page = alloc_page(GFP_ATOMIC | GFP_DMA | __GFP_ZERO);
		if (!page)
			return BLKPREP_DEFER;
		rq->completion_data = page;
		rq->timeout = SD_TIMEOUT;
		cmd->sc_data_direction = DMA_TO_DEVICE;
	}
	rq->__sector = sector;
	rq->__data_len = nr_sectors << 9;
	ret = sd_zbc_setup_discard(cmd);
	if (ret != BLKPREP_OK)
		goto out;

	cmd->allowed = SD_MAX_RETRIES;
	if (cmd->transfersize) {
		blk_add_request_payload(rq, rq->completion_data,
					0, cmd->transfersize);
		ret = scsi_init_io(cmd);
	}
	rq->__data_len = nr_sectors << 9;
	if (ret != BLKPREP_OK && rq->completion_data) {
		__free_page(rq->completion_data);
		rq->completion_data = NULL;
	}
out:
	return ret;
}


/**
 * bzrpt_fill() - Fill a ZEPORT ZONES request in pages
 * @rq: Request where zone cache lives
 * @bzrpt: Zone report header.
 * @bzd: Zone report descriptors.
 * @sz: allocated size of bzrpt or bzd.
 * @lba: LBA to start filling in.
 * @opt: Report option.
 *
 * Returns: next_lba
 */
static sector_t bzrpt_fill(struct request *rq,
			   struct bdev_zone_report *bzrpt,
			   struct bdev_zone_descriptor *bzd,
			   size_t sz, sector_t lba, u8 opt)
{
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	struct scsi_device *sdp = sdkp->device;
	struct zone_wps *zi = rq->q->zones;
	struct contiguous_wps *wpdscr;
	struct blk_zone *zone = NULL;
	sector_t progress = lba;
	sector_t clen = ~0ul;
	sector_t z_start, z_len, z_wp_abs;
	unsigned long flags;
	u32 max_entries = bzrpt ? max_report_entries(sz) : sz / sizeof(*bzd);
	u32 entry = 0;
	u32 iter, idscr;
	int len_diffs = 0;
	int type_diffs = 0;
	u8 ctype;
	u8 same = 0;

	for (iter = 0; entry < max_entries && iter < zi->wps_count; iter++) {
		wpdscr = zi->wps[iter];
		if (lba > wpdscr->last_lba)
			continue;

		spin_lock_irqsave(&wpdscr->lock, flags);
		for (idscr = 0;
		     entry < max_entries && idscr < wpdscr->zone_count;
		     idscr++) {
			struct bdev_zone_descriptor *dscr;
			u64 zoff = idscr * wpdscr->zone_size;
			u8 cond, flgs = 0;

			z_len = wpdscr->zone_size;
			zoff = idscr * z_len;
			z_start = wpdscr->start_lba + zoff;
			if (lba >= z_start + z_len)
				continue;

			zone = &wpdscr->zones[idscr];
			if (blk_zone_is_cmr(zone))
				z_wp_abs = z_start + wpdscr->zone_size;
			else
				z_wp_abs = z_start + zone->wp;

			switch (opt & ZBC_REPORT_OPTION_MASK) {
			case ZBC_ZONE_REPORTING_OPTION_EMPTY:
				if (z_wp_abs != z_start)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_IMPLICIT_OPEN:
				if (zone->state != BLK_ZONE_OPEN)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_EXPLICIT_OPEN:
				if (zone->state != BLK_ZONE_OPEN_EXPLICIT)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_CLOSED:
				if (zone->state != BLK_ZONE_CLOSED)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_FULL:
				if (zone->state != BLK_ZONE_FULL)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_READONLY:
				if (zone->state == BLK_ZONE_READONLY)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_OFFLINE:
				if (zone->state == BLK_ZONE_OFFLINE)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_NEED_RESET_WP:
				if (z_wp_abs == z_start)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_NON_WP:
				if (zone->state == BLK_ZONE_NO_WP)
					continue;
				break;
			case ZBC_ZONE_REPORTING_OPTION_NON_SEQWRITE:
				/* this can only be reported by the HW */
				break;
			case ZBC_ZONE_REPORTING_OPTION_ALL:
			default:
				break;
			}

			/* if same code only applies to returned zones */
			if (opt & ZBC_REPORT_ZONE_PARTIAL) {
				if (clen != ~0ul) {
					clen = z_len;
					ctype = zone->type;
				}
				if (z_len != clen)
					len_diffs++;
				if (zone->type != ctype)
					type_diffs++;
				ctype = zone->type;
			}
			progress = z_start + z_len;

			if (!bzd) {
				if (bzrpt)
					bzrpt->descriptor_count =
						cpu_to_be32(++entry);
				continue;
			}

			/* shift to device units */
			z_start >>= ilog2(sdp->sector_size) - 9;
			z_len >>= ilog2(sdp->sector_size) - 9;
			z_wp_abs >>= ilog2(sdp->sector_size) - 9;

			cond = zone->state;
			if (zone->type == BLK_ZONE_TYPE_CONVENTIONAL)
				flgs |= 0x02;
			else if (zone->wp)
				flgs |= 0x01; /* flag as RWP recommended? */

			dscr = &bzd[entry];
			dscr->lba_start = cpu_to_be64(z_start);
			dscr->length = cpu_to_be64(z_len);
			dscr->lba_wptr = cpu_to_be64(z_wp_abs);
			dscr->type = zone->type;
			dscr->flags = cond << 4 | flgs;
			entry++;
			if (bzrpt)
				bzrpt->descriptor_count = cpu_to_be32(entry);
		}
		spin_unlock_irqrestore(&wpdscr->lock, flags);
	}

	/* if same code applies to all zones */
	if (bzrpt && !(opt & ZBC_REPORT_ZONE_PARTIAL)) {
		for (iter = 0; iter < zi->wps_count; iter++) {
			wpdscr = zi->wps[iter];
			spin_lock_irqsave(&wpdscr->lock, flags);
			for (idscr = 0; idscr < wpdscr->zone_count; idscr++) {
				z_len = wpdscr->zone_size;
				zone = &wpdscr->zones[idscr];
				if (clen != ~0ul) {
					clen = z_len;
					ctype = zone->type;
				}
				if (z_len != clen)
					len_diffs++;
				if (zone->type != ctype)
					type_diffs++;
				ctype = zone->type;
			}
			spin_unlock_irqrestore(&wpdscr->lock, flags);
		}
	}

	if (bzrpt) {
		/* calculate same code  */
		if (len_diffs == 0) {
			if (type_diffs == 0)
				same = BLK_ZONE_SAME_ALL;
			else
				same = BLK_ZONE_SAME_LEN_TYPES_DIFFER;
		} else if (len_diffs == 1 && type_diffs == 0) {
			same = BLK_ZONE_SAME_LAST_DIFFERS;
		} else {
			same = BLK_ZONE_SAME_ALL_DIFFERENT;
		}
		bzrpt->same_field = same;
		bzrpt->maximum_lba = cpu_to_be64(
			logical_to_bytes(sdkp->device, sdkp->capacity));
	}

	return progress;
}

/**
 * copy_buffer_into_request() - Copy a buffer into a (part) of a request
 * @rq: Request to copy into
 * @src: Buffer to copy from
 * @bytes: Number of bytes in src
 * @skip: Number of bytes of request to 'seek' into before overwriting with src
 *
 * Return: Number of bytes copied into the request.
 */
static int copy_buffer_into_request(struct request *rq, void *src, uint bytes,
				    off_t skip)
{
	struct req_iterator iter;
	struct bio_vec bvec;
	off_t skipped = 0;
	uint copied = 0;

	rq_for_each_segment(bvec, rq, iter) {
		void *buf;
		unsigned long flags;
		uint len;
		uint remain = 0;

		buf = bvec_kmap_irq(&bvec, &flags);
		if (skip > skipped)
			remain = skip - skipped;

		if (remain < bvec.bv_len) {
			len = min_t(uint, bvec.bv_len - remain, bytes - copied);
			memcpy(buf + remain, src + copied, len);
			copied += len;
		}
		skipped += min(remain, bvec.bv_len);
		bvec_kunmap_irq(buf, &flags);
		if (copied >= bytes)
			break;
	}
	return copied;
}

/**
 * sd_zbc_setup_zone_report_cmnd() - Handle a zone report request
 * @cmd: SCSI command request
 * @ropt: Current report option flags to use
 *
 * Use the zone cache to fill in a report zones request.
 *
 * Return: BLKPREP_DONE if copy was sucessful.
 *         BLKPREP_KILL if payload size was invalid.
 *         BLKPREP_DEFER if unable to acquire a temporary page of working data.
 */
int sd_zbc_setup_zone_report_cmnd(struct scsi_cmnd *cmd, u8 ropt)
{
	struct request *rq = cmd->request;
	sector_t sector = blk_rq_pos(rq);
	struct bdev_zone_descriptor *bzd;
	struct bdev_zone_report *bzrpt;
	void *pbuf;
	off_t skip = 0;
	unsigned int nr_bytes = blk_rq_bytes(rq);
	int rnum;
	int ret = BLKPREP_DEFER;
	unsigned int chunk;

	if (nr_bytes < 512) {
		ret = BLKPREP_KILL;
		goto out;
	}

	pbuf = (void *)__get_free_page(GFP_ATOMIC);
	if (!pbuf)
		goto out;

	bzrpt = pbuf;
	/* fill in the header and first chunk of data*/
	bzrpt_fill(rq, bzrpt, NULL, nr_bytes, sector, ropt);

	bzd = pbuf + sizeof(struct bdev_zone_report);
	chunk = min_t(unsigned int, nr_bytes, PAGE_SIZE) -
		sizeof(struct bdev_zone_report);
	sector = bzrpt_fill(rq, NULL, bzd, chunk, sector, ropt);
	chunk += sizeof(struct bdev_zone_report);
	do {
		rnum = copy_buffer_into_request(rq, pbuf, chunk, skip);
		if (rnum != chunk) {
			pr_err("buffer_to_request_partial() failed to copy "
			       "zone report to command data [%u != %u]\n",
				chunk, rnum);
			ret = BLKPREP_KILL;
			goto out;
		}
		skip += chunk;
		if (skip >= nr_bytes)
			break;
		/*
		 * keep loading more descriptors until nr_bytes have been
		 * copied to the command
		 */
		chunk = min_t(unsigned int, nr_bytes - skip, PAGE_SIZE);
		sector = bzrpt_fill(rq, NULL, pbuf, chunk, sector, ropt);
	} while (skip < nr_bytes);
	ret = BLKPREP_DONE;

out:
	if (pbuf)
		free_page((unsigned long) pbuf);
	return ret;
}

/**
 * sd_zbc_setup_read_write() - ZBC device hook for sd_setup_read_write
 * @sdkp: SCSI Disk
 * @rq: Request being setup
 * @sector: Request sector
 * @num_sectors: Request size
 */
int sd_zbc_setup_read_write(struct scsi_disk *sdkp, struct request *rq,
			    sector_t sector, unsigned int *num_sectors)
{
	struct request_queue *q = sdkp->disk->queue;
	struct blk_zone *zone;
	sector_t z_start, z_len;
	spinlock_t *lck;
	unsigned int sectors = *num_sectors;
	int ret = BLKPREP_OK;
	unsigned long flags;

	zone = blk_lookup_zone(q, sector, &z_start, &z_len, &lck);
	if (!zone) {
		/* Might happen during zone initialization */
		sd_zbc_debug_ratelimit(sdkp,
				       "zone for sector %zu not found, skipping\n",
				       sector);
		return BLKPREP_OK;
	}

	spin_lock_irqsave(lck, flags);

	if (blk_zone_is_cmr(zone))
		goto out;

	if (zone->state == BLK_ZONE_UNKNOWN ||
	    zone->state == BLK_ZONE_BUSY) {
		sd_zbc_debug_ratelimit(sdkp,
				       "zone %zu state %x, deferring\n",
				       z_start, zone->state);
		ret = BLKPREP_DEFER;
		goto out;
	}

	if (blk_zone_is_seq_pref(zone)) {
		if (op_is_write(req_op(rq))) {
			u64 nwp = sector + sectors;

			while (nwp > (z_start + z_len)) {
				zone->wp = z_len;
				sector = z_start + z_len;
				sectors = nwp - sector;
				spin_unlock_irqrestore(lck, flags);

				zone = blk_lookup_zone(q, sector,
						       &z_start, &z_len, &lck);
				if (!zone)
					return BLKPREP_OK;

				spin_lock_irqsave(lck, flags);
				nwp = sector + sectors;
			}
			if (nwp > z_start + zone->wp)
				zone->wp = nwp - z_start;
		}
		goto out;
	}

	if (zone->state == BLK_ZONE_OFFLINE) {
		/* let the drive fail the command */
		sd_zbc_debug_ratelimit(sdkp,
				       "zone %zu offline\n",
				       z_start);
		goto out;
	}

	if (op_is_write(req_op(rq))) {
		if (zone->state == BLK_ZONE_READONLY)
			goto out;
		if (zone->wp == z_len) {
			sd_zbc_debug(sdkp,
				     "Write to full zone %zu/%zu/%zu\n",
				     sector, (sector_t)zone->wp, z_len);
			ret = BLKPREP_KILL;
			goto out;
		}
		if (sector != (z_start + zone->wp)) {
			sd_zbc_debug(sdkp,
				     "Misaligned write %zu/%zu\n",
				     sector, z_start + zone->wp);
			ret = BLKPREP_KILL;
			goto out;
		}
		zone->wp += sectors;
	} else if (z_start + zone->wp <= sector + sectors) {
		if (z_start + zone->wp <= sector) {
			/* Read beyond WP: clear request buffer */
			struct req_iterator iter;
			struct bio_vec bvec;
			void *buf;
			sd_zbc_debug(sdkp,
				     "Read beyond wp %zu+%u/%zu\n",
				     sector, sectors, z_start + zone->wp);
			rq_for_each_segment(bvec, rq, iter) {
				buf = bvec_kmap_irq(&bvec, &flags);
				memset(buf, 0, bvec.bv_len);
				flush_dcache_page(bvec.bv_page);
				bvec_kunmap_irq(buf, &flags);
			}
			ret = BLKPREP_DONE;
			goto out;
		}
		/* Read straddle WP position: limit request size */
		*num_sectors = z_start + zone->wp - sector;
		sd_zbc_debug(sdkp,
			     "Read straddle wp %zu+%u/%zu => %zu+%u\n",
			     sector, sectors, z_start + zone->wp,
			     sector, *num_sectors);
	}

out:
	spin_unlock_irqrestore(lck, flags);

	return ret;
}

/**
 * update_zones_from_report() - Update zone WP's and state [condition]
 * @cmd: SCSI command request
 * @nr_bytes: Number of 'good' bytes in this request.
 *
 * Read the result data from a REPORT ZONES in PAGE_SIZE chunks util
 * the full report is digested into the zone cache.
 */
static void update_zones_from_report(struct scsi_cmnd *cmd, u32 nr_bytes)
{
	struct request *rq = cmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	struct bdev_zone_descriptor *bzde;
	u32 nread = 0;
	u32 dmax = 0;
	void *pbuf = (void *)__get_free_page(GFP_ATOMIC);

	if (!pbuf)
		goto done;

	/* read a page at a time and update the zone cache's WPs */
	while (nr_bytes > nread) {
		u32 iter, count;
		u32 len = min_t(u32, nr_bytes - nread, PAGE_SIZE);
		size_t rnum = sg_pcopy_to_buffer(scsi_sglist(cmd),
						 scsi_sg_count(cmd), pbuf, len,
						 nread);
		if (rnum != len) {
			pr_err("%s: FAIL sg_pcopy_to_buffer: %u of %u bytes\n",
				__func__, (uint)rnum, len);
			goto done;
		}
		bzde = pbuf;
		count = len / sizeof(struct bdev_zone_report);
		if (nread == 0) {
			const struct bdev_zone_report *bzrpt = pbuf;

			dmax = min_t(u32, be32_to_cpu(bzrpt->descriptor_count),
					  max_report_entries(nr_bytes));
			bzde = pbuf + sizeof(struct bdev_zone_report);
			count--;
		}
		for (iter = 0; iter < count && dmax > 0; dmax--, iter++) {
			struct blk_zone *zone;
			struct bdev_zone_descriptor *entry = &bzde[iter];
			sector_t s = get_start_from_desc(sdkp, entry);
			sector_t z_len = get_len_from_desc(sdkp, entry);
			sector_t z_strt;
			spinlock_t *lck;
			unsigned long flags;

			if (!z_len)
				goto done;

			zone = blk_lookup_zone(rq->q, s, &z_strt, &z_len, &lck);
			if (!zone)
				goto done;

			spin_lock_irqsave(lck, flags);
			zone->type = entry->type & 0xF;
			zone->state = (entry->flags >> 4) & 0xF;
			zone->wp = get_wp_from_desc(sdkp, entry);
			spin_unlock_irqrestore(lck, flags);
		}
		nread += len;
		if (!dmax)
			goto done;
	}
done:
	if (pbuf)
		free_page((unsigned long) pbuf);
}

/**
 * sd_zbc_done() - ZBC device hook for sd_done
 * @cmd: SCSI command that is done.
 * @good_bytes: number of bytes (successfully) completed.
 *
 * Cleanup or sync zone cache with cmd.
 * Currently a successful REPORT ZONES w/FUA flag will pull data
 * from the media. On done the zone cache is re-sync'd with the
 * result which is presumed to be the most accurate picture of
 * the zone condition and WP location.
 */
void sd_zbc_done(struct scsi_cmnd *cmd, int good_bytes)
{
	struct request *rq = cmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	sector_t sector = blk_rq_pos(rq);
	int result = cmd->result;
	int op = req_op(rq);
	bool is_fua = (rq->cmd_flags & REQ_META) ? true : false;

	if (sdkp->zoned != 1 && sdkp->device->type != TYPE_ZBC)
		return;

	switch (op) {
	case REQ_OP_ZONE_REPORT:
		if (is_fua && good_bytes > 0)
			update_zones_from_report(cmd, good_bytes);
		break;
	case REQ_OP_ZONE_OPEN:
	case REQ_OP_ZONE_CLOSE:
	case REQ_OP_ZONE_FINISH:
	case REQ_OP_ZONE_RESET:
		if (result == 0)
			update_zone_state(rq, sector, op);
		break;
	default:
		break;
	}
}

/**
 * sd_zbc_uninit_command() - ZBC device hook for sd_uninit_command
 * @cmd: SCSI command that is done.
 *
 * On uninit if a RESET WP (w/o FUA flag) was translated to a
 * DISCARD and then to an SCT Write Same then there may be a
 * page of completion_data that was allocated and needs to be freed.
 */
void sd_zbc_uninit_command(struct scsi_cmnd *cmd)
{
	struct request *rq = cmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	int op = req_op(rq);
	bool is_fua = (rq->cmd_flags & REQ_META) ? true : false;

	if (sdkp->zoned != 1 && sdkp->device->type != TYPE_ZBC)
		return;

	if (!is_fua && op == REQ_OP_ZONE_RESET && rq->completion_data)
		__free_page(rq->completion_data);
}

/**
 * alloc_cpws() - Allocate space for a contiguous set of write pointers
 * @items: Number of wps needed.
 * @lba: lba of the start of the next zone.
 * @z_start: Starting lba of this contiguous set.
 * @z_size: Size of each zone this contiguous set.
 *
 * Return: Allocated wps or NULL on error.
 */
static struct contiguous_wps *alloc_cpws(int items, u64 lba, u64 z_start,
					 u64 z_size)
{
	struct contiguous_wps *cwps = NULL;
	size_t sz;

	sz = sizeof(struct contiguous_wps) + (items * sizeof(struct blk_zone));
	if (items) {
		cwps = vzalloc(sz);
		if (!cwps)
			goto out;
		spin_lock_init(&cwps->lock);
		cwps->start_lba = z_start;
		cwps->last_lba = lba - 1;
		cwps->zone_size = z_size;
		cwps->is_zoned = items > 1 ? 1 : 0;
		cwps->zone_count = items;
	}

out:
	return cwps;
}

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

static int wps_realloc(struct zone_wps *zi, gfp_t gfp_mask)
{
	int rcode = 0;
	struct contiguous_wps **old;
	struct contiguous_wps **tmp;
	int n = zi->wps_count * 2;

	old = zi->wps;
	tmp = kcalloc(n, sizeof(*zi->wps), gfp_mask);
	if (!tmp) {
		rcode = -ENOMEM;
		goto out;
	}
	memcpy(tmp, zi->wps, zi->wps_count * sizeof(*zi->wps));
	zi->wps = tmp;
	kfree(old);

out:
	return rcode;
}

#define FMT_CHANGING_CAPACITY "Changing capacity from %zu to Max LBA+1 %zu"

/**
 * zbc_init_zones() - Re-Sync expected WP location with drive
 * @sdkp: scsi_disk
 * @gfp_mask: Allocation mask.
 *
 * Return: 0 on success, otherwise error.
 */
int zbc_init_zones(struct scsi_disk *sdkp, gfp_t gfp_mask)
{
	struct request_queue *q = sdkp->disk->queue;
	int rcode = 0;
	int entry = 0;
	int offset;
	int offmax;
	u64 iter;
	u64 z_start = 0ul;
	u64 z_size = 0; /* size of zone */
	int z_count = 0; /* number of zones of z_size */
	int do_fill = 0;
	int array_count = 0;
	int one_time_setup = 0;
	u8 opt = ZBC_ZONE_REPORTING_OPTION_ALL;
	size_t bufsz = SD_ZBC_BUF_SIZE;
	struct bdev_zone_report *rpt = NULL;
	struct zone_wps *zi = NULL;
	struct contiguous_wps *cwps = NULL;

	if (q->zones)
		goto out;

	zi = kzalloc(sizeof(*zi), gfp_mask);
	if (!zi) {
		rcode = -ENOMEM;
		goto out;
	}

	if (sdkp->zoned != 1 && sdkp->device->type != TYPE_ZBC) {
		struct gendisk *disk = sdkp->disk;

		zi->wps = kzalloc(sizeof(*zi->wps), gfp_mask);
		zi->wps[0] = alloc_cpws(1, disk->part0.nr_sects, z_start, 1);
		if (!zi->wps[0]) {
			rcode = -ENOMEM;
			goto out;
		}
		zi->wps_count = 1;
		goto out;
	}

	rpt = kmalloc(bufsz, gfp_mask);
	if (!rpt) {
		rcode = -ENOMEM;
		goto out;
	}

	/*
	 * Start by handling upto 32 different zone sizes. 2 will work
	 * for all the current drives, but maybe something exotic will
	 * surface.
	 */
	zi->wps = kcalloc(32, sizeof(*zi->wps), gfp_mask);
	zi->wps_count = 32;
	if (!zi->wps) {
		rcode = -ENOMEM;
		goto out;
	}

fill:
	offset = 0;
	offmax = 0;
	for (entry = 0, iter = 0; iter < sdkp->capacity; entry++) {
		struct bdev_zone_descriptor *bzde;
		int stop_end = 0;
		int stop_size = 0;

		if (offset == 0) {
			int err;

			err = sd_zbc_report_zones(sdkp, rpt, bufsz, iter, opt);
			if (err) {
				pr_err("report zones-> %d\n", err);
				if (err != -ENOTSUPP)
					rcode = err;
				goto out;
			}
			if (sdkp->rc_basis == 0) {
				sector_t lba = be64_to_cpu(rpt->maximum_lba);

				if (lba + 1 > sdkp->capacity) {
					sd_printk(KERN_WARNING, sdkp,
						  FMT_CHANGING_CAPACITY "\n",
						  sdkp->capacity, lba + 1);
					sdkp->capacity = lba + 1;
				}
			}
			offmax = max_report_entries(bufsz);
		}
		bzde = &rpt->descriptors[offset];
		if (z_size == 0)
			z_size = get_len_from_desc(sdkp, bzde);
		if (z_size != get_len_from_desc(sdkp, bzde))
			stop_size = 1;
		if ((iter + z_size) >= sdkp->capacity)
			stop_end = 1;

		if (!one_time_setup) {
			u8 type = bzde->type & 0x0F;

			if (type != BLK_ZONE_TYPE_CONVENTIONAL) {
				one_time_setup = 1;
				blk_queue_chunk_sectors(sdkp->disk->queue,
							z_size);
			}
		}

		if (do_fill == 0) {
			if (stop_end || stop_size) {
				/* include the next/last zone? */
				if (!stop_size) {
					z_count++;
					iter += z_size;
				}
				cwps = alloc_cpws(z_count, iter,
						  z_start, z_size);
				if (!cwps) {
					rcode = -ENOMEM;
					goto out;
				}
				if (array_count > 0)
					cwps->is_zoned = 1;

				zi->wps[array_count] = cwps;
				z_start = iter;
				z_size = 0;
				z_count = 0;
				array_count++;
				if (array_count >= zi->wps_count) {
					rcode = wps_realloc(zi, gfp_mask);
					if (rcode)
						goto out;
				}
				/* add the runt zone */
				if (stop_end && stop_size) {
					z_count++;
					z_size = get_len_from_desc(sdkp, bzde);
					cwps = alloc_cpws(z_count,
							  iter + z_size,
							  z_start, z_size);
					if (!cwps) {
						rcode = -ENOMEM;
						goto out;
					}
					if (array_count > 0)
						cwps->is_zoned = 1;
					zi->wps[array_count] = cwps;
					array_count++;
				}
				if (stop_end) {
					do_fill = 1;
					array_count = 0;
					z_count = 0;
					z_size = 0;
					goto fill;
				}
			}
			z_size = get_len_from_desc(sdkp, bzde);
			iter += z_size;
			z_count++;
		} else {
			fill_zone(zi->wps[array_count], z_count, sdkp, bzde);
			z_count++;
			iter += z_size;
			if (zi->wps[array_count]->zone_count == z_count) {
				z_count = 0;
				array_count++;
				zi->wps_count = array_count;
			}
		}
		offset++;
		if (offset >= offmax)
			offset = 0;
	}
out:
	kfree(rpt);

	if (rcode) {
		if (zi) {
			free_zone_wps(zi);
			kfree(zi);
		}
	} else {
		q->zones = zi;
	}

	return rcode;
}

/**
 * sd_zbc_config() - Configure a ZBC device (on attach)
 * @sdkp: SCSI disk being attached.
 * @gfp_mask: Memory allocation strategy
 *
 * Return: true of SD_ZBC_RESET_WP provisioning is supported
 */
bool sd_zbc_config(struct scsi_disk *sdkp, gfp_t gfp_mask)
{
	bool can_reset_wp = false;

	if (zbc_init_zones(sdkp, gfp_mask)) {
		sdev_printk(KERN_WARNING, sdkp->device,
			    "Initialize zone cache failed\n");
		goto out;
	}

	if (sdkp->zoned == 1 || sdkp->device->type == TYPE_ZBC)
		can_reset_wp = true;

	if (!sdkp->zone_work_q) {
		char wq_name[32];

		sprintf(wq_name, "zbc_wq_%s", sdkp->disk->disk_name);
		sdkp->zone_work_q = create_singlethread_workqueue(wq_name);
		if (!sdkp->zone_work_q) {
			sdev_printk(KERN_WARNING, sdkp->device,
				    "create zoned disk workqueue failed\n");
			goto out;
		}
	} else if (!test_and_set_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags)) {
		drain_workqueue(sdkp->zone_work_q);
		clear_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags);
	}

out:
	return can_reset_wp;
}

/**
 * sd_zbc_remove - Prepare for device removal.
 * @sdkp: SCSI Disk being removed.
 */
void sd_zbc_remove(struct scsi_disk *sdkp)
{
	if (sdkp->zone_work_q) {
		if (!test_and_set_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags))
			drain_workqueue(sdkp->zone_work_q);
		clear_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags);
		destroy_workqueue(sdkp->zone_work_q);
	}
}

/**
 * sd_zbc_discard_granularity - Determine discard granularity.
 * @sdkp: SCSI disk used to calculate discard granularity.
 *
 * Discard granularity should match the (maximum non-CMR) zone
 * size reported on the drive.
 */
unsigned int sd_zbc_discard_granularity(struct scsi_disk *sdkp)
{
	struct request_queue *q = sdkp->disk->queue;
	struct zone_wps *zi = q->zones;
	unsigned int bytes = 1;

	if (zi && zi->wps_count > 0) {
		struct contiguous_wps *wp = zi->wps[0];

		bytes = wp->zone_size;
	}

	bytes <<= ilog2(sdkp->device->sector_size);
	return bytes;
}
