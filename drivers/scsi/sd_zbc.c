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

enum zbc_zone_cond {
	ZBC_ZONE_COND_NO_WP,
	ZBC_ZONE_COND_EMPTY,
	ZBC_ZONE_COND_IMPLICIT_OPEN,
	ZBC_ZONE_COND_EXPLICIT_OPEN,
	ZBC_ZONE_COND_CLOSED,
	ZBC_ZONE_COND_READONLY = 0xd,
	ZBC_ZONE_COND_FULL,
	ZBC_ZONE_COND_OFFLINE,
};

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
	struct work_struct zone_work;
	struct scsi_disk *sdkp;
	sector_t	zone_sector;
	int		zone_buflen;
	char		zone_buf[0];
};

struct blk_zone *zbc_desc_to_zone(struct scsi_disk *sdkp, unsigned char *rec)
{
	struct blk_zone *zone;
	enum zbc_zone_cond zone_cond;
	sector_t wp = (sector_t)-1;

	zone = kzalloc(sizeof(struct blk_zone), GFP_KERNEL);
	if (!zone)
		return NULL;

	spin_lock_init(&zone->lock);
	zone->type = rec[0] & 0xf;
	zone_cond = (rec[1] >> 4) & 0xf;
	zone->len = logical_to_sectors(sdkp->device,
				       get_unaligned_be64(&rec[8]));
	zone->start = logical_to_sectors(sdkp->device,
					 get_unaligned_be64(&rec[16]));

	if (blk_zone_is_smr(zone)) {
		wp = logical_to_sectors(sdkp->device,
					get_unaligned_be64(&rec[24]));
		if (zone_cond == ZBC_ZONE_COND_READONLY) {
			zone->state = BLK_ZONE_READONLY;
		} else if (zone_cond == ZBC_ZONE_COND_OFFLINE) {
			zone->state = BLK_ZONE_OFFLINE;
		} else {
			zone->state = BLK_ZONE_OPEN;
		}
	} else
		zone->state = BLK_ZONE_NO_WP;

	zone->wp = wp;
	/*
	 * Fixup block zone state
	 */
	if (zone_cond == ZBC_ZONE_COND_EMPTY &&
	    zone->wp != zone->start) {
		sd_zbc_debug(sdkp,
			     "zone %zu state EMPTY wp %zu: adjust wp\n",
			     zone->start, zone->wp);
		zone->wp = zone->start;
	}
	if (zone_cond == ZBC_ZONE_COND_FULL &&
	    zone->wp != zone->start + zone->len) {
		sd_zbc_debug(sdkp,
			     "zone %zu state FULL wp %zu: adjust wp\n",
			     zone->start, zone->wp);
		zone->wp = zone->start + zone->len;
	}

	return zone;
}

sector_t zbc_parse_zones(struct scsi_disk *sdkp, unsigned char *buf,
			 unsigned int buf_len)
{
	struct request_queue *q = sdkp->disk->queue;
	unsigned char *rec = buf;
	int rec_no = 0;
	unsigned int list_length;
	sector_t next_sector = -1;
	u8 same;

	/* Parse REPORT ZONES header */
	list_length = get_unaligned_be32(&buf[0]);
	same = buf[4] & 0xf;
	rec = buf + 64;
	list_length += 64;

	if (list_length < buf_len)
		buf_len = list_length;

	while (rec < buf + buf_len) {
		struct blk_zone *this, *old;
		unsigned long flags;

		this = zbc_desc_to_zone(sdkp, rec);
		if (!this)
			break;

		next_sector = this->start + this->len;
		old = blk_insert_zone(q, this);
		if (old) {
			spin_lock_irqsave(&old->lock, flags);
			if (blk_zone_is_smr(old)) {
				old->wp = this->wp;
				old->state = this->state;
			}
			spin_unlock_irqrestore(&old->lock, flags);
			kfree(this);
		}
		rec += 64;
		rec_no++;
	}

	sd_zbc_debug(sdkp,
		     "Inserted %d zones, next sector %zu len %d\n",
		     rec_no, next_sector, list_length);

	return next_sector;
}

void sd_zbc_refresh_zone_work(struct work_struct *work)
{
	struct zbc_update_work *zbc_work =
		container_of(work, struct zbc_update_work, zone_work);
	struct scsi_disk *sdkp = zbc_work->sdkp;
	struct request_queue *q = sdkp->disk->queue;
	unsigned int zone_buflen;
	int ret;
	sector_t last_sector;
	sector_t capacity = logical_to_sectors(sdkp->device, sdkp->capacity);

	zone_buflen = zbc_work->zone_buflen;
	ret = sd_zbc_report_zones(sdkp, zbc_work->zone_buf, zone_buflen,
				  zbc_work->zone_sector,
				  ZBC_ZONE_REPORTING_OPTION_ALL, true);
	if (ret)
		goto done_free;

	last_sector = zbc_parse_zones(sdkp, zbc_work->zone_buf, zone_buflen);
	if (last_sector != -1 && last_sector < capacity) {
		if (test_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags)) {
			sd_zbc_debug(sdkp,
				     "zones in reset, cancelling refresh\n");
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
			     "Cancelling zone initialisation\n");
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
 * @update: true if existing zones should be updated
 */
void sd_zbc_update_zones(struct scsi_disk *sdkp, sector_t sector, int bufsize,
			 bool update)
{
	struct request_queue *q = sdkp->disk->queue;
	struct zbc_update_work *zbc_work;
	struct blk_zone *zone;
	struct rb_node *node;
	int zone_num = 0, zone_busy = 0, num_rec;
	sector_t next_sector = sector;

	if (test_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags)) {
		sd_zbc_debug(sdkp,
			     "zones in reset, not starting update\n");
		return;
	}

retry:
	zbc_work = kzalloc(sizeof(struct zbc_update_work) + bufsize,
			   update ? GFP_NOWAIT : GFP_KERNEL);
	if (!zbc_work) {
		if (bufsize > 512) {
			sd_zbc_debug(sdkp,
				     "retry with buffer size %d\n", bufsize);
			bufsize = bufsize >> 1;
			goto retry;
		}
		sd_zbc_debug(sdkp,
			     "failed to allocate %d bytes\n", bufsize);
		if (!update)
			clear_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags);
		return;
	}
	zbc_work->zone_sector = sector;
	zbc_work->zone_buflen = bufsize;
	zbc_work->sdkp = sdkp;
	INIT_WORK(&zbc_work->zone_work, sd_zbc_refresh_zone_work);
	num_rec = (bufsize / 64) - 1;

	/*
	 * Mark zones under update as BUSY
	 */
	if (update) {
		for (node = rb_first(&q->zones); node; node = rb_next(node)) {
			unsigned long flags;

			zone = rb_entry(node, struct blk_zone, node);
			if (num_rec == 0)
				break;
			if (zone->start != next_sector)
				continue;
			next_sector += zone->len;
			num_rec--;

			spin_lock_irqsave(&zone->lock, flags);
			if (blk_zone_is_smr(zone)) {
				if (zone->state == BLK_ZONE_BUSY) {
					zone_busy++;
				} else {
					zone->state = BLK_ZONE_BUSY;
					zone->wp = zone->start;
				}
				zone_num++;
			}
			spin_unlock_irqrestore(&zone->lock, flags);
		}
		if (zone_num && (zone_num == zone_busy)) {
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
 * sd_zbc_report_zones - Issue a REPORT ZONES scsi command
 * @sdkp: SCSI disk to which the command should be send
 * @buffer: response buffer
 * @bufflen: length of @buffer
 * @start_sector: logical sector for the zone information should be reported
 * @option: reporting option to be used
 * @partial: flag to set the 'partial' bit for report zones command
 */
int sd_zbc_report_zones(struct scsi_disk *sdkp, unsigned char *buffer,
			int bufflen, sector_t start_sector,
			enum zbc_zone_reporting_options option, bool partial)
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

	sd_zbc_debug(sdkp, "REPORT ZONES lba %zu len %d\n",
		     start_lba, bufflen);

	memset(cmd, 0, 16);
	cmd[0] = ZBC_IN;
	cmd[1] = ZI_REPORT_ZONES;
	put_unaligned_be64(start_lba, &cmd[2]);
	put_unaligned_be32(bufflen, &cmd[10]);
	cmd[14] = (partial ? ZBC_REPORT_ZONE_PARTIAL : 0) | option;
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

int sd_zbc_setup_discard(struct scsi_disk *sdkp, struct request *rq,
			 sector_t sector, unsigned int num_sectors)
{
	struct blk_zone *zone;
	int ret = BLKPREP_OK;
	unsigned long flags;

	zone = blk_lookup_zone(rq->q, sector);
	if (!zone)
		return BLKPREP_KILL;

	spin_lock_irqsave(&zone->lock, flags);

	if (zone->state == BLK_ZONE_UNKNOWN ||
	    zone->state == BLK_ZONE_BUSY) {
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding zone %zu state %x, deferring\n",
				       zone->start, zone->state);
		ret = BLKPREP_DEFER;
		goto out;
	}
	if (zone->state == BLK_ZONE_OFFLINE) {
		/* let the drive fail the command */
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding offline zone %zu\n",
				       zone->start);
		goto out;
	}

	if (!blk_zone_is_smr(zone)) {
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding %s zone %zu\n",
				       blk_zone_is_cmr(zone) ? "CMR" : "unknown",
				       zone->start);
		ret = BLKPREP_DONE;
		goto out;
	}
	if (blk_zone_is_empty(zone)) {
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding empty zone %zu\n",
				       zone->start);
		ret = BLKPREP_DONE;
		goto out;
	}

	if (zone->start != sector ||
	    zone->len < num_sectors) {
		sd_printk(KERN_ERR, sdkp,
			  "Misaligned RESET WP, start %zu/%zu "
			  "len %zu/%u\n",
			  zone->start, sector, zone->len, num_sectors);
		ret = BLKPREP_KILL;
		goto out;
	}

	/*
	 * Opportunistic setting, will be fixed up with
	 * zone update if RESET WRITE POINTER fails.
	 */
	zone->wp = zone->start;

out:
	spin_unlock_irqrestore(&zone->lock, flags);

	return ret;
}

int sd_zbc_setup_read_write(struct scsi_disk *sdkp, struct request *rq,
			    sector_t sector, unsigned int *num_sectors)
{
	struct blk_zone *zone;
	unsigned int sectors = *num_sectors;
	int ret = BLKPREP_OK;
	unsigned long flags;

	zone = blk_lookup_zone(sdkp->disk->queue, sector);
	if (!zone) {
		/* Might happen during zone initialization */
		sd_zbc_debug_ratelimit(sdkp,
				       "zone for sector %zu not found, skipping\n",
				       sector);
		return BLKPREP_OK;
	}

	spin_lock_irqsave(&zone->lock, flags);

	if (zone->state == BLK_ZONE_UNKNOWN ||
	    zone->state == BLK_ZONE_BUSY) {
		sd_zbc_debug_ratelimit(sdkp,
				       "zone %zu state %x, deferring\n",
				       zone->start, zone->state);
		ret = BLKPREP_DEFER;
		goto out;
	}
	if (zone->state == BLK_ZONE_OFFLINE) {
		/* let the drive fail the command */
		sd_zbc_debug_ratelimit(sdkp,
				       "zone %zu offline\n",
				       zone->start);
		goto out;
	}

	if (req_op(rq) == REQ_OP_WRITE || req_op(rq) == REQ_OP_WRITE_SAME) {
		if (zone->type != BLK_ZONE_TYPE_SEQWRITE_REQ)
			goto out;
		if (zone->state == BLK_ZONE_READONLY)
			goto out;
		if (blk_zone_is_full(zone)) {
			sd_zbc_debug(sdkp,
				     "Write to full zone %zu/%zu\n",
				     sector, zone->wp);
			ret = BLKPREP_KILL;
			goto out;
		}
		if (zone->wp != sector) {
			sd_zbc_debug(sdkp,
				     "Misaligned write %zu/%zu\n",
				     sector, zone->wp);
			ret = BLKPREP_KILL;
			goto out;
		}
		zone->wp += sectors;
	} else if (zone->type == BLK_ZONE_TYPE_SEQWRITE_REQ &&
		   zone->wp <= sector + sectors) {
		if (zone->wp <= sector) {
			/* Read beyond WP: clear request buffer */
			struct req_iterator iter;
			struct bio_vec bvec;
			void *buf;
			sd_zbc_debug(sdkp,
				     "Read beyond wp %zu+%u/%zu\n",
				     sector, sectors, zone->wp);
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
		*num_sectors = zone->wp - sector;
		sd_zbc_debug(sdkp,
			     "Read straddle wp %zu+%u/%zu => %zu+%u\n",
			     sector, sectors, zone->wp,
			     sector, *num_sectors);
	}

out:
	spin_unlock_irqrestore(&zone->lock, flags);

	return ret;
}

int sd_zbc_setup(struct scsi_disk *sdkp, char *buf, int buf_len)
{
	sector_t capacity = logical_to_sectors(sdkp->device, sdkp->capacity);
	sector_t last_sector;

	if (test_and_set_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags)) {
		sdev_printk(KERN_WARNING, sdkp->device,
			    "zone initialisation already running\n");
		return 0;
	}

	if (!sdkp->zone_work_q) {
		char wq_name[32];

		sprintf(wq_name, "zbc_wq_%s", sdkp->disk->disk_name);
		sdkp->zone_work_q = create_singlethread_workqueue(wq_name);
		if (!sdkp->zone_work_q) {
			sdev_printk(KERN_WARNING, sdkp->device,
				    "create zoned disk workqueue failed\n");
			return -ENOMEM;
		}
	} else if (!test_and_set_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags)) {
		drain_workqueue(sdkp->zone_work_q);
		clear_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags);
	}

	last_sector = zbc_parse_zones(sdkp, buf, buf_len);
	if (last_sector != -1 && last_sector < capacity) {
		sd_zbc_update_zones(sdkp, last_sector, SD_ZBC_BUF_SIZE, false);
	} else
		clear_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags);

	return 0;
}

void sd_zbc_remove(struct scsi_disk *sdkp)
{
	if (sdkp->zone_work_q) {
		if (!test_and_set_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags))
			drain_workqueue(sdkp->zone_work_q);
		clear_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags);
		destroy_workqueue(sdkp->zone_work_q);
	}
}
