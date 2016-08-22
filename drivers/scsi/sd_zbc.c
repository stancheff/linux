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

static
struct blk_zone *zbc_desc_to_zone(struct scsi_disk *sdkp, unsigned char *rec)
{
	struct blk_zone *zone;
	sector_t wp = (sector_t)-1;

	zone = kzalloc(sizeof(struct blk_zone), GFP_KERNEL);
	if (!zone)
		return NULL;

	spin_lock_init(&zone->lock);
	zone->type = rec[0] & 0xf;
	zone->state = (rec[1] >> 4) & 0xf;
	zone->len = logical_to_sectors(sdkp->device,
				       get_unaligned_be64(&rec[8]));
	zone->start = logical_to_sectors(sdkp->device,
					 get_unaligned_be64(&rec[16]));

	if (blk_zone_is_smr(zone))
		wp = logical_to_sectors(sdkp->device,
					get_unaligned_be64(&rec[24]));
	zone->wp = wp;
	/*
	 * Fixup block zone state
	 */
	if (zone->state == BLK_ZONE_EMPTY &&
	    zone->wp != zone->start) {
		sd_zbc_debug(sdkp,
			     "zone %zu state EMPTY wp %zu: adjust wp\n",
			     zone->start, zone->wp);
		zone->wp = zone->start;
	}
	if (zone->state == BLK_ZONE_FULL &&
	    zone->wp != zone->start + zone->len) {
		sd_zbc_debug(sdkp,
			     "zone %zu state FULL wp %zu: adjust wp\n",
			     zone->start, zone->wp);
		zone->wp = zone->start + zone->len;
	}

	return zone;
}

static
sector_t zbc_parse_zones(struct scsi_disk *sdkp, u64 zlen, unsigned char *buf,
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

		if (same == 0 && this->len != zlen) {
			next_sector = this->start + this->len;
			break;
		}

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

static void sd_zbc_refresh_zone_work(struct work_struct *work)
{
	struct zbc_update_work *zbc_work =
		container_of(work, struct zbc_update_work, zone_work);
	struct scsi_disk *sdkp = zbc_work->sdkp;
	struct request_queue *q = sdkp->disk->queue;
	unsigned char *zone_buf = zbc_work->zone_buf;
	unsigned int zone_buflen = zbc_work->zone_buflen;
	int ret;
	u8 same;
	u64 zlen = 0;
	sector_t last_sector;
	sector_t capacity = logical_to_sectors(sdkp->device, sdkp->capacity);

	ret = sd_zbc_report_zones(sdkp, zone_buf, zone_buflen,
				  zbc_work->zone_sector,
				  ZBC_ZONE_REPORTING_OPTION_ALL, true);
	if (ret)
		goto done_free;

	/* this whole path is unlikely so extra reports shouldn't be a
	 * large impact */
	same = zone_buf[4] & 0xf;
	if (same == 0) {
		unsigned char *desc = &zone_buf[64];
		unsigned int blen = zone_buflen;

		/* just pull the first zone */
		if (blen > 512)
			blen = 512;
		ret = sd_zbc_report_zones(sdkp, zone_buf, blen, 0,
					  ZBC_ZONE_REPORTING_OPTION_ALL, true);
		if (ret)
			goto done_free;

		/* Read the zone length from the first zone descriptor */
		zlen = logical_to_sectors(sdkp->device,
					  get_unaligned_be64(&desc[8]));

		ret = sd_zbc_report_zones(sdkp, zone_buf, zone_buflen,
					  zbc_work->zone_sector,
					  ZBC_ZONE_REPORTING_OPTION_ALL, true);
		if (ret)
			goto done_free;
	}

	last_sector = zbc_parse_zones(sdkp, zlen, zone_buf, zone_buflen);
	capacity = logical_to_sectors(sdkp->device, sdkp->capacity);
	if (last_sector != -1 && last_sector < capacity) {
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
	struct blk_zone *zone;
	struct rb_node *node;
	int zone_num = 0, zone_busy = 0, num_rec;
	sector_t next_sector = sector;

	if (test_bit(SD_ZBC_ZONE_RESET, &sdkp->zone_flags)) {
		sd_zbc_debug(sdkp,
			     "zones in reset, not starting reason\n");
		return;
	}

	if (reason != SD_ZBC_INIT) {
		/* lookup sector, is zone pref? then ignore */
		struct blk_zone *zone = blk_lookup_zone(q, sector);

		if (reason == SD_ZBC_RESET_WP)
			sd_zbc_debug(sdkp, "RESET WP failed %lx\n", sector);

		if (zone && blk_zone_is_seq_pref(zone))
			return;
	}

retry:
	zbc_work = kzalloc(sizeof(struct zbc_update_work) + bufsize,
			   reason != SD_ZBC_INIT ? GFP_NOWAIT : GFP_KERNEL);
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

	sd_zbc_debug(sdkp, "REPORT ZONES lba %zu len %d\n", start_lba, bufflen);

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
	struct scsi_device *sdp = cmd->device;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	sector_t sector = blk_rq_pos(rq);
	unsigned int nr_sectors = blk_rq_sectors(rq);
	int ret = BLKPREP_OK;
	struct blk_zone *zone;
	unsigned long flags;
	u32 wp_offset;
	bool use_write_same = false;

	zone = blk_lookup_zone(rq->q, sector);
	if (!zone) {
		/* Test for a runt zone before giving up */
		if (sdp->type != TYPE_ZBC) {
			struct request_queue *q = rq->q;
			struct rb_node *node;

			node = rb_last(&q->zones);
			if (node)
				zone = rb_entry(node, struct blk_zone, node);
			if (zone) {
				spin_lock_irqsave(&zone->lock, flags);
				if ((zone->start + zone->len) <= sector)
					goto out;
				spin_unlock_irqrestore(&zone->lock, flags);
				zone = NULL;
			}
		}
		return BLKPREP_KILL;
	}

	spin_lock_irqsave(&zone->lock, flags);
	if (zone->state == BLK_ZONE_UNKNOWN ||
	    zone->state == BLK_ZONE_BUSY) {
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding zone %zx state %x, deferring\n",
				       zone->start, zone->state);
		ret = BLKPREP_DEFER;
		goto out;
	}
	if (zone->state == BLK_ZONE_OFFLINE) {
		/* let the drive fail the command */
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding offline zone %zx\n",
				       zone->start);
		goto out;
	}
	if (blk_zone_is_cmr(zone)) {
		use_write_same = true;
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding CMR zone %zx\n",
				       zone->start);
		goto out;
	}
	if (zone->start != sector || zone->len < nr_sectors) {
		sd_printk(KERN_ERR, sdkp,
			  "Misaligned RESET WP %zx/%x on zone %zx/%zx\n",
			  sector, nr_sectors, zone->start, zone->len);
		ret = BLKPREP_KILL;
		goto out;
	}
	/* Protect against Reset WP when more data had been written to the
	 * zone than is being discarded.
	 */
	wp_offset = zone->wp - zone->start;
	if (wp_offset > nr_sectors) {
		sd_printk(KERN_ERR, sdkp,
			  "Will Corrupt RESET WP %zx/%x/%x on zone %zx/%zx/%zx\n",
			  sector, wp_offset, nr_sectors,
			  zone->start, zone->wp, zone->len);
		ret = BLKPREP_KILL;
		goto out;
	}
	if (blk_zone_is_empty(zone)) {
		sd_zbc_debug_ratelimit(sdkp,
				       "Discarding empty zone %zx [WP: %zx]\n",
				       zone->start, zone->wp);
		ret = BLKPREP_DONE;
		goto out;
	}

out:
	/*
	 * Opportunistic setting, will be fixed up with
	 * zone update if RESET WRITE POINTER fails.
	 */
	if (ret == BLKPREP_OK && !use_write_same)
		zone->wp = zone->start;
	spin_unlock_irqrestore(&zone->lock, flags);

	if (ret == BLKPREP_OK)
		discard_or_write_same(cmd, sector, nr_sectors, use_write_same);

	return ret;
}


static void __set_zone_state(struct blk_zone *zone, int op)
{
	unsigned long flags;

	spin_lock_irqsave(&zone->lock, flags);
	if (blk_zone_is_cmr(zone))
		goto out_unlock;

	switch (op) {
	case REQ_OP_ZONE_OPEN:
		zone->state = BLK_ZONE_OPEN_EXPLICIT;
		break;
	case REQ_OP_ZONE_FINISH:
		zone->state = BLK_ZONE_FULL;
		zone->wp = zone->start + zone->len;
		break;
	case REQ_OP_ZONE_CLOSE:
		zone->state = BLK_ZONE_CLOSED;
		break;
	case REQ_OP_ZONE_RESET:
		zone->wp = zone->start;
		break;
	default:
		WARN_ONCE(1, "%s: invalid op code: %u\n", __func__, op);
	}
out_unlock:
	spin_unlock_irqrestore(&zone->lock, flags);
}

static void update_zone_state(struct request *rq, sector_t lba, unsigned int op)
{
	struct request_queue *q = rq->q;
	struct blk_zone *zone = NULL;

	if (lba == ~0ul) {
		struct rb_node *node;

		for (node = rb_first(&q->zones); node; node = rb_next(node)) {
			zone = rb_entry(node, struct blk_zone, node);
			__set_zone_state(zone, op);
		}
		return;
	} else {
		zone = blk_lookup_zone(q, lba);
		if (zone)
			__set_zone_state(zone, op);
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
	unsigned long flags;
	unsigned int nr_sectors;
	int ret = BLKPREP_DONE;
	int op = req_op(rq);
	bool is_fua = (rq->cmd_flags & REQ_META) ? true : false;
	bool use_write_same = false;

	if (is_fua || op != REQ_OP_ZONE_RESET)
		goto out;

	zone = blk_lookup_zone(rq->q, sector);
	if (!zone || sdkp->provisioning_mode != SD_ZBC_RESET_WP)
		goto out;

	/* Map a Reset WP w/o FUA to a discard request */
	spin_lock_irqsave(&zone->lock, flags);
	sector = zone->start;
	nr_sectors = zone->len;
	if (blk_zone_is_cmr(zone))
		use_write_same = true;
	spin_unlock_irqrestore(&zone->lock, flags);

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
	struct request_queue *q = rq->q;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	struct blk_zone *zone = NULL;
	struct rb_node *node = NULL;
	sector_t progress = lba;
	sector_t clen = ~0ul;
	unsigned long flags;
	u32 max_entries = bzrpt ? max_report_entries(sz) : sz / sizeof(*bzd);
	u32 entry = 0;
	int len_diffs = 0;
	int type_diffs = 0;
	u8 ctype;
	u8 same = 0;

	zone = blk_lookup_zone(q, lba);
	if (zone)
		node = &zone->node;

	for (entry = 0; entry < max_entries && node; node = rb_next(node)) {
		u64 z_len, z_start, z_wp_abs;
		u8 cond = 0;
		u8 flgs = 0;

		spin_lock_irqsave(&zone->lock, flags);
		z_len = zone->len;
		z_start = zone->start;
		z_wp_abs = zone->wp;
		progress = z_start + z_len;
		cond = zone->state;
		if (blk_zone_is_cmr(zone))
			flgs |= 0x02;
		else if (zone->wp != zone->start)
			flgs |= 0x01; /* flag as RWP recommended? */
		spin_unlock_irqrestore(&zone->lock, flags);

		switch (opt & ZBC_REPORT_OPTION_MASK) {
		case ZBC_ZONE_REPORTING_OPTION_EMPTY:
			if (z_wp_abs != z_start)
				continue;
			break;
		case ZBC_ZONE_REPORTING_OPTION_IMPLICIT_OPEN:
			if (cond != BLK_ZONE_OPEN)
				continue;
			break;
		case ZBC_ZONE_REPORTING_OPTION_EXPLICIT_OPEN:
			if (cond != BLK_ZONE_OPEN_EXPLICIT)
				continue;
			break;
		case ZBC_ZONE_REPORTING_OPTION_CLOSED:
			if (cond != BLK_ZONE_CLOSED)
				continue;
			break;
		case ZBC_ZONE_REPORTING_OPTION_FULL:
			if (cond != BLK_ZONE_FULL)
				continue;
			break;
		case ZBC_ZONE_REPORTING_OPTION_READONLY:
			if (cond == BLK_ZONE_READONLY)
				continue;
			break;
		case ZBC_ZONE_REPORTING_OPTION_OFFLINE:
			if (cond == BLK_ZONE_OFFLINE)
				continue;
			break;
		case ZBC_ZONE_REPORTING_OPTION_NEED_RESET_WP:
			if (z_wp_abs == z_start)
				continue;
			break;
		case ZBC_ZONE_REPORTING_OPTION_NON_WP:
			if (cond == BLK_ZONE_NO_WP)
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

		/* shift to device units */
		z_start >>= ilog2(sdkp->device->sector_size) - 9;
		z_len >>= ilog2(sdkp->device->sector_size) - 9;
		z_wp_abs >>= ilog2(sdkp->device->sector_size) - 9;

		if (!bzd) {
			if (bzrpt)
				bzrpt->descriptor_count =
					cpu_to_be32(++entry);
			continue;
		}

		bzd[entry].lba_start = cpu_to_be64(z_start);
		bzd[entry].length = cpu_to_be64(z_len);
		bzd[entry].lba_wptr = cpu_to_be64(z_wp_abs);
		bzd[entry].type = zone->type;
		bzd[entry].flags = cond << 4 | flgs;
		entry++;
		if (bzrpt)
			bzrpt->descriptor_count = cpu_to_be32(entry);
	}

	/* if same code applies to all zones */
	if (bzrpt && !(opt & ZBC_REPORT_ZONE_PARTIAL)) {
		for (node = rb_first(&q->zones); node; node = rb_next(node)) {
			zone = rb_entry(node, struct blk_zone, node);

			spin_lock_irqsave(&zone->lock, flags);
			if (clen != ~0ul) {
				clen = zone->len;
				ctype = zone->type;
			}
			if (zone->len != clen)
				len_diffs++;
			if (zone->type != ctype)
				type_diffs++;
			ctype = zone->type;
			spin_unlock_irqrestore(&zone->lock, flags);
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

	if (blk_zone_is_cmr(zone))
		goto out;

	if (zone->state == BLK_ZONE_UNKNOWN ||
	    zone->state == BLK_ZONE_BUSY) {
		sd_zbc_debug_ratelimit(sdkp,
				       "zone %zu state %x, deferring\n",
				       zone->start, zone->state);
		ret = BLKPREP_DEFER;
		goto out;
	}

	if (blk_zone_is_seq_pref(zone)) {
		if (op_is_write(req_op(rq))) {
			u64 nwp = sector + sectors;

			while (nwp > (zone->start + zone->len)) {
				struct rb_node *node = rb_next(&zone->node);

				zone->wp = zone->start + zone->len;
				sector = zone->wp;
				sectors = nwp - zone->wp;
				spin_unlock_irqrestore(&zone->lock, flags);

				if (!node)
					return BLKPREP_OK;
				zone = rb_entry(node, struct blk_zone, node);
				if (!zone)
					return BLKPREP_OK;

				spin_lock_irqsave(&zone->lock, flags);
				nwp = sector + sectors;
			}
			if (nwp > zone->wp)
				zone->wp = nwp;
		}
		goto out;
	}

	if (zone->state == BLK_ZONE_OFFLINE) {
		/* let the drive fail the command */
		sd_zbc_debug_ratelimit(sdkp,
				       "zone %zu offline\n",
				       zone->start);
		goto out;
	}

	if (op_is_write(req_op(rq))) {
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
	} else if (zone->wp <= sector + sectors) {
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
			unsigned long flags;

			if (!z_len)
				goto done;

			zone = blk_lookup_zone(rq->q, s);
			if (!zone)
				goto done;

			spin_lock_irqsave(&zone->lock, flags);
			zone->type = entry->type & 0xF;
			zone->state = (entry->flags >> 4) & 0xF;
			zone->wp = get_wp_from_desc(sdkp, entry);
			zone->len = z_len;
			spin_unlock_irqrestore(&zone->lock, flags);
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
 * sd_zbc_setup - Load zones of matching zlen size into rb tree.
 *
 */
int sd_zbc_setup(struct scsi_disk *sdkp, u64 zlen, char *buf, int buf_len)
{
	sector_t capacity = logical_to_sectors(sdkp->device, sdkp->capacity);
	sector_t last_sector;

	if (test_and_set_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags)) {
		sdev_printk(KERN_WARNING, sdkp->device,
			    "zone initialization already running\n");
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

	last_sector = zbc_parse_zones(sdkp, zlen, buf, buf_len);
	capacity = logical_to_sectors(sdkp->device, sdkp->capacity);
	if (last_sector != -1 && last_sector < capacity) {
		sd_zbc_update_zones(sdkp, last_sector,
				    SD_ZBC_BUF_SIZE, SD_ZBC_INIT);
	} else
		clear_bit(SD_ZBC_ZONE_INIT, &sdkp->zone_flags);

	return 0;
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
	unsigned int bytes = 1;
	struct request_queue *q = sdkp->disk->queue;
	struct rb_node *node = rb_first(&q->zones);

	if (node) {
		struct blk_zone *zone = rb_entry(node, struct blk_zone, node);

		bytes = zone->len;
	}
	bytes <<= ilog2(sdkp->device->sector_size);
	return bytes;
}
