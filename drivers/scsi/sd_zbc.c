/*
 * SCSI Zoned Block commands
 *
 * Copyright (C) 2014-2015 SUSE Linux GmbH
 * Written by: Hannes Reinecke <hare@suse.de>
 * Modified by: Damien Le Moal <damien.lemoal@hgst.com>
 * Modified by: Shaun Tancheff <shaun.tancheff@seagate.com>
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

enum zbc_zone_type {
	ZBC_ZONE_TYPE_CONV = 0x1,
	ZBC_ZONE_TYPE_SEQWRITE_REQ,
	ZBC_ZONE_TYPE_SEQWRITE_PREF,
	ZBC_ZONE_TYPE_RESERVED,
};

enum zbc_zone_cond {
	ZBC_ZONE_COND_NO_WP,
	ZBC_ZONE_COND_EMPTY,
	ZBC_ZONE_COND_IMP_OPEN,
	ZBC_ZONE_COND_EXP_OPEN,
	ZBC_ZONE_COND_CLOSED,
	ZBC_ZONE_COND_READONLY = 0xd,
	ZBC_ZONE_COND_FULL,
	ZBC_ZONE_COND_OFFLINE,
};

/**
 * Convert a zone descriptor to a zone struct.
 */
static void sd_zbc_parse_report(struct scsi_disk *sdkp,
				u8 *buf,
				struct blk_zone *zone)
{
	struct scsi_device *sdp = sdkp->device;

	memset(zone, 0, sizeof(struct blk_zone));

	zone->type = buf[0] & 0x0f;
	zone->cond = (buf[1] >> 4) & 0xf;
	if (buf[1] & 0x01)
		zone->reset = 1;
	if (buf[1] & 0x02)
		zone->non_seq = 1;

	zone->len = logical_to_sectors(sdp, get_unaligned_be64(&buf[8]));
	zone->start = logical_to_sectors(sdp, get_unaligned_be64(&buf[16]));
	zone->wp = logical_to_sectors(sdp, get_unaligned_be64(&buf[24]));
	if (zone->type != ZBC_ZONE_TYPE_CONV &&
	    zone->cond == ZBC_ZONE_COND_FULL)
		zone->wp = zone->start + zone->len;
}

/**
 * Issue a REPORT ZONES scsi command.
 */
static int sd_zbc_report_zones(struct scsi_disk *sdkp, unsigned char *buf,
			       unsigned int buflen, sector_t start_sector)
{
	struct scsi_device *sdp = sdkp->device;
	const int timeout = sdp->request_queue->rq_timeout;
	struct scsi_sense_hdr sshdr;
	sector_t start_lba = sectors_to_logical(sdkp->device, start_sector);
	unsigned char cmd[16];
	unsigned int rep_len;
	int result;

	memset(cmd, 0, 16);
	cmd[0] = ZBC_IN;
	cmd[1] = ZI_REPORT_ZONES;
	put_unaligned_be64(start_lba, &cmd[2]);
	put_unaligned_be32(buflen, &cmd[10]);
	memset(buf, 0, buflen);

	result = scsi_execute_req(sdp, cmd, DMA_FROM_DEVICE,
				  buf, buflen, &sshdr,
				  timeout, SD_MAX_RETRIES, NULL);
	if (result) {
		sd_printk(KERN_ERR, sdkp,
			  "REPORT ZONES lba %llu failed with %d/%d\n",
			  (unsigned long long)start_lba,
			  host_byte(result), driver_byte(result));
		return -EIO;
	}

	rep_len = get_unaligned_be32(&buf[0]);
	if (rep_len < 64) {
		sd_printk(KERN_ERR, sdkp,
			  "REPORT ZONES report invalid length %u\n",
			  rep_len);
		return -EIO;
	}

	return 0;
}

int sd_zbc_setup_report_cmnd(struct scsi_cmnd *cmd)
{
	struct request *rq = cmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	sector_t lba, sector = blk_rq_pos(rq);
	unsigned int nr_bytes = blk_rq_bytes(rq);
	int ret;

	WARN_ON(nr_bytes == 0);

	if (!sd_is_zoned(sdkp))
		/* Not a zoned device */
		return BLKPREP_KILL;

	ret = scsi_init_io(cmd);
	if (ret != BLKPREP_OK)
		return ret;

	cmd->cmd_len = 16;
	memset(cmd->cmnd, 0, cmd->cmd_len);
	cmd->cmnd[0] = ZBC_IN;
	cmd->cmnd[1] = ZI_REPORT_ZONES;
	lba = sectors_to_logical(sdkp->device, sector);
	put_unaligned_be64(lba, &cmd->cmnd[2]);
	put_unaligned_be32(nr_bytes, &cmd->cmnd[10]);
	/* Do partial report for speeding things up */
	cmd->cmnd[14] = ZBC_REPORT_ZONE_PARTIAL;

	cmd->sc_data_direction = DMA_FROM_DEVICE;
	cmd->sdb.length = nr_bytes;
	cmd->transfersize = sdkp->device->sector_size;
	cmd->allowed = 0;

	/*
	 * Report may return less bytes than requested. Make sure
	 * to report completion on the entire initial request.
	 */
	rq->__data_len = nr_bytes;

	return BLKPREP_OK;
}

static void sd_zbc_report_zones_complete(struct scsi_cmnd *scmd,
					 unsigned int good_bytes)
{
	struct request *rq = scmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	struct sg_mapping_iter miter;
	struct blk_zone_report_hdr hdr;
	struct blk_zone zone;
	unsigned int offset, bytes = 0;
	unsigned long flags;
	u8 *buf;

	if (good_bytes < 64)
		return;

	memset(&hdr, 0, sizeof(struct blk_zone_report_hdr));

	sg_miter_start(&miter, scsi_sglist(scmd), scsi_sg_count(scmd),
		       SG_MITER_TO_SG | SG_MITER_ATOMIC);

	local_irq_save(flags);
	while (sg_miter_next(&miter) && bytes < good_bytes) {

		buf = miter.addr;
		offset = 0;

		if (bytes == 0) {
			/* Set the report header */
			hdr.nr_zones = min_t(unsigned int,
					 (good_bytes - 64) / 64,
					 get_unaligned_be32(&buf[0]) / 64);
			memcpy(buf, &hdr, sizeof(struct blk_zone_report_hdr));
			offset += 64;
			bytes += 64;
		}

		/* Parse zone descriptors */
		while (offset < miter.length && hdr.nr_zones) {
			WARN_ON(offset > miter.length);
			buf = miter.addr + offset;
			sd_zbc_parse_report(sdkp, buf, &zone);
			memcpy(buf, &zone, sizeof(struct blk_zone));
			offset += 64;
			bytes += 64;
			hdr.nr_zones--;
		}

		if (!hdr.nr_zones)
			break;

	}
	sg_miter_stop(&miter);
	local_irq_restore(flags);
}

int sd_zbc_setup_reset_cmnd(struct scsi_cmnd *cmd)
{
	struct request *rq = cmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	sector_t sector = blk_rq_pos(rq);
	unsigned int zone_no = sector >> sdkp->zone_shift;

	if (!sd_is_zoned(sdkp))
		/* Not a zoned device */
		return BLKPREP_KILL;

	if (sector & (sdkp->zone_sectors - 1))
		/* Unaligned request */
		return BLKPREP_KILL;

	/* Do not allow concurrent reset and writes */
	if (!test_and_set_bit(zone_no, sdkp->zones_wlock))
		return BLKPREP_DEFER;

	cmd->cmd_len = 16;
	memset(cmd->cmnd, 0, cmd->cmd_len);
	cmd->cmnd[0] = ZBC_OUT;
	cmd->cmnd[1] = ZO_RESET_WRITE_POINTER;
	put_unaligned_be64(sectors_to_logical(sdkp->device, sector),
			   &cmd->cmnd[2]);

	rq->timeout = SD_TIMEOUT;
	cmd->sc_data_direction = DMA_NONE;
	cmd->transfersize = 0;
	cmd->allowed = 0;

	return BLKPREP_OK;
}

int sd_zbc_setup_read_write(struct scsi_disk *sdkp, struct request *rq,
			    sector_t sector, unsigned int sectors)
{
	sector_t zone_ofst = sector & (sdkp->zone_sectors - 1);

	/* Do not allow zone boundaries crossing */
	if (zone_ofst + sectors > sdkp->zone_sectors)
		return BLKPREP_KILL;

	/*
	 * Do not issue more than one write at a time per
	 * zone. This solves write ordering problems due to
	 * the unlocking of the request queue in the dispatch
	 * path in the non scsi-mq case. For scsi-mq, this
	 * also avoids potential write reordering when multiple
	 * threads running on different CPUs write to the same
	 * zone (with a synchronized sequential pattern).
	 */
	if (req_op(rq) == REQ_OP_WRITE ||
	    req_op(rq) == REQ_OP_WRITE_SAME) {
		unsigned int zone_no = sector >> sdkp->zone_shift;
		if (!test_and_set_bit(zone_no, sdkp->zones_wlock))
			return BLKPREP_DEFER;
	}

	return BLKPREP_OK;
}

void sd_zbc_complete(struct scsi_cmnd *cmd,
		     unsigned int good_bytes,
		     struct scsi_sense_hdr *sshdr)
{
	int result = cmd->result;
	struct request *rq = cmd->request;
	struct scsi_disk *sdkp = scsi_disk(rq->rq_disk);
	unsigned int zone_no;

	switch (req_op(rq)) {
	case REQ_OP_WRITE:
	case REQ_OP_WRITE_SAME:

		if (result &&
		    sshdr->sense_key == ILLEGAL_REQUEST &&
		    sshdr->asc == 0x21)
			/*
			 * It is unlikely that retrying write requests failed
			 * with any kind of alignement error will result in
			 * success. So don't.
			 */
			cmd->allowed = 0;

		/* Fallthru */

	case REQ_OP_ZONE_RESET:

		/* Unlock the zone */
		zone_no = blk_rq_pos(rq) >> sdkp->zone_shift;
		clear_bit_unlock(zone_no, sdkp->zones_wlock);
		smp_mb__after_atomic();

		if (result &&
		    sshdr->sense_key == ILLEGAL_REQUEST &&
		    sshdr->asc == 0x24)
			/*
			 * INVALID FIELD IN CDB error: Reset of a conventional
			 * zone was attempted. Nothing to worry about,
			 * so be quiet about the error.
			 */
			rq->cmd_flags |= REQ_QUIET;

		break;

	case REQ_OP_ZONE_REPORT:

		if (!result)
			sd_zbc_report_zones_complete(cmd, good_bytes);
		break;

	}
}

/**
 * Read zoned block device characteristics (VPD page B6).
 */
static int sd_zbc_read_zoned_charateristics(struct scsi_disk *sdkp,
					    unsigned char *buf)
{

	if (scsi_get_vpd_page(sdkp->device, 0xb6, buf, 64))
		return -ENODEV;

	if (sdkp->device->type != TYPE_ZBC) {
		/* Host-aware */
		sdkp->urswrz = 1;
		sdkp->zones_optimal_open = get_unaligned_be64(&buf[8]);
		sdkp->zones_optimal_nonseq = get_unaligned_be64(&buf[12]);
		sdkp->zones_max_open = 0;
	} else {
		/* Host-managed */
		sdkp->urswrz = buf[4] & 1;
		sdkp->zones_optimal_open = 0;
		sdkp->zones_optimal_nonseq = 0;
		sdkp->zones_max_open = get_unaligned_be64(&buf[16]);
	}

	return 0;
}

/**
 * Check reported capacity.
 */
static int sd_zbc_check_capacity(struct scsi_disk *sdkp,
				 unsigned char *buf)
{
	sector_t lba;
	int ret;

	if (sdkp->rc_basis != 0)
		return 0;

	/* Do a report zone to get the maximum LBA to check capacity */
	ret = sd_zbc_report_zones(sdkp, buf, SD_BUF_SIZE, 0);
	if (ret)
		return ret;

	/* The max_lba field is the capacity of this device */
	lba = get_unaligned_be64(&buf[8]);
	if (lba + 1 > sdkp->capacity) {
		if (sdkp->first_scan)
			sd_printk(KERN_WARNING, sdkp,
				  "Changing capacity from %zu "
				  "to max LBA+1 %llu\n",
				  sdkp->capacity,
				  (unsigned long long)lba + 1);
		sdkp->capacity = lba + 1;
	}

	return 0;
}

#define SD_ZBC_BUF_SIZE 131072

static int sd_zbc_check_zone_size(struct scsi_disk *sdkp)
{
	sector_t capacity = logical_to_sectors(sdkp->device, sdkp->capacity);
	struct blk_zone zone;
	sector_t sector = 0;
	unsigned char *buf;
	unsigned char *rec;
	unsigned int buf_len;
	unsigned int list_length;
	int ret;
	u8 same;

	/* Get a buffer */
	buf = kmalloc(SD_ZBC_BUF_SIZE, GFP_KERNEL);
	if (!buf)
		return -ENOMEM;

	/* Do a report zone to get the same field */
	ret = sd_zbc_report_zones(sdkp, buf, SD_ZBC_BUF_SIZE, 0);
	if (ret)
		goto out;

	same = buf[4] & 0x0f;
	if (same > 0) {
		unsigned char *rec = &buf[64];
		sdkp->zone_sectors = logical_to_sectors(sdkp->device,
						get_unaligned_be64(&rec[8]));
		goto out;
	}

	/* Check the size of all zones */
	sdkp->zone_sectors = (sector_t)-1;
	do {

		/* Parse REPORT ZONES header */
		list_length = get_unaligned_be32(&buf[0]) + 64;
		rec = buf + 64;
		if (list_length < SD_ZBC_BUF_SIZE)
			buf_len = list_length;
		else
			buf_len = SD_ZBC_BUF_SIZE;

		/* Parse zone descriptors */
		while (rec < buf + buf_len) {
			sd_zbc_parse_report(sdkp, rec, &zone);
			if (sdkp->zone_sectors == (sector_t)-1) {
				sdkp->zone_sectors = zone.len;
			} else if (sector + zone.len < capacity &&
				   zone.len != sdkp->zone_sectors) {
				sdkp->zone_sectors = 0;
				goto out;
			}
			sector += zone.len;
			rec += 64;
		}

		if (sector < capacity) {
			ret = sd_zbc_report_zones(sdkp, buf,
					SD_ZBC_BUF_SIZE, sector);
			if (ret)
				return ret;
		}

	} while (sector < capacity);

out:
	kfree(buf);

	if (!sdkp->zone_sectors) {
		if (sdkp->first_scan)
			sd_printk(KERN_NOTICE, sdkp,
				  "Devices with non constant zone "
				  "size are not supported\n");
		return -ENODEV;
	}

	if (!is_power_of_2(sdkp->zone_sectors)) {
		if (sdkp->first_scan)
			sd_printk(KERN_NOTICE, sdkp,
				  "Devices with non power of 2 zone "
				  "size are not supported\n");
		return -ENODEV;
	}

	if ((sdkp->zone_sectors << 9) > UINT_MAX) {
		if (sdkp->first_scan)
			sd_printk(KERN_NOTICE, sdkp,
				  "Zone size too large\n");
		return -ENODEV;
	}

	return 0;
}

static int sd_zbc_setup(struct scsi_disk *sdkp)
{
	sector_t capacity = logical_to_sectors(sdkp->device, sdkp->capacity);

	/* chunk_sectors indicates the zone size */
	blk_queue_chunk_sectors(sdkp->disk->queue, sdkp->zone_sectors);
	sdkp->zone_shift = ilog2(sdkp->zone_sectors);
	sdkp->nr_zones = capacity >> sdkp->zone_shift;
	if (capacity & (sdkp->zone_sectors - 1))
		sdkp->nr_zones++;

	if (!sdkp->zones_wlock) {
		sdkp->zones_wlock = kzalloc(BITS_TO_LONGS(sdkp->nr_zones),
					    GFP_KERNEL);
		if (!sdkp->zones_wlock)
			return -ENOMEM;
	}

	return 0;
}

int sd_zbc_read_zones(struct scsi_disk *sdkp,
		      unsigned char *buf)
{
	sector_t capacity;
	int ret = 0;

	if (!sd_is_zoned(sdkp))
		/*
		 * Device managed or normal SCSI disk,
		 * no special handling required
		 */
		return 0;


	/* Get zoned block device characteristics */
	ret = sd_zbc_read_zoned_charateristics(sdkp, buf);
	if (ret)
		return ret;

	/*
	 * Check for unconstrained reads: host-managed devices with
	 * constrained reads (drives failing read after write pointer)
	 * are not supported.
	 */
	if (!sdkp->urswrz) {
		if (sdkp->first_scan)
			sd_printk(KERN_NOTICE, sdkp,
			  "constrained reads devices are not supported\n");
		return -ENODEV;
	}

	/* Check capacity */
	ret = sd_zbc_check_capacity(sdkp, buf);
	if (ret)
		return ret;
	capacity = logical_to_sectors(sdkp->device, sdkp->capacity);

	/*
	 * Check zone size: only devices with a constant zone size (except
	 * an eventual last runt zone) that is a power of 2 are supported.
	 */
	ret = sd_zbc_check_zone_size(sdkp);
	if (ret)
		return ret;

	/* The drive satisfies the kernel restrictions: set it up */
	ret = sd_zbc_setup(sdkp);
	if (ret)
		return ret;

	if (sdkp->first_scan) {
		if (sdkp->nr_zones * sdkp->zone_sectors == capacity)
			sd_printk(KERN_NOTICE, sdkp,
				  "%u zones of %llu sectors\n",
				  sdkp->nr_zones,
				  (unsigned long long)sdkp->zone_sectors);
		else
			sd_printk(KERN_NOTICE, sdkp,
				  "%u zones of %llu sectors "
				  "+ 1 runt zone\n",
				  sdkp->nr_zones - 1,
				  (unsigned long long)sdkp->zone_sectors);
	}

	return 0;
}

void sd_zbc_remove(struct scsi_disk *sdkp)
{
	kfree(sdkp->zones_wlock);
	sdkp->zones_wlock = NULL;
}
