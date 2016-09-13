/*
 * Functions for zone based SMR devices.
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

#ifndef _UAPI_BLKZONED_API_H
#define _UAPI_BLKZONED_API_H

#include <linux/types.h>

#define ZBC_REPORT_OPTION_MASK  0x3f
#define ZBC_REPORT_ZONE_PARTIAL 0x80

/**
 * enum zone_report_option - Report Zones types to be included.
 *
 * @ZBC_ZONE_REPORTING_OPTION_ALL: Default (all zones).
 * @ZBC_ZONE_REPORTING_OPTION_EMPTY: Zones which are empty.
 * @ZBC_ZONE_REPORTING_OPTION_IMPLICIT_OPEN:
 *	Zones open but not explicitly opened
 * @ZBC_ZONE_REPORTING_OPTION_EXPLICIT_OPEN: Zones opened explicitly
 * @ZBC_ZONE_REPORTING_OPTION_CLOSED: Zones closed for writing.
 * @ZBC_ZONE_REPORTING_OPTION_FULL: Zones that are full.
 * @ZBC_ZONE_REPORTING_OPTION_READONLY: Zones that are read-only
 * @ZBC_ZONE_REPORTING_OPTION_OFFLINE: Zones that are offline
 * @ZBC_ZONE_REPORTING_OPTION_NEED_RESET_WP: Zones with Reset WP Recommended
 * @ZBC_ZONE_REPORTING_OPTION_RESERVED: Zones that with Non-Sequential
 *	Write Resources Active
 * @ZBC_ZONE_REPORTING_OPTION_NON_WP: Zones that do not have Write Pointers
 *	(conventional)
 * @ZBC_ZONE_REPORTING_OPTION_RESERVED: Undefined
 * @ZBC_ZONE_REPORTING_OPTION_PARTIAL: Modifies the definition of the Zone List
 *	Length field.
 *
 * Used by Report Zones in bdev_zone_get_report: report_option
 */
enum zbc_zone_reporting_options {
	ZBC_ZONE_REPORTING_OPTION_ALL = 0,
	ZBC_ZONE_REPORTING_OPTION_EMPTY,
	ZBC_ZONE_REPORTING_OPTION_IMPLICIT_OPEN,
	ZBC_ZONE_REPORTING_OPTION_EXPLICIT_OPEN,
	ZBC_ZONE_REPORTING_OPTION_CLOSED,
	ZBC_ZONE_REPORTING_OPTION_FULL,
	ZBC_ZONE_REPORTING_OPTION_READONLY,
	ZBC_ZONE_REPORTING_OPTION_OFFLINE,
	ZBC_ZONE_REPORTING_OPTION_NEED_RESET_WP = 0x10,
	ZBC_ZONE_REPORTING_OPTION_NON_SEQWRITE,
	ZBC_ZONE_REPORTING_OPTION_NON_WP = 0x3f,
	ZBC_ZONE_REPORTING_OPTION_RESERVED = 0x40,
	ZBC_ZONE_REPORTING_OPTION_PARTIAL = ZBC_REPORT_ZONE_PARTIAL
};

/**
 * enum blk_zone_type - Types of zones allowed in a zoned device.
 *
 * @BLK_ZONE_TYPE_RESERVED: Reserved.
 * @BLK_ZONE_TYPE_CONVENTIONAL: Zone has no WP. Zone commands are not available.
 * @BLK_ZONE_TYPE_SEQWRITE_REQ: Zone must be written sequentially
 * @BLK_ZONE_TYPE_SEQWRITE_PREF: Zone may be written non-sequentially
 */
enum blk_zone_type {
	BLK_ZONE_TYPE_RESERVED,
	BLK_ZONE_TYPE_CONVENTIONAL,
	BLK_ZONE_TYPE_SEQWRITE_REQ,
	BLK_ZONE_TYPE_SEQWRITE_PREF,
	BLK_ZONE_TYPE_UNKNOWN,
};

/**
 * enum blk_zone_state - State [condition] of a zone in a zoned device.
 *
 * @BLK_ZONE_NO_WP: Zone has not write pointer it is CMR/Conventional
 * @BLK_ZONE_EMPTY: Zone is empty. Write pointer is at the start of the zone.
 * @BLK_ZONE_OPEN: Zone is open, but not explicitly opened by a zone open cmd.
 * @BLK_ZONE_OPEN_EXPLICIT: Zones was explicitly opened by a zone open cmd.
 * @BLK_ZONE_CLOSED: Zone was [explicitly] closed for writing.
 * @BLK_ZONE_UNKNOWN: Zone states 0x5 through 0xc are reserved by standard.
 * @BLK_ZONE_FULL: Zone was [explicitly] marked full by a zone finish cmd.
 * @BLK_ZONE_READONLY: Zone is read-only.
 * @BLK_ZONE_OFFLINE: Zone is offline.
 * @BLK_ZONE_BUSY: [INTERNAL] Kernel zone cache for this zone is being updated.
 *
 * The Zone Condition state machine also maps the above deinitions as:
 *   - ZC1: Empty         | BLK_ZONE_EMPTY
 *   - ZC2: Implicit Open | BLK_ZONE_OPEN
 *   - ZC3: Explicit Open | BLK_ZONE_OPEN_EXPLICIT
 *   - ZC4: Closed        | BLK_ZONE_CLOSED
 *   - ZC5: Full          | BLK_ZONE_FULL
 *   - ZC6: Read Only     | BLK_ZONE_READONLY
 *   - ZC7: Offline       | BLK_ZONE_OFFLINE
 *
 * States 0x5 to 0xC are reserved by the current ZBC/ZAC spec.
 */
enum blk_zone_state {
	BLK_ZONE_NO_WP,
	BLK_ZONE_EMPTY,
	BLK_ZONE_OPEN,
	BLK_ZONE_OPEN_EXPLICIT,
	BLK_ZONE_CLOSED,
	BLK_ZONE_UNKNOWN = 0x5,
	BLK_ZONE_READONLY = 0xd,
	BLK_ZONE_FULL = 0xe,
	BLK_ZONE_OFFLINE = 0xf,
	BLK_ZONE_BUSY = 0x10,
};

/**
 * enum bdev_zone_same - Report Zones same code.
 *
 * @BLK_ZONE_SAME_ALL_DIFFERENT: All zones differ in type and size.
 * @BLK_ZONE_SAME_ALL: All zones are the same size and type.
 * @BLK_ZONE_SAME_LAST_DIFFERS: All zones are the same size and type
 *    except the last zone which differs by size.
 * @BLK_ZONE_SAME_LEN_TYPES_DIFFER: All zones are the same length
 *    but zone types differ.
 *
 * Returned from Report Zones. See bdev_zone_report* same_field.
 */
enum blk_zone_same {
	BLK_ZONE_SAME_ALL_DIFFERENT     = 0,
	BLK_ZONE_SAME_ALL               = 1,
	BLK_ZONE_SAME_LAST_DIFFERS      = 2,
	BLK_ZONE_SAME_LEN_TYPES_DIFFER  = 3,
};

/**
 * struct bdev_zone_get_report - ioctl: Report Zones request
 *
 * @zone_locator_lba: starting lba for first [reported] zone
 * @return_page_count: number of *bytes* allocated for result
 * @report_option: see: zone_report_option enum
 * @force_unit_access: Force report from media
 *
 * Used to issue report zones command to connected device
 */
struct bdev_zone_get_report {
	__u64 zone_locator_lba;
	__u32 return_page_count;
	__u8  report_option;
	__u8  force_unit_access;
} __packed;

/**
 * struct bdev_zone_action - ioctl: Perform Zone Action
 *
 * @zone_locator_lba: starting lba for first [reported] zone
 * @return_page_count: number of *bytes* allocated for result
 * @action: One of the ZONE_ACTION_*'s Close,Finish,Open, or Reset
 * @all_zones: Flag to indicate if command should apply to all zones.
 * @force_unit_access: Force command to media and update zone cache on success
 *
 * Used to issue report zones command to connected device
 */
struct bdev_zone_action {
	__u64 zone_locator_lba;
	__u32 action;
	__u8  all_zones;
	__u8  force_unit_access;
} __packed;

/**
 * struct bdev_zone_descriptor - A Zone descriptor entry from report zones
 *
 * @type: see zone_type enum
 * @flags: Bits 0:reset, 1:non-seq, 2-3: resv, 4-7: see zone_condition enum
 * @reserved1: padding
 * @length: length of zone in sectors
 * @lba_start:  lba where the zone starts.
 * @lba_wptr: lba of the current write pointer.
 * @reserved: padding
 *
 * NOTE: lba_start/lba_wptr are reported in absolute drive offsets from lba 0.
 *       Zone reports made against an partition are adjusted in the block-layer
 *       to report relative offsets from the start of the partition. Therefore
 *       a negative start/wptr value is possible when the partition is not
 *       zone aligned.
 */
struct bdev_zone_descriptor {
	__u8 type;
	__u8 flags;
	__u8  reserved1[6];
	__be64 length;
	__be64 lba_start;
	__be64 lba_wptr;
	__u8 reserved[32];
} __packed;

/**
 * struct bdev_zone_report - Report Zones result
 *
 * @descriptor_count: Number of descriptor entries that follow
 * @same_field: bits 0-3: enum zone_same (MASK: 0x0F)
 * @reserved1: padding
 * @maximum_lba: LBA of the last logical sector on the device, inclusive
 *               of all logical sectors in all zones.
 * @reserved2: padding
 * @descriptors: array of descriptors follows.
 */
struct bdev_zone_report {
	__be32 descriptor_count;
	__u8 same_field;
	__u8 reserved1[3];
	__be64 maximum_lba;
	__u8 reserved2[48];
	struct bdev_zone_descriptor descriptors[0];
} __packed;

/**
 * struct bdev_zone_report_io - Report Zones ioctl argument.
 *
 * @in: Report Zones inputs
 * @out: Report Zones output
 */
struct bdev_zone_report_io {
	union {
		struct bdev_zone_get_report in;
		struct bdev_zone_report out;
	} data;
} __packed;


static inline u32 max_report_entries(size_t bytes)
{
	bytes -= sizeof(struct bdev_zone_report);
	return bytes / sizeof(struct bdev_zone_descriptor);
}

/* continuing from uapi/linux/fs.h: */
#define BLKREPORT	_IOWR(0x12, 130, struct bdev_zone_report_io)
#define BLKZONEACTION	_IOW(0x12, 131, struct bdev_zone_action)

#define ZONE_ACTION_CLOSE	0x01
#define ZONE_ACTION_FINISH	0x02
#define ZONE_ACTION_OPEN	0x03
#define ZONE_ACTION_RESET	0x04

#endif /* _UAPI_BLKZONED_API_H */
