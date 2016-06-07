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

/**
 * enum zone_report_option - Report Zones types to be included.
 *
 * @ZOPT_NON_SEQ_AND_RESET: Default (all zones).
 * @ZOPT_ZC1_EMPTY: Zones which are empty.
 * @ZOPT_ZC2_OPEN_IMPLICIT: Zones open but not explicitly opened
 * @ZOPT_ZC3_OPEN_EXPLICIT: Zones opened explicitly
 * @ZOPT_ZC4_CLOSED: Zones closed for writing.
 * @ZOPT_ZC5_FULL: Zones that are full.
 * @ZOPT_ZC6_READ_ONLY: Zones that are read-only
 * @ZOPT_ZC7_OFFLINE: Zones that are offline
 * @ZOPT_RESET: Zones that are empty
 * @ZOPT_NON_SEQ: Zones that have HA media-cache writes pending
 * @ZOPT_NON_WP_ZONES: Zones that do not have Write Pointers (conventional)
 *
 * @ZOPT_USE_ATA_PASS: Flag used in kernel to service command I/O
 *
 * Used by Report Zones in bdev_zone_get_report: report_option
 */
enum zone_report_option {
	ZOPT_NON_SEQ_AND_RESET   = 0x00,
	ZOPT_ZC1_EMPTY,
	ZOPT_ZC2_OPEN_IMPLICIT,
	ZOPT_ZC3_OPEN_EXPLICIT,
	ZOPT_ZC4_CLOSED,
	ZOPT_ZC5_FULL,
	ZOPT_ZC6_READ_ONLY,
	ZOPT_ZC7_OFFLINE,
	ZOPT_RESET               = 0x10,
	ZOPT_NON_SEQ             = 0x11,
	ZOPT_NON_WP_ZONES        = 0x3f,
	ZOPT_USE_ATA_PASS        = 0x80,
};

/**
 * enum bdev_zone_type - Type of zone in descriptor
 *
 * @ZTYP_RESERVED: Reserved
 * @ZTYP_CONVENTIONAL: Conventional random write zone (No Write Pointer)
 * @ZTYP_SEQ_WRITE_REQUIRED: Non-sequential writes are rejected.
 * @ZTYP_SEQ_WRITE_PREFERRED: Non-sequential writes allowed but discouraged.
 *
 * Returned from Report Zones. See bdev_zone_descriptor* type.
 */
enum bdev_zone_type {
	ZTYP_RESERVED            = 0,
	ZTYP_CONVENTIONAL        = 1,
	ZTYP_SEQ_WRITE_REQUIRED  = 2,
	ZTYP_SEQ_WRITE_PREFERRED = 3,
};


/**
 * enum bdev_zone_condition - Condition of zone in descriptor
 *
 * @ZCOND_CONVENTIONAL: N/A
 * @ZCOND_ZC1_EMPTY: Empty
 * @ZCOND_ZC2_OPEN_IMPLICIT: Opened via write to zone.
 * @ZCOND_ZC3_OPEN_EXPLICIT: Opened via open zone command.
 * @ZCOND_ZC4_CLOSED: Closed
 * @ZCOND_ZC6_READ_ONLY:
 * @ZCOND_ZC5_FULL: No remaining space in zone.
 * @ZCOND_ZC7_OFFLINE: Offline
 *
 * Returned from Report Zones. See bdev_zone_descriptor* flags.
 */
enum bdev_zone_condition {
	ZCOND_CONVENTIONAL       = 0,
	ZCOND_ZC1_EMPTY          = 1,
	ZCOND_ZC2_OPEN_IMPLICIT  = 2,
	ZCOND_ZC3_OPEN_EXPLICIT  = 3,
	ZCOND_ZC4_CLOSED         = 4,
	/* 0x5 to 0xC are reserved */
	ZCOND_ZC6_READ_ONLY      = 0xd,
	ZCOND_ZC5_FULL           = 0xe,
	ZCOND_ZC7_OFFLINE        = 0xf,
};


/**
 * enum bdev_zone_same - Report Zones same code.
 *
 * @ZS_ALL_DIFFERENT: All zones differ in type and size.
 * @ZS_ALL_SAME: All zones are the same size and type.
 * @ZS_LAST_DIFFERS: All zones are the same size and type except the last zone.
 * @ZS_SAME_LEN_DIFF_TYPES: All zones are the same length but types differ.
 *
 * Returned from Report Zones. See bdev_zone_report* same_field.
 */
enum bdev_zone_same {
	ZS_ALL_DIFFERENT        = 0,
	ZS_ALL_SAME             = 1,
	ZS_LAST_DIFFERS         = 2,
	ZS_SAME_LEN_DIFF_TYPES  = 3,
};


/**
 * struct bdev_zone_get_report - ioctl: Report Zones request
 *
 * @zone_locator_lba: starting lba for first [reported] zone
 * @return_page_count: number of *bytes* allocated for result
 * @report_option: see: zone_report_option enum
 *
 * Used to issue report zones command to connected device
 */
struct bdev_zone_get_report {
	__u64 zone_locator_lba;
	__u32 return_page_count;
	__u8  report_option;
} __packed;

/**
 * struct bdev_zone_descriptor_le - See: bdev_zone_descriptor
 */
struct bdev_zone_descriptor_le {
	__u8 type;
	__u8 flags;
	__u8 reserved1[6];
	__le64 length;
	__le64 lba_start;
	__le64 lba_wptr;
	__u8 reserved[32];
} __packed;


/**
 * struct bdev_zone_report_le - See: bdev_zone_report
 */
struct bdev_zone_report_le {
	__le32 descriptor_count;
	__u8 same_field;
	__u8 reserved1[3];
	__le64 maximum_lba;
	__u8 reserved2[48];
	struct bdev_zone_descriptor_le descriptors[0];
} __packed;


/**
 * struct bdev_zone_descriptor - A Zone descriptor entry from report zones
 *
 * @type: see zone_type enum
 * @flags: Bits 0:reset, 1:non-seq, 2-3: resv, 4-7: see zone_condition enum
 * @reserved1: padding
 * @length: length of zone in sectors
 * @lba_start: lba where the zone starts.
 * @lba_wptr: lba of the current write pointer.
 * @reserved: padding
 *
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

/* continuing from uapi/linux/fs.h: */
#define BLKREPORT	_IOWR(0x12, 130, struct bdev_zone_report_io)
#define BLKOPENZONE	_IO(0x12, 131)
#define BLKCLOSEZONE	_IO(0x12, 132)
#define BLKRESETZONE	_IO(0x12, 133)

#endif /* _UAPI_BLKZONED_API_H */
