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

#ifndef _UAPI_BLK_ZONED_CTRL_H
#define _UAPI_BLK_ZONED_CTRL_H

#include <linux/types.h>

/* Used for Zone based SMR devices */
#define SCSI_IOCTL_INQUIRY		0x10000
#define SCSI_IOCTL_CLOSE_ZONE		0x10001
#define SCSI_IOCTL_FINISH_ZONE		0x10002
#define SCSI_IOCTL_OPEN_ZONE		0x10003
#define SCSI_IOCTL_RESET_WP		0x10004
#define SCSI_IOCTL_REPORT_ZONES		0x10005

/**
 * struct zoned_inquiry - deterine if device is block zoned.
 *
 * @evpd:
 * @pg_op:
 * @mx_resp_len:
 * @result:
 *
 */
struct zoned_inquiry {
	__u8  evpd;
	__u8  pg_op;
	__u16 mx_resp_len;
	__u8  result[0];
} __packed;

/**
 * enum zoned_identity_type_id - Zoned, HA or HM.
 *
 * @NOT_ZONED: Not a zoned device.
 * @HOST_AWARE: ZAC/ZBC device reports as Host Aware.
 * @HOST_MANAGE: ZAC/ZBC device reports as Host Managed.
 *
 */
enum zoned_identity_type_id {
	NOT_ZONED    = 0x00,
	HOST_AWARE   = 0x01,
	HOST_MANAGE  = 0x02,
};

/**
 * struct zoned_identity
 *
 * @type_id: See zoned_identity_type_id
 * @reserved: reserved.
 *
 * Used for ata pass through to detected devices which support ZAC.
 */
struct zoned_identity {
	__u8 type_id;
	__u8 reserved[3];
} __packed;

#endif /* _UAPI_BLK_ZONED_CTRL_H */
