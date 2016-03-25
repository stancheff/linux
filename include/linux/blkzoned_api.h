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

#ifndef _BLKZONED_API_H
#define _BLKZONED_API_H

#include <uapi/linux/blkzoned_api.h>

extern int blkdev_issue_zone_action(struct block_device *, unsigned long bi_rw,
				    sector_t, gfp_t);
extern int blkdev_issue_zone_report(struct block_device *, unsigned long bi_rw,
				    sector_t, u8 opt, struct page *, size_t,
				    gfp_t);

#endif /* _BLKZONED_API_H */
