/*
 * Kernel Device Mapper for abstracting ZAC/ZBC devices as normal
 * block devices for linux file systems.
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

#ifndef _DM_ZONED_H
#define _DM_ZONED_H

#define ALLOC_DEBUG		0
#define USE_KTHREAD		0
#define KFIFO_SIZE		(1 << 14)

#define NORMAL			0
#define CRIT			GFP_NOIO

#define DM_MSG_PREFIX		"zdm"

#define PROC_WP			"wp.bin"
#define PROC_FREE		"free.bin"
#define PROC_DATA		"data.bin"
#define PROC_STATUS		"status"

#define ZDM_RESERVED_ZNR	0
#define ZDM_CRC_STASH_ZNR	1 /* first 64 blocks */
#define ZDM_RMAP_ZONE		2
#define ZDM_SECTOR_MAP_ZNR	3
#define ZDM_DATA_START_ZNR	4

#define Z_WP_GC_FULL		(1u << 31)
#define Z_WP_GC_ACTIVE		(1u << 30)
#define Z_WP_GC_TARGET		(1u << 29)
#define Z_WP_GC_READY		(1u << 28)
#define Z_WP_GC_BITS		(0xFu << 28)

#define Z_WP_GC_PENDING		(Z_WP_GC_FULL|Z_WP_GC_ACTIVE)
#define Z_WP_NON_SEQ		(1u << 27)
#define Z_WP_RRECALC		(1u << 26)
#define Z_WP_RESV_02		(1u << 25)
#define Z_WP_RESV_03		(1u << 24)

#define Z_WP_VALUE_MASK		(~0u >> 8)
#define Z_WP_FLAGS_MASK		(~0u << 24)
#define Z_WP_STREAM_MASK	Z_WP_FLAGS_MASK

#define Z_AQ_GC			(1u << 31)
#define Z_AQ_META		(1u << 30)
#define Z_AQ_NORMAL		(1u << 29)
#define Z_AQ_STREAM_ID		(1u << 28)
#define Z_AQ_STREAM_MASK	(0xFF)
#define Z_AQ_META_STREAM	(Z_AQ_META | Z_AQ_STREAM_ID | 0xFE)

#define Z_C4K			(4096ul)
#define Z_UNSORTED		(Z_C4K / sizeof(struct map_sect_to_lba))
#define Z_BLOCKS_PER_DM_SECTOR	(Z_C4K/512)
#define MZ_METADATA_ZONES	(8ul)
#define Z_SHFT4K		(3)


#define LBA_SB_START		1
#define SUPERBLOCK_MAGIC	0x5a6f4e65ul	/* ZoNe */
#define SUPERBLOCK_CSUM_XOR	146538381
#define MIN_ZONED_VERSION	1
#define Z_VERSION		1
#define MAX_ZONED_VERSION	1

#define UUID_LEN		16

#define Z_TYPE_SMR		2
#define Z_TYPE_SMR_HA		1
#define Z_VPD_INFO_BYTE		8

/* hash map for CRC's and Lookup Tables */
#define HCRC_ORDER		4
#define HLUT_ORDER		8

/*
 * INCR = distance between superblock writes from LBA 1.
 *
 * Superblock at LBA 1, 512, 1024 ...
 * Maximum reserved size of each is 400 cache blocks (map, and discard cache)
 *
 * At 1536 - 1664 are WP + ZF_EST blocks
 *
 * From 1664 to the end of the zone [Z1 LBA] is metadata write-back
 * journal blocks.
 *
 * The wb journal is used for metadata blocks between Z1 LBA and data_start LBA.
 * When Z1 LBA == data_start LBA the journal is disabled and used for the
 * key FWD lookup table blocks [the FWD blocks needed to map the FWD table].
 * Ex. When the FWD table consumes 64 zones (16TiB) then a lookup table
 * for 64*64 -> 4096 entries {~4 4k pages for 32bit LBAs) needs to be
 * maintained within the superblock generations.
 *
 * MAX_CACHE_SYNC - Space in SB INCR reserved for map/discard cache blocks.
 * CACHE_COPIES   - Number of SB to keep (Active, Previous, Backup)
 *
 */
#define MAX_SB_INCR_SZ		512ul
#define MAX_CACHE_SYNC		400ul
#define CACHE_COPIES		3
/* WP is 4k, and ZF_Est is 4k written in pairs: Hence 2x */
#define MAX_WP_BLKS		64
#define WP_ZF_BASE		(MAX_SB_INCR_SZ * CACHE_COPIES)
#define FWD_KEY_BLOCKS		8
#define FWD_KEY_BASE		(WP_ZF_BASE + (MAX_WP_BLKS * 2))

#define WB_JRNL_MIN		4096u
#define WB_JRNL_MAX		16384u
#define WB_JRNL_BLKS		(WB_JRNL_MAX >> 10)
#define WB_JRNL_IDX		(FWD_KEY_BASE + FWD_KEY_BLOCKS)
#define WB_JRNL_BASE		(WB_JRNL_IDX + WB_JRNL_BLKS)

#define IO_VCACHE_ORDER		8
#define IO_VCACHE_PAGES		(1 << IO_VCACHE_ORDER)  /* 256 pages => 1MiB */

#ifdef __cplusplus
extern "C" {
#endif

enum superblock_flags_t {
	SB_DIRTY = 1,
};

struct z_io_req_t {
	struct dm_io_region *where;
	struct dm_io_request *io_req;
	struct work_struct work;
	int result;
};

#define Z_LOWER48 (~0ul >> 16)
#define Z_UPPER16 (~Z_LOWER48)

#define STREAM_SIZE	256

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/**
 * enum pg_flag_enum - Map Pg flags
 * @IS_DIRTY: Block is modified from on-disk copy.
 * @IS_STALE: ??
 * @IS_FLUSH: If flush was issued when IS_DIRTY was not set.
 *
 * @IS_FWD: Is part of Forward ZLT or CRC table.
 * @IS_REV: Is part of Reverse ZLT or CRC table.
 * @IS_CRC: Is part of CRC table.
 * @IS_LUT: Is part of ZTL table.
 *
 * @WB_JOURNAL: If WB is targeted to Journal or LUT entries.
 *
 * @R_IN_FLIGHT: Async read in progress.
 * @W_IN_FLIGHT: ASync write in progress.
 * @DELAY_ADD: Spinlock was busy for zltlst, added to lazy for transit.
 * @STICKY: ???
 * @IS_READA: Block will pulled for Read Ahead. Cleared when used.
 * @IS_DROPPED: Has clean/expired from zlt_lst and is waiting free().
 *
 * @IS_LAZY: Had been added to 'lazy' lzy_lst.
 * @IN_ZLT: Had been added to 'inpool' zlt_lst.
 *
 */
enum pg_flag_enum {
	IS_DIRTY,
	IS_STALE,
	IS_FLUSH,

	IS_FWD,
	IS_REV,
	IS_CRC,
	IS_LUT,

	WB_JRNL_1,
	WB_JRNL_2,
	WB_DIRECT,
	WB_RE_CACHE,

	R_IN_FLIGHT,
	W_IN_FLIGHT,
	DELAY_ADD,
	STICKY,
	IS_READA,
	IS_DROPPED,
	IS_LAZY,
	IN_ZLT,
};

/**
 * enum gc_flags_enum - Garbage Collection [GC] states.
 */
enum gc_flags_enum {
	DO_GC_NEW,
	DO_GC_PREPARE,		/* -> READ or COMPLETE state */
	DO_GC_WRITE,
	DO_GC_META,		/* -> PREPARE state */
	DO_GC_COMPLETE,
};

/**
 * enum znd_flags_enum - zdm state/feature/action flags
 */
enum znd_flags_enum {
	ZF_FREEZE,
	ZF_POOL_FWD,
	ZF_POOL_REV,
	ZF_POOL_CRCS,

	ZF_RESV_1,
	ZF_RESV_2,
	ZF_RESV_3,
	ZF_RESV_4,

	DO_MAPCACHE_MOVE,
	DO_MEMPOOL,
	DO_SYNC,
	DO_FLUSH,
	DO_ZDM_RELOAD,
	DO_GC_NO_PURGE,
	DO_METAWORK_QD,
};

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

struct zdm;

/**
 * struct gc_state - A page of map table
 * @znd: ZDM Instance
 * @gc_flags: See gc_flags_enum
 * @r_ptr: Next read in zone.
 * @w_ptr: Next write in target zone.
 * @nblks: Number of blocks in I/O
 * @result: GC operation result.
 * @z_gc: Zone undergoing compacation
 * @tag: System wide serial number (debugging).
 *
 * Longer description of this structure.
 */
struct gc_state {
	struct zdm *znd;
	unsigned long gc_flags;

	u32 r_ptr;
	u32 w_ptr;

	u32 nblks;		/* 1-65536 */
	u32 z_gc;

	int result;
};

/**
 * struct map_addr - A page of map table
 * @dm_s:    full map on dm layer
 * @zone_id: z_id match zone_list_t.z_id
 * @pg_idx:  entry in lut (0-1023)
 * @lut_s:   sector table lba
 * @lut_r:   reverse table lba
 *
 * Longer description of this structure.
 */
struct map_addr {
	u64 dm_s;
	u64 lut_s;
	u64 lut_r;

	u32 zone_id;
	u32 pg_idx;
};

/**
 * struct map_sect_to_lba - Sector to LBA mapping.
 * @tlba: tlba
 * @physical: blba or number of blocks
 *
 * Longer description of this structure.
 */
struct map_sect_to_lba {
	__le64 tlba;		/* record type [16 bits] + logical sector # */
	__le64 bval;	/* csum 16 [16 bits] + 'physical' block lba */
} __packed;

/**
 * enum map_type_enum - Map Cache pool types
 * @IS_MAP: Is an ingress map pool
 * @IS_JOURNAL: Is a meta data journal pool.
 * @IS_DISCARD: Is a discard pool.
 */
enum map_type_enum {
	IS_MAP,
	IS_DISCARD,
	IS_JRNL_PG,
	MAP_COUNT,
};

/**
 * struct map_cache - A page of map table
 * @mclist:
 * @jdata: 4k page of data
 * @refcount:
 * @cached_lock:
 * @busy_locked:
 * @jcount:
 * @jsorted:
 * @jsize:
 * @map_content: See map_type_enum
 *
 * Longer description of this structure.
 */
struct map_cache {
	struct list_head mclist;
	struct map_sect_to_lba *jdata;
	atomic_t refcount;
	struct mutex cached_lock;
	atomic_t busy_locked;
	u32 jcount;
	u32 jsorted;
	u32 jsize;
	u32 map_content; /* IS_MAP / IS_DISCARD / IS_JOURNAL */
};

/**
 * union map_pg_data - A page of map data
 * @addr:	Array of LBA's in little endian.
 * @crc:	Array of 16 bit CRCs in little endian.
 */
union map_pg_data {
	__le32 *addr;
	__le16 *crc;
};

/**
 * struct map_pg - A page of map table
 *
 * @data:	A block/page [4K] of table entries
 * @refcount:	In use (reference counted).
 * @age:	Most recent access time in jiffies
 * @lba:	Logical position (use lookups to find actual)
 * @md_lock:	Lock on data during i/o [TBD: Use spin locks/CRCs]
 * @last_write:	Last known LBA written on disk
 * @flags:	is dirty flag, is fwd, is rev, is crc, is lut
 * @zltlst:	Entry in lookup table list.
 * @lazy:	Entry in lazy list.
 * @znd:	ZDM Instance (for async metadata I/O)
 * @crc_pg:	Backing CRC page (for IS_LUT pages)
 * @hotness:	Additional time to keep cache block in memory.
 * @index:	Offset from base [array index].
 * @md_crc:	TBD: Use to test for dirty/clean during I/O instead of locking.
 *
 * Longer description of this structure.
 */
struct map_pg {
	union map_pg_data data;
	atomic_t refcount;
	u64 age;
	u64 lba;
	u64 last_write;
	struct hlist_node hentry;
	struct mutex md_lock;
	unsigned long flags;
	struct list_head zltlst;

	/* in flight/cache hit tracking */
	struct list_head lazy;
	struct zdm *znd;
	struct map_pg *crc_pg;
	int hotness;
	int index;
	__le16 md_crc;
};

/**
 * struct map_crc - Map to backing crc16.
 *
 * @pg_no:	Page [index] of table entry if applicable.
 * @pg_idx:	Offset within page (From: zdm::md_crcs when table is null)
 *
 * Longer description of this structure.
 */
struct map_crc {
	u64 lba;
	int pg_no;
	int pg_idx;
};

/**
 * struct mpinfo - Map to backing lookup table.
 *
 * @table:        backing table
 * @crc:          backing crc16 detail.
 * @index:        index [page] of table entry. Use map_addr::pg_idx for offset.
 * @bit_type:     IS_LUT or IS_CRC
 * @bit_dir:      IS_FWD or IS_REV
 *
 * Longer description of this structure.
 */
struct mpinfo {
	struct hlist_head *htable;
	struct map_crc crc;
	spinlock_t *lock;
	int ht_order;
	int index;
	int bit_type;
	int bit_dir;
};

/**
 * struct meta_pg - A page of zone WP mapping.
 *
 * @wp_alloc: Bits 23-0: wp alloc location.   Bits 31-24: GC Flags, Type Flags
 * @zf_est:   Bits 23-0: free block count.    Bits 31-24: Stream Id
 * @wp_used:  Bits 23-0: wp written location. Bits 31-24: Ratio ReCalc flag.
 * @lba: pinned LBA in conventional/preferred zone.
 * @wplck: spinlock held during data updates.
 * @flags: IS_DIRTY flag
 *
 * One page is used per 1024 zones on media.
 * For an 8TB drive this uses 30 entries or about 360k RAM.
 */
struct meta_pg {
	__le32 *wp_alloc;
	__le32 *zf_est;
	__le32 *wp_used;
	u64 lba;
	spinlock_t wplck;
	unsigned long flags;
};

/**
 * struct md_journal_forward - In-Memory lba sorted journal access.
 * @lba: Metadata lba (sorted)
 * @entry: Location in md_journal (wb_map)
 */
struct md_journal_forward {
	u32 lba; /* origin lba */
	u32 entry; /* entry in md_journal */
};

/**
 * @wb: Reverse Map table: 0xFFFFFFFF for unused.
 * @wb_next: Next location for ready for wb.
 * @size: Allocated size of wb/map.
 * @in_use: Number of wb blocks in use (schedule flush when > 70% in use)
 * @wb_alloc: Spin lock around wb
 * @flags: Dirty/Flush flags.
 */
struct md_journal {
	__le32 *wb; /* [RMAP] array of origin lbas stored at WB_JRNL_IDX */
	u32 wb_next;
	u32 size;
	u32 in_use;
	spinlock_t wb_alloc;
	unsigned long flags;
};

/**
 * struct zdm_superblock - A page of map table
 * @uuid:
 * @nr_zones:
 * @magic:
 * @zdstart:
 * @version:
 * @packed_meta:
 * @flags:
 * @csum:
 *
 * Longer description of this structure.
 */
struct zdm_superblock {
	u8 uuid[UUID_LEN];	/* 16 */
	__le64 nr_zones;	/*  8 */
	__le64 magic;		/*  8 */
	__le32 resvd;		/*  4 */
	__le32 zdstart;		/*  4 */
	__le32 version;		/*  4 */
	__le32 packed_meta;	/*  4 */
	__le32 flags;		/*  4 */
	__le32 csum;		/*  4 */
} __packed;			/* 56 */

/**
 * struct mz_superkey - A page of map table
 * @sig0:           8 - Native endian
 * @sig1:           8 - Little endian
 * @sblock:        56 -
 * @stream:      1024 -
 * @reserved:    2982 -
 * @gc_resv:        4 -
 * @meta_resv:      4 -
 * @generation:     8 -
 * @key_crc:        2 -
 * @magic:          8 -
 *
 * Longer description of this structure.
 */
struct mz_superkey {
	u64 sig0;
	__le64 sig1;
	struct zdm_superblock sblock;
	__le32 stream[STREAM_SIZE];
	__le32 gc_resv;
	__le32 meta_resv;
	__le16 n_crcs;
	__le16 crcs[MAX_CACHE_SYNC];
	__le16 md_crc;
	__le16 wp_crc[64];
	__le16 zf_crc[64];
	__le16 discards;
	__le16 maps;
	u8 reserved[1900];
	__le32 wb_next;
	__le32 wb_crc32;
	__le32 crc32;
	__le64 generation;
	__le64 magic;
} __packed;

/**
 * struct io_4k_block - Sector to LBA mapping.
 * @data:	A 4096 byte block
 *
 * Longer description of this structure.
 */
struct io_4k_block {
	u8 data[Z_C4K];
};

/**
 * struct io_dm_block - Sector to LBA mapping.
 * @data:	A 512 byte block
 *
 * Longer description of this structure.
 */
struct io_dm_block {
	u8 data[512];
};

struct stale_tracking {
	u32 binsz;
	u32 count;
	int bins[STREAM_SIZE];
};

/*
 *
 * Partition -----------------------------------------------------------------+
 *     Table ---+                                                             |
 *              |                                                             |
 *   SMR Drive |^-------------------------------------------------------------^|
 * CMR Zones     ^^^^^^^^^
 *  meta data    |||||||||
 *
 *   Remaining partition is filesystem data
 *
 */

/**
 * struct zdm - A page of map table
 * @ti:		dm_target entry
 * @dev:	dm_dev entry
 * @mclist:	list of pages of in-memory LBA mappings.
 * @mclck:	in memory map-cache lock (spinlock)

 * @zltpool:		pages of lookup table entries
 * @zlt_lck:	zltpool: memory pool lock
 * @lzy_pool:
 * @lzy_lock:
 * @fwd_tm:
 * @rev_tm:

 * @bg_work:	background worker (periodic GC/Mem purge)
 * @bg_wq:	background work queue
 * @stats_lock:
 * @gc_active:	Current GC state
 * @gc_lock:	GC Lock
 * @gc_work:	GC Worker
 * @gc_wq:	GC Work Queue
 * @data_zones:	# of data zones on device
 * @gz_count:	# of 256G mega-zones
 * @nr_blocks:	4k blocks on backing device
 * @md_start:	LBA at start of metadata pool
 * @data_lba:	LBA at start of data pool
 * @zdstart:	ZONE # at start of data pool (first Possible WP ZONE)
 * @start_sect:	where ZDM partition starts (RAW LBA)
 * @flags:	See: enum znd_flags_enum
 * @gc_backlog:
 * @gc_io_buf:
 * @io_vcache[32]:
 * @io_vcache_flags:
 * @z_sballoc:
 * @super_block:
 * @z_mega:
 * @meta_wq:
 * @gc_postmap:
 * @io_client:
 * @io_wq:
 * @zone_action_wq:
 * @timer:
 * @bins:	Memory usage accounting/reporting.
 * @bdev_name:
 * @memstat:
 * @suspended:
 * @gc_mz_pref:
 * @mz_provision:	Number of zones per 1024 of over-provisioning.
 * @ata_passthrough:
 * @is_empty:		For fast discards on initial format
 *
 * Longer description of this structure.
 */
struct zdm {
	struct dm_target *ti;
	struct dm_dev *dev;

	struct list_head zltpool;
	struct list_head lzy_pool;
	struct list_head mclist[MAP_COUNT];
	spinlock_t       mclck[MAP_COUNT];

	struct work_struct bg_work;
	struct workqueue_struct *bg_wq;

	spinlock_t zlt_lck;
	spinlock_t lzy_lck;
	spinlock_t stats_lock;
	spinlock_t mapkey_lock; /* access LUT and CRC array of pointers */
	spinlock_t ct_lock; /* access LUT and CRC array of pointers */

	struct mutex gc_wait;
	struct mutex pool_mtx;
	struct mutex mz_io_mutex;
	struct mutex vcio_lock;

#if USE_KTHREAD /* experimental++ */
	struct task_struct *bio_kthread;
	DECLARE_KFIFO_PTR(bio_fifo, struct bio *);
	wait_queue_head_t wait_bio;
	wait_queue_head_t wait_fifo;
#endif
	struct bio_set *bio_set;

	struct gc_state *gc_active;
	spinlock_t gc_lock;
	struct delayed_work gc_work;
	struct workqueue_struct *gc_wq;

	u64 nr_blocks;
	u64 start_sect;

	u64 md_start;
	u64 md_end;
	u64 data_lba;

	unsigned long flags;

	u64 r_base;
	u64 s_base;
	u64 c_base;
	u64 c_mid;
	u64 c_end;

	u64 sk_low;   /* unused ? */
	u64 sk_high;  /* unused ? */

	struct meta_pg *wp;
	DECLARE_HASHTABLE(fwd_hm, HLUT_ORDER);
	DECLARE_HASHTABLE(rev_hm, HLUT_ORDER);
	DECLARE_HASHTABLE(fwd_hcrc, HCRC_ORDER);
	DECLARE_HASHTABLE(rev_hcrc, HCRC_ORDER);
	__le16 *md_crcs;   /* one of crc16's for fwd, 1 for rev */
	u32 crc_count;
	u32 map_count;

	struct md_journal jrnl;

	void *z_sballoc;
	struct mz_superkey *bmkeys;
	struct zdm_superblock *super_block;

	struct work_struct meta_work;
	sector_t last_w;
	u8 *cow_block;
	u64 cow_addr;
	u32 data_zones;
	u32 gz_count;
	u32 zdstart;
	u32 z_gc_free;
	atomic_t incore;
	u32 discard_count;
	u32 z_current;
	u32 z_meta_resv;
	u32 z_gc_resv;
	u32 gc_events;
	int mc_entries;
	int journal_entries;
	int dc_entries;
	int in_zlt;
	int in_lzy;
	int meta_result;
	struct stale_tracking stale;

	int gc_backlog;
	void *gc_io_buf;
	struct mutex gc_vcio_lock;
	struct io_4k_block *io_vcache[32];
	unsigned long io_vcache_flags;
	u64 age;
	struct workqueue_struct *meta_wq;
	struct map_cache gc_postmap;
	struct dm_io_client *io_client;
	struct workqueue_struct *io_wq;
	struct workqueue_struct *zone_action_wq;
	struct timer_list timer;

	u32 bins[40];
	char bdev_name[BDEVNAME_SIZE];
	char proc_name[BDEVNAME_SIZE+4];
	struct proc_dir_entry *proc_fs;
	loff_t wp_proc_at;
	loff_t used_proc_at;

	size_t memstat;
	atomic_t suspended;
	atomic_t gc_throttle;

#if ALLOC_DEBUG
	atomic_t allocs;
	int hw_allocs;
	void **alloc_trace;
#endif
	u32 filled_zone;
	u16 mz_provision;
	unsigned bdev_is_zoned:1;
	unsigned ata_passthrough:1;
	unsigned issue_open_zone:1;
	unsigned issue_close_zone:1;
	unsigned is_empty:1;
	unsigned trim:1;
	unsigned raid5_trim:1;

};

/**
 * struct zdm_ioc_request - Sector to LBA mapping.
 * @result_size:
 * @megazone_nr:
 *
 * Longer description of this structure.
 */
struct zdm_ioc_request {
	u32 result_size;
	u32 megazone_nr;
};

/**
 * struct zdm_ioc_status - Sector to LBA mapping.
 * @b_used: Number of blocks used
 * @b_available: Number of blocks free
 * @b_discard: Number of blocks stale
 * @m_zones: Number of zones.
 * @mc_entries: Mem cache blocks in use
 * @dc_entries: Discard cache blocks in use.
 * @mlut_blocks:
 * @crc_blocks:
 * @memstat: Total memory in use by ZDM via *alloc()
 * @bins: Allocation by subsystem.
 *
 * This status structure is used to pass run-time information to
 * user spaces tools (zdm-tools) for diagnostics and tuning.
 */
struct zdm_ioc_status {
	u64 b_used;
	u64 b_available;
	u64 b_discard;
	u64 m_zones;
	u32 map_cache_entries;
	u32 discard_cache_entries;
	u64 mlut_blocks;
	u64 crc_blocks;
	u64 memstat;
	u32 journal_pages;
	u32 journal_entries;
	u32 bins[40];
};

#ifdef __cplusplus
}
#endif

#endif /* _DM_ZONED_H */
