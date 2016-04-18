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

#define BUILD_NO		110

#define EXTRA_DEBUG		0

#define MZ_MEMPOOL_SZ		512
#define READ_AHEAD		16   /* number of LUT entries to read-ahead */
#define MEMCACHE_HWMARK		5
#define MEM_PURGE_MSECS		15000
#define MEM_HOT_BOOST_INC	5000
#define DISCARD_IDLE_MSECS	2000
#define DISCARD_MAX_INGRESS	150
#define MAX_WSET		2048

/* acceptable 'free' count for MZ free levels */
#define GC_PRIO_DEFAULT		0xFF00
#define GC_PRIO_LOW		0x7FFF
#define GC_PRIO_HIGH		0x0400
#define GC_PRIO_CRIT		0x0040

/*
 *  For performance tuning:
 *   Q? smaller strips give smoother performance
 *      a single drive I/O is 8 (or 32?) blocks?
 *   A? Does not seem to ...
 */
#define GC_MAX_STRIPE		256
#define REPORT_ORDER		7
#define REPORT_FILL_PGS		65 /* 65 -> min # pages for 4096 descriptors */
#define SYNC_IO_ORDER		2
#define SYNC_IO_SZ		((1 << SYNC_IO_ORDER) * PAGE_SIZE)

#define MZTEV_UNUSED		(cpu_to_le32(0xFFFFFFFFu))
#define MZTEV_NF		(cpu_to_le32(0xFFFFFFFEu))

#define Z_TABLE_MAGIC		0x123456787654321Eul
#define Z_KEY_SIG		0xFEDCBA987654321Ful

#define Z_CRC_4K		4096
#define ZONE_SECT_BITS		19
#define Z_BLKBITS		16
#define Z_BLKSZ			(1ul << Z_BLKBITS)
#define MAX_ZONES_PER_MZ	1024
#define Z_SMR_SZ_BYTES		(Z_C4K << Z_BLKBITS)

#define GC_READ			(1ul << 15)
#define BAD_ADDR		(~0ul)
#define MC_INVALID		(cpu_to_le64(BAD_ADDR))
#define NOZONE			(~0u)

#define GZ_BITS 10
#define GZ_MMSK ((1u << GZ_BITS) - 1)

#define CRC_BITS 11
#define CRC_MMSK ((1u << CRC_BITS) - 1)

#define MD_CRC_INIT		(cpu_to_le16(0x5249u))

static int zoned_wp_sync(struct zdm *znd, int reset_non_empty);
static u32 dmz_report_count(struct zdm *znd, void *rpt_in, size_t bufsz);
static int map_addr_calc(struct zdm *, u64 dm_s, struct map_addr *out);
static int zoned_io_flush(struct zdm *znd);
static int zoned_wp_sync(struct zdm *znd, int reset_non_empty);
static void cache_if_dirty(struct zdm *znd, struct map_pg *pg, int wq);
static int write_if_dirty(struct zdm *, struct map_pg *, int use_wq, int snc);
static void gc_work_task(struct work_struct *work);
static void meta_work_task(struct work_struct *work);
static u64 mcache_greatest_gen(struct zdm *, int, u64 *, u64 *);
static u64 mcache_find_gen(struct zdm *, u64 base, int, u64 *out);
static int find_superblock(struct zdm *znd, int use_wq, int do_init);
static int sync_mapped_pages(struct zdm *znd, int sync, int drop);
static int unused_phy(struct zdm *znd, u64 lba, u64 orig_s, gfp_t gfp);
static struct io_4k_block *get_io_vcache(struct zdm *znd, gfp_t gfp);
static int put_io_vcache(struct zdm *znd, struct io_4k_block *cache);
static struct map_pg *get_map_entry(struct zdm *, u64 lba, gfp_t gfp);
static void put_map_entry(struct map_pg *);
static int cache_pg(struct zdm *znd, struct map_pg *pg, gfp_t gfp,
		    struct mpinfo *mpi);
static int move_to_map_tables(struct zdm *znd, struct map_cache *mcache);
static void update_stale_ratio(struct zdm *znd, u32 zone);
static int zoned_create_disk(struct dm_target *ti, struct zdm *znd);
static int do_init_zoned(struct dm_target *ti, struct zdm *znd);
static void update_all_stale_ratio(struct zdm *znd);
static int z_discard_partial(struct zdm *znd, u32 blks, gfp_t gfp);
static u64 z_discard_range(struct zdm *znd, u64 addr, gfp_t gfp);
static u64 z_lookup_cache(struct zdm *znd, u64 addr, int type);
static u64 z_lookup_table(struct zdm *znd, u64 addr, gfp_t gfp);
static u64 current_mapping(struct zdm *znd, u64 addr, gfp_t gfp);
static int z_mapped_add_one(struct zdm *znd, u64 dm_s, u64 lba, gfp_t gfp);
static int z_mapped_discard(struct zdm *znd, u64 tlba, u64 count, gfp_t gfp);
static int z_mapped_addmany(struct zdm *znd, u64 dm_s, u64 lba, u64,
			    gfp_t gfp);
static int z_to_map_list(struct zdm *znd, u64 dm_s, u64 lba, gfp_t gfp);
static int z_to_journal_list(struct zdm *znd, u64 dm_s, u64 lba, gfp_t gfp);
static int z_to_discard_list(struct zdm *znd, u64 dm_s, u64 blks, gfp_t gfp);
static int discard_merge(struct zdm *znd, u64 tlba, u64 blks);
static int z_mapped_sync(struct zdm *znd);
static int z_mapped_init(struct zdm *znd);
static u64 z_acquire(struct zdm *znd, u32 flags, u32 nblks, u32 *nfound);
static __le32 sb_crc32(struct zdm_superblock *sblock);
static int update_map_entry(struct zdm *, struct map_pg *,
			    struct map_addr *, u64, int);
static int read_block(struct dm_target *, enum dm_io_mem_type,
		      void *, u64, unsigned int, int);
static int write_block(struct dm_target *, enum dm_io_mem_type,
		       void *, u64, unsigned int, int);
static int writef_block(struct dm_target *ti, enum dm_io_mem_type dtype,
			void *data, u64 lba, unsigned int op_flags,
			unsigned int count, int queue);
static int zoned_init_disk(struct dm_target *ti, struct zdm *znd,
			   int create, int force);

#define MutexLock(m)	test_and_lock((m), __LINE__)
#define SpinLock(s)	test_and_spin((s), __LINE__)

static __always_inline void test_and_lock(struct mutex *m, int lineno)
{
	if (!mutex_trylock(m)) {
		pr_debug("mutex stall at %d\n", lineno);
		mutex_lock(m);
	}
}

static __always_inline void test_and_spin(spinlock_t *lock, int lineno)
{
	if (!spin_trylock(lock)) {
		pr_debug("spin stall at %d\n", lineno);
		spin_lock(lock);
	}
}

/**
 * ref_pg() - Decrement refcount on page of ZLT
 * @pg: Page of ZLT map
 */
static __always_inline void deref_pg(struct map_pg *pg)
{
	atomic_dec(&pg->refcount);
}

/**
 * ref_pg() - Increment refcount on page of ZLT
 * @pg: Page of ZLT map
 */
static __always_inline void ref_pg(struct map_pg *pg)
{
	atomic_inc(&pg->refcount);
#if 0 /* ALLOC_DEBUG */
	if (atomic_read(&pg->refcount) > 20) {
		pr_err("Excessive %d who dunnit?\n",
			atomic_read(&pg->refcount));
		dump_stack();
	}
#endif
}

/**
 * getref_pg() - Read the refcount
 * @pg: Page of ZLT map
 */
static __always_inline int getref_pg(struct map_pg *pg)
{
	return atomic_read(&pg->refcount);
}


/**
 * mcache_ref() - Increment the reference count of mcache()
 * @mcache: Page of map cache
 */
static __always_inline void mcache_ref(struct map_cache *mcache)
{
	atomic_inc(&mcache->refcount);
}

/**
 * mcache_deref() - Decrement the reference count of mcache()
 * @mcache: Page of map cache
 */
static __always_inline void mcache_deref(struct map_cache *mcache)
{
	atomic_dec(&mcache->refcount);
}

/**
 * mcache_getref() - Read the refcount
 * @mcache: Page of map cache
 */
static __always_inline int mcache_getref(struct map_cache *mcache)
{
	return atomic_read(&mcache->refcount);
}

/**
 * mcache_busy() - Increment the busy test of mcache()
 * @mcache: Page of map cache
 */
static __always_inline void mcache_busy(struct map_cache *mcache)
{
	atomic_inc(&mcache->busy_locked);
}

/**
 * mcache_unbusy() - Decrement the busy test count of mcache()
 * @mcache: Page of map cache
 */
static __always_inline void mcache_unbusy(struct map_cache *mcache)
{
	atomic_dec(&mcache->busy_locked);
}

/**
 * mcache_is_busy() - Test if mcache is busy
 * @mcache: Page of map cache
 * Return non-zero if busy otherwise 0.
 */
static __always_inline int mcache_is_busy(struct map_cache *mcache)
{
	return atomic_read(&mcache->busy_locked);
}

/**
 * crc16_md() - 16 bit CRC on metadata blocks
 * @data: Block of metadata.
 * @len:  Number of bytes in block.
 *
 * Return: 16 bit CRC.
 */
static inline u16 crc16_md(void const *data, size_t len)
{
	const u16 init = 0xFFFF;
	const u8 *p = data;

	return crc16(init, p, len);
}

/**
 * crc_md_le16() - 16 bit CRC on metadata blocks in little endian
 * @data: Block of metadata.
 * @len:  Number of bytes in block.
 *
 * Return: 16 bit CRC.
 */
static inline __le16 crc_md_le16(void const *data, size_t len)
{
	u16 crc = crc16_md(data, len);

	return cpu_to_le16(crc);
}

/**
 * crcpg() - 32 bit CRC [NOTE: 32c is HW assisted on Intel]
 * @data: Block of metadata [4K bytes].
 *
 * Return: 32 bit CRC.
 */
static inline u32 crcpg(void *data)
{
	return crc32c(~0u, data, Z_CRC_4K) ^ SUPERBLOCK_CSUM_XOR;
}

/**
 * crc32c_le32() - 32 bit CRC [NOTE: 32c is HW assisted on Intel]
 * @init: Starting value (usually: ~0u)
 * @data: Data to be CRC'd.
 * @sz: Number of bytes to be CRC'd
 *
 * Return: 32 bit CRC in little endian format.
 */
static inline __le32 crc32c_le32(u32 init, void * data, u32 sz)
{
	return cpu_to_le32(crc32c(init, data, sz));
}

/**
 * le64_to_lba48() - Return the lower 48 bits of LBA
 * @enc: 64 bit LBA + flags
 * @flg: optional 16 bits of classification.
 *
 * Return: 48 bits of LBA [and flg].
 */
static inline u64 le64_to_lba48(__le64 enc, u16 *flg)
{
	const u64 lba64 = le64_to_cpu(enc);

	if (flg)
		*flg = (lba64 >> 48) & 0xFFFF;

	return lba64 & Z_LOWER48;
}

/**
 * lba48_to_le64() - Encode 48 bits of lba + 16 bits of flags.
 * @flags: flags to encode.
 * @lba48: LBA to encode
 *
 * Return: Little endian u64.
 */
static inline __le64 lba48_to_le64(u16 flags, u64 lba48)
{
	u64 high_bits = flags;

	return cpu_to_le64((high_bits << 48) | (lba48 & Z_LOWER48));
}

/**
 * sb_test_flag() - Test if flag is set in Superblock.
 * @sb: zdm_superblock.
 * @bit_no: superblock flag
 *
 * Return: non-zero if flag is set.
 */
static inline int sb_test_flag(struct zdm_superblock *sb, int bit_no)
{
	u32 flags = le32_to_cpu(sb->flags);

	return (flags & (1 << bit_no)) ? 1 : 0;
}

/**
 * sb_set_flag() - Set a flag in superblock.
 * @sb: zdm_superblock.
 * @bit_no: superblock flag
 */
static inline void sb_set_flag(struct zdm_superblock *sb, int bit_no)
{
	u32 flags = le32_to_cpu(sb->flags);

	flags |= (1 << bit_no);
	sb->flags = cpu_to_le32(flags);
}

/**
 * zone_to_sector() - Calculate starting LBA of zone
 * @zone: zone number (0 based)
 *
 * Return: LBA at start of zone.
 */
static inline u64 zone_to_sector(u64 zone)
{
	return zone << ZONE_SECT_BITS;
}

/**
 * is_expired_msecs() - Determine if age + msecs is older than now.
 * @age: jiffies at last access
 * @msecs: msecs of extra time.
 *
 * Return: non-zero if block is expired.
 */
static inline int is_expired_msecs(u64 age, u32 msecs)
{
	u64 expire_at = age + msecs_to_jiffies(msecs);
	int expired = time_after64(jiffies_64, expire_at);

	return expired;
}

/**
 * is_expired() - Determine if age is older than MEM_PURGE_MSECS.
 * @age: jiffies at last access
 *
 * Return: non-zero if block is expired.
 */
static inline int is_expired(u64 age)
{
	return is_expired_msecs(age, MEM_PURGE_MSECS);
}

/**
 * _calc_zone() - Determine zone number from addr
 * @addr: 4k sector number
 *
 * Return: znum or 0xFFFFFFFF if addr is in metadata space.
 */
static inline u32 _calc_zone(struct zdm *znd, u64 addr)
{
	u32 znum = NOZONE;

	if (addr < znd->data_lba)
		return znum;

	addr -= znd->data_lba;
	znum = addr >> Z_BLKBITS;

	return znum;
}

/**
 * lazy_pool_add - Set a flag and add map page to the lazy pool
 * @znd: ZDM Instance
 * @expg: Map table page.
 * @bit: Flag to set on page.
 *
 * Lazy pool is used for deferred adding and delayed removal.
 */
static __always_inline
void lazy_pool_add(struct zdm *znd, struct map_pg *expg, int bit)
{
	unsigned long flags;

	spin_lock_irqsave(&znd->lzy_lck, flags);
	if (!test_bit(IS_LAZY, &expg->flags)) {
		set_bit(IS_LAZY, &expg->flags);
		list_add(&expg->lazy, &znd->lzy_pool);
		znd->in_lzy++;
	}
	set_bit(bit, &expg->flags);
	spin_unlock_irqrestore(&znd->lzy_lck, flags);
}

/**
 * lazy_pool_splice - Add a list of pages to the lazy pool
 * @znd: ZDM Instance
 * @list: List of map table page to add.
 *
 * Lazy pool is used for deferred adding and delayed removal.
 */
static __always_inline
void lazy_pool_splice(struct zdm *znd, struct list_head *list)
{
	unsigned long flags;

	spin_lock_irqsave(&znd->lzy_lck, flags);
	list_splice_tail(list, &znd->lzy_pool);
	spin_unlock_irqrestore(&znd->lzy_lck, flags);
}

/**
 * pool_add() - Add metadata block to zltlst
 * @znd: ZDM instance
 * @expg: current metadata block to add to zltlst list.
 */
static inline int pool_add(struct zdm *znd, struct map_pg *expg)
{
	int rcode = 0;
	unsigned long flags;

	/* undrop from journal will be in lazy list */
	if (test_bit(IS_LAZY, &expg->flags)) {
		set_bit(DELAY_ADD, &expg->flags);
		return rcode;
	}

	if (spin_trylock_irqsave(&znd->zlt_lck, flags)) {
		if (test_bit(IN_ZLT, &expg->flags)) {
			Z_ERR(znd, "Double list_add from:");
			dump_stack();
		} else {
			set_bit(IN_ZLT, &expg->flags);
			list_add(&expg->zltlst, &znd->zltpool);
			znd->in_zlt++;
		}
		spin_unlock_irqrestore(&znd->zlt_lck, flags);
		rcode = 0;
	} else {
		lazy_pool_add(znd, expg, DELAY_ADD);
	}

	return rcode;
}

/**
 * to_table_entry() - Deconstrct metadata page into mpinfo
 * @znd: ZDM instance
 * @lba: Address (4k resolution)
 * @expg: current metadata block in zltlst list.
 *
 * Return: Index into mpinfo.table.
 */
static int to_table_entry(struct zdm *znd, u64 lba, struct mpinfo *mpi)
{
	int index = -1;

	mpi->lock = &znd->mapkey_lock;

	if (lba >= znd->s_base && lba < znd->r_base) {
		index		= lba - znd->s_base;
		mpi->htable	= znd->fwd_hm;
		mpi->ht_order	= HASH_BITS(znd->fwd_hm);
		mpi->bit_type	= IS_LUT;
		mpi->bit_dir	= IS_FWD;
		mpi->crc.pg_no	= index >> CRC_BITS;
		mpi->crc.lba    = znd->c_base + mpi->crc.pg_no;
		mpi->crc.pg_idx	= index & CRC_MMSK;
		if (index < 0 ||  index >= znd->map_count) {
			Z_ERR(znd, "%s: FWD BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->map_count);
			dump_stack();
		}
	} else if (lba >= znd->r_base && lba < znd->c_base) {
		index		= lba - znd->r_base;
		mpi->htable	= znd->rev_hm;
		mpi->ht_order	= HASH_BITS(znd->rev_hm);
		mpi->bit_type	= IS_LUT;
		mpi->bit_dir	= IS_REV;
		mpi->crc.pg_no	= index >> CRC_BITS;
		mpi->crc.lba    = znd->c_mid + mpi->crc.pg_no;
		mpi->crc.pg_idx	= index & CRC_MMSK;
		if (index < 0 ||  index >= znd->map_count) {
			Z_ERR(znd, "%s: REV BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->map_count);
			dump_stack();
		}
	} else if (lba >= znd->c_base && lba < znd->c_mid) {
		index		= lba - znd->c_base;
		mpi->htable	= znd->fwd_hcrc;
		mpi->ht_order	= HASH_BITS(znd->fwd_hcrc);
		mpi->lock	= &znd->ct_lock;
		mpi->bit_type	= IS_CRC;
		mpi->bit_dir	= IS_FWD;
		mpi->crc.lba    = ~0ul;
		mpi->crc.pg_no	= 0;
		mpi->crc.pg_idx	= index & CRC_MMSK;
		if (index < 0 ||  index >= znd->crc_count) {
			Z_ERR(znd, "%s: CRC BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->crc_count);
			dump_stack();
		}
	} else if (lba >= znd->c_mid && lba < znd->c_end) {
		index		= lba - znd->c_mid;
		mpi->htable	= znd->rev_hcrc;
		mpi->ht_order	= HASH_BITS(znd->rev_hcrc);
		mpi->lock	= &znd->ct_lock;
		mpi->bit_type	= IS_CRC;
		mpi->bit_dir	= IS_REV;
		mpi->crc.lba    = ~0ul;
		mpi->crc.pg_no	= 1;
		mpi->crc.pg_idx	= (1 << CRC_BITS) + (index & CRC_MMSK);
		if (index < 0 ||  index >= znd->crc_count) {
			Z_ERR(znd, "%s: CRC BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->crc_count);
			dump_stack();
		}
	} else {
		Z_ERR(znd, "** Corrupt lba %" PRIx64 " not in range.", lba);
		znd->meta_result = -EIO;
		dump_stack();
	}
	mpi->index = index;
	return index;
}

/**
 * get_htbl_entry() - Get map_pg from Hashtable using Map Page Info
 * @znd: ZDM Instance
 * @mpi: Map Page Information descriptor.
 *
 * Returns the current map_pg or NULL if map_pg is not in core.
 */
static inline struct map_pg *get_htbl_entry(struct zdm *znd,
					    struct mpinfo *mpi)
{
	struct map_pg *obj;
	struct hlist_head *hlist;

	hlist = &mpi->htable[hash_min(mpi->index, mpi->ht_order)];
	hlist_for_each_entry(obj, hlist, hentry)
		if (obj->index == mpi->index)
			return obj;

	return NULL;
}

/**
 * add_htbl_entry() - Add a map_pg to Hashtable using Map Page Info
 * @znd: ZDM Instance
 * @mpi: Map Page Information descriptor.
 * @pg: Map Page to add
 *
 * Returns 1 if the pages was added. 0 if the page was already added.
 *
 * Since locks are not held between lookup and new page allocation races
 * can happen, caller is responsible for cleanup.
 */
static inline int add_htbl_entry(struct zdm *znd, struct mpinfo *mpi,
				 struct map_pg *pg)
{
	struct hlist_head *hlist;
	struct map_pg *obj = get_htbl_entry(znd, mpi);

	if (obj) {
		if (obj->lba != pg->lba) {
			Z_ERR(znd, "Page %" PRIx64 " already added %" PRIx64,
				obj->lba, pg->lba);
			dump_stack();
		}
		return 0;
	}

	hlist = &mpi->htable[hash_min(mpi->index, mpi->ht_order)];
	hlist_add_head(&pg->hentry, hlist);

	return 1;
}

/**
 * htbl_lut_hentry() - Get hash table head for map page lookup
 * @znd: ZDM Instance
 * @is_fwd: If the entry is for the forward map or reverse map.
 * @idx: The map page index.
 *
 * Returns the corresponding hash entry head.
 */
static inline struct hlist_head *htbl_lut_hentry(struct zdm *znd, int is_fwd,
						 int idx)
{
	struct hlist_head *hlist;

	if (is_fwd)
		hlist = &znd->fwd_hm[hash_min(idx, HASH_BITS(znd->fwd_hm))];
	else
		hlist = &znd->rev_hm[hash_min(idx, HASH_BITS(znd->rev_hm))];

	return hlist;
}

/**
 * htbl_crc_hentry() - Get hash table head for CRC page entry
 * @znd: ZDM Instance
 * @is_fwd: If the entry is for the forward map or reverse map.
 * @idx: The map page index.
 *
 * Returns the corresponding hash entry head.
 */
static inline struct hlist_head *htbl_crc_hentry(struct zdm *znd, int is_fwd,
						 int idx)
{
	struct hlist_head *hlist;

	if (is_fwd)
		hlist = &znd->fwd_hcrc[hash_min(idx, HASH_BITS(znd->fwd_hcrc))];
	else
		hlist = &znd->rev_hcrc[hash_min(idx, HASH_BITS(znd->rev_hcrc))];

	return hlist;
}

/**
 * del_htbl_entry() - Delete an entry from a hash table
 * @znd: ZDM Instance
 * @pg: Map Page to add
 *
 * Returns 1 if the pages was removed. 0 if the page was not found.
 *
 * Since locks are not held between lookup and page removal races
 * can happen, caller is responsible for cleanup.
 */
static inline int del_htbl_entry(struct zdm *znd, struct map_pg *pg)
{
	struct map_pg *obj;
	struct hlist_head *hlist;
	int is_fwd = test_bit(IS_FWD, &pg->flags);

	if (test_bit(IS_LUT, &pg->flags))
		hlist = htbl_lut_hentry(znd, is_fwd, pg->index);
	else
		hlist = htbl_crc_hentry(znd, is_fwd, pg->index);

	hlist_for_each_entry(obj, hlist, hentry) {
		if (obj->index == pg->index) {
			hash_del(&pg->hentry);
			return 1;
		}
	}

	return 0;
}

/**
 * is_ready_for_gc() - Test zone flags for GC sanity and ready flag.
 * @znd: ZDM instance
 * @z_id: Address (4k resolution)
 *
 * Return: non-zero if zone is suitable for GC.
 */
static inline int is_ready_for_gc(struct zdm *znd, u32 z_id)
{
	u32 gzno  = z_id >> GZ_BITS;
	u32 gzoff = z_id & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];
	u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
	u32 used = le32_to_cpu(wpg->wp_used[gzoff]) & Z_WP_VALUE_MASK;

	if (((wp & Z_WP_GC_BITS) == Z_WP_GC_READY) && (used == Z_BLKSZ))
		return 1;
	return 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 *  generic-ish n-way alloc/free
 *  Use kmalloc for small (< 4k) allocations.
 *  Use vmalloc for multi-page alloctions
 *  Except:
 *  Use multipage allocations for dm_io'd pages that a frequently hit.
 *
 *  NOTE: ALL allocations are zero'd before returning.
 *	  alloc/free count is tracked for dynamic analysis.
 */

#define GET_ZPG		0x040000
#define GET_KM		0x080000
#define GET_VM		0x100000

#define PG_01    (GET_ZPG |  1)
#define PG_02    (GET_ZPG |  2)
#define PG_05    (GET_ZPG |  5)
#define PG_06    (GET_ZPG |  6)
#define PG_08    (GET_ZPG |  8)
#define PG_09    (GET_ZPG |  9)
#define PG_10    (GET_ZPG | 10)
#define PG_11    (GET_ZPG | 11)
#define PG_13    (GET_ZPG | 13)
#define PG_17    (GET_ZPG | 17)
#define PG_27    (GET_ZPG | 27)

#define KM_00    (GET_KM  |  0)
#define KM_07    (GET_KM  |  7)
#define KM_14    (GET_KM  | 14)
#define KM_15    (GET_KM  | 15)
#define KM_16    (GET_KM  | 16)
#define KM_18    (GET_KM  | 18)
#define KM_19    (GET_KM  | 19)
#define KM_20    (GET_KM  | 20)
#define KM_25    (GET_KM  | 25)
#define KM_26    (GET_KM  | 26)
#define KM_28    (GET_KM  | 28)
#define KM_29    (GET_KM  | 29)
#define KM_30    (GET_KM  | 30)

#define VM_03    (GET_VM  |  3)
#define VM_04    (GET_VM  |  4)
#define VM_12    (GET_VM  | 12)
#define VM_21    (GET_VM  | 21)
#define VM_22    (GET_VM  | 22)

#define ZDM_FREE(z, _p, sz, id) \
	do { zdm_free((z), (_p), (sz), (id)); (_p) = NULL; } while (0)

#define ZDM_ALLOC(z, sz, id, gfp)       zdm_alloc((z), (sz), (id), (gfp))
#define ZDM_CALLOC(z, n, sz, id, gfp)   zdm_calloc((z), (n), (sz), (id), (gfp))

/**
 * zdm_free_debug() - Extra cleanup for memory debugging.
 * @znd: ZDM instance
 * @p: memory to be released.
 * @sz: allocated size.
 * @id: allocation bin.
 *
 * Additional alloc/free debugging and statistics handling.
 */
static inline void zdm_free_debug(struct zdm *znd, void *p, size_t sz, int id)
{
#if ALLOC_DEBUG
	int iter;

	if (atomic_read(&znd->allocs) > znd->hw_allocs)
		znd->hw_allocs = atomic_read(&znd->allocs);
	atomic_dec(&znd->allocs);
	for (iter = 0; iter < znd->hw_allocs; iter++) {
		if (p == znd->alloc_trace[iter]) {
			znd->alloc_trace[iter] = NULL;
			break;
		}
	}
	if (iter == znd->hw_allocs) {
		Z_ERR(znd, "Free'd something *NOT* allocated? %d", id);
		dump_stack();
	}
#endif

	SpinLock(&znd->stats_lock);
	if (sz > znd->memstat)
		Z_ERR(znd, "Free'd more mem than allocated? %d", id);

	if (sz > znd->bins[id]) {
		Z_ERR(znd, "Free'd more mem than allocated? %d", id);
		dump_stack();
	}
	znd->memstat -= sz;
	znd->bins[id] -= sz;
	spin_unlock(&znd->stats_lock);
}

/**
 * zdm_free() - Unified free by allocation 'code'
 * @znd: ZDM instance
 * @p: memory to be released.
 * @sz: allocated size.
 * @code: allocation size
 *
 * This (ugly) unified scheme helps to find leaks and monitor usage
 *   via ioctl tools.
 */
static void zdm_free(struct zdm *znd, void *p, size_t sz, u32 code)
{
	int id    = code & 0x00FFFF;
	int flag  = code & 0xFF0000;

	if (p) {

		if (znd)
			zdm_free_debug(znd, p, sz, id);

		memset(p, 0, sz); /* DEBUG */

		switch (flag) {
		case GET_ZPG:
			free_page((unsigned long)p);
			break;
		case GET_KM:
			kfree(p);
			break;
		case GET_VM:
			vfree(p);
			break;
		default:
			Z_ERR(znd,
			      "zdm_free %p scheme %x not mapped.", p, code);
			break;
		}
	} else {
		Z_ERR(znd, "double zdm_free %p [%d]", p, id);
		dump_stack();
	}
}

/**
 * zdm_free_debug() - Extra tracking for memory debugging.
 * @znd: ZDM instance
 * @p: memory to be released.
 * @sz: allocated size.
 * @id: allocation bin.
 *
 * Additional alloc/free debugging and statistics handling.
 */
static inline
void zdm_alloc_debug(struct zdm *znd, void *p, size_t sz, int id)
{
#if ALLOC_DEBUG
	int iter;
	int count;

	atomic_inc(&znd->allocs);

	count = atomic_read(&znd->allocs);
	for (iter = 0; iter < count; iter++) {
		if (!znd->alloc_trace[iter]) {
			znd->alloc_trace[iter] = p;
			break;
		}
	}
#endif

	SpinLock(&znd->stats_lock);
	znd->memstat += sz;
	znd->bins[id] += sz;
	spin_unlock(&znd->stats_lock);
}


/**
 * zdm_alloc() - Unified alloc by 'code':
 * @znd: ZDM instance
 * @sz: allocated size.
 * @code: allocation size
 * @gfp: kernel allocation flags.
 *
 * There a few things (like dm_io) that seem to need pages and not just
 *   kmalloc'd memory.
 *
 * This (ugly) unified scheme helps to find leaks and monitor usage
 *   via ioctl tools.
 */
static void *zdm_alloc(struct zdm *znd, size_t sz, int code, gfp_t gfp)
{
	void *pmem = NULL;
	int id    = code & 0x00FFFF;
	int flag  = code & 0xFF0000;
	gfp_t gfp_mask;

#if USE_KTHREAD
	gfp_mask = GFP_KERNEL;
#else
	gfp_mask = gfp ? GFP_ATOMIC : GFP_KERNEL;
#endif

	if (flag == GET_VM)
		might_sleep();

	if (gfp_mask != GFP_ATOMIC)
		might_sleep();

	switch (flag) {
	case GET_ZPG:
		pmem = (void *)get_zeroed_page(gfp_mask);
		if (!pmem && gfp_mask == GFP_ATOMIC) {
			Z_ERR(znd, "No atomic for %d, try noio.", id);
			pmem = (void *)get_zeroed_page(GFP_NOIO);
		}
		break;
	case GET_KM:
		pmem = kzalloc(sz, gfp_mask);
		if (!pmem && gfp_mask == GFP_ATOMIC) {
			Z_ERR(znd, "No atomic for %d, try noio.", id);
			pmem = kzalloc(sz, GFP_NOIO);
		}
		break;
	case GET_VM:
		WARN_ON(gfp);
		pmem = vzalloc(sz);
		break;
	default:
		Z_ERR(znd, "zdm alloc scheme for %u unknown.", code);
		break;
	}

	if (pmem) {
		if (znd)
			zdm_alloc_debug(znd, pmem, sz, id);
	} else {
		Z_ERR(znd, "Out of memory. %d", id);
		dump_stack();
	}

	return pmem;
}

/**
 * zdm_calloc() - Unified alloc by 'code':
 * @znd: ZDM instance
 * @n: number of elements in array.
 * @sz: allocation size of each element.
 * @c: allocation strategy (VM, KM, PAGE, N-PAGES).
 * @q: kernel allocation flags.
 *
 * calloc is just an zeroed memory array alloc.
 * all zdm_alloc schemes are for zeroed memory so no extra memset needed.
 */
static void *zdm_calloc(struct zdm *znd, size_t n, size_t sz, int c, gfp_t q)
{
	return zdm_alloc(znd, sz * n, c, q);
}

/**
 * get_io_vcache() - Get a pre-allocated pool of memory for IO.
 * @znd: ZDM instance
 * @gfp: Allocation flags if no pre-allocated pool can be found.
 *
 * Return: Pointer to pool memory or NULL.
 */
static struct io_4k_block *get_io_vcache(struct zdm *znd, gfp_t gfp)
{
	struct io_4k_block *cache = NULL;
	int avail;

	might_sleep();

	for (avail = 0; avail < ARRAY_SIZE(znd->io_vcache); avail++) {
		if (!test_and_set_bit(avail, &znd->io_vcache_flags)) {
			cache = znd->io_vcache[avail];
			if (!cache)
				znd->io_vcache[avail] = cache =
					ZDM_CALLOC(znd, IO_VCACHE_PAGES,
						sizeof(*cache), VM_12, gfp);
			if (cache)
				break;
		}
	}
	return cache;
}

/**
 * put_io_vcache() - Get a pre-allocated pool of memory for IO.
 * @znd: ZDM instance
 * @cache: Allocated cache entry.
 *
 * Return: Pointer to pool memory or NULL.
 */
static int put_io_vcache(struct zdm *znd, struct io_4k_block *cache)
{
	int err = -ENOENT;
	int avail;

	if (cache) {
		for (avail = 0; avail < ARRAY_SIZE(znd->io_vcache); avail++) {
			if (cache == znd->io_vcache[avail]) {
				WARN_ON(!test_and_clear_bit(avail,
					&znd->io_vcache_flags));
				err = 0;
				break;
			}
		}
	}
	return err;
}

/**
 * map_value() - translate a lookup table entry to a Sector #, or LBA.
 * @znd: ZDM instance
 * @delta: little endian map entry.
 *
 * Return: LBA or 0 if invalid.
 */
static inline u64 map_value(struct zdm *znd, __le32 delta)
{
	u64 mval = 0ul;

	if ((delta != MZTEV_UNUSED) && (delta != MZTEV_NF))
		mval = le32_to_cpu(delta);

	return mval;
}

/**
 * map_encode() - Encode a Sector # or LBA to a lookup table entry value.
 * @znd: ZDM instance
 * @to_addr: address to encode.
 * @value: encoded value
 *
 * Return: 0.
 */
static int map_encode(struct zdm *znd, u64 to_addr, __le32 *value)
{
	int err = 0;

	*value = MZTEV_UNUSED;
	if (to_addr != BAD_ADDR)
		*value = cpu_to_le32((u32)to_addr);

	return err;
}

/**
 * release_memcache() - Free all the memcache blocks.
 * @znd: ZDM instance
 *
 * Return: 0.
 */
static int release_memcache(struct zdm *znd)
{
	int no;

	for (no = 0; no < MAP_COUNT; no++) {
		struct list_head *head = &(znd->mclist[no]);
		struct map_cache *mcache;
		struct map_cache *_mc;

		if (list_empty(head))
			return 0;

		SpinLock(&znd->mclck[no]);
		list_for_each_entry_safe(mcache, _mc, head, mclist) {
			list_del(&mcache->mclist);
			ZDM_FREE(znd, mcache->jdata, Z_C4K, PG_08);
			ZDM_FREE(znd, mcache, sizeof(*mcache), KM_07);
		}
		spin_unlock(&znd->mclck[no]);

	}
	return 0;
}

/**
 * warn_bad_lba() - Warn if a give LBA is not valid (Esp if beyond a WP)
 * @znd: ZDM instance
 * @lba48: 48 bit lba.
 *
 * Return: non-zero if lba is not valid.
 */
static inline int warn_bad_lba(struct zdm *znd, u64 lba48)
{
#define FMT_ERR "LBA %" PRIx64 " is not valid: Z# %u, off:%x wp:%x"
	int rcode = 0;
	u32 zone;

	if (lba48 < znd->data_lba)
		return rcode;

	zone = _calc_zone(znd, lba48);
	if (zone < znd->data_zones) { /* FIXME: MAYBE? md_end */
		u32 gzoff = zone & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[zone >> GZ_BITS];
		u32 wp_at = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_VALUE_MASK;
		u16 off = (lba48 - znd->data_lba) % Z_BLKSZ;

		if (off >= wp_at) {
			rcode = 1;
			Z_ERR(znd, FMT_ERR, lba48, zone, off, wp_at);
			dump_stack();
		}
	} else {
		rcode = 1;
		Z_ERR(znd, "LBA is not valid - Z# %u, count %u",
		      zone, znd->data_zones);
	}

	return rcode;
}

/**
 * mapped_free() - Release a page of lookup table entries.
 * @znd: ZDM instance
 * @mapped: mapped page struct to free.
 */
static void mapped_free(struct zdm *znd, struct map_pg *mapped)
{
	if (mapped) {
		MutexLock(&mapped->md_lock);
		WARN_ON(test_bit(IS_DIRTY, &mapped->flags));
		if (mapped->data.addr) {
			ZDM_FREE(znd, mapped->data.addr, Z_C4K, PG_27);
			atomic_dec(&znd->incore);
		}
		mutex_unlock(&mapped->md_lock);
		ZDM_FREE(znd, mapped, sizeof(*mapped), KM_20);
	}
}

/**
 * flush_map() - write dirty map entries to disk.
 * @znd: ZDM instance
 * @map: Array of mapped pages.
 * @count: number of elements in range.
 * Return: non-zero on error.
 */
static int flush_map(struct zdm *znd, struct hlist_head *hmap, u32 count)
{
	struct map_pg *pg;
	const int use_wq = 1;
	const int sync = 1;
	int err = 0;
	u32 ii;

	if (!hmap)
		return err;

	for (ii = 0; ii < count; ii++) {
		hlist_for_each_entry(pg, &hmap[ii], hentry) {
			if (pg && pg->data.addr) {
				cache_if_dirty(znd, pg, use_wq);
				err |= write_if_dirty(znd, pg, use_wq, sync);
			}
		}
	}

	return err;
}

/**
 * zoned_io_flush() - flush all pending IO.
 * @znd: ZDM instance
 */
static int zoned_io_flush(struct zdm *znd)
{
	int err = 0;

	set_bit(ZF_FREEZE, &znd->flags);
	atomic_inc(&znd->gc_throttle);

	mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
	flush_delayed_work(&znd->gc_work);

	clear_bit(DO_GC_NO_PURGE, &znd->flags);
	set_bit(DO_MAPCACHE_MOVE, &znd->flags);
	set_bit(DO_MEMPOOL, &znd->flags);
	set_bit(DO_SYNC, &znd->flags);
	set_bit(DO_FLUSH, &znd->flags);
	queue_work(znd->meta_wq, &znd->meta_work);
	flush_workqueue(znd->meta_wq);
	flush_workqueue(znd->bg_wq);

	mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
	flush_delayed_work(&znd->gc_work);
	atomic_dec(&znd->gc_throttle);

	spin_lock(&znd->lzy_lck);
	INIT_LIST_HEAD(&znd->lzy_pool);
	spin_unlock(&znd->lzy_lck);

	spin_lock(&znd->zlt_lck);
	INIT_LIST_HEAD(&znd->zltpool);
	spin_unlock(&znd->zlt_lck);

	err = flush_map(znd, znd->fwd_hm, ARRAY_SIZE(znd->fwd_hm));
	if (err)
		goto out;

	err = flush_map(znd, znd->rev_hm, ARRAY_SIZE(znd->rev_hm));
	if (err)
		goto out;

	err = flush_map(znd, znd->fwd_hcrc, ARRAY_SIZE(znd->fwd_hcrc));
	if (err)
		goto out;

	err = flush_map(znd, znd->rev_hcrc, ARRAY_SIZE(znd->rev_hcrc));
	if (err)
		goto out;

	set_bit(DO_SYNC, &znd->flags);
	set_bit(DO_FLUSH, &znd->flags);
	queue_work(znd->meta_wq, &znd->meta_work);
	flush_workqueue(znd->meta_wq);

out:
	return err;
}

/**
 * release_table_pages() - flush and free all table map entries.
 * @znd: ZDM instance
 */
static void release_table_pages(struct zdm *znd)
{
	int entry;
	struct map_pg *expg;
	struct hlist_node *_tmp;

	spin_lock(&znd->mapkey_lock);
	hash_for_each_safe(znd->fwd_hm, entry, _tmp, expg, hentry) {
		hash_del(&expg->hentry);
		mapped_free(znd, expg);
	}
	hash_for_each_safe(znd->rev_hm, entry, _tmp, expg, hentry) {
		hash_del(&expg->hentry);
		mapped_free(znd, expg);
	}
	spin_unlock(&znd->mapkey_lock);

	spin_lock(&znd->ct_lock);
	hash_for_each_safe(znd->fwd_hcrc, entry, _tmp, expg, hentry) {
		hash_del(&expg->hentry);
		mapped_free(znd, expg);
	}
	hash_for_each_safe(znd->rev_hcrc, entry, _tmp, expg, hentry) {
		hash_del(&expg->hentry);
		mapped_free(znd, expg);
	}
	spin_unlock(&znd->ct_lock);
}

/**
 * _release_wp() - free all WP alloc/usage/used data.
 * @znd: ZDM instance
 * @wp: Object to free.
 */
static void _release_wp(struct zdm *znd, struct meta_pg *wp)
{
	u32 gzno;

	for (gzno = 0; gzno < znd->gz_count; gzno++) {
		struct meta_pg *wpg = &wp[gzno];

		if (wpg->wp_alloc)
			ZDM_FREE(znd, wpg->wp_alloc, Z_C4K, PG_06);
		if (wpg->zf_est)
			ZDM_FREE(znd, wpg->zf_est, Z_C4K, PG_06);
		if (wpg->wp_used)
			ZDM_FREE(znd, wpg->wp_used, Z_C4K, PG_06);
	}
	ZDM_FREE(znd, wp, znd->gz_count * sizeof(*wp), VM_21);
	znd->wp = NULL;
}

/**
 * _free_md_journal() - Free MD Journal memory.
 * @znd: ZDM instance
 */
static void _free_md_journal(struct zdm *znd)
{
	struct md_journal *jrnl = &znd->jrnl;
	u32 avail = jrnl->size;

	if (znd->jrnl.wb && avail)
		ZDM_FREE(znd, jrnl->wb, avail * sizeof(*jrnl->wb), VM_03);
	jrnl->size = 0;
}

/**
 * zoned_destroy() - Teardown a zdm device mapper instance.
 * @znd: ZDM instance
 */
static void zoned_destroy(struct zdm *znd)
{
	int purge;

	if (znd->timer.data)
		del_timer_sync(&znd->timer);

	if (znd->gc_wq && znd->meta_wq)
		if (zoned_io_flush(znd))
			Z_ERR(znd, "sync/flush failure");

	release_table_pages(znd);
	release_memcache(znd);

	if (znd->dev) {
		dm_put_device(znd->ti, znd->dev);
		znd->dev = NULL;
	}
	if (znd->io_wq) {
		destroy_workqueue(znd->io_wq);
		znd->io_wq = NULL;
	}
	if (znd->zone_action_wq) {
		destroy_workqueue(znd->zone_action_wq);
		znd->zone_action_wq = NULL;
	}
	if (znd->bg_wq) {
		destroy_workqueue(znd->bg_wq);
		znd->bg_wq = NULL;
	}
	if (znd->gc_wq) {
		destroy_workqueue(znd->gc_wq);
		znd->gc_wq = NULL;
	}
	if (znd->meta_wq) {
		destroy_workqueue(znd->meta_wq);
		znd->meta_wq = NULL;
	}
	if (znd->io_client)
		dm_io_client_destroy(znd->io_client);
	if (znd->wp)
		_release_wp(znd, znd->wp);
	if (znd->md_crcs)
		ZDM_FREE(znd, znd->md_crcs, Z_C4K * 2, VM_22);
	if (znd->gc_io_buf)
		ZDM_FREE(znd, znd->gc_io_buf, Z_C4K * GC_MAX_STRIPE, VM_04);
	if (znd->gc_postmap.jdata) {
		size_t sz = Z_BLKSZ * sizeof(*znd->gc_postmap.jdata);

		ZDM_FREE(znd, znd->gc_postmap.jdata, sz, VM_03);
	}

	for (purge = 0; purge < ARRAY_SIZE(znd->io_vcache); purge++) {
		size_t vcsz = IO_VCACHE_PAGES * sizeof(struct io_4k_block *);

		if (znd->io_vcache[purge]) {
			if (test_and_clear_bit(purge, &znd->io_vcache_flags))
				Z_ERR(znd, "sync cache entry %d still in use!",
				      purge);
			ZDM_FREE(znd, znd->io_vcache[purge], vcsz, VM_12);
		}
	}
	if (znd->z_sballoc)
		ZDM_FREE(znd, znd->z_sballoc, Z_C4K, PG_05);

	_free_md_journal(znd);

	ZDM_FREE(NULL, znd, sizeof(*znd), KM_00);
}

/**
 * _init_md_journal() - Allocate space for meta data journaling.
 * @znd: ZDM instance
 *
 * Return: 0 on success, else error code.
 */
static int _init_md_journal(struct zdm *znd)
{
	struct md_journal *jrnl = &znd->jrnl;
	u32 blks = (znd->md_start - WB_JRNL_BASE) >> 12;
	u32 avail = (blks << 12) / sizeof(*jrnl->wb);
	u32 iter;

	Z_ERR(znd, "MD journal space: %u [%u]", avail, blks);

	spin_lock_init(&znd->jrnl.wb_alloc);
	if (avail > WB_JRNL_MAX)
		avail = WB_JRNL_MAX;

//	if (avail > WB_JRNL_MIN)
//		avail = WB_JRNL_MIN;

	jrnl->wb = ZDM_ALLOC(znd, avail * sizeof(*jrnl->wb), VM_03, NORMAL);
	jrnl->size = avail;

	if (!jrnl->wb) {
		Z_ERR(znd, "Failed to alloc wb reverse map: %u", avail);
		_free_md_journal(znd);
		return -ENOMEM;
	}

	for (iter = 0; iter < avail; iter++)
		jrnl->wb[iter] = MZTEV_UNUSED;

	return 0;
}

/**
 * _init_streams() - Setup initial conditions for streams and reserved zones.
 * @znd: ZDM instance
 */
static void _init_streams(struct zdm *znd)
{
	u64 stream;

	for (stream = 0; stream < ARRAY_SIZE(znd->bmkeys->stream); stream++)
		znd->bmkeys->stream[stream] = cpu_to_le32(NOZONE);

	znd->z_meta_resv = cpu_to_le32(znd->data_zones - 2);
	znd->z_gc_resv   = cpu_to_le32(znd->data_zones - 1);
	znd->z_gc_free   = znd->data_zones - 2;
}

/**
 * _init_mdcrcs() - Setup initial values for empty CRC blocks.
 * @znd: ZDM instance
 */
static void _init_mdcrcs(struct zdm *znd)
{
	int idx;

	for (idx = 0; idx < Z_C4K; idx++)
		znd->md_crcs[idx] = MD_CRC_INIT;
}

/**
 * _init_wp() - Setup initial usage for empty data zones.
 * @znd: ZDM instance
 */
static void _init_wp(struct zdm *znd, u32 gzno, struct meta_pg *wpg)
{
	u32 gzcount = 1 << GZ_BITS;
	u32 iter;

	if (znd->data_zones < ((gzno+1) << GZ_BITS))
		gzcount = znd->data_zones & GZ_MMSK;

	/* mark as empty */
	for (iter = 0; iter < gzcount; iter++)
		wpg->zf_est[iter] = cpu_to_le32(Z_BLKSZ);

	/* mark as n/a -- full */
	gzcount = 1 << GZ_BITS;
	for (; iter < gzcount; iter++) {
		wpg->wp_alloc[iter] = cpu_to_le32(~0u);
		wpg->wp_used[iter] = cpu_to_le32(~0u);
	}
}

/**
 * _alloc_wp() - Allocate needed WP (meta_pg) objects
 * @znd: ZDM instance
 */
struct meta_pg *_alloc_wp(struct zdm *znd)
{
	struct meta_pg *wp;
	u32 gzno;

	wp = ZDM_CALLOC(znd, znd->gz_count, sizeof(*znd->wp), VM_21, NORMAL);
	if (!wp)
		goto out;
	for (gzno = 0; gzno < znd->gz_count; gzno++) {
		struct meta_pg *wpg = &wp[gzno];

		spin_lock_init(&wpg->wplck);
		wpg->lba      = 2048ul + (gzno * 2);
		wpg->wp_alloc = ZDM_ALLOC(znd, Z_C4K, PG_06, NORMAL);
		wpg->zf_est   = ZDM_ALLOC(znd, Z_C4K, PG_06, NORMAL);
		wpg->wp_used  = ZDM_ALLOC(znd, Z_C4K, PG_06, NORMAL);
		if (!wpg->wp_alloc || !wpg->zf_est || !wpg->wp_used) {
			_release_wp(znd, wp);
			wp = NULL;
			goto out;
		}
		_init_wp(znd, gzno, wpg);
		set_bit(IS_DIRTY, &wpg->flags);
		clear_bit(IS_FLUSH, &wpg->flags);
	}

out:
	return wp;
}

/**
 * dmz_report_count() - Read number of zones returned.
 * @znd: ZDM Instance
 * @rpt_in: Report data.
 * @bufsz: size of space allocated for report
 *
 * Return: Number of zones in report.
 */
static u32 dmz_report_count(struct zdm *znd, void *rpt_in, size_t bufsz)
{
	u32 count;
	u32 max_count = (bufsz - sizeof(struct bdev_zone_report))
		      /	 sizeof(struct bdev_zone_descriptor);

	if (znd->ata_passthrough) {
		struct bdev_zone_report_le *report = rpt_in;

		/* ZAC: ata results are little endian */
		if (max_count > le32_to_cpu(report->descriptor_count))
			report->descriptor_count = cpu_to_le32(max_count);
		count = le32_to_cpu(report->descriptor_count);
	} else {
		struct bdev_zone_report *report = rpt_in;

		/* ZBC: scsi results are big endian */
		if (max_count > be32_to_cpu(report->descriptor_count))
			report->descriptor_count = cpu_to_be32(max_count);
		count = be32_to_cpu(report->descriptor_count);
	}
	return count;
}

/**
 * is_conventional() - Determine if zone is conventional.
 * @dentry: Zone descriptor entry.
 *
 * Return: 1 if zone type is conventional.
 */
static inline int is_conventional(struct bdev_zone_descriptor *dentry)
{
	return (ZTYP_CONVENTIONAL == (dentry->type & 0x0F)) ? 1 : 0;
}

/**
 * is_zone_reset() - Determine if zone is reset / ready for writing.
 * @dentry: Zone descriptor entry.
 *
 * Return: 1 if zone condition is empty or zone type is conventional.
 */
static inline int is_zone_reset(struct bdev_zone_descriptor *dentry)
{
	u8 type = dentry->type & 0x0F;
	u8 cond = (dentry->flags & 0xF0) >> 4;

	return (ZCOND_ZC1_EMPTY == cond || ZTYP_CONVENTIONAL == type) ? 1 : 0;
}

/**
 * get_wp_from_descriptor() - Decode write pointer as # of blocks from start
 * @znd: ZDM Instance
 * @dentry_in: Zone descriptor entry.
 *
 * Return: Write Pointer as number of blocks from start of zone.
 */
static inline u32 get_wp_from_descriptor(struct zdm *znd, void *dentry_in)
{
	u32 wp = 0;

	/*
	 * If ATA passthrough was used then ZAC results are little endian.
	 * otherwise ZBC results are big endian.
	 */

	if (znd->ata_passthrough) {
		struct bdev_zone_descriptor_le *lil = dentry_in;

		wp = le64_to_cpu(lil->lba_wptr) - le64_to_cpu(lil->lba_start);
	} else {
		struct bdev_zone_descriptor *big = dentry_in;

		wp = be64_to_cpu(big->lba_wptr) - be64_to_cpu(big->lba_start);
	}
	return wp;
}

/**
 * _dec_wp_avail_by_lost() - Update free count due to lost/unusable blocks.
 * @wpg: Write pointer metadata page.
 * @gzoff: Zone entry in page.
 * @lost: Number of blocks 'lost'.
 */
static inline
void _dec_wp_avail_by_lost(struct meta_pg *wpg, u32 gzoff, u32 lost)
{
	wpg->zf_est[gzoff] = cpu_to_le32(le32_to_cpu(wpg->zf_est[gzoff])+lost);
}

/**
 * zoned_wp_sync() - Re-Sync expected WP location with drive
 * @znd: ZDM Instance
 * @reset_non_empty: Reset the non-empty zones.
 *
 * Return: 0 on success, otherwise error.
 */
static int zoned_wp_sync(struct zdm *znd, int reset_non_empty)
{
	int rcode = 0;
	u32 rcount = 0;
	u32 iter;
	struct bdev_zone_report *report = NULL;
	int order = REPORT_ORDER;
	size_t bufsz = REPORT_FILL_PGS * Z_C4K;
	struct page *pgs = alloc_pages(GFP_KERNEL, order);

	if (pgs)
		report = page_address(pgs);

	if (!report) {
		rcode = -ENOMEM;
		goto out;
	}

	for (iter = 0; iter < znd->data_zones; iter++) {
		u32 entry = iter % 4096;
		u32 gzno  = iter >> 10;
		u32 gzoff = iter & ((1 << 10) - 1);
		struct meta_pg *wpg = &znd->wp[gzno];
		struct bdev_zone_descriptor *dentry;
		u32 wp_flgs;
		u32 wp_at;
		u32 wp;

		if (entry == 0) {
			int err = dmz_report_zones(znd, iter, pgs, bufsz);

			if (err) {
				Z_ERR(znd, "report zones-> %d", err);
				if (err != -ENOTSUPP)
					rcode = err;
				goto out;
			}
			rcount = dmz_report_count(znd, report, bufsz);
		}

		dentry = &report->descriptors[entry];
		if (reset_non_empty && !is_conventional(dentry)) {
			int err = 0;

			if (!is_zone_reset(dentry))
				err = dmz_reset_wp(znd, iter);

			if (err) {
				Z_ERR(znd, "reset wp-> %d", err);
				if (err != -ENOTSUPP)
					rcode = err;
				goto out;
			}
			wp = wp_at = 0;
			wpg->wp_alloc[gzoff] = cpu_to_le32(0);
			wpg->zf_est[gzoff] = cpu_to_le32(Z_BLKSZ);
			wpg->wp_used[gzoff] = wpg->wp_alloc[gzoff];
			continue;
		}

		wp = get_wp_from_descriptor(znd, dentry);
		wp >>= Z_SHFT4K; /* 512 sectors to 4k sectors */
		wp_at = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_VALUE_MASK;
		wp_flgs = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_FLAGS_MASK;

		if (is_conventional(dentry)) {
			wp = wp_at;
			wp_flgs |= Z_WP_NON_SEQ;
		} else {
			wp_flgs &= ~Z_WP_NON_SEQ;
		}

		if (wp > wp_at) {
			u32 lost = wp - wp_at;

			wp_at = wp;
			_dec_wp_avail_by_lost(wpg, gzoff, lost);

			Z_ERR(znd, "Z#%u z:%x [wp:%x rz:%x] lost %u blocks.",
			      iter, gzoff, wp_at, wp, lost);
		}
		wpg->wp_alloc[gzoff] = cpu_to_le32(wp_at|wp_flgs);
		wpg->wp_used[gzoff] = cpu_to_le32(wp_at);
	}

out:
	if (pgs)
		__free_pages(pgs, order);

	return rcode;
}

/**
 * do_init_zoned() - Initialize a zdm device mapper instance
 * @ti: DM Target Info
 * @znd: ZDM Target
 *
 * Return: 0 on success.
 *
 * Setup the zone pointer table and do a one time calculation of some
 * basic limits.
 *
 *  While start of partition may not be zone aligned
 *  md_start, data_lba and md_end are all zone aligned.
 *  From start of partition [or device] to md_start some conv/pref
 *  space is required for superblocks, memcache, zone pointers, crcs
 *  and optionally pinned forward lookup blocks.
 *
 *    0 < znd->md_start <= znd->data_lba <= znd->md_end
 *
 *  incoming [FS sectors] are linearly mapped after md_end.
 *  And blocks following data_lba are serialzied into zones either with
 *  explicit stream id support from BIOs [FUTURE], or implictly by LBA
 *  or type of data.
 */
static int do_init_zoned(struct dm_target *ti, struct zdm *znd)
{
	u64 size = i_size_read(get_bdev_bd_inode(znd));
	u64 bdev_nr_sect4k = size / Z_C4K;
	u64 data_zones = (bdev_nr_sect4k >> Z_BLKBITS) - znd->zdstart;
	u64 mzcount = dm_div_up(data_zones, MAX_ZONES_PER_MZ);
	u64 reserved = WB_JRNL_BASE;
	u64 z0_lba = dm_round_up(znd->start_sect, Z_BLKSZ);
	u64 mapct = dm_div_up(data_zones << Z_BLKBITS, 1024);
	u64 crcct = dm_div_up(mapct, 2048);
	int type;
	u32 mz_min = 0; /* cache */
	int rcode = 0;

	INIT_LIST_HEAD(&znd->zltpool);
	INIT_LIST_HEAD(&znd->lzy_pool);

	for (type = 0; type < MAP_COUNT; type++) {
		INIT_LIST_HEAD(&znd->mclist[type]);
		spin_lock_init(&znd->mclck[type]);
	}
	spin_lock_init(&znd->zlt_lck);
	spin_lock_init(&znd->lzy_lck);
	spin_lock_init(&znd->gc_lock);
	spin_lock_init(&znd->stats_lock);
	spin_lock_init(&znd->mapkey_lock);
	spin_lock_init(&znd->ct_lock);

	mutex_init(&znd->pool_mtx);
	mutex_init(&znd->gc_wait);
	mutex_init(&znd->gc_postmap.cached_lock);
	mutex_init(&znd->gc_vcio_lock);
	mutex_init(&znd->vcio_lock);
	mutex_init(&znd->mz_io_mutex);

	znd->data_zones = data_zones;
	znd->gz_count = mzcount;
	znd->crc_count = crcct;
	znd->map_count = mapct;

	/* z0_lba - lba of first full zone in disk addr space      */

	znd->md_start = z0_lba - znd->start_sect;
	if (znd->md_start < (reserved + WB_JRNL_MIN)) {
		znd->md_start += Z_BLKSZ;
		mz_min++;
	}

	/* md_start - lba of first full zone in partition addr space */
	znd->s_base = znd->md_start;
	mz_min += mzcount;
	if (mz_min < znd->zdstart)
		set_bit(ZF_POOL_FWD, &znd->flags);

	znd->r_base = znd->s_base + (mzcount << Z_BLKBITS);
	mz_min += mzcount;
	if (mz_min < znd->zdstart)
		set_bit(ZF_POOL_REV, &znd->flags);

	znd->c_base = znd->r_base + (mzcount << Z_BLKBITS);
	znd->c_mid  = znd->c_base + (mzcount * 0x20);
	znd->c_end  = znd->c_mid + (mzcount * 0x20);
	mz_min++;

	if (mz_min < znd->zdstart) {
		Z_ERR(znd, "Conv space for CRCs: Seting ZF_POOL_CRCS");
		set_bit(ZF_POOL_CRCS, &znd->flags);
	}

	if (test_bit(ZF_POOL_FWD, &znd->flags)) {
		znd->sk_low = znd->sk_high = 0;
	} else {
		znd->sk_low = znd->s_base;
		znd->sk_high = znd->sk_low + (mzcount * 0x40);
	}

	/* logical *ending* lba for meta [bumped up to next zone alignment] */
	znd->md_end = znd->c_base + (1 << Z_BLKBITS);

	/* actual starting lba for data pool */
	znd->data_lba = znd->md_end;
	if (!test_bit(ZF_POOL_CRCS, &znd->flags))
		znd->data_lba = znd->c_base;
	if (!test_bit(ZF_POOL_REV, &znd->flags))
		znd->data_lba = znd->r_base;
	if (!test_bit(ZF_POOL_FWD, &znd->flags))
		znd->data_lba = znd->s_base;

	/* NOTE: md_end == data_lba => all meta is in conventional zones. */
	Z_INFO(znd, "ZDM #%u", BUILD_NO);
	Z_INFO(znd, "Start Sect: %" PRIx64 " ZDStart# %u md- start %" PRIx64
		   " end %" PRIx64 " Data LBA %" PRIx64,
		znd->start_sect, znd->zdstart, znd->md_start,
		znd->md_end, znd->data_lba);

	Z_INFO(znd, "%s: size:%" PRIu64 " zones: %u, gzs %u, resvd %d",
	       __func__, size, znd->data_zones, znd->gz_count,
	       znd->gz_count * znd->mz_provision);

#if ALLOC_DEBUG
	znd->alloc_trace = vzalloc(sizeof(*znd->alloc_trace) * 65536);
	if (!znd->alloc_trace) {
		ti->error = "couldn't allocate in-memory mem debug trace";
		rcode = -ENOMEM;
		goto out;
	}
#endif

	znd->z_sballoc = ZDM_ALLOC(znd, Z_C4K, PG_05, NORMAL);
	if (!znd->z_sballoc) {
		ti->error = "couldn't allocate in-memory superblock";
		rcode = -ENOMEM;
		goto out;
	}
	if (_init_md_journal(znd)) {
		ti->error = "couldn't allocate in-memory meta data journal";
		rcode = -ENOMEM;
		goto out;
	}

	/*
	* MD journal space: 15360 [15]
	* Journal Index     688
	* Journal data      698
	* Metadata start    ff00
	* Forward table     ff00
	* Reverse table     2ff00
	* Forward CRC table 4ff00
	* Reverse CRC table 4ff40
	* Normal data start 5ff00
	*/

	Z_INFO(znd, "Journal Index     %" PRIx64, (u64) WB_JRNL_IDX);
	Z_INFO(znd, "Journal data      %" PRIx64, (u64) WB_JRNL_BASE);
	Z_INFO(znd, "Metadata start    %" PRIx64, znd->md_start);
	Z_INFO(znd, "Forward table     %" PRIx64, znd->s_base );
	Z_INFO(znd, "Reverse table     %" PRIx64, znd->r_base );
	Z_INFO(znd, "Forward CRC table %" PRIx64, znd->c_base );
	Z_INFO(znd, "Reverse CRC table %" PRIx64, znd->c_mid );
	Z_INFO(znd, "Normal data start %" PRIx64, znd->data_lba);

	znd->gc_postmap.jdata = ZDM_CALLOC(znd, Z_BLKSZ,
		sizeof(*znd->gc_postmap.jdata), VM_03, NORMAL);
	znd->md_crcs = ZDM_ALLOC(znd, Z_C4K * 2, VM_22, NORMAL);
	znd->gc_io_buf = ZDM_CALLOC(znd, GC_MAX_STRIPE, Z_C4K, VM_04, NORMAL);
	znd->wp = _alloc_wp(znd);
	znd->io_vcache[0] = ZDM_CALLOC(znd, IO_VCACHE_PAGES,
				sizeof(struct io_4k_block), VM_12, NORMAL);
	znd->io_vcache[1] = ZDM_CALLOC(znd, IO_VCACHE_PAGES,
				sizeof(struct io_4k_block), VM_12, NORMAL);

	if (!znd->gc_postmap.jdata || !znd->md_crcs || !znd->gc_io_buf ||
	    !znd->wp || !znd->io_vcache[0] || !znd->io_vcache[1]) {
		rcode = -ENOMEM;
		goto out;
	}
	znd->gc_postmap.jsize = Z_BLKSZ;
	_init_mdcrcs(znd);

	znd->io_client = dm_io_client_create();
	if (!znd->io_client) {
		rcode = -ENOMEM;
		goto out;
	}

	znd->meta_wq = create_singlethread_workqueue("znd_meta_wq");
	if (!znd->meta_wq) {
		ti->error = "couldn't start metadata worker thread";
		rcode = -ENOMEM;
		goto out;
	}

	znd->gc_wq = create_singlethread_workqueue("znd_gc_wq");
	if (!znd->gc_wq) {
		ti->error = "couldn't start GC workqueue.";
		rcode = -ENOMEM;
		goto out;
	}

	znd->bg_wq = create_singlethread_workqueue("znd_bg_wq");
	if (!znd->bg_wq) {
		ti->error = "couldn't start background workqueue.";
		rcode = -ENOMEM;
		goto out;
	}

	znd->io_wq = create_singlethread_workqueue("kzoned_dm_io_wq");
	if (!znd->io_wq) {
		ti->error = "couldn't start DM I/O thread";
		rcode = -ENOMEM;
		goto out;
	}

	znd->zone_action_wq = create_singlethread_workqueue("zone_act_wq");
	if (!znd->zone_action_wq) {
		ti->error = "couldn't start zone action worker";
		rcode = -ENOMEM;
		goto out;
	}

#if USE_KTHREAD
	init_waitqueue_head(&znd->wait_bio);
	init_waitqueue_head(&znd->wait_fifo);
#endif

	INIT_WORK(&znd->meta_work, meta_work_task);
	INIT_WORK(&znd->bg_work, bg_work_task);
	INIT_DELAYED_WORK(&znd->gc_work, gc_work_task);
	setup_timer(&znd->timer, activity_timeout, (unsigned long)znd);
	znd->last_w = BAD_ADDR;
	set_bit(DO_SYNC, &znd->flags);

out:
	return rcode;
}

/**
 * check_metadata_version() - Test ZDM version for compatibility.
 * @sblock: Super block
 *
 * Return 0 if valud, or -EINVAL if version is not recognized.
 */
static int check_metadata_version(struct zdm_superblock *sblock)
{
	u32 metadata_version = le32_to_cpu(sblock->version);

	if (metadata_version < MIN_ZONED_VERSION
	    || metadata_version > MAX_ZONED_VERSION) {
		DMERR("Unsupported metadata version %u found.",
		      metadata_version);
		DMERR("Only versions between %u and %u supported.",
		      MIN_ZONED_VERSION, MAX_ZONED_VERSION);
		return -EINVAL;
	}

	return 0;
}

/**
 * sb_crc32() - CRC check for superblock.
 * @sblock: Superblock to check.
 */
static __le32 sb_crc32(struct zdm_superblock *sblock)
{
	const __le32 was = sblock->csum;
	u32 crc;

	sblock->csum = 0;
	crc = crc32c(~(u32) 0u, sblock, sizeof(*sblock)) ^ SUPERBLOCK_CSUM_XOR;

	sblock->csum = was;
	return cpu_to_le32(crc);
}

/**
 * sb_check() - Check the superblock to see if it is valid and not corrupt.
 * @sblock: Superblock to check.
 */
static int sb_check(struct zdm_superblock *sblock)
{
	__le32 csum_le;

	if (le64_to_cpu(sblock->magic) != SUPERBLOCK_MAGIC) {
		DMERR("sb_check failed: magic %" PRIx64 ": wanted %lx",
		      le64_to_cpu(sblock->magic), SUPERBLOCK_MAGIC);
		return -EILSEQ;
	}

	csum_le = sb_crc32(sblock);
	if (csum_le != sblock->csum) {
		DMERR("sb_check failed: csum %u: wanted %u",
		      csum_le, sblock->csum);
		return -EILSEQ;
	}

	return check_metadata_version(sblock);
}

/**
 * zoned_create_disk() - Initialize the on-disk format of a zdm device mapper.
 * @ti: DM Target Instance
 * @znd: ZDM Instance
 */
static int zoned_create_disk(struct dm_target *ti, struct zdm *znd)
{
	const int reset_non_empty = 1;
	struct zdm_superblock *sblock = znd->super_block;
	int err;

	memset(sblock, 0, sizeof(*sblock));
	generate_random_uuid(sblock->uuid);
	sblock->magic = cpu_to_le64(SUPERBLOCK_MAGIC);
	sblock->version = cpu_to_le32(Z_VERSION);
	sblock->zdstart = cpu_to_le32(znd->zdstart);

	err = zoned_wp_sync(znd, reset_non_empty);

	return err;
}

/**
 * zoned_repair() - Attempt easy on-line fixes.
 * @znd: ZDM Instance
 *
 * Repair an otherwise good device mapper instance that was not cleanly removed.
 */
static int zoned_repair(struct zdm *znd)
{
	Z_INFO(znd, "Is Dirty .. zoned_repair consistency fixer TODO!!!.");
	return -ENOMEM;
}

/**
 * zoned_init_disk() - Init from exising or re-create DM Target (ZDM)
 * @ti: DM Target Instance
 * @znd: ZDM Instance
 * @create: Create if not found.
 * @force: Force create even if it looks like a ZDM was here.
 *
 * Locate the existing SB on disk and re-load or create the device-mapper
 * instance based on the existing disk state.
 */
static int zoned_init_disk(struct dm_target *ti, struct zdm *znd,
			   int create, int force)
{
	struct mz_superkey *key_blk = znd->z_sballoc;

	int jinit = 1;
	int n4kblks = 1;
	int use_wq = 1;
	int rc = 0;
	u32 zdstart = znd->zdstart;

	memset(key_blk, 0, sizeof(*key_blk));

	znd->super_block = &key_blk->sblock;

	znd->bmkeys = key_blk;
	znd->bmkeys->sig0 = Z_KEY_SIG;
	znd->bmkeys->sig1 = cpu_to_le64(Z_KEY_SIG);
	znd->bmkeys->magic = cpu_to_le64(Z_TABLE_MAGIC);

	_init_streams(znd);

	znd->stale.binsz = STREAM_SIZE;
	if (znd->data_zones < STREAM_SIZE)
		znd->stale.binsz = znd->data_zones;
	else if ((znd->data_zones / STREAM_SIZE) > STREAM_SIZE)
		znd->stale.binsz = dm_div_up(znd->data_zones, STREAM_SIZE);
	znd->stale.count = dm_div_up(znd->data_zones, znd->stale.binsz);

	Z_ERR(znd, "Bin: Sz %u, count %u", znd->stale.binsz, znd->stale.count);

	if (create && force) {
		Z_ERR(znd, "Force Creating a clean instance.");
	} else if (find_superblock(znd, use_wq, 1)) {
		u64 sb_lba = 0;
		u64 generation;

/* FIXME: Logic [zdstart] not well tested or ... logicial */

		Z_INFO(znd, "Found existing superblock");
		if (zdstart != znd->zdstart) {
			if (force) {
				Z_ERR(znd, "  (force) zdstart: %u <- %u",
					zdstart, znd->zdstart);
			} else {
				znd->zdstart = zdstart;
				jinit = 0;
			}
		}

		generation = mcache_greatest_gen(znd, use_wq, &sb_lba, NULL);
		Z_DBG(znd, "Generation: %" PRIu64 " @ %" PRIx64,
			generation, sb_lba);

		rc = read_block(ti, DM_IO_KMEM, key_blk, sb_lba,
				n4kblks, use_wq);
		if (rc) {
			ti->error = "Superblock read error.";
			return rc;
		}
	}

	rc = sb_check(znd->super_block);
	if (rc) {
		jinit = 0;
		if (create) {
			DMWARN("Check failed .. creating superblock.");
			zoned_create_disk(ti, znd);
			znd->super_block->nr_zones =
				cpu_to_le64(znd->data_zones);
			DMWARN("in-memory superblock created.");
			znd->is_empty = 1;
		} else {
			ti->error = "Superblock check failed.";
			return rc;
		}
	}

	if (sb_test_flag(znd->super_block, SB_DIRTY)) {
		int repair_check = zoned_repair(znd);

		if (!force) {
			/* if repair failed -- don't load from disk */
			if (repair_check)
				jinit = 0;
		} else if (repair_check && jinit) {
			Z_ERR(znd, "repair failed, force enabled loading ...");
		}
	}

	if (jinit) {
		Z_ERR(znd, "INIT: Reloading DM Zoned metadata from DISK");
		znd->zdstart = le32_to_cpu(znd->super_block->zdstart);
		set_bit(DO_ZDM_RELOAD, &znd->flags);
		queue_work(znd->meta_wq, &znd->meta_work);
		Z_ERR(znd, "Waiting for load to complete.");
		flush_workqueue(znd->meta_wq);
	}

	Z_ERR(znd, "ZONED: Build No %d marking superblock dirty.", BUILD_NO);

	/* write the 'dirty' flag back to disk. */
	sb_set_flag(znd->super_block, SB_DIRTY);
	znd->super_block->csum = sb_crc32(znd->super_block);

	return 0;
}

/**
 * compare_tlba() - Compare on tlba48 ignoring high 16 bits.
 * @x1: Page of map cache
 * @x2: Page of map cache
 *
 * Return: -1 less than, 1 greater than, 0 if equal.
 */
static int compare_tlba(const void *x1, const void *x2)
{
	const struct map_sect_to_lba *r1 = x1;
	const struct map_sect_to_lba *r2 = x2;
	const u64 v1 = le64_to_lba48(r1->tlba, NULL);
	const u64 v2 = le64_to_lba48(r2->tlba, NULL);

	return (v1 < v2) ? -1 : ((v1 > v2) ? 1 : 0);
}

/**
 * _lsearch_tlba() - Do a linear search over a page of map_cache entries.
 * @mcache: Page of map cache entries to search.
 * @dm_s: tlba being sought.
 *
 * Return: 0 to jcount - 1 if found. -1 if not found
 */
static int _lsearch_tlba(struct map_cache *mcache, u64 dm_s)
{
	int at = -1;
	int jentry;

	for (jentry = mcache->jcount; jentry > 0; jentry--) {
		u64 logi = le64_to_lba48(mcache->jdata[jentry].tlba, NULL);

		if (logi == dm_s) {
			at = jentry - 1;
			goto out;
		}
	}

out:
	return at;
}

/**
 * _lsearch_extent() - Do a linear search over a page of discard entries.
 * @mcache: Page of map cache entries to search.
 * @dm_s: tlba being sought.
 *
 * Return: 0 to jcount - 1 if found. -1 if not found
 *
 * NOTE: In this case the match is if the tlba is include in the extent
 */
static int _lsearch_extent(struct map_cache *mcache, u64 dm_s)
{
	int at = -1;
	int jentry;

	for (jentry = mcache->jcount; jentry > 0; jentry--) {
		u64 addr = le64_to_lba48(mcache->jdata[jentry].tlba, NULL);
		u64 blocks = le64_to_lba48(mcache->jdata[jentry].bval, NULL);

		if ((dm_s >= addr) && dm_s < (addr+blocks)) {
			at = jentry - 1;
			goto out;
		}
	}

out:
	return at;
}

/**
 * _bsrch_tlba() - Do a binary search over a page of map_cache entries.
 * @mcache: Page of map cache entries to search.
 * @tlba: tlba being sought.
 *
 * Return: 0 to jcount - 1 if found. -1 if not found
 */
static int _bsrch_tlba(struct map_cache *mcache, u64 tlba)
{
	int at = -1;
	void *base = &mcache->jdata[1];
	void *map;
	struct map_sect_to_lba find;

	find.tlba = lba48_to_le64(0, tlba);

	if (mcache->jcount < 0 || mcache->jcount > mcache->jsize)
		return at;

	map = bsearch(&find, base, mcache->jcount, sizeof(find), compare_tlba);
	if (map)
		at = (map - base) / sizeof(find);

	return at;
}

/**
 * compare_ext() - Compare on tlba48 ignoring high 16 bits.
 * @x1: Page of map cache
 * @x2: Page of map cache
 *
 * Return: -1 less than, 1 greater than, 0 if equal.
 */
static int compare_ext(const void *x1, const void *x2)
{
	const struct map_sect_to_lba *r1 = x1;
	const struct map_sect_to_lba *r2 = x2;
	const u64 key = le64_to_lba48(r1->tlba, NULL);
	const u64 val = le64_to_lba48(r2->tlba, NULL);
	const u64 ext = le64_to_lba48(r2->bval, NULL);

	if (key < val)
		return -1;
	if (key >= val && key < (val+ext))
		return 0;
	return 1;
}


/**
 * _bsrch_extent() - Do a binary search over a page of map_cache extents.
 * @mcache: Page of map cache extent entries to search.
 * @tlba: tlba being sought.
 *
 * Return: 0 to jcount - 1 if found. -1 if not found
 */
static int _bsrch_extent(struct map_cache *mcache, u64 tlba)
{
	int at = -1;
	void *base = &mcache->jdata[1];
	void *map;
	struct map_sect_to_lba find;

	find.tlba = lba48_to_le64(0, tlba);
	map = bsearch(&find, base, mcache->jcount, sizeof(find), compare_ext);
	if (map)
		at = (map - base) / sizeof(find);

#if 1
	{
		int cmp = _lsearch_extent(mcache, tlba);

		if (cmp != at) {
			pr_err("_bsrch_extent is failing got %d need %d.\n",
				at, cmp);
			at = cmp;
		}
	}
#endif

	return at;
}

/**
 * _bsrch_tlba() - Mutex Lock and binary search.
 * @mcache: Page of map cache entries to search.
 * @tlba: tlba being sought.
 *
 * Return: 0 to jcount - 1 if found. -1 if not found
 */
static int _bsrch_tlba_lck(struct map_cache *mcache, u64 tlba)
{
	int at = -1;

	MutexLock(&mcache->cached_lock);
	at = _bsrch_tlba(mcache, tlba);
	mutex_unlock(&mcache->cached_lock);

	return at;
}

/**
 * _bsrch_extent_lck() - Mutex Lock and binary search.
 * @mcache: Page of map cache extent entries to search.
 * @tlba: tlba being sought.
 *
 * Return: 0 to jcount - 1 if found. -1 if not found
 */
static int _bsrch_extent_lck(struct map_cache *mcache, u64 tlba)
{
	int at = -1;

	MutexLock(&mcache->cached_lock);
	at = _bsrch_extent(mcache, tlba);
	mutex_unlock(&mcache->cached_lock);

	return at;
}

/**
 * mcache_bsearch() - Do a binary search over a page of map_cache entries.
 * @mcache: Page of map cache entries to search.
 * @tlba: tlba being sought.
 */
static inline int mcache_bsearch(struct map_cache *mcache, u64 tlba)
{
	int at = -1;

	switch (mcache->map_content) {
	case IS_MAP:
		at = _bsrch_tlba(mcache, tlba);
		break;
	case IS_DISCARD:
		at = _bsrch_extent(mcache, tlba);
		break;
	case IS_JRNL_PG:
		at = _bsrch_tlba(mcache, tlba);
		break;
	default:
		pr_err("Invalid map type: %u\n", mcache->map_content);
		dump_stack();
		break;
	}

	return at;
}


/**
 * mcache_lsearch() - Do a linear search over a page of map/discard entries.
 * @mcache: Page of map cache entries to search.
 * @dm_s: tlba being sought.
 */
static inline int mcache_lsearch(struct map_cache *mcache, u64 tlba)
{
	int at = -1;

	switch (mcache->map_content) {
	case IS_MAP:
		at = _lsearch_tlba(mcache, tlba);
		break;
	case IS_DISCARD:
		at = _lsearch_extent(mcache, tlba);
		break;
	case IS_JRNL_PG:
		at = _lsearch_tlba(mcache, tlba);
		break;
	default:
		pr_err("Invalid map type: %u\n", mcache->map_content);
		dump_stack();
		break;
	}

	return at;
}


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u64 z_lookup_key_range(struct zdm *znd, u64 addr)
{
	u64 found = 0ul;

	if (test_bit(ZF_POOL_FWD, &znd->flags))
		return found;

	if ((znd->sk_low <= addr) && (addr < znd->sk_high))
		found = FWD_KEY_BASE + (addr - znd->sk_low);

	return found;
}

/**
 * increment_used_blks() - Update the 'used' WP when data hits disk.
 * @znd: ZDM Instance
 * @lba: blba if bio completed.
 * @blks: number of blocks of bio completed.
 *
 * Called from a BIO end_io function so should not sleep or deadlock
 * The 'critical' piece here is ensuring that the wp is advanced to 0x10000
 * Secondarily is triggering filled_zone which ultimatly sets the
 *   Z_WP_GC_READY in wp_alloc. While important this flag could be set
 *   during other non-critical passes over wp_alloc and wp_used such
 *   as during update_stale_ratio().
 */
static void increment_used_blks(struct zdm *znd, u64 lba, u32 blks)
{
	u32 zone = _calc_zone(znd, lba);
	u32 wwp = ((lba - znd->data_lba) - (zone << Z_BLKBITS)) + blks;

	if (zone < znd->data_zones) {
		u32 gzno  = zone >> GZ_BITS;
		u32 gzoff = zone & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 used, uflags;

		used = le32_to_cpu(wpg->wp_used[gzoff]);
		uflags = used & Z_WP_FLAGS_MASK;
		used &= Z_WP_VALUE_MASK;

		if (wwp > used) {
			wpg->wp_used[gzoff] = cpu_to_le32(wwp | uflags);
			/* signal zone closure */
			if (wwp == Z_BLKSZ)
				znd->filled_zone = zone;
		}
	}
}

/**
 * current_mapping() - Lookup a logical sector address to find the disk LBA
 * @znd: ZDM instance
 * @nodisk: Optional ignore the discard cache
 * @addr: Logical LBA of page.
 * @gfp: Memory allocation rule
 *
 * Return: Disk LBA or 0 if not found.
 */
static u64 _current_mapping(struct zdm *znd, int nodisc, u64 addr, gfp_t gfp)
{
	u64 found = 0ul;

	if (addr < znd->data_lba) {
		found = z_lookup_cache(znd, addr, IS_JRNL_PG);
		if (!found)
			found = addr;
	}
	if (!found)
		found = z_lookup_key_range(znd, addr);
	if (!found && !nodisc) {
		found = z_lookup_cache(znd, addr, IS_DISCARD);
		if (found) {
			found = 0;
			goto out;
		}
	}
	if (!found)
		found = z_lookup_cache(znd, addr, IS_MAP);
	if (!found)
		found = z_lookup_table(znd, addr, gfp);

out:
	return found;
}

/**
 * current_mapping() - Lookup a logical sector address to find the disk LBA
 * @znd: ZDM instance
 * @addr: Logical LBA of page.
 * @gfp: Memory allocation rule
 *
 * NOTE: Discard cache is checked.
 *
 * Return: Disk LBA or 0 if not found.
 */
static u64 current_mapping(struct zdm *znd, u64 addr, gfp_t gfp)
{
	const int nodisc = 0;

	return _current_mapping(znd, nodisc, addr, gfp);
}

/**
 * z_mapped_add_one() - Add an entry to the map cache block mapping.
 * @znd: ZDM Instance
 * @tlba: Address being discarded.
 * @blks: number of blocks being discard.
 * @gfp: Current memory allocation scheme.
 *
 * Add a new extent entry.
 */
static int z_mapped_add_one(struct zdm *znd, u64 dm_s, u64 lba, gfp_t gfp)
{
	int err = 0;

	if (dm_s < znd->data_lba)
		return err;

#if 0 /* FIXME: Do the RIGHT thing ... whatever that is. */

	/*
	 * location of the SLT key sectors need to be
	 * stashed into the sector lookup table block map
	 * Does dm_s point in the sector lookup table block map
	 */
	if (!test_bit(ZF_POOL_FWD, &znd->flags)) {
		if ((znd->sk_low <= dm_s) && (dm_s < znd->sk_high))
			lba = FWD_KEY_BASE + (dm_s - znd->sk_low);
	}
#endif

	/*
	 * If this mapping over-writes a discard entry then we need to punch a
	 * hole in the discard entry.
	 *  - Find the discard entry.
	 *  - Translate a chunk of the discard into map entries.
	 *  - Break [or trim] the discard entry.
	 */
	if (lba) {
		int is_locked = mutex_is_locked(&znd->mz_io_mutex);

		if (is_locked)
			mutex_unlock(&znd->mz_io_mutex);
		z_discard_range(znd, dm_s, gfp);
		if (is_locked)
			MutexLock(&znd->mz_io_mutex);
	}

	do {
		err = z_to_map_list(znd, dm_s, lba, gfp);
	} while (-EBUSY == err);

	return err;
}

/**
 * md_journal_add_map() - Add an entry to the map cache block mapping.
 * @znd: ZDM Instance
 * @tlba: Address being discarded.
 * @blks: number of blocks being discard.
 * @gfp: Current memory allocation scheme.
 *
 * Add a new extent entry.
 */
static int md_journal_add_map(struct zdm *znd, u64 dm_s, u64 lba, gfp_t gfp)
{
	int err = 0;

	if (dm_s < znd->data_lba) {
		struct md_journal *jrnl = &znd->jrnl;
		u32 entry = lba - WB_JRNL_BASE;

		if (entry < jrnl->size) {
			SpinLock(&jrnl->wb_alloc);
			jrnl->wb[entry] = dm_s;
			set_bit(IS_DIRTY, &jrnl->flags);
			spin_unlock(&jrnl->wb_alloc);
		}

		do {
			err = z_to_journal_list(znd, dm_s, lba, gfp);
		} while (-EBUSY == err);
	}

	return err;
}

/**
 * z_discard_small() - Translate discard extents to map cache block mapping.
 * @znd: ZDM Instance
 * @tlba: Address being discarded.
 * @blks: number of blocks being discard.
 * @gfp: Current memory allocation scheme.
 *
 * Add a new extent entry.
 */
static int z_discard_small(struct zdm *znd, u64 tlba, u64 count, gfp_t gfp)
{
	const int nodisc = 1;
	int err = 0;
	u64 ii;

	for (ii = 0; ii < count; ii++) {
		u64 lba = _current_mapping(znd, nodisc, tlba+ii, gfp);

		if (lba)
			do {
				err = z_to_map_list(znd, tlba+ii, 0ul, gfp);
			} while (-EBUSY == err);
		if (err)
			break;
	}
	return err;
}

/**
 * z_discard_large() - Add a discard extent to the mapping cache
 * @znd: ZDM Instance
 * @tlba: Address being discarded.
 * @blks: number of blocks being discard.
 * @gfp: Current memory allocation scheme.
 *
 * Add a new extent entry.
 */
static int z_discard_large(struct zdm *znd, u64 tlba, u64 blks, gfp_t gfp)
{
	int rcode = 0;

	/* for a large discard ... add it to the discard queue */
	do {
		rcode = z_to_discard_list(znd, tlba, blks, gfp);
	} while (-EBUSY == rcode);

	return rcode;
}

/**
 * mapped_discard() - Add a discard extent to the mapping cache
 * @znd: ZDM Instance
 * @tlba: Address being discarded.
 * @blks: number of blocks being discard.
 * @merge: Attempt to merge with an existing extent.
 * @gfp: Current memory allocation scheme.
 *
 * First attempt to merge with an existing entry.
 * Otherwise attempt to push small entires into the map_cache 1:1 mapping
 * Otherwise add a new extent entry.
 */
static
int mapped_discard(struct zdm *znd, u64 tlba, u64 blks, int merge, gfp_t gfp)
{
	int err = 0;

	if (merge && discard_merge(znd, tlba, blks))
		goto done;

	if (blks < DISCARD_MAX_INGRESS || znd->dc_entries > 39) {
		/* io_mutex is avoid adding map cache entries during SYNC */
		MutexLock(&znd->mz_io_mutex);
		err = z_discard_small(znd, tlba, blks, gfp);
		mutex_unlock(&znd->mz_io_mutex);
		goto done;
	}

	err = z_discard_large(znd, tlba, blks, gfp);

done:
	return err;
}

/**
 * z_mapped_discard() - Add a discard extent to the mapping cache
 * @znd: ZDM Instance
 * @tlba: Address being discarded.
 * @blks: number of blocks being discard.
 * @gfp: Current memory allocation scheme.
 */
static int z_mapped_discard(struct zdm *znd, u64 tlba, u64 blks, gfp_t gfp)
{
	const int merge = 1;

	return mapped_discard(znd, tlba, blks, merge, gfp);
}

/**
 * mcache_alloc() - Allocate a new chunk of map cache
 * @znd: ZDM Instance
 * @gfp: Current memory allocation scheme.
 */
static struct map_cache *mcache_alloc(struct zdm *znd, gfp_t gfp)
{
	struct map_cache *mcache;

	mcache = ZDM_ALLOC(znd, sizeof(*mcache), KM_07, gfp);
	if (mcache) {
		mutex_init(&mcache->cached_lock);
		mcache->jcount = 0;
		mcache->jsorted = 0;
		mcache->jdata = ZDM_CALLOC(znd, Z_UNSORTED,
			sizeof(*mcache->jdata), PG_08, gfp);

		if (mcache->jdata) {
			u64 logical = Z_LOWER48;
			u64 physical = Z_LOWER48;

			mcache->jdata[0].tlba = cpu_to_le64(logical);
			mcache->jdata[0].bval = cpu_to_le64(physical);
			mcache->jsize = Z_UNSORTED - 1;

		} else {
			Z_ERR(znd, "%s: mcache is out of space.",
			      __func__);
			ZDM_FREE(znd, mcache, sizeof(*mcache), KM_07);
			mcache = NULL;
		}
	}
	return mcache;
}

/**
 * mcache_first_get() - Get the first chunk from the list.
 * @znd: ZDM Instance
 * @mcache: Map cache chunk to add.
 * @type: Type flag of mcache entry (MAP or DISCARD extent).
 *
 * The mcache chunk will have it's refcount elevated in the xchg.
 * Caller should use xchg_next to navigate through the list or
 * deref_cache when exiting from the list walk early.
 */
static struct map_cache *mcache_first_get(struct zdm *znd, int type)
{
	unsigned long flags;
	struct map_cache *mc;

	spin_lock_irqsave(&znd->mclck[type], flags);
	mc = list_first_entry_or_null(&znd->mclist[type], typeof(*mc), mclist);
	if (mc && (&mc->mclist != &znd->mclist[type]))
		mcache_ref(mc);
	else
		mc = NULL;
	spin_unlock_irqrestore(&znd->mclck[type], flags);

	return mc;
}

/**
 * mcache_put_get_next() - Get the next map cache page and put current.
 * @znd: ZDM Instance
 * @mcache: Map cache chunk to add.
 * @type: Type flag of mcache entry (MAP or DISCARD extent).
 *
 * The current mcache chunk will have an elevated refcount,
 * that will dropped, while the new the returned chunk will
 * have it's refcount elevated in the xchg.
 */
static inline struct map_cache *mcache_put_get_next(struct zdm *znd,
						    struct map_cache *mcache,
						    int type)
{
	unsigned long flags;
	struct map_cache *next;

	spin_lock_irqsave(&znd->mclck[type], flags);
	next = list_next_entry(mcache, mclist);
	if (next && (&next->mclist != &znd->mclist[type]))
		mcache_ref(next);
	else
		next = NULL;
	mcache_deref(mcache);
	spin_unlock_irqrestore(&znd->mclck[type], flags);

	return next;
}

/**
 * mclist_add() - Add a new chunk to the map_cache pool.
 * @znd: ZDM Instance
 * @mcache: Map cache chunk to add.
 * @type: Type flag of mcache entry (MAP or DISCARD extent).
 */
static inline void mclist_add(struct zdm *znd,
			      struct map_cache *mcache, int type)
{
	unsigned long flags;

	spin_lock_irqsave(&znd->mclck[type], flags);
	list_add(&mcache->mclist, &znd->mclist[type]);
	spin_unlock_irqrestore(&znd->mclck[type], flags);
}

/**
 * mclist_del() - Release an (empty) chunk of map cache.
 * @znd: ZDM Instance
 * @mcache: Map cache chunk to add.
 * @type: Type flag of mcache entry (MAP or DISCARD extent).
 */
static inline int mclist_del(struct zdm *znd,
			     struct map_cache *mcache, int type)
{
	unsigned long flags;
	int deleted = 0;

	if (spin_trylock_irqsave(&znd->mclck[type], flags)) {
		MutexLock(&mcache->cached_lock);
		if (mcache->jcount == 0 && mcache_getref(mcache) == 1) {
			list_del(&mcache->mclist);
			deleted = 1;
		}
		mutex_unlock(&mcache->cached_lock);
		spin_unlock_irqrestore(&znd->mclck[type], flags);
	}
	return deleted;
}

/**
 * mcache_put() - Dereference an mcache page
 * @mcache: Map cache page.
 */
static inline void mcache_put(struct map_cache *mcache)
{
	mcache_deref(mcache);
}

/**
 * memcache_sort() - Sort an unsorted map cache page
 * @znd: ZDM instance
 * @mcache: Map cache page.
 *
 * Sort a map cache entry if is sorted.
 * Lock using mutex if not already locked.
 */
static void memcache_sort(struct zdm *znd, struct map_cache *mcache)
{
	mcache_ref(mcache);
	if (mcache->jsorted < mcache->jcount) {
		sort(&mcache->jdata[1], mcache->jcount,
		     sizeof(*mcache->jdata), compare_tlba, NULL);
		mcache->jsorted = mcache->jcount;
	}
	mcache_deref(mcache);
}

/**
 * memcache_lock_and_sort() - Attempt to sort an unsorted map cache page
 * @znd: ZDM instance
 * @mcache: Map cache page.
 *
 * Sort a map cache entry if is not busy being purged to the ZLT.
 *
 * Return: -EBUSY if busy, or 0 if sorted.
 */
static int memcache_lock_and_sort(struct zdm *znd, struct map_cache *mcache)
{
	if (mcache_is_busy(mcache))
		return -EBUSY;

	if (mcache->jsorted == mcache->jcount)
		return 0;

	mutex_lock_nested(&mcache->cached_lock, SINGLE_DEPTH_NESTING);
	memcache_sort(znd, mcache);
	mutex_unlock(&mcache->cached_lock);
	return 0;
}

/**
 * z_lookup_cache() - Scan mcache entries for addr
 * @znd: ZDM Instance
 * @addr: Address [tLBA] to find.
 * @type: mcache type (MAP or DISCARD cache).
 */
static u64 z_lookup_cache(struct zdm *znd, u64 addr, int type)
{
	struct map_cache *mcache;
	u64 found = 0ul;

	mcache = mcache_first_get(znd, type);
	while (mcache) {
		int at;
		int err;

		/* sort, if needed. only err is -EBUSY so do a linear find. */
		err = memcache_lock_and_sort(znd, mcache);
		if (!err)
			at = mcache_bsearch(mcache, addr);
		else
			at = mcache_lsearch(mcache, addr);

		if (at != -1) {
			struct map_sect_to_lba *data = &mcache->jdata[at + 1];

			found = le64_to_lba48(data->bval, NULL);
		}
		if (found) {
			mcache_put(mcache);
			goto out;
		}

		mcache = mcache_put_get_next(znd, mcache, type);
	}
out:
	return found;
}

/**
 * mcache_insert - Insertion sort (for mostly sorted data)
 * @znd: ZDM Instance
 * @mcache: Map Cache block
 * @tlba: upper stack sector
 * @blba: lower mapped lba
 */
static void mcache_insert(struct zdm *znd, struct map_cache *mcache,
			 u64 tlba, u64 blba)
{
	u16 fido = ++mcache->jcount;
	u16 idx;

	WARN_ON(mcache->jcount > mcache->jsize);

	for (idx = fido; idx > 1; --idx) {
		u16 prev = idx - 1;
		u64 a0 = le64_to_lba48(mcache->jdata[prev].tlba, NULL);

		if (a0 < tlba) {
			mcache->jdata[idx].tlba = lba48_to_le64(0, tlba);
			mcache->jdata[idx].bval = lba48_to_le64(0, blba);
			if ((mcache->jsorted + 1) == mcache->jcount)
				mcache->jsorted = mcache->jcount;
			break;
		}
		/* move UP .. tlba < a0 */
		mcache->jdata[idx].tlba = mcache->jdata[prev].tlba;
		mcache->jdata[idx].bval = mcache->jdata[prev].bval;
	}
	if (idx == 1) {
		mcache->jdata[idx].tlba = lba48_to_le64(0, tlba);
		mcache->jdata[idx].bval = lba48_to_le64(0, blba);
		mcache->jsorted = mcache->jcount;
	}
}


/**
 * mc_delete_entry - Overwrite a map_cache entry (for mostly sorted data)
 * @mcache: Map Cache block
 * @entry: Entry to remove. [1 .. count]
 */
void mc_delete_entry(struct map_cache *mcache, int entry)
{
	int iter;

	MutexLock(&mcache->cached_lock);

	mcache->jdata[entry].tlba = lba48_to_le64(0, 0ul);
	mcache->jdata[entry].bval = lba48_to_le64(0, 0ul);

	for (iter = entry; iter < mcache->jcount; iter++) {
		int next = iter + 1;

		mcache->jdata[iter].tlba = mcache->jdata[next].tlba;
		mcache->jdata[iter].bval = mcache->jdata[next].bval;
	}
	if (mcache->jcount > 0) {
		if (mcache->jsorted == mcache->jcount)
			mcache->jsorted--;
		mcache->jcount--;
	} else {
		pr_err("mcache delete ... discard below empty\n");
	}
	mutex_unlock(&mcache->cached_lock);
}

/**
 * _discard_split_entry() - Split a discard into two parts.
 * @znd: ZDM Instance
 * @mcahce: mcache holding the current entry
 * @at: location of entry in mcache (0 .. count - 1)
 * @addr: address to punch a hole at
 * @gfp: current memory allocation rules.
 *
 * Discard 'extents' need to broken up when a sub range is being allocated
 * for writing. Ideally the punch only requires the extent to be shrunk.
 */
static int _discard_split_entry(struct zdm *znd, struct map_cache *mcache,
				int at, u64 addr, gfp_t gfp)
{
	const int merge = 0;
	int err = 0;
	struct map_sect_to_lba *entry = &mcache->jdata[at + 1];
	u64 tlba   = le64_to_lba48(entry->tlba, NULL);
	u64 blocks = le64_to_lba48(entry->bval, NULL);
	u64 dblks  = blocks;
	const u64 chunk = addr - tlba;

	/*
	 * Now chunk is a number of blocks until addr starts.
	 * If the chunk is small push the discard immediately.
	 * Otherwise punch a hole and see if we need to create
	 * a new entry with what remains.
	 */
	if (chunk > 0 && chunk < DISCARD_MAX_INGRESS) {

		err = mapped_discard(znd, tlba, chunk, merge, gfp);
		if (err)
			goto out;

		blocks -= chunk;
		tlba += chunk;

		entry->tlba = lba48_to_le64(0, tlba);
		entry->bval = lba48_to_le64(0, blocks);

		if (blocks == 0ul) {
			mc_delete_entry(mcache, at + 1);
			goto out;
		}
		dblks = blocks;
	} else {
		dblks = blocks - chunk; /* nblks from addr to end */
	}

	/*
	 * When chunk >= DISCARD_MAX_INGRESS
	 * then the current entry needs to be ended at 'chunk'.
	 * Note that tlba != addr in this case and 'chunk' will
	 *	be until we get around to committing to truncating
	 *	the entry extent. Specifcily we need to ensure that
	 *	the hole doens't expose blocks covered by the current
	 *	extent.
	 */
	if (dblks > DISCARD_MAX_INGRESS)
		dblks = DISCARD_MAX_INGRESS - 1;

	err = mapped_discard(znd, addr, dblks, merge, gfp);
	if (err)
		goto out;

	/*
	 * Now we have moved the hole into the map cache we are free
	 * to update the entry.
	 * If tlba == addr the we have continued to discard from the
	 * from of this extent.
	 *
	 * If tlba != addr then we need discarded from the middle or
	 * the end. Either there is a hole and we need a second extent
	 * entry or we ran to the end of the extent and we can just
	 * truncate the extent.
	 */

	if (tlba == addr) {
		blocks -= dblks;
		tlba += dblks;
		entry->tlba = lba48_to_le64(0, tlba);
		entry->bval = lba48_to_le64(0, blocks);
		if (blocks == 0ul)
			mc_delete_entry(mcache, at + 1);
	} else {
		u64 start = addr + dblks;

		/*
		 * Because we are doing insertion sorting
		 * and tlba < addr we can safely add a new
		 * discard extent entry.
		 * As the current entry is 'stable' we don't need to
		 * re-acquire it.
		 */
		blocks -= (chunk + dblks);
		if (blocks > 0ul) {
			err = mapped_discard(znd, start, blocks, merge, gfp);
			if (err)
				goto out;
		}
		entry->bval = lba48_to_le64(0, chunk);
	}

out:
	return err;
}


/**
 * z_discard_range() - Migrate a discard range to memcache/ZLT
 * @znd: ZDM Instance
 * @addr: tLBA target for breaking up a discard range.
 * @gfp: Memory allocation scheme
 *
 * When an address is being written to a check is made to see if the tLBA
 * overlaps with an exisitng discard pool. If so the discard pool is
 * split and multiple smaller discard entries are pushed into the memcache.
 *
 * The overall purpose of the discard pool is to reduce the amount of intake
 * on the memcache to avoid starving the I/O requests.
 */
static u64 z_discard_range(struct zdm *znd, u64 addr, gfp_t gfp)
{
	struct map_cache *mcache;
	u64 found = 0ul;
	int type = IS_DISCARD;

	mcache = mcache_first_get(znd, type);
	while (mcache) {
		int at;
		int err;

		/* sort, if needed. only err is -EBUSY so do a linear find. */
		err = memcache_lock_and_sort(znd, mcache);
		if (!err)
			at = mcache_bsearch(mcache, addr);
		else
			at = mcache_lsearch(mcache, addr);

		if (at != -1) {
			/* break apart the entry */
			err = _discard_split_entry(znd, mcache, at, addr, gfp);
			if (err) {
				mcache_put(mcache);
				goto out;
			}
			found = 1;
		}

		if (found) {
			mcache_put(mcache);
			goto out;
		}

		mcache = mcache_put_get_next(znd, mcache, type);
	}
out:
	return found;
}

/**
 * z_discard_partial() - Migrate a discard range to memcache/ZLT
 * @znd: ZDM Instance
 * @minblks: Target number of blocks to cull.
 * @gfp: Memory allocation scheme
 *
 * When an address is being written to a check is made to see if the tLBA
 * overlaps with an exisitng discard pool. If so the discard pool is
 * split and multiple smaller discard entries are pushed into the memcache.
 *
 * The overall purpose of the discard pool is to reduce the amount of intake
 * on the memcache to avoid starving the I/O requests.
 */
static int z_discard_partial(struct zdm *znd, u32 minblks, gfp_t gfp)
{
	struct map_cache *mcache;
	int completions = 0;
	int type = IS_DISCARD;
	int lumax = minblks;

	mcache = mcache_first_get(znd, type);
	while (mcache) {
		int at = -1;
		int err;

		if (mcache->jcount > 1)
			at = 0;

		if (at != -1) {
			const int nodisc = 1;
			s32 e = at + 1;
			u64 tlba = le64_to_lba48(mcache->jdata[e].tlba, NULL);
			u64 blks = le64_to_lba48(mcache->jdata[e].bval, NULL);
			u64 chunk = DISCARD_MAX_INGRESS - 1;
			u32 dcount = 0;
			u32 dstop = minblks;

			while (blks > 0 && dcount < dstop && --lumax > 0) {
				u64 lba = _current_mapping(znd, nodisc,
							   tlba, gfp);
				if (lba) {
					do {
						err = z_to_map_list(znd, tlba,
								    0ul, gfp);
					} while (-EBUSY == err);
					if (err)
						break;
					dcount++;
				}
				tlba++;
				blks--;
			}

			if (chunk > blks)
				chunk = blks;

			if (chunk == 0) {
				mc_delete_entry(mcache, e);
				completions++;
			} else if (chunk < DISCARD_MAX_INGRESS) {
				err = mapped_discard(znd, tlba, chunk, 0, gfp);
				if (err) {
					mcache_put(mcache);
					goto out;
				}
				blks -= chunk;
				tlba += chunk;

				mcache->jdata[e].tlba = lba48_to_le64(0, tlba);
				mcache->jdata[e].bval = lba48_to_le64(0, blks);
				if (blks == 0ul)
					mc_delete_entry(mcache, e);
			}
			completions++;
		}
		if (completions) {
			int deleted = 0;

			if (completions > 1 &&
			    znd->dc_entries > MEMCACHE_HWMARK &&
			    mcache->jcount == 0 &&
			    mcache_getref(mcache) == 1)
				deleted = mclist_del(znd, mcache, type);

			mcache_put(mcache);

			if (deleted) {
				ZDM_FREE(znd, mcache->jdata, Z_C4K, PG_08);
				ZDM_FREE(znd, mcache, sizeof(*mcache), KM_07);
				mcache = NULL;
				znd->dc_entries--;
			}
			goto out;
		}
		mcache = mcache_put_get_next(znd, mcache, type);
	}
out:
	return completions;
}

/**
 * lba_in_zone() - Scan a map cache entry for any targets pointing to zone.
 * @znd: ZDM instance
 * @mcache: Map cache page.
 * @zone: Zone (that was gc'd).
 *
 * Scan the map cache page to see if any entries are pointing to a zone.
 *
 * Return: 0 if none are found. 1 if any are found.
 */
static int lba_in_zone(struct zdm *znd, struct map_cache *mcache, u32 zone)
{
	int jentry;

	if (zone >= znd->data_zones)
		goto out;

	for (jentry = mcache->jcount; jentry > 0; jentry--) {
		u64 lba = le64_to_lba48(mcache->jdata[jentry].bval, NULL);

		if (lba && _calc_zone(znd, lba) == zone)
			return 1;
	}
out:
	return 0;
}

/**
 * gc_verify_cache() - Test map cache to see if any entries point to zone.
 * @znd: ZDM instance
 * @zone: Zone (that was gc'd).
 *
 * Scan the map cache to see if any entries are pointing to a zone that
 * was just GC'd.
 * If any are found it indicates a bug.
 *
 * Return: 0 on success or 1 on error and sets meta_result to ENOSPC
 *         to effectivly freeze ZDM.
 */
static int gc_verify_cache(struct zdm *znd, u32 zone)
{
	struct map_cache *mcache = NULL;
	int err = 0;
	int type = IS_MAP;

	mcache = mcache_first_get(znd, type);
	while (mcache) {
		MutexLock(&mcache->cached_lock);
		if (lba_in_zone(znd, mcache, zone)) {
			Z_ERR(znd, "GC: **ERR** %" PRIx32
			      " LBA in cache <= Corrupt", zone);
			err = 1;
			znd->meta_result = -ENOSPC;
		}
		mutex_unlock(&mcache->cached_lock);
		mcache = mcache_put_get_next(znd, mcache, type);
	}

	return err;
}


/**
 * __cached_to_tables() - Migrate map cache entries to ZLT
 * @znd: ZDM instance
 * @type: Which type (MAP) of cache entries to migrate.
 * @zone: zone to force migration for partial memcache block
 *
 * Scan the memcache and move any full blocks to lookup tables
 * If a (the) partial memcache block contains lbas that map to zone force
 * early migration of the memcache block to ensure it is properly accounted
 * for and migrated during and upcoming GC pass.
 *
 * Return: 0 on success or -errno value
 */
static int __cached_to_tables(struct zdm *znd, int type, u32 zone)
{
	struct map_cache *mcache = NULL;
	int err = 0;
	int try_free = 0;

	mcache = mcache_first_get(znd, type);
	while (mcache) {
		int deleted = 0;
		struct map_cache *jskip;

		mcache_busy(mcache);
		MutexLock(&mcache->cached_lock);
		if (mcache->jcount == mcache->jsize) {
			memcache_sort(znd, mcache);
			err = move_to_map_tables(znd, mcache);
			if (!err && (mcache->jcount == 0))
				try_free++;
		} else {
			if (lba_in_zone(znd, mcache, zone)) {
				Z_ERR(znd,
					"Moving %d Runts because z: %u",
					mcache->jcount, zone);

				memcache_sort(znd, mcache);
				err = move_to_map_tables(znd, mcache);
			}
		}
		mutex_unlock(&mcache->cached_lock);
		mcache_unbusy(mcache);
		if (err) {
			mcache_put(mcache);
			Z_ERR(znd, "%s: Sector map failed. [Type %d]",
				__func__, type);
			goto out;
		}

		mcache_ref(mcache);
		jskip = mcache_put_get_next(znd, mcache, type);

		if (try_free > 0 && znd->mc_entries > MEMCACHE_HWMARK &&
		    mcache->jcount == 0 && mcache_getref(mcache) == 1) {
			deleted = mclist_del(znd, mcache, type);
			try_free--;
		}

		mcache_deref(mcache);

		if (deleted) {
			ZDM_FREE(znd, mcache->jdata, Z_C4K, PG_08);
			ZDM_FREE(znd, mcache, sizeof(*mcache), KM_07);
			mcache = NULL;
			znd->mc_entries--;
		}
		mcache = jskip;
	}
out:

	return err;
}


/**
 * _cached_to_tables() - Migrate memcache entries to lookup tables
 * @znd: ZDM instance
 * @zone: zone to force migration for partial memcache block
 *
 * Scan the memcache and move any full blocks to lookup tables
 * If a (the) partial memcache block contains lbas that map to zone force
 * early migration of the memcache block to ensure it is properly accounted
 * for and migrated during and upcoming GC pass.
 *
 * Return: 0 on success or -errno value
 */
static int _cached_to_tables(struct zdm *znd, u32 zone)
{
	int err;

	err = __cached_to_tables(znd, IS_MAP, zone);
	return err;
}


/**
 * z_flush_bdev() - Request backing device flushed to disk.
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int z_flush_bdev(struct zdm *znd, gfp_t gfp)
{
	int err;
	sector_t bi_done;

	err = blkdev_issue_flush(znd->dev->bdev, gfp, &bi_done);
	if (err)
		Z_ERR(znd, "%s: flush failing sector %lu!", __func__, bi_done);

	return err;
}

/**
 * pg_delete - Free a map_pg with spin locks held.
 * @znd: ZDM Instance
 * @expg: Page being released.
 *
 * Forced inline as it is 'optional' and because it is called with
 * spin locks enabled and only from a single caller.
 */
static __always_inline int pg_delete(struct zdm *znd, struct map_pg *expg)
{
	int req_flush = 0;
	int dropped = 0;
	int is_lut = test_bit(IS_LUT, &expg->flags);
	spinlock_t *lock = is_lut ? &znd->mapkey_lock : &znd->ct_lock;

	if (!spin_trylock(lock))
		return req_flush;

	if (test_bit(WB_JRNL_1, &expg->flags) ||
	    test_bit(WB_JRNL_2, &expg->flags)) {
		req_flush = !test_bit(IS_FLUSH, &expg->flags);
		if (req_flush)
			goto out;
		if (test_bit(IS_CRC, &expg->flags))
			goto out;
		if (!test_bit(IS_DROPPED, &expg->flags))
			goto out;

		MutexLock(&expg->md_lock);
		if (expg->data.addr) {
			void *pg = expg->data.addr;

			Z_DBG(znd, "** jrnl pg %"PRIx64" data dropped (%"PRIx64
			           ") %04x",
			      expg->lba, expg->last_write,
			      crc16_md(pg, Z_C4K));

			expg->data.addr = NULL;
			ZDM_FREE(znd, pg, Z_C4K, PG_27);
			req_flush = !test_bit(IS_FLUSH, &expg->flags);
			atomic_dec(&znd->incore);
		}
		mutex_unlock(&expg->md_lock);
	} else if (test_bit(IS_DROPPED, &expg->flags)) {
		del_htbl_entry(znd, expg);

		MutexLock(&expg->md_lock);
		if (test_and_clear_bit(IS_LAZY, &expg->flags)) {
			clear_bit(IS_DROPPED, &expg->flags);
			clear_bit(DELAY_ADD, &expg->flags);
			if (expg->data.addr) {
				void *pg = expg->data.addr;

				expg->data.addr = NULL;
				ZDM_FREE(znd, pg, Z_C4K, PG_27);
				atomic_dec(&znd->incore);
			}
			list_del(&expg->lazy);
			znd->in_lzy--;
			req_flush = !test_bit(IS_FLUSH, &expg->flags);
			dropped = 1;
		} else {
			Z_ERR(znd, "Detected double list del.");
		}
		mutex_unlock(&expg->md_lock);
		if (dropped)
			ZDM_FREE(znd, expg, sizeof(*expg), KM_20);
	}

out:
	spin_unlock(lock);
	return req_flush;
}

static int _pool_read(struct zdm *znd, struct map_pg **wset, int count);

/**
 * manage_lazy_activity() - Migrate delayed 'add' entries to the 'ZTL'
 * @znd: ZDM Instance
 *
 * The lzy list is used to perform less critical activities that could
 * be done via the ZTL primary list but gives a second chance when
 *   - Adding if the spin lock would lock.
 *   - Deleting ... if the cache entry turns out to be 'hotter' than
 *     the default we can catch it and make it 'hotter' before the
 *     hotness indicator is lost.
 */
static int manage_lazy_activity(struct zdm *znd)
{
	struct map_pg *expg;
	struct map_pg *_tpg;
	int want_flush = 0;
	const u32 msecs = MEM_PURGE_MSECS;
	struct map_pg **wset = NULL;
	int entries = 0;

	wset = ZDM_CALLOC(znd, sizeof(*wset), MAX_WSET, KM_19, NORMAL);
	if (!wset)
		return -ENOMEM;

	spin_lock(&znd->lzy_lck);
	expg = list_first_entry_or_null(&znd->lzy_pool, typeof(*expg), lazy);
	if (!expg || (&expg->lazy == &znd->lzy_pool))
		goto out;

	_tpg = list_next_entry(expg, lazy);
	while (&expg->lazy != &znd->lzy_pool) {
		/*
		 * this should never happen:
		 */
		if (test_bit(IS_DIRTY, &expg->flags))
			set_bit(DELAY_ADD, &expg->flags);

		if (test_bit(WB_RE_CACHE, &expg->flags))
			if (entries < MAX_WSET)
				wset[entries++] = expg;

		/*
		 * Migrage pg to zltlst list
		 */
		if (test_bit(DELAY_ADD, &expg->flags)) {
			if (spin_trylock(&znd->zlt_lck)) {
				if (!test_bit(IN_ZLT, &expg->flags)) {
					list_del(&expg->lazy);
					znd->in_lzy--;
					clear_bit(IS_LAZY, &expg->flags);
					clear_bit(IS_DROPPED, &expg->flags);
					clear_bit(DELAY_ADD, &expg->flags);
					set_bit(IN_ZLT, &expg->flags);
					list_add(&expg->zltlst, &znd->zltpool);
					znd->in_zlt++;
				} else {
					Z_ERR(znd, "** ZLT double add? %"PRIx64"",
						expg->lba);
				}
				spin_unlock(&znd->zlt_lck);
			}
		} else {
			/*
			 * Delete page
			 */
			if (!test_bit(IN_ZLT, &expg->flags) &&
			     test_bit(IS_DROPPED, &expg->flags)) {
				if (is_expired_msecs(expg->age, msecs))
					want_flush |= pg_delete(znd, expg);
			}
		}
		expg = _tpg;
		_tpg = list_next_entry(expg, lazy);
	}

out:
	spin_unlock(&znd->lzy_lck);

	if (entries > 0) {
		_pool_read(znd, wset, entries);
	}

	if (wset)
		ZDM_FREE(znd, wset, sizeof(*wset) * MAX_WSET, KM_19);

	return want_flush;
}

/**
 * pg_toggle_wb_journal() - Handle bouncing from WB JOURNAL <-> WB DIRECT
 * @znd: ZDM Instance
 * @expg: Page of map data being toggled.
 *
 */
static inline void pg_toggle_wb_journal(struct zdm *znd, struct map_pg *expg)
{
	if (test_and_clear_bit(WB_JRNL_2, &expg->flags)) {
		/* migrating from journal -> home, must return home */
		set_bit(WB_DIRECT,  &expg->flags);
		set_bit(WB_RE_CACHE,  &expg->flags);
	} else if (test_and_clear_bit(WB_JRNL_1, &expg->flags)) {
		/* migrating from 1 to 2 is okay stay in journal */
		set_bit(WB_JRNL_2,  &expg->flags);
	} else {
		/* migrating from home to journal is okay to stay home */
		clear_bit(WB_DIRECT, &expg->flags);
		set_bit(WB_JRNL_1, &expg->flags);
	}
}

/**
 * mark_clean_flush_zlt() - Mark all non-dirty ZLT blocks as 'FLUSH'
 * @znd: ZDM instance
 *
 * After a FLUSH/FUA these blocks are on disk and redundant FLUSH
 * can be skipped if the block is later ejected.
 */
static void mark_clean_flush_zlt(struct zdm *znd, int wb_toggle)
{
	struct map_pg *expg = NULL;
	struct map_pg *_tpg;

	spin_lock(&znd->zlt_lck);
	if (list_empty(&znd->zltpool))
		goto out;

	expg = list_last_entry(&znd->zltpool, typeof(*expg), zltlst);
	if (!expg || &expg->zltlst == (&znd->zltpool))
		goto out;

	_tpg = list_prev_entry(expg, zltlst);
	while (&expg->zltlst != &znd->zltpool) {
		ref_pg(expg);
		if (!test_bit(IS_DIRTY, &expg->flags))
			set_bit(IS_FLUSH, &expg->flags);
		if (wb_toggle)
			pg_toggle_wb_journal(znd, expg);
		deref_pg(expg);
		expg = _tpg;
		_tpg = list_prev_entry(expg, zltlst);
	}

out:
	spin_unlock(&znd->zlt_lck);
}

/**
 * mark_clean_flush_lzy() - Mark all non-dirty ZLT blocks as 'FLUSH'
 * @znd: ZDM instance
 *
 * After a FLUSH/FUA these blocks are on disk and redundant FLUSH
 * can be skipped if the block is later ejected.
 */
static void mark_clean_flush_lzy(struct zdm *znd, int wb_toggle)
{
	struct map_pg *expg = NULL;
	struct map_pg *_tpg;

	spin_lock(&znd->lzy_lck);
	expg = list_first_entry_or_null(&znd->lzy_pool, typeof(*expg), lazy);
	if (!expg || (&expg->lazy == &znd->lzy_pool))
		goto out;

	_tpg = list_next_entry(expg, lazy);
	while (&expg->lazy != &znd->lzy_pool) {
		ref_pg(expg);
		if (!test_bit(IS_DIRTY, &expg->flags))
			set_bit(IS_FLUSH, &expg->flags);
		if (wb_toggle)
			pg_toggle_wb_journal(znd, expg);
		deref_pg(expg);
		expg = _tpg;
		_tpg = list_next_entry(expg, lazy);
	}

out:
	spin_unlock(&znd->lzy_lck);
}

/**
 * mark_clean_flush() - Mark all non-dirty ZLT/LZY blocks as 'FLUSH'
 * @znd: ZDM instance
 *
 * After a FLUSH/FUA these blocks are on disk and redundant FLUSH
 * can be skipped if the block is later ejected.
 */
static void mark_clean_flush(struct zdm *znd, int wb_toggle)
{
	mark_clean_flush_zlt(znd, wb_toggle);
	mark_clean_flush_lzy(znd, wb_toggle);
}

/**
 * do_sync_metadata() - Write ZDM state to disk.
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int do_sync_metadata(struct zdm *znd, int sync, int drop)
{
	int err = 0;
	int want_flush;

	want_flush = manage_lazy_activity(znd);
	if (want_flush)
		set_bit(DO_FLUSH, &znd->flags);

	/* if drop is non-zero, DO_FLUSH may be set on return */
	err = sync_mapped_pages(znd, sync, drop);
	if (err) {
		Z_ERR(znd, "Uh oh: sync_mapped_pages -> %d", err);
		goto out;
	}

	/*
	 * If we are lucky then this sync will get us to a 'clean'
	 * state and the follow on bdev flush is redunant and skipped
	 *
	 * If not we will suffer a performance stall because we were
	 * ejected blocks.
	 *
	 * TODO: On Sync/Flush/FUA we can mark all of our clean ZLT
	 *       as flushed and we can bypass elevating the drop count
	 *       to trigger a flush for such already flushed blocks.
	 */
	want_flush = test_bit(DO_FLUSH, &znd->flags);
	err = z_mapped_sync(znd);
	if (err) {
		Z_ERR(znd, "Uh oh. z_mapped_sync -> %d", err);
		goto out;
	}

	if (test_and_clear_bit(DO_FLUSH, &znd->flags)) {
		err = z_flush_bdev(znd, GFP_KERNEL);
		if (err) {
			Z_ERR(znd, "Uh oh. flush_bdev failed. -> %d", err);
			goto out;
		}
		mark_clean_flush(znd, 0);
	}

out:
	return err;
}

/**
 * do_zdm_reload_from_disc() - Restore ZDM state from disk.
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int do_zdm_reload_from_disc(struct zdm *znd)
{
	int err = 0;

	if (test_and_clear_bit(DO_ZDM_RELOAD, &znd->flags))
		err = z_mapped_init(znd);

	return err;
}

/**
 * do_move_map_cache_to_table() - Migrate memcache entries to lookup tables
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int do_move_map_cache_to_table(struct zdm *znd)
{
	int err = 0;

	if (test_and_clear_bit(DO_MAPCACHE_MOVE, &znd->flags) ||
	    test_bit(DO_SYNC, &znd->flags))
		err = _cached_to_tables(znd, znd->data_zones);

	return err;
}

/**
 * do_sync_to_disk() - Write ZDM state to disk.
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int do_sync_to_disk(struct zdm *znd)
{
	int err = 0;
	int drop = 0;
	int sync = 0;

	if (test_and_clear_bit(DO_SYNC, &znd->flags) ||
	    test_bit(DO_FLUSH, &znd->flags))
		sync = 1;

	if (test_and_clear_bit(DO_MEMPOOL, &znd->flags)) {
		int pool_size = MZ_MEMPOOL_SZ * 4;

		/**
		 * Trust our cache miss algo
		 */
		pool_size = MZ_MEMPOOL_SZ * 3;
		if (is_expired_msecs(znd->age, MEM_PURGE_MSECS * 2))
			pool_size = 3;
		else if (is_expired_msecs(znd->age, MEM_PURGE_MSECS))
			pool_size = MZ_MEMPOOL_SZ;

		if (atomic_read(&znd->incore) > pool_size)
			drop = atomic_read(&znd->incore) - pool_size;
	}
	if (sync || drop)
		err = do_sync_metadata(znd, sync, drop);

	return err;
}

/**
 * meta_work_task() - Worker thread from metadata activity.
 * @work: Work struct containing ZDM instance.
 */
static void meta_work_task(struct work_struct *work)
{
	int err = 0;
	int locked = 0;
	struct zdm *znd;

	if (!work)
		return;

	znd = container_of(work, struct zdm, meta_work);
	if (!znd)
		return;

	err = do_zdm_reload_from_disc(znd);

	if (test_bit(DO_MAPCACHE_MOVE, &znd->flags) ||
	    test_bit(DO_FLUSH, &znd->flags) ||
	    test_bit(DO_SYNC, &znd->flags)) {
		MutexLock(&znd->mz_io_mutex);
		locked = 1;
	}

	/*
	 * Reduce memory pressure on map cache list of arrays
	 * by pushing them into the sector map lookup tables
	 */
	if (!err)
		err = do_move_map_cache_to_table(znd);

	/* force a consistent set of meta data out to disk */
	if (!err)
		err = do_sync_to_disk(znd);

	if (locked)
		mutex_unlock(&znd->mz_io_mutex);

	znd->age = jiffies_64;
	if (err < 0)
		znd->meta_result = err;

	clear_bit(DO_METAWORK_QD, &znd->flags);
}

/**
 * _update_entry() - Update an existing map cache entry.
 * @znd: ZDM instance
 * @mcache: Map cache block
 * @at: Data entry in map cache block
 * @dm_s: tBLA mapping from
 * @lba: New lba
 * @gfp: Allocation flags to use.
 *
 * If overwriting an entry will trigger an 'unused' to be pushed recording
 * the old block being stale.
 */
static int _update_entry(struct zdm *znd, struct map_cache *mcache, int at,
			 u64 dm_s, u64 lba, gfp_t gfp)
{
	struct map_sect_to_lba *data;
	u64 lba_was;
	int err = 0;

	data = &mcache->jdata[at + 1];
	lba_was = le64_to_lba48(data->bval, NULL);
	lba &= Z_LOWER48;
	data->bval = lba48_to_le64(0, lba);

	if (lba != lba_was) {
		Z_DBG(znd, "Remap %" PRIx64 " -> %" PRIx64
			   " (was %" PRIx64 "->%" PRIx64 ")",
		      dm_s, lba, le64_to_lba48(data->tlba, NULL), lba_was);
		err = unused_phy(znd, lba_was, 0, gfp);
		if (err == 1)
			err = 0;
	}

	return err;
}

/**
 * _update_disc() - Update an existing discard cache entry
 * @znd: ZDM instance
 * @mcache: Map cache block
 * @at: Data entry in map cache block
 * @dm_s: tBLA mapping from
 * @blks: number of blocks
 */
static int _update_disc(struct zdm *znd, struct map_cache *mcache, int at,
			u64 dm_s, u64 blks)
{
	struct map_sect_to_lba *data;
	u64 oldsz;
	int err = 0;

	data = &mcache->jdata[at + 1];
	oldsz = le64_to_lba48(data->bval, NULL);
	data->bval = lba48_to_le64(0, blks);

	return err;
}

/**
 * _update_journal() - Update an existing map cache entry.
 * @znd: ZDM instance
 * @mcache: Map cache block
 * @at: Data entry in map cache block
 * @dm_s: tBLA mapping from
 * @lba: New lba
 * @gfp: Allocation flags to use.
 */
static int _update_journal(struct zdm *znd, struct map_cache *mcache, int at,
			   u64 dm_s, u64 lba)
{
	struct map_sect_to_lba *data;
	u64 lba_was;

	data = &mcache->jdata[at + 1];
	lba_was = le64_to_lba48(data->bval, NULL);
	lba &= Z_LOWER48;
	data->bval = lba48_to_le64(0, lba);

	if (lba_was) {
		struct md_journal *jrnl = &znd->jrnl;
		u32 entry = lba_was - WB_JRNL_BASE;

		if (entry >= jrnl->size) {
			Z_ERR(znd, "BAD WB Journal! Got %" PRIx64 " max %x",
			           lba_was, jrnl->size);
			return -EIO;
		}

		SpinLock(&jrnl->wb_alloc);
		jrnl->in_use--;
		jrnl->wb[entry] = MZTEV_UNUSED;
		set_bit(IS_DIRTY, &jrnl->flags);
		spin_unlock(&jrnl->wb_alloc);
	}

	return 0;
}

/**
 * mc_update() - Update an existing cache entry
 * @znd: ZDM instance
 * @mcache: Map cache block
 * @at: Data entry in map cache block
 * @dm_s: tBLA mapping from
 * @lba: Value (lba or number of blocks)
 * @type: List (type) to be adding to (MAP or DISCARD)
 * @gfp: Allocation (kmalloc) flags
 */
static int mc_update(struct zdm *znd, struct map_cache *mcache, int at,
		     u64 dm_s, u64 lba, int type, gfp_t gfp)
{
	int rcode = -EIO;

	if (type == IS_MAP)
		rcode = _update_entry(znd, mcache, at, dm_s, lba, gfp);
	else if (type == IS_DISCARD)
		rcode = _update_disc(znd, mcache, at, dm_s, lba);
	else if (type == IS_JRNL_PG)
		rcode = _update_journal(znd, mcache, at, dm_s, lba);
	else
		dump_stack();

	return rcode;
}

/**
 * _mapped_list() - Add a entry to the discard or map cache
 * @znd: ZDM instance
 * @dm_s: tBLA mapping from
 * @lba: Value (lba or number of blocks)
 * @type: List (type) to be adding to (MAP or DISCARD)
 * @gfp: Allocation (kmalloc) flags
 */
static
int _mapped_list(struct zdm *znd, u64 dm_s, u64 lba, int type, gfp_t gfp)
{
	struct map_cache *mcache = NULL;
	struct map_cache *mc_add = NULL;
	int handled = 0;
	int list_count = 0;
	int err = 0;
	int add_to_list = 0;

	mcache = mcache_first_get(znd, type);
	while (mcache) {
		int at;

		MutexLock(&mcache->cached_lock);
		memcache_sort(znd, mcache);
		at = _bsrch_tlba(mcache, dm_s);
		if (at != -1) {
			mcache_ref(mcache);
			err = mc_update(znd, mcache, at, dm_s, lba, type, gfp);
			mcache_deref(mcache);
			handled = 1;
		} else if (!mc_add) {
			if (mcache->jcount < mcache->jsize) {
				mc_add = mcache;
				mcache_ref(mc_add);
			}
		}
		mutex_unlock(&mcache->cached_lock);
		if (handled) {
			if (mc_add)
				mcache_deref(mc_add);
			mcache_put(mcache);
			goto out;
		}
		mcache = mcache_put_get_next(znd, mcache, type);
		list_count++;
	}

	/* ------------------------------------------------------------------ */
	/* ------------------------------------------------------------------ */

	if (!mc_add) {
		mc_add = mcache_alloc(znd, gfp);
		if (!mc_add) {
			Z_ERR(znd, "%s: in memory journal is out of space.",
			      __func__);
			err = -ENOMEM;
			goto out;
		}
		mcache_ref(mc_add);
		if (type == IS_MAP) {
			if (list_count > MEMCACHE_HWMARK)
				set_bit(DO_MAPCACHE_MOVE, &znd->flags);
			znd->mc_entries = list_count + 1;
		} else if (type == IS_DISCARD) {
			znd->dc_entries = list_count + 1;
		} else {
			znd->journal_entries = list_count + 1;
		}
		add_to_list = 1;
	}

	/* ------------------------------------------------------------------ */
	/* ------------------------------------------------------------------ */

	if (mc_add) {
		MutexLock(&mc_add->cached_lock);

		if (!mc_add->jdata) {
			mc_add->jdata = ZDM_CALLOC(znd, Z_UNSORTED,
				sizeof(*mc_add->jdata), PG_08, gfp);
		}
		if (!mc_add->jdata) {
			Z_ERR(znd, "%s: in memory journal is out of space.",
			      __func__);
			err = -ENOMEM;
			goto out;
		}

		if (mc_add->jcount < mc_add->jsize) {
			mcache_insert(znd, mc_add, dm_s, lba);
			if (add_to_list)
				mclist_add(znd, mc_add, type);
		} else {
			err = -EBUSY;
		}
		mutex_unlock(&mc_add->cached_lock);
		mcache_put(mc_add);
	}
out:
	return err;
}

/**
 * z_to_discard_list() - Add a discard extent entry to the discard cache
 * @znd: ZDM instance
 * @dm_s: tBLA mapping from
 * @blks: Number of blocks in the extent.
 * @gfp: Allocation (kmalloc) flags
 */
static int z_to_discard_list(struct zdm *znd, u64 dm_s, u64 blks, gfp_t gfp)
{
	return _mapped_list(znd, dm_s, blks, IS_DISCARD, gfp);
}

/**
 * z_to_map_list() - Add a map cache entry to the map cache
 * @znd: ZDM instance
 * @dm_s: tBLA mapping from
 * @lba: bLBA mapping to
 * @gfp: Allocation (kmalloc) flags
 */
static int z_to_map_list(struct zdm *znd, u64 dm_s, u64 lba, gfp_t gfp)
{
	return _mapped_list(znd, dm_s, lba, IS_MAP, gfp);
}

/**
 * z_to_journal_list() - Add a map cache entry to the map cache
 * @znd: ZDM instance
 * @dm_s: tBLA mapping from
 * @lba: bLBA mapping to
 * @gfp: Allocation (kmalloc) flags
 */
static int z_to_journal_list(struct zdm *znd, u64 dm_s, u64 lba, gfp_t gfp)
{
	if (lba != 0ul) {
		u32 entry = lba - WB_JRNL_BASE;

		if (entry >= znd->jrnl.size)
			Z_ERR(znd, "Warning: Invalid bLBA: %"PRIx64, lba);
	}

	return _mapped_list(znd, dm_s, lba, IS_JRNL_PG, gfp);
}

/**
 * discard_merge() - Merge a discard request with a existing entry.
 * @znd: ZDM Instance
 * @tlba: Starting address
 * @blks: Number of blocks in discard.
 * @gfp: Current memory allocation scheme.
 *
 */
static int discard_merge(struct zdm *znd, u64 tlba, u64 blks)
{
	struct map_cache *mcache;
	u64 ends = tlba - 1;
	u64 next = tlba + blks;
	int type = IS_DISCARD;
	int merged = 0;

	mcache = mcache_first_get(znd, type);
	while (mcache) {
		int at;
		int err;

		/*
		 * 1) Modify existing discard entry, if it exists
		 * 2) Otherwise: If an existing entry *starts* where this
		 *    entry *ends* extend that entry.
		 * 3) Otherwise: If an existing entry ends where this
		 *    entry starts, extend *that* entry.
		 */
		err = memcache_lock_and_sort(znd, mcache);
		if (err == -EBUSY)
			at = _lsearch_tlba(mcache, tlba);
		else
			at = _bsrch_tlba_lck(mcache, tlba);

		if (at != -1) {
			mcache_ref(mcache);
			err = _update_disc(znd, mcache, at, tlba, blks);
			mcache_deref(mcache);
			merged = 1;
			at = -1;
		}

		/* Existing entry stars where this discard ends */
		if (!merged) {
			if (err == -EBUSY)
				at = _lsearch_tlba(mcache, next);
			else
				at = _bsrch_tlba_lck(mcache, next);
		}
		at = _bsrch_tlba(mcache, ends);
		if (at != -1) {
			struct map_sect_to_lba *data;
			u64 oldsz;

			mcache_ref(mcache);
			data = &mcache->jdata[at + 1];
			data->tlba = lba48_to_le64(0, tlba);
			oldsz = le64_to_lba48(data->bval, NULL);
			data->bval = lba48_to_le64(0, oldsz + blks);
			mcache_deref(mcache);
			merged = 1;
			at = -1;
		}

		/*
		 * Find a discard that includes 'ends', if so
		 * determine if the containing discard should be extended
		 * or if this discard is fully contained within the current
		 * discard entry.
		 */
		if (!merged) {
			if (err == -EBUSY)
				at = _lsearch_extent(mcache, ends);
			else
				at = _bsrch_extent_lck(mcache, ends);
		}
		if (at != -1) {
			struct map_sect_to_lba *data;
			u64 oldsz;
			u64 origin;
			u64 extend;

			mcache_ref(mcache);
			data = &mcache->jdata[at + 1];
			origin = le64_to_lba48(data->tlba, NULL);
			oldsz = le64_to_lba48(data->bval, NULL);
			extend = (tlba - origin) + blks;
			if (extend > oldsz)
				data->bval = lba48_to_le64(0, extend);
			mcache_deref(mcache);
			merged = 1;
			at = -1;
		}
		if (merged) {
			mcache_put(mcache);
			goto done;
		}
		mcache = mcache_put_get_next(znd, mcache, type);
	}
done:
	return merged;
}

/**
 * next_generation() - Increment generation number for superblock.
 * @znd: ZDM instance
 */
static inline u64 next_generation(struct zdm *znd)
{
	u64 generation = le64_to_cpu(znd->bmkeys->generation);

	if (generation == 0)
		generation = 2;

	generation++;
	if (generation == 0)
		generation++;

	return generation;
}

/**
 * z_mapped_sync() - Write cache entries and bump superblock generation.
 * @znd: ZDM instance
 */
static int z_mapped_sync(struct zdm *znd)
{
	struct md_journal *jrnl = &znd->jrnl;
	struct dm_target *ti = znd->ti;
	struct map_cache *mcache;
	int nblks = 1;
	int use_wq = 0;
	int rc = 1;
	int discards = 0;
	int maps = 0;
	int jwrote = 0;
	int cached = 0;
	int idx = 0;
	int no;
	int need_sync_io = 1;
	u64 lba = LBA_SB_START;
	u64 generation = next_generation(znd);
	u64 modulo = CACHE_COPIES;
	u64 incr = MAX_SB_INCR_SZ;
	struct io_4k_block *io_vcache;

	MutexLock(&znd->vcio_lock);
	io_vcache = get_io_vcache(znd, NORMAL);
	if (!io_vcache) {
		Z_ERR(znd, "%s: FAILED to get IO CACHE.", __func__);
		rc = -ENOMEM;
		goto out;
	}

	/* write dirty WP/ZF_EST blocks */
	lba = WP_ZF_BASE;
	for (idx = 0; idx < znd->gz_count; idx++) {
		struct meta_pg *wpg = &znd->wp[idx];

		if (test_bit(IS_DIRTY, &wpg->flags)) {
			cached = 0;
			memcpy(io_vcache[cached].data, wpg->wp_alloc, Z_C4K);
			znd->bmkeys->wp_crc[idx] =
				crc_md_le16(io_vcache[cached].data, Z_CRC_4K);
			cached++;
			memcpy(io_vcache[cached].data, wpg->zf_est, Z_C4K);
			znd->bmkeys->zf_crc[idx] =
				crc_md_le16(io_vcache[cached].data, Z_CRC_4K);
			cached++;
			rc = write_block(ti, DM_IO_VMA, io_vcache, lba,
				cached, use_wq);
			if (rc)
				goto out;
			clear_bit(IS_DIRTY, &wpg->flags);

			Z_DBG(znd, "%d# -- WP: %04x | ZF: %04x",
			      idx, znd->bmkeys->wp_crc[idx],
				   znd->bmkeys->zf_crc[idx]);
		}
		lba += 2;
	}

	if (test_bit(IS_DIRTY, &jrnl->flags)) {
		znd->bmkeys->wb_next = cpu_to_le32(jrnl->wb_next);
		znd->bmkeys->wb_crc32 = crc32c_le32(~0u, jrnl->wb, jrnl->size);
		lba = WB_JRNL_IDX;
		cached = jrnl->size >> 10;
		rc = write_block(ti, DM_IO_VMA, jrnl->wb, lba, cached, use_wq);
		if (rc)
			goto out;
		clear_bit(IS_DIRTY, &jrnl->flags);
	}

	/* Begin mem cache writeback (  */
	lba = (generation % modulo) * incr;
	if (lba == 0)
		lba++;

	znd->bmkeys->generation = cpu_to_le64(generation);
	znd->bmkeys->gc_resv = cpu_to_le32(znd->z_gc_resv);
	znd->bmkeys->meta_resv = cpu_to_le32(znd->z_meta_resv);

	idx = 0;
	for (no = 0; no < MAP_COUNT; no++) {
		if (no == IS_JRNL_PG)
			continue;

		mcache = mcache_first_get(znd, no);
		while (mcache) {
			u64 phy = le64_to_lba48(mcache->jdata[0].bval, NULL);
			u16 jcount = mcache->jcount & 0xFFFF;

			if (jcount) {
				mcache->jdata[0].bval =
					lba48_to_le64(jcount, phy);
				znd->bmkeys->crcs[idx] =
					crc_md_le16(mcache->jdata, Z_CRC_4K);
				idx++;
				memcpy(io_vcache[cached].data,
					mcache->jdata, Z_C4K);
				cached++;

				if (no == IS_DISCARD)
					discards++;
				else
					maps++;
			}

			if (cached == IO_VCACHE_PAGES) {
				rc = write_block(ti, DM_IO_VMA, io_vcache, lba,
						 cached, use_wq);
				if (rc) {
					Z_ERR(znd, "%s: cache-> %" PRIu64
					      " [%d blks] %p -> %d",
					      __func__, lba, nblks,
					      mcache->jdata, rc);
					mcache_put(mcache);
					goto out;
				}
				lba    += cached;
				jwrote += cached;
				cached  = 0;
			}
			mcache = mcache_put_get_next(znd, mcache, no);
		}
	}
	jwrote += cached;
	if (discards > 40)
		Z_ERR(znd, "**WARNING** large discard cache %d", discards);
	if (maps > 40)
		Z_ERR(znd, "**WARNING** large map cache %d", maps);

	znd->bmkeys->md_crc = crc_md_le16(znd->md_crcs, 2 * Z_CRC_4K);
	znd->bmkeys->n_crcs = cpu_to_le16(jwrote);
	znd->bmkeys->discards = cpu_to_le16(discards);
	znd->bmkeys->maps = cpu_to_le16(maps);
	znd->bmkeys->crc32 = 0;
	znd->bmkeys->crc32 = cpu_to_le32(crc32c(~0u, znd->bmkeys, Z_CRC_4K));
	if (cached < (IO_VCACHE_PAGES - 3)) {
		memcpy(io_vcache[cached].data, znd->bmkeys, Z_C4K);
		cached++;
		memcpy(io_vcache[cached].data, znd->md_crcs, Z_C4K * 2);
		cached += 2;
		need_sync_io = 0;
	}

	do {
		if (cached > 0) {
			unsigned int op_flags = 0;

			if (!need_sync_io &&
			    test_and_clear_bit(DO_FLUSH, &znd->flags))
				op_flags = REQ_PREFLUSH | REQ_FUA;

			rc = writef_block(ti, DM_IO_VMA, io_vcache, lba,
					  op_flags, cached, use_wq);
			if (rc) {
				Z_ERR(znd, "%s: mcache-> %" PRIu64
				      " [%d blks] %p -> %d",
				      __func__, lba, cached, io_vcache, rc);
				goto out;
			}
			lba += cached;
		}

		cached = 0;
		if (need_sync_io) {
			memcpy(io_vcache[cached].data, znd->bmkeys, Z_C4K);
			cached++;
			memcpy(io_vcache[cached].data, znd->md_crcs, Z_C4K * 2);
			cached += 2;
			need_sync_io = 0;
		}
	} while (cached > 0);

	mark_clean_flush(znd, 1);

out:
	put_io_vcache(znd, io_vcache);
	mutex_unlock(&znd->vcio_lock);
	return rc;
}

/**
 * is_key_page() - Probe block for magic and crc to see if it is recognized.
 * @_data: ZDM instance
 */
static inline int is_key_page(void *_data)
{
	int is_key = 0;
	struct mz_superkey *data = _data;

	/* Starts with Z_KEY_SIG and ends with magic */

	if (le64_to_cpu(data->sig1) == Z_KEY_SIG &&
	    le64_to_cpu(data->magic) == Z_TABLE_MAGIC) {
		__le32 orig = data->crc32;
		__le32 crc_check;

		data->crc32 = 0;
		crc_check = cpu_to_le32(crc32c(~0u, data, Z_CRC_4K));
		data->crc32 = orig;
		if (crc_check == orig)
			is_key = 1;
	}
	return is_key;
}

/**
 * zoned_personality() - Update zdstart value from superblock
 * @znd: ZDM instance
 * @sblock: Lba to start scanning for superblock.
 */
static inline
void zoned_personality(struct zdm *znd, struct zdm_superblock *sblock)
{
	znd->zdstart = le32_to_cpu(sblock->zdstart);
}

/**
 * find_superblock_at() - Find superblock following lba
 * @znd: ZDM instance
 * @lba: Lba to start scanning for superblock.
 * @use_wq: If a work queue is needed to scanning.
 * @do_init: Set zdstart from found superblock.
 */
static
int find_superblock_at(struct zdm *znd, u64 lba, int use_wq, int do_init)
{
	struct dm_target *ti = znd->ti;
	int found = 0;
	int nblks = 1;
	int rc = -ENOMEM;
	u32 count = 0;
	u64 *data = ZDM_ALLOC(znd, Z_C4K, PG_10, NORMAL);

	if (!data) {
		Z_ERR(znd, "No memory for finding generation ..");
		return 0;
	}
	if (lba == 0)
		lba++;
	do {
		rc = read_block(ti, DM_IO_KMEM, data, lba, nblks, use_wq);
		if (rc) {
			Z_ERR(znd, "%s: read @%" PRIu64 " [%d blks] %p -> %d",
			      __func__, lba, nblks, data, rc);
			goto out;
		}
		if (is_key_page(data)) {
			struct mz_superkey *kblk = (struct mz_superkey *) data;
			struct zdm_superblock *sblock = &kblk->sblock;
			int err = sb_check(sblock);

			if (!err) {
				found = 1;
				if (do_init)
					zoned_personality(znd, sblock);
			}
			goto out;
		}
		if (data[0] == 0 && data[1] == 0) {
			/* No SB here. */
			Z_ERR(znd, "FGen: Invalid block %" PRIx64 "?", lba);
			goto out;
		}
		lba++;
		count++;
		if (count > MAX_CACHE_SYNC) {
			Z_ERR(znd, "FSB: Too deep to be useful.");
			goto out;
		}
	} while (!found);

out:
	ZDM_FREE(znd, data, Z_C4K, PG_10);
	return found;
}

/**
 * find_superblock() - Find (any) superblock
 * @znd: ZDM instance
 * @use_wq: If a work queue is needed to scanning.
 * @do_init: Set/Retrieve zdstart from found super block.
 */
static int find_superblock(struct zdm *znd, int use_wq, int do_init)
{
	int found = 0;
	int iter = 0;
	u64 lba = LBA_SB_START;
	u64 last = MAX_SB_INCR_SZ * CACHE_COPIES;

	do {
		found = find_superblock_at(znd, lba, use_wq, do_init);
		if (found)
			break;
		iter++;
		lba = MAX_SB_INCR_SZ * iter;
	} while (lba < last);

	return found;
}

/**
 * mcache_find_gen() - Find the super block following lba and get gen#
 * @znd: ZDM instance
 * @lba: LBA to start scanning for the super block.
 * @use_wq: If a work queue is needed to scanning.
 * @sb_lba: LBA where the super block was found.
 */
static u64 mcache_find_gen(struct zdm *znd, u64 lba, int use_wq, u64 *sb_lba)
{
	struct dm_target *ti = znd->ti;
	u64 generation = 0;
	int nblks = 1;
	int rc = 1;
	int done = 0;
	u32 count = 0;
	u64 *data = ZDM_ALLOC(znd, Z_C4K, PG_11, NORMAL);

	if (!data) {
		Z_ERR(znd, "No memory for finding generation ..");
		return 0;
	}
	do {
		rc = read_block(ti, DM_IO_KMEM, data, lba, nblks, use_wq);
		if (rc) {
			Z_ERR(znd, "%s: mcache-> %" PRIu64
				    " [%d blks] %p -> %d",
			      __func__, lba, nblks, data, rc);
			goto out;
		}
		if (is_key_page(data)) {
			struct mz_superkey *kblk = (struct mz_superkey *) data;

			generation = le64_to_cpu(kblk->generation);
			done = 1;
			if (sb_lba)
				*sb_lba = lba;
			goto out;
		}
		lba++;
		count++;
		if (count > MAX_CACHE_SYNC) {
			Z_ERR(znd, "FGen: Too deep to be useful.");
			goto out;
		}
	} while (!done);

out:
	ZDM_FREE(znd, data, Z_C4K, PG_11);
	return generation;
}

/**
 * cmp_gen() - compare two u64 numbers considering rollover
 * @left: a u64
 * @right: a u64
 * Return: -1, 0, 1 if left < right, equal, or > respectively.
 */
static inline int cmp_gen(u64 left, u64 right)
{
	int result = 0;

	if (left != right) {
		u64 delta = (left > right) ? left - right : right - left;

		result = (left > right) ? -1 : 1;
		if (delta > 0xFFFFFFFF) {
			if (left == BAD_ADDR)
				result = 1;
		} else {
			if (right > left)
				result = 1;
		}
	}

	return result;
}

/**
 * mcache_greatest_gen() - Pick the lba where the super block should start.
 * @znd: ZDM instance
 * @use_wq: If a workqueue is needed for IO.
 * @sb: LBA of super block itself.
 * @at_lba: LBA where sync data starts (in front of the super block).
 */
static
u64 mcache_greatest_gen(struct zdm *znd, int use_wq, u64 *sb, u64 *at_lba)
{
	u64 lba = LBA_SB_START;
	u64 gen_no[CACHE_COPIES] = { 0ul, 0ul, 0ul };
	u64 gen_lba[CACHE_COPIES] = { 0ul, 0ul, 0ul };
	u64 gen_sb[CACHE_COPIES] = { 0ul, 0ul, 0ul };
	u64 incr = MAX_SB_INCR_SZ;
	int locations = ARRAY_SIZE(gen_lba);
	int pick = 0;
	int idx;

	for (idx = 0; idx < locations; idx++) {
		u64 *pAt = &gen_sb[idx];

		gen_lba[idx] = lba;
		gen_no[idx] = mcache_find_gen(znd, lba, use_wq, pAt);
		if (gen_no[idx])
			pick = idx;
		lba = idx * incr;
	}

	for (idx = 0; idx < locations; idx++) {
		if (cmp_gen(gen_no[pick], gen_no[idx]) > 0)
			pick = idx;
	}

	if (gen_no[pick]) {
		if (at_lba)
			*at_lba = gen_lba[pick];
		if (sb)
			*sb = gen_sb[pick];
	}

	return gen_no[pick];
}

/**
 * count_stale_blocks() - Number of stale blocks covered by meta_pg.
 * @znd: ZDM instance
 * @gzno: Meta page # to scan.
 * @wpg: Meta page to scan.
 */
static u64 count_stale_blocks(struct zdm *znd, u32 gzno, struct meta_pg *wpg)
{
	u32 gzcount = 1 << GZ_BITS;
	u32 iter;
	u64 stale = 0;

	if ((gzno << GZ_BITS) > znd->data_zones)
		gzcount = znd->data_zones & GZ_MMSK;

	/* mark as empty */
	for (iter = 0; iter < gzcount; iter++) {
		u32 wp = le32_to_cpu(wpg->wp_alloc[iter]) & Z_WP_VALUE_MASK;
		u32 nf = le32_to_cpu(wpg->zf_est[iter]) & Z_WP_VALUE_MASK;

		if (wp > (Z_BLKSZ - nf))
			stale += (wp - (Z_BLKSZ - nf));
	}

	return stale;
}

/**
 * do_load_cache() - Read a series of map cache blocks to restore from disk.
 * @znd: ZDM Instance
 * @type: Map cache list (MAP, DISCARD, JOURNAL .. )
 * @lba: Starting LBA for reading
 * @idx: Saved/Expected CRC of block.
 * @wq: True when I/O needs to use worker thread.
 */
static int do_load_cache(struct zdm *znd, int type, u64 lba, int idx, int wq)
{
	u16 count;
	__le16 crc;
	int rc = 0;
	int blks = 1;
	struct map_cache *mcache = mcache_alloc(znd, NORMAL);

	if (!mcache) {
		rc = -ENOMEM;
		goto out;
	}

	rc = read_block(znd->ti, DM_IO_KMEM, mcache->jdata, lba, blks, wq);
	if (rc) {
		Z_ERR(znd, "%s: mcache-> %" PRIu64
			   " [%d blks] %p -> %d",
		      __func__, lba, blks, mcache->jdata, rc);

		ZDM_FREE(znd, mcache->jdata, Z_C4K, PG_08);
		ZDM_FREE(znd, mcache, sizeof(*mcache), KM_07);
		goto out;
	}
	crc = crc_md_le16(mcache->jdata, Z_CRC_4K);
	if (crc != znd->bmkeys->crcs[idx]) {
		rc = -EIO;
		Z_ERR(znd, "%s: bad crc %" PRIu64,  __func__, lba);
		goto out;
	}
	(void)le64_to_lba48(mcache->jdata[0].bval, &count);
	mcache->jcount = count;
	mclist_add(znd, mcache, type);

out:
	return rc;
}

/**
 * do_load_map_cache() - Read a series of map cache blocks to restore from disk.
 * @znd: ZDM Instance
 * @lba: Starting LBA for reading
 * @idx: Saved/Expected CRC of block.
 * @wq: True when I/O needs to use worker thread.
 */
static int do_load_map_cache(struct zdm *znd, u64 lba, int idx, int wq)
{
	return do_load_cache(znd, IS_MAP, lba, idx, wq);
}

/**
 * do_load_discard_cache() - Read a DISCARD map cache blocks.
 * @znd: ZDM Instance
 * @lba: Starting LBA for reading
 * @idx: Saved/Expected CRC of block.
 * @wq: True when I/O needs to use worker thread.
 */
static int do_load_discard_cache(struct zdm *znd, u64 lba, int idx, int wq)
{
	return do_load_cache(znd, IS_DISCARD, lba, idx, wq);
}

/**
 * z_mapped_init() - Re-Load an existing ZDM instance from the block device.
 * @znd: ZDM instance
 *
 * FIXME: Discard extent read-back does not match z_mapped_sync writing
 */
static int z_mapped_init(struct zdm *znd)
{
	struct dm_target *ti = znd->ti;
	struct md_journal *jrnl = &znd->jrnl;
	int nblks = 1;
	int wq = 0;
	int rc = 1;
	int idx = 0;
	int jcount = 0;
	u64 sblba = 0;
	u64 lba = 0;
	u64 generation;
	__le32 crc_chk;
	gfp_t gfp = NORMAL;
	struct io_4k_block *io_vcache;

	MutexLock(&znd->vcio_lock);
	io_vcache = get_io_vcache(znd, gfp);

	if (!io_vcache) {
		Z_ERR(znd, "%s: FAILED to get SYNC CACHE.", __func__);
		rc = -ENOMEM;
		goto out;
	}

	generation = mcache_greatest_gen(znd, wq, &sblba, &lba);
	if (generation == 0) {
		rc = -ENODATA;
		goto out;
	}

	if (lba == 0)
		lba++;

	/* read superblock */
	rc = read_block(ti, DM_IO_VMA, io_vcache, sblba, nblks, wq);
	if (rc)
		goto out;

	memcpy(znd->bmkeys, io_vcache, sizeof(*znd->bmkeys));

	/* read in map cache */
	for (idx = 0; idx < le16_to_cpu(znd->bmkeys->maps); idx++) {
		rc = do_load_map_cache(znd, lba++, jcount++, wq);
		if (rc)
			goto out;
	}

	/* read in discard cache */
	for (idx = 0; idx < le16_to_cpu(znd->bmkeys->discards); idx++) {
		rc = do_load_discard_cache(znd, lba++, jcount++, wq);
		if (rc)
			goto out;
	}

	/* skip re-read of superblock */
	if (lba == sblba)
		lba++;

	/* read in CRC pgs */
	rc = read_block(ti, DM_IO_VMA, znd->md_crcs, lba, 2, wq);
	if (rc)
		goto out;

	crc_chk = znd->bmkeys->crc32;
	znd->bmkeys->crc32 = 0;
	znd->bmkeys->crc32 = cpu_to_le32(crc32c(~0u, znd->bmkeys, Z_CRC_4K));

	if (crc_chk != znd->bmkeys->crc32) {
		Z_ERR(znd, "Bad Block Map KEYS!");
		Z_ERR(znd, "Key CRC: Ex: %04x vs %04x <- calculated",
		      le32_to_cpu(crc_chk),
		      le32_to_cpu(znd->bmkeys->crc32));
		rc = -EIO;
		goto out;
	}

	if (jcount != le16_to_cpu(znd->bmkeys->n_crcs)) {
		Z_ERR(znd, " ... mcache entries: found = %u, expected = %u",
		      jcount, le16_to_cpu(znd->bmkeys->n_crcs));
		rc = -EIO;
		goto out;
	}

	crc_chk = crc_md_le16(znd->md_crcs, Z_CRC_4K * 2);
	if (crc_chk != znd->bmkeys->md_crc) {
		Z_ERR(znd, "CRC of CRC PGs: Ex %04x vs %04x  <- calculated",
		      le16_to_cpu(znd->bmkeys->md_crc),
		      le16_to_cpu(crc_chk));
		rc = -EIO;
		goto out;
	}

	/*
	 * Read write pointers / free counters.
	 */
	lba = WP_ZF_BASE;
	znd->discard_count = 0;
	for (idx = 0; idx < znd->gz_count; idx++) {
		struct meta_pg *wpg = &znd->wp[idx];
		__le16 crc_wp;
		__le16 crc_zf;

		rc = read_block(ti, DM_IO_KMEM, wpg->wp_alloc, lba, 1, wq);
		if (rc)
			goto out;
		crc_wp = crc_md_le16(wpg->wp_alloc, Z_CRC_4K);
		if (znd->bmkeys->wp_crc[idx] != crc_wp)
			Z_ERR(znd, "WP @ %d does not match written.", idx);

		rc = read_block(ti, DM_IO_KMEM, wpg->zf_est, lba + 1, 1, wq);
		if (rc)
			goto out;
		crc_zf = crc_md_le16(wpg->zf_est, Z_CRC_4K);
		if (znd->bmkeys->zf_crc[idx] != crc_zf)
			Z_ERR(znd, "ZF @ %d does not match written.", idx);

		Z_DBG(znd, "%d# -- WP: %04x [%04x] | ZF: %04x [%04x]",
		      idx, znd->bmkeys->wp_crc[idx], crc_wp,
			   znd->bmkeys->zf_crc[idx], crc_zf);

		if (znd->bmkeys->wp_crc[idx] == crc_wp &&
		    znd->bmkeys->zf_crc[idx] == crc_zf)
			znd->discard_count += count_stale_blocks(znd, idx, wpg);

		lba += 2;
	}
	znd->z_gc_resv   = le32_to_cpu(znd->bmkeys->gc_resv);
	znd->z_meta_resv = le32_to_cpu(znd->bmkeys->meta_resv);

	lba = WB_JRNL_IDX;
	nblks = jrnl->size >> 10;
	rc = read_block(ti, DM_IO_VMA, jrnl->wb, lba, nblks, wq);
	if (rc)
		goto out;

	jrnl->wb_next = le32_to_cpu(znd->bmkeys->wb_next);
	if (znd->bmkeys->wb_crc32 != crc32c_le32(~0u, jrnl->wb, jrnl->size)) {
		Z_ERR(znd, "WB Journal corrupt.");
		goto out;
	}

	for (idx = 0; idx < jrnl->size; idx++) {
		u64 blba;
		u64 tlba;

		if (jrnl->wb[idx] == MZTEV_UNUSED)
			continue;
		if (jrnl->wb[idx] == MZTEV_NF)
			continue;

		blba = idx + WB_JRNL_BASE;
		tlba = le32_to_cpu(jrnl->wb[idx]);

		Z_ERR(znd, " .. restore map: %"PRIx64 " -> %"PRIx64,
		      tlba, blba);

		/* Create the journal mcache entries */
		do {
			rc = z_to_journal_list(znd, tlba, blba, gfp);
		} while (-EBUSY == rc);

		if (rc)
			goto out;
	}

out:
	put_io_vcache(znd, io_vcache);
	mutex_unlock(&znd->vcio_lock);
	return rc;
}

/**
 * z_mapped_addmany() - Add multiple entries into the map_cache
 * @znd: ZDM instance
 * @dm_s: tLBA
 * @lba: lba on backing device.
 * @c: Number of (contiguous) map entries to add.
 * @fp: Allocation flags (for _ALLOC)
 */
static int z_mapped_addmany(struct zdm *znd, u64 dm_s, u64 lba,
			    u64 count, gfp_t gfp)
{
	int rc = 0;
	sector_t blk;

	for (blk = 0; blk < count; blk++) {
		rc = z_mapped_add_one(znd, dm_s + blk, lba + blk, gfp);
		if (rc)
			goto out;
	}

out:
	return rc;
}

/**
 * alloc_pg() - Allocate a map page
 * @znd: ZDM instance
 * @entry: entry (in mpi table) to update on allocation.
 * @lba: LBA associated with the page of ZLT
 * @mpi: Map page information (lookup table entry, bit flags, etc)
 * @ahead: Flag to set READA flag on page
 * @gfp: Allocation flags (for _ALLOC)
 */
static struct map_pg *alloc_pg(struct zdm *znd, int entry, u64 lba,
			       struct mpinfo *mpi, int ahead, gfp_t gfp)
{
	struct map_pg *found = ZDM_ALLOC(znd, sizeof(*found), KM_20, gfp);

	if (found) {
		found->lba = lba;
		mutex_init(&found->md_lock);
		set_bit(mpi->bit_dir, &found->flags);
		set_bit(mpi->bit_type, &found->flags);
		found->age = jiffies_64;
		found->index = entry;
		INIT_LIST_HEAD(&found->zltlst);
		INIT_LIST_HEAD(&found->lazy);
		INIT_HLIST_NODE(&found->hentry);
		found->znd = znd;
		found->crc_pg = NULL; /* */
		ref_pg(found);
		if (ahead)
			set_bit(IS_READA, &found->flags);
		set_bit(WB_JRNL_1, &found->flags);

		/* allocation done. check and see if there as a
		 * concurrent race
		 */
		spin_lock(mpi->lock);
		if (!add_htbl_entry(znd, mpi, found))
			ZDM_FREE(znd, found, sizeof(*found), KM_20);
		spin_unlock(mpi->lock);
	} else {
		Z_ERR(znd, "NO MEM for mapped_t !!!");
	}
	return found;
}

/**
 * _maybe_undrop() - If a page is on its way out of cache pull it back.
 * @znd: ZDM instance
 * @pg: Page to claim
 *
 * When a table page is being dropped from the cache it may transition
 * through the lazy pool. It a page is caught in the lazy pool it is
 * deemed to be 'warm'. Not hot enough to be frequently hit but clearly
 * too warm to be dropped quickly. Give it a boost to keep in in cache
 * longer.
 */
static __always_inline int _maybe_undrop(struct zdm *znd, struct map_pg *pg)
{
	int undrop = 0;

	if (test_bit(IS_DROPPED, &pg->flags)) {
		spin_lock(&znd->lzy_lck);
		if (test_bit(IS_DROPPED, &pg->flags) &&
		    test_bit(IS_LAZY, &pg->flags)) {
			clear_bit(IS_DROPPED, &pg->flags);
			set_bit(DELAY_ADD, &pg->flags);
		}
		spin_unlock(&znd->lzy_lck);

		if (!pg->data.addr)
			Z_ERR(znd, "Undrop jrnl'd %"PRIx64, pg->lba);
		else
			Z_DBG(znd, "Undrop normal'd %"PRIx64, pg->lba);

		if (pg->data.addr && pg->hotness < MEM_PURGE_MSECS)
			pg->hotness += MEM_HOT_BOOST_INC;
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
		undrop = 1;
	}
	ref_pg(pg);
	return undrop;
}

/**
 * _load_backing_pages() - Cache a backing page
 * @znd: ZDM instance
 * @lba: Logical LBA of page.
 * @gfp: Memory allocation rule
 *
 * When metadata is pooled with data the FWD table lookup can
 * be recursive (the page needed to resolve the FWD entry is
 * itself on disk). The recursion is never deep but it can
 * be avoided or mitigated by keep such 'key' pages in cache.
 */
static int _load_backing_pages(struct zdm *znd, u64 lba, gfp_t gfp)
{
	const int raflg = 1;
	int rc = 0;
	int entry;
	struct mpinfo mpi;
	struct map_addr maddr;
	struct map_pg *found;

	if (lba < znd->data_lba)
		goto out;

	map_addr_calc(znd, lba, &maddr);
	entry = to_table_entry(znd, maddr.lut_s, &mpi);
	if (entry > -1) {
		spin_lock(mpi.lock);
		found = get_htbl_entry(znd, &mpi);
		if (found)
			_maybe_undrop(znd, found);
		spin_unlock(mpi.lock);
		if (!found)
			found = alloc_pg(znd, entry, lba, &mpi, raflg, gfp);
		if (found) {
			if (!found->data.addr) {
				rc = cache_pg(znd, found, gfp, &mpi);
				if (rc < 0 && rc != -EBUSY)
					znd->meta_result = rc;
			}
			deref_pg(found);

			if (getref_pg(found) != 0)
				Z_ERR(znd, "Backing page with elevated ref: %u",
				      getref_pg(found));
		}
	}

out:
	return rc;
}

/**
 * _load_crc_page() - Cache a page of CRC
 * @znd: ZDM instance
 * @lba: Logical LBA of page.
 * @gfp: Memory allocation rule
 *
 * When a table page is cached the page containing its CRC is also pulled
 * into cache. Rather than defer it to cache_pg() it's brought into the
 * cache here.
 */
static int _load_crc_page(struct zdm *znd, struct mpinfo *mpi, gfp_t gfp)
{
	const int raflg = 0;
	int rc = 0;
	int entry;
	struct map_pg *found;
	u64 base = (mpi->bit_dir == IS_REV) ? znd->c_mid : znd->c_base;

	base += mpi->crc.pg_no;
	entry = to_table_entry(znd, base, mpi);

	if (mpi->bit_type != IS_CRC)
		return rc;
	if (entry >= znd->crc_count)
		return rc;

	spin_lock(mpi->lock);
	found = get_htbl_entry(znd, mpi);
	if (found)
		_maybe_undrop(znd, found);
	spin_unlock(mpi->lock);
	if (!found)
		found = alloc_pg(znd, entry, base, mpi, raflg, gfp);
	if (found) {
		if (!found->data.crc) {
			rc = cache_pg(znd, found, gfp, mpi);
			if (rc < 0 && rc != -EBUSY)
				znd->meta_result = rc;
		}
		deref_pg(found);
	}
	return rc;
}

/**
 * put_map_entry() - Decrement refcount of mapped page.
 * @pg: mapped page
 */
static inline void put_map_entry(struct map_pg *pg)
{
	if (pg)
		deref_pg(pg);
}

/**
 * get_map_entry() - Find a page of LUT or CRC table map.
 * @znd: ZDM instance
 * @lba: Logical LBA of page.
 * @gfp: Memory allocation rule
 *
 * Return: struct map_pg * or NULL on error.
 *
 * Page will be loaded from disk it if is not already in core memory.
 */
static struct map_pg *get_map_entry(struct zdm *znd, u64 lba, gfp_t gfp)
{
	struct mpinfo mpi;
	struct map_pg *ahead[READ_AHEAD];
	int entry;
	int iter;
	int range;
	u32 count;
	int empty_val;

	memset(&ahead, 0, sizeof(ahead));
	entry = to_table_entry(znd, lba, &mpi);
	if (entry < 0)
		return NULL;

	if (mpi.bit_type == IS_LUT) {
		count = znd->map_count;
		empty_val = 0xff;

		if (mpi.bit_dir == IS_FWD) {
			/*
			 * pre-load any backing pages
			 * to unwind recursive page lookups.
			 */
			_load_backing_pages(znd, lba, gfp);
		}
		_load_crc_page(znd, &mpi, gfp);

		range = entry + ARRAY_SIZE(ahead);
	} else {
		count = znd->crc_count;
		empty_val = 0;

		/* CRC's cover 2k pages .. so only pull two extra */
		range = entry + 2;
	}
	if (range > count)
		range = count;

	iter = 0;
	while (entry < range) {
		int want_cached = 1;
		struct map_pg *found;

		entry = to_table_entry(znd, lba, &mpi);
		if (entry < 0)
			break;

		spin_lock(mpi.lock);
		found = get_htbl_entry(znd, &mpi);
		if (found &&
		    _maybe_undrop(znd, found) &&
		    test_bit(IS_READA, &found->flags)
		    && iter > 0)
			want_cached = 0;

		if (found) {
			if (want_cached)
				found->age = jiffies_64
					   + msecs_to_jiffies(found->hotness);
			else
				found->age +=
					msecs_to_jiffies(MEM_HOT_BOOST_INC);
		}
		spin_unlock(mpi.lock);

		if (want_cached && !found)
			found = alloc_pg(znd, entry, lba, &mpi, iter, gfp);

		if (found) {
			if (want_cached)
				ahead[iter] = found;
			else
				deref_pg(found);
		}
		iter++;
		entry++;
		lba++;
	}

	/*
	 *  Each entry in ahead has an elevated refcount.
	 * Only allow the target of get_map_entry() to remain elevated.
	 */
	for (iter = 0; iter < ARRAY_SIZE(ahead); iter++) {
		struct map_pg *pg = ahead[iter];

		if (pg) {
			if (!pg->data.addr) {
				int rc;

				to_table_entry(znd, pg->lba, &mpi);
				rc = cache_pg(znd, pg, gfp, &mpi);

				if (rc < 0 && rc != -EBUSY) {
					znd->meta_result = rc;
					ahead[iter] = NULL;
				}
			}
			if (iter > 0)
				deref_pg(pg);
		}
	}

	return ahead[0];
}

/**
 * metadata_dirty_fling() - Force a ZLT block into cache and flag it dirty.
 * @znd: ZDM Instance
 * @dm_s: Current lba to consider.
 *
 * Used when data and ZDM's metadata are co-mingled. If dm_s is a block
 * of ZDM's metadata it needs to be relocated. Since we re-locate
 * blocks that are in dirty and in the cache ... if this block is
 * metadata, force it into the cache and flag it as dirty.
 */
static int metadata_dirty_fling(struct zdm *znd, u64 dm_s)
{
	struct map_pg *pg = NULL;
	int is_flung = 0;

	/*
	 * When co
	 * nothing in the GC reverse map should point to
	 * a block *before* a data pool block
	 */
	if (dm_s < znd->data_lba)
		return is_flung;

	if (dm_s >= znd->r_base && dm_s < znd->c_end) {
		pg = get_map_entry(znd, dm_s, NORMAL);
		if (!pg)
			Z_ERR(znd, "Failed to fling: %" PRIx64, dm_s);
	}
	if (pg) {
		MutexLock(&pg->md_lock);
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
		clear_bit(IS_READA, &pg->flags);
		set_bit(IS_DIRTY, &pg->flags);
		clear_bit(IS_FLUSH, &pg->flags);
		is_flung = 1;
		mutex_unlock(&pg->md_lock);

		if (pg->lba != dm_s)
			Z_ERR(znd, "Excess churn? lba %"PRIx64
				   " [last: %"PRIx64"]", dm_s, pg->lba);
		put_map_entry(pg);
	}
	return is_flung;
}

/**
 * z_do_copy_more() - GC transition to read more blocks.
 * @gc_state: GC State to be updated.
 */
static inline void z_do_copy_more(struct gc_state *gc_entry)
{
	unsigned long flags;
	struct zdm *znd = gc_entry->znd;

	spin_lock_irqsave(&znd->gc_lock, flags);
	set_bit(DO_GC_PREPARE, &gc_entry->gc_flags);
	spin_unlock_irqrestore(&znd->gc_lock, flags);
}

/**
 * gc_post_add() - Add a tLBA and current bLBA origin.
 * @znd: ZDM Instance
 * @dm_s: tLBA
 * @lba: bLBA
 *
 * Return: 1 if tLBA is added, 0 if block was stale.
 *
 * Stale block checks are performed before tLBA is added.
 * Add a non-stale block to the list of blocks for moving and
 * metadata updating.
 */
static int gc_post_add(struct zdm *znd, u64 addr, u64 lba)
{
	struct map_cache *post = &znd->gc_postmap;
	int handled = 0;

	if (z_lookup_cache(znd, addr, IS_DISCARD))
		return handled;

	if (metadata_dirty_fling(znd, addr))
		return handled;

	if (post->jcount < post->jsize) {
		u16 idx = ++post->jcount;

		WARN_ON(post->jcount > post->jsize);

		post->jdata[idx].tlba = lba48_to_le64(0, addr);
		post->jdata[idx].bval = lba48_to_le64(0, lba);
		handled = 1;
	} else {
		Z_ERR(znd, "*CRIT* post overflow L:%" PRIx64 "-> S:%" PRIx64,
		      lba, addr);
	}
	return handled;
}


/**
 * _fwd_to_cache() - Helper to pull the forward map block into cache.
 * @znd: ZDM Instance
 * @addr: Address (from reverse map table entry).
 */
static __always_inline int _fwd_to_cache(struct zdm *znd, u64 addr)
{
	int err = 0;
	struct map_pg *pg;
	struct map_addr maddr;

	if (addr < znd->nr_blocks) {
		map_addr_calc(znd, addr, &maddr);
		pg = get_map_entry(znd, maddr.lut_s, NORMAL);
		if (!pg)
			err = -ENOMEM;
		put_map_entry(pg);
	} else {
		Z_ERR(znd, "Invalid rmap entry: %" PRIx64, addr);
	}

	return err;
}

/**
 * z_zone_gc_metadata_to_ram() - Load affected metadata blocks to ram.
 * @gc_entry: Compaction event in progress
 *
 * Return: 0, otherwise errno.
 *
 * Use the reverse ZLT to find the forward ZLT entries that need to be
 * remapped in this zone.
 * When complete the znd->gc_postmap have a map of all the non-stale
 * blocks remaining in the zone.
 */
static int z_zone_gc_metadata_to_ram(struct gc_state *gc_entry)
{
	struct zdm *znd = gc_entry->znd;
	u64 from_lba = (gc_entry->z_gc << Z_BLKBITS) + znd->md_end;
	struct map_pg *rev_pg;
	struct map_addr origin;
	int count;
	int rcode = 0;

	/* pull all of the affected struct map_pg and crc pages into memory: */
	for (count = 0; count < Z_BLKSZ; count++) {
		__le32 ORencoded;
		u64 blba = from_lba + count;

		map_addr_calc(znd, blba, &origin);
		rev_pg = get_map_entry(znd, origin.lut_r, NORMAL);
		if (rev_pg && rev_pg->data.addr) {
			ref_pg(rev_pg);
			MutexLock(&rev_pg->md_lock);
			ORencoded = rev_pg->data.addr[origin.pg_idx];
			mutex_unlock(&rev_pg->md_lock);
			if (ORencoded != MZTEV_UNUSED) {
				u64 tlba = map_value(znd, ORencoded);
				int err = _fwd_to_cache(znd, tlba);

				if (err)
					rcode = err;
				gc_post_add(znd, tlba, blba);
			}
			deref_pg(rev_pg);
		}
		put_map_entry(rev_pg);
	}

	return rcode;
}

/**
 * append_blks() - Read (more) blocks into buffer.
 * @znd: ZDM Instance
 * @lba: Starting blba
 * @io_buf: Buffer to read into
 * @count: Number of blocks to read.
 */
static int append_blks(struct zdm *znd, u64 lba,
		       struct io_4k_block *io_buf, int count)
{
	int rcode = 0;
	int rc;
	u32 chunk;
	struct io_4k_block *io_vcache;

	MutexLock(&znd->gc_vcio_lock);
	io_vcache = get_io_vcache(znd, NORMAL);
	if (!io_vcache) {
		Z_ERR(znd, "%s: FAILED to get SYNC CACHE.", __func__);
		rc = -ENOMEM;
		goto out;
	}

	for (chunk = 0; chunk < count; chunk += IO_VCACHE_PAGES) {
		u32 nblks = count - chunk;

		if (nblks > IO_VCACHE_PAGES)
			nblks = IO_VCACHE_PAGES;

		rc = read_block(znd->ti, DM_IO_VMA, io_vcache, lba, nblks, 0);
		if (rc) {
			Z_ERR(znd, "Reading error ... disable zone: %u",
				(u32)(lba >> 16));
			rcode = -EIO;
			goto out;
		}
		memcpy(&io_buf[chunk], io_vcache, nblks * Z_C4K);
		lba += nblks;
	}
out:
	put_io_vcache(znd, io_vcache);
	mutex_unlock(&znd->gc_vcio_lock);
	return rcode;
}

/**
 * z_zone_gc_read() - Read (up to) a buffer worth of data from zone.
 * @gc_entry: Active GC state
 */
static int z_zone_gc_read(struct gc_state *gc_entry)
{
	struct zdm *znd = gc_entry->znd;
	struct io_4k_block *io_buf = znd->gc_io_buf;
	struct map_cache *post = &znd->gc_postmap;
	unsigned long flags;
	u64 start_lba;
	int nblks;
	int rcode = 0;
	int fill = 0;
	int jstart;
	int jentry;

	spin_lock_irqsave(&znd->gc_lock, flags);
	jstart = gc_entry->r_ptr;
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	if (!jstart)
		jstart++;

	MutexLock(&post->cached_lock);

	/* A discard may have puched holes in the postmap. re-sync lba */
	jentry = jstart;
	while (jentry <= post->jcount && (Z_LOWER48 ==
			le64_to_lba48(post->jdata[jentry].bval, NULL))) {
		jentry++;
	}
	/* nothing left to move */
	if (jentry > post->jcount)
		goto out_finished;

	/* skip over any discarded blocks */
	if (jstart != jentry)
		jstart = jentry;

	start_lba = le64_to_lba48(post->jdata[jentry].bval, NULL);
	post->jdata[jentry].bval = lba48_to_le64(GC_READ, start_lba);
	nblks = 1;
	jentry++;

	while (jentry <= post->jcount && (nblks+fill) < GC_MAX_STRIPE) {
		u64 dm_s = le64_to_lba48(post->jdata[jentry].tlba, NULL);
		u64 lba = le64_to_lba48(post->jdata[jentry].bval, NULL);

		if (Z_LOWER48 == dm_s || Z_LOWER48 == lba) {
			jentry++;
			continue;
		}

		post->jdata[jentry].bval = lba48_to_le64(GC_READ, lba);

		/* if the block is contiguous add it to the read */
		if (lba == (start_lba + nblks)) {
			nblks++;
		} else {
			if (nblks) {
				int err;

				err = append_blks(znd, start_lba,
						  &io_buf[fill], nblks);
				if (err) {
					rcode = err;
					goto out;
				}
				fill += nblks;
			}
			start_lba = lba;
			nblks = 1;
		}
		jentry++;
	}

	/* Issue a copy of 'nblks' blocks */
	if (nblks > 0) {
		int err;

		err = append_blks(znd, start_lba, &io_buf[fill], nblks);
		if (err) {
			rcode = err;
			goto out;
		}
		fill += nblks;
	}

out_finished:
	Z_DBG(znd, "Read %d blocks from %d", fill, gc_entry->r_ptr);

	spin_lock_irqsave(&znd->gc_lock, flags);
	gc_entry->nblks = fill;
	gc_entry->r_ptr = jentry;
	if (fill > 0)
		set_bit(DO_GC_WRITE, &gc_entry->gc_flags);
	else
		set_bit(DO_GC_COMPLETE, &gc_entry->gc_flags);

	spin_unlock_irqrestore(&znd->gc_lock, flags);

out:
	mutex_unlock(&post->cached_lock);

	return rcode;
}

/**
 * z_zone_gc_write() - Write (up to) a buffer worth of data to WP.
 * @gc_entry: Active GC state
 * @stream_id: Stream Id to prefer for allocation.
 */
static int z_zone_gc_write(struct gc_state *gc_entry, u32 stream_id)
{
	struct zdm *znd = gc_entry->znd;
	struct dm_target *ti = znd->ti;
	struct io_4k_block *io_buf = znd->gc_io_buf;
	struct map_cache *post = &znd->gc_postmap;
	unsigned long flags;
	u32 aq_flags = Z_AQ_GC | Z_AQ_STREAM_ID | stream_id;
	u64 lba;
	u32 nblks;
	u32 out = 0;
	int err = 0;
	int jentry;

	spin_lock_irqsave(&znd->gc_lock, flags);
	jentry = gc_entry->w_ptr;
	nblks = gc_entry->nblks;
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	if (!jentry)
		jentry++;

	MutexLock(&post->cached_lock);
	while (nblks > 0) {
		u32 nfound = 0;
		u32 added = 0;

		/*
		 * When lba is zero blocks were not allocated.
		 * Retry with the smaller request
		 */
		lba = z_acquire(znd, aq_flags, nblks, &nfound);
		if (!lba) {
			if (nfound) {
				u32 avail = nfound;

				nfound = 0;
				lba = z_acquire(znd, aq_flags, avail, &nfound);
			}
		}

		if (!lba) {
			err = -ENOSPC;
			goto out;
		}

		err = write_block(ti, DM_IO_VMA, &io_buf[out], lba, nfound, 0);
		if (err) {
			Z_ERR(znd, "Write %d blocks to %"PRIx64". ERROR: %d",
			      nfound, lba, err);
			goto out;
		}
		out += nfound;

		while ((jentry <= post->jcount) && (added < nfound)) {
			u16 rflg;
			u64 orig = le64_to_lba48(
					post->jdata[jentry].bval, &rflg);
			u64 dm_s = le64_to_lba48(
					post->jdata[jentry].tlba, NULL);

			if ((Z_LOWER48 == dm_s || Z_LOWER48 == orig)) {
				jentry++;

				if (rflg & GC_READ) {
					Z_ERR(znd, "ERROR: %" PRIx64
					      " read and not written %" PRIx64,
					      orig, dm_s);
					lba++;
					added++;
				}
				continue;
			}
			rflg &= ~GC_READ;
			post->jdata[jentry].bval = lba48_to_le64(rflg, lba);
			lba++;
			added++;
			jentry++;
		}
		nblks -= nfound;
	}
	Z_DBG(znd, "Write %d blocks from %d", gc_entry->nblks, gc_entry->w_ptr);
	set_bit(DO_GC_META, &gc_entry->gc_flags);

out:
	spin_lock_irqsave(&znd->gc_lock, flags);
	gc_entry->nblks = 0;
	gc_entry->w_ptr = jentry;
	spin_unlock_irqrestore(&znd->gc_lock, flags);
	mutex_unlock(&post->cached_lock);

	return err;
}

/**
 * gc_finalize() - Final sanity check on GC'd block map.
 * @gc_entry: Active GC state
 *
 * gc_postmap is expected to be empty (all blocks original
 * scheduled to be moved to a new zone have been accounted for...
 */
static int gc_finalize(struct gc_state *gc_entry)
{
	int err = 0;
	struct zdm *znd = gc_entry->znd;
	struct map_cache *post = &znd->gc_postmap;
	int jentry;

	MutexLock(&post->cached_lock);
	for (jentry = post->jcount; jentry > 0; jentry--) {
		u64 dm_s = le64_to_lba48(post->jdata[jentry].tlba, NULL);
		u64 lba = le64_to_lba48(post->jdata[jentry].bval, NULL);

		if (dm_s != Z_LOWER48 || lba != Z_LOWER48) {
			Z_ERR(znd, "GC: Failed to move %" PRIx64
				   " from %"PRIx64" [%d]",
			      dm_s, lba, jentry);
			err = -EIO;
		}
	}
	mutex_unlock(&post->cached_lock);
	post->jcount = jentry;
	post->jsorted = 0;

	return err;
}

/**
 * clear_gc_target_flag() - Clear any zone tagged as a GC target.
 * @znd: ZDM Instance
 *
 * FIXME: Can we reduce the weight of this ?
 *	Ex. execute as zones are closed and specify the zone to clear
 *	    at GC completion/cleanup.
 */
static void clear_gc_target_flag(struct zdm *znd)
{
	int z_id;

	for (z_id = 0; z_id < znd->data_zones; z_id++) {
		u32 gzno  = z_id >> GZ_BITS;
		u32 gzoff = z_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp;

		SpinLock(&wpg->wplck);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		if (wp & Z_WP_GC_TARGET) {
			wp &= ~Z_WP_GC_TARGET;
			wpg->wp_alloc[gzoff] = cpu_to_le32(wp);
		}
		set_bit(IS_DIRTY, &wpg->flags);
		clear_bit(IS_FLUSH, &wpg->flags);
		spin_unlock(&wpg->wplck);
	}
}

/**
 * z_zone_gc_metadata_update() - Update ZLT as needed.
 * @gc_entry: Active GC state
 *
 * Dispose or account for all blocks originally scheduled to be
 * moved. Update ZLT (via map cache) for all moved blocks.
 */
static int z_zone_gc_metadata_update(struct gc_state *gc_entry)
{
	struct zdm *znd = gc_entry->znd;
	struct map_cache *post = &znd->gc_postmap;
	u32 used = post->jcount;
	int err = 0;
	int jentry;

	for (jentry = post->jcount; jentry > 0; jentry--) {
		int discard = 0;
		int mapping = 0;
		struct map_pg *mapped = NULL;
		u64 dm_s = le64_to_lba48(post->jdata[jentry].tlba, NULL);
		u64 lba = le64_to_lba48(post->jdata[jentry].bval, NULL);
		struct mpinfo mpi;

		if ((znd->s_base <= dm_s) && (dm_s < znd->md_end)) {
			if (to_table_entry(znd, dm_s, &mpi) >= 0)
				mapped = get_htbl_entry(znd, &mpi);
			mapping = 1;
		}

		if (mapping && !mapped)
			Z_ERR(znd, "MD: dm_s: %" PRIx64 " -> lba: %" PRIx64
				   " no mapping in ram.", dm_s, lba);

		if (mapped) {
			u32 in_z;

			ref_pg(mapped);
			MutexLock(&mapped->md_lock);
			in_z = _calc_zone(znd, mapped->last_write);
			if (in_z != gc_entry->z_gc) {
				Z_ERR(znd, "MD: %" PRIx64
				      " Discarded - %" PRIx64
				      " already flown to: %x",
				      dm_s, mapped->last_write, in_z);
				discard = 1;
			} else if (mapped->data.addr &&
				   test_bit(IS_DIRTY, &mapped->flags)) {
				Z_ERR(znd,
				      "MD: %" PRIx64 " Discarded - %"PRIx64
				      " is in-flight",
				      dm_s, mapped->last_write);
				discard = 2;
			}
			if (!discard)
				mapped->last_write = lba;
			mutex_unlock(&mapped->md_lock);
			deref_pg(mapped);
		}

		MutexLock(&post->cached_lock);
		if (discard == 1) {
			Z_ERR(znd, "Dropped: %" PRIx64 " ->  %"PRIx64,
			      le64_to_cpu(post->jdata[jentry].tlba),
			      le64_to_cpu(post->jdata[jentry].bval));

			post->jdata[jentry].tlba = MC_INVALID;
			post->jdata[jentry].bval = MC_INVALID;
		}
		if (post->jdata[jentry].tlba  == MC_INVALID &&
		    post->jdata[jentry].bval == MC_INVALID) {
			used--;
		} else if (lba) {
			increment_used_blks(znd, lba, 1);
		}
		mutex_unlock(&post->cached_lock);
	}
	err = move_to_map_tables(znd, post);
	if (err)
		Z_ERR(znd, "Move to tables post GC failure");

	clear_gc_target_flag(znd);

	return err;
}

/**
 * _blkalloc() - Attempt to reserve blocks at z_at in ZDM znd
 * @znd:	 ZDM instance.
 * @z_at:	 Zone to write data to
 * @flags:	 Acquisition type.
 * @nblks:	 Number of blocks desired.
 * @nfound:	 Number of blocks allocated or available.
 *
 * Attempt allocation of @nblks within fron the current WP of z_at
 * When nblks are not available 0 is returned and @nfound is the
 * contains the number of blocks *available* but not *allocated*.
 * When nblks are available the starting LBA in 4k space is returned and
 * nblks are allocated *allocated* and *nfound is the number of blocks
 * remaining in zone z_at from the LBA returned.
 *
 * Return: LBA if request is met, otherwise 0. nfound will contain the
 *	 available blocks remaining.
 */
static sector_t _blkalloc(struct zdm *znd, u32 z_at, u32 flags,
			  u32 nblks, u32 *nfound)
{
#define ALLOC_STICKY (Z_WP_GC_TARGET|Z_WP_NON_SEQ|Z_WP_RRECALC)

	sector_t found = 0;
	u32 avail = 0;
	int do_open_zone = 0;
	u32 gzno  = z_at >> GZ_BITS;
	u32 gzoff = z_at & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];
	u32 wp;
	u32 wptr;
	u32 gc_tflg;

	if (gzno >= znd->gz_count || z_at >= znd->data_zones) {
		Z_ERR(znd, "Invalid zone for allocation: %u", z_at);
		dump_stack();
		return 0ul;
	}

	SpinLock(&wpg->wplck);
	wp      = le32_to_cpu(wpg->wp_alloc[gzoff]);
	gc_tflg = wp & ALLOC_STICKY;
	wptr    = wp & ~ALLOC_STICKY;
	if (wptr < Z_BLKSZ)
		avail = Z_BLKSZ - wptr;

#if 0 /* DEBUG START: Testing zm_write_pages() */
	if (avail > 7)
		avail = 7;
#endif /* DEBUG END: Testing zm_write_pages() */

	*nfound = avail;
	if (nblks <= avail) {
		u64 lba = ((u64)z_at << Z_BLKBITS) + znd->data_lba;
		u32 zf_est = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;

		found = lba + wptr;
		*nfound = nblks;
		if (wptr == 0)
			do_open_zone = 1;

		wptr += nblks;
		zf_est -= nblks;
		if (wptr == Z_BLKSZ)
			znd->discard_count += zf_est;

		wptr |= gc_tflg;
		if (flags & Z_AQ_GC)
			wptr |= Z_WP_GC_TARGET;

		if (flags & Z_AQ_STREAM_ID)
			zf_est |= (flags & Z_AQ_STREAM_MASK) << 24;
		else
			zf_est |= le32_to_cpu(wpg->zf_est[gzoff])
				& Z_WP_STREAM_MASK;

		wpg->wp_alloc[gzoff] = cpu_to_le32(wptr);
		wpg->zf_est[gzoff] = cpu_to_le32(zf_est);
		set_bit(IS_DIRTY, &wpg->flags);
		clear_bit(IS_FLUSH, &wpg->flags);
	}
	spin_unlock(&wpg->wplck);

	if (do_open_zone)
		dmz_open_zone(znd, z_at);

	return found;
}

/**
 * update_stale_ratio() - Update the stale ratio for the finished bin.
 * @znd: ZDM instance
 * @zone: Zone that needs update.
 */
static void update_stale_ratio(struct zdm *znd, u32 zone)
{
	u64 total_stale = 0;
	u64 free_zones = 1;
	u32 bin = zone / znd->stale.binsz;
	u32 z_id = bin * znd->stale.binsz;
	u32 s_end = z_id + znd->stale.binsz;

	if (s_end > znd->data_zones)
		s_end = znd->data_zones;

	for (; z_id < s_end; z_id++) {
		u32 gzno  = z_id >> GZ_BITS;
		u32 gzoff = z_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 stale = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
		u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_VALUE_MASK;
		u32 wflg = le32_to_cpu(wpg->wp_alloc[gzoff]);

		if (wflg & Z_WP_RRECALC) {
			SpinLock(&wpg->wplck);
			wflg = le32_to_cpu(wpg->wp_alloc[gzoff])
			     & ~Z_WP_RRECALC;
			wpg->wp_alloc[gzoff] = cpu_to_le32(wflg);
			spin_unlock(&wpg->wplck);
		}

		if (wp == Z_BLKSZ)
			total_stale += stale;
		else
			free_zones++;
	}

	total_stale /= free_zones;
	znd->stale.bins[bin] = (total_stale > ~0u) ? ~0u : total_stale;
}

/**
 * update_all_stale_ratio() - Update the stale ratio for all bins.
 * @znd: ZDM instance
 */
static void update_all_stale_ratio(struct zdm *znd)
{
	u32 iter;

	for (iter = 0; iter < znd->stale.count; iter += znd->stale.binsz)
		update_stale_ratio(znd, iter);
}

/**
 * z_zone_compact_queue() - Queue zone compaction.
 * @znd: ZDM instance
 * @z_gc: Zone to queue.
 * @delay: Delay queue metric
 * @gfp: Allocation scheme.
 *
 * Return: 1 on success, 0 if not queued/busy, negative on error.
 */
static
int z_zone_compact_queue(struct zdm *znd, u32 z_gc, int delay, gfp_t gfp)
{
	unsigned long flags;
	int do_queue = 0;
	int err = 0;
	struct gc_state *gc_entry;

	gc_entry = ZDM_ALLOC(znd, sizeof(*gc_entry), KM_16, gfp);
	if (!gc_entry) {
		Z_ERR(znd, "No Memory for compact!!");
		return -ENOMEM;
	}
	gc_entry->znd = znd;
	gc_entry->z_gc = z_gc;
	set_bit(DO_GC_NEW, &gc_entry->gc_flags);
	znd->gc_backlog++;

	spin_lock_irqsave(&znd->gc_lock, flags);
	if (!znd->gc_active) {
		znd->gc_active = gc_entry;
		do_queue = 1;
	} else {
		Z_ERR(znd, "GC: Tried to queue but already active");
	}
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	if (do_queue) {
		unsigned long tval = msecs_to_jiffies(delay);

		if (queue_delayed_work(znd->gc_wq, &znd->gc_work, tval))
			err = 1;
	} else {
		ZDM_FREE(znd, gc_entry, sizeof(*gc_entry), KM_16);
		znd->gc_backlog--;
		Z_ERR(znd, "GC: FAILED to queue work");
	}

	return err;
}

/**
 * zone_zfest() - Queue zone compaction.
 * @znd: ZDM instance
 * @z_id: Zone to queue.
 */
static u32 zone_zfest(struct zdm *znd, u32 z_id)
{
	u32 gzno  = z_id >> GZ_BITS;
	u32 gzoff = z_id & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];

	return le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
}

/**
 * gc_request_queued() - Called periodically to initiate GC
 *
 * @znd: ZDM instance
 * @bin: Bin with stale zones to scan for GC
 * @delay: Metric for delay queuing.
 * @gfp: Default memory allocation scheme.
 *
 */
static int gc_request_queued(struct zdm *znd, int bin, int delay, gfp_t gfp)
{
	unsigned long flags;
	int queued = 0;
	u32 top_roi = NOZONE;
	u32 stale = 0;
	u32 z_gc = bin * znd->stale.binsz;
	u32 s_end = z_gc + znd->stale.binsz;

	if (znd->meta_result)
		goto out;

	if (test_bit(ZF_FREEZE, &znd->flags)) {
		Z_ERR(znd, "Is frozen -- GC paused.");
		goto out;
	}

	spin_lock_irqsave(&znd->gc_lock, flags);
	if (znd->gc_active)
		queued = 1;
	spin_unlock_irqrestore(&znd->gc_lock, flags);
	if (queued)
		goto out;

	if (s_end > znd->data_zones)
		s_end = znd->data_zones;

	/* scan for most stale zone in STREAM [top_roi] */
	for (; z_gc < s_end; z_gc++) {
		u32 gzno  = z_gc >> GZ_BITS;
		u32 gzoff = z_gc & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp_v = le32_to_cpu(wpg->wp_alloc[gzoff]);
		u32 nfree = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
		u32 wp_f = wp_v & Z_WP_FLAGS_MASK;

		wp_v &= Z_WP_VALUE_MASK;
		if (wp_v == 0)
			continue;
		if ((wp_f & Z_WP_GC_PENDING) != 0)
			continue;

		if (wp_v == Z_BLKSZ) {
			stale += nfree;
			if ((wp_f & Z_WP_GC_BITS) == Z_WP_GC_READY) {
				if (top_roi == NOZONE)
					top_roi = z_gc;
				else if (nfree > zone_zfest(znd, top_roi))
					top_roi = z_gc;
			}
		}
	}

	if (!delay && top_roi == NOZONE)
		Z_ERR(znd, "No GC candidate in bin: %u -> %u", z_gc, s_end);

	/* determine the cut-off for GC based on MZ overall staleness */
	if (top_roi != NOZONE) {
		int rc;
		u32 state_metric = GC_PRIO_DEFAULT;
		u32 n_empty = znd->z_gc_free;
		int pctfree = n_empty * 100 / znd->data_zones;

		/*
		 * -> at less than 5 zones free switch to critical
		 * -> at less than 5% zones free switch to HIGH
		 * -> at less than 25% free switch to LOW
		 * -> high level is 'cherry picking' near empty zones
		 */
		if (znd->z_gc_free < 7)
			state_metric = GC_PRIO_CRIT;
		else if (pctfree < 5)
			state_metric = GC_PRIO_HIGH;
		else if (pctfree < 25)
			state_metric = GC_PRIO_LOW;

		if (zone_zfest(znd, top_roi) > state_metric) {
			rc = z_zone_compact_queue(znd, top_roi, delay * 5, gfp);
			if (rc == 1)
				queued = 1;
			else if (rc < 0)
				Z_ERR(znd, "GC: Z#%u !Q: ERR: %d", top_roi, rc);
		}

		if (!delay && !queued) {
			rc = z_zone_compact_queue(znd, top_roi, delay * 5, gfp);
			if (rc == 1)
				queued = 1;
		}

		if (!delay && !queued)
			Z_ERR(znd, "GC: Z#%u !Q .. M: %u E: %u PCT: %d ZF: %u",
				   top_roi, state_metric, n_empty, pctfree,
				   zone_zfest(znd, top_roi));
	}
out:
	return queued;
}

/**
 * z_zone_gc_compact() - Primary compaction worker.
 * @gc_entry: GC State
 */
static int z_zone_gc_compact(struct gc_state *gc_entry)
{
	unsigned long flags;
	int err = 0;
	struct zdm *znd = gc_entry->znd;
	u32 z_gc = gc_entry->z_gc;
	u32 gzno  = z_gc >> GZ_BITS;
	u32 gzoff = z_gc & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];

	znd->age = jiffies_64;

	if (test_bit(DO_GC_NEW, &gc_entry->gc_flags)) {
		err = z_flush_bdev(znd, GFP_KERNEL);
		if (err) {
			gc_entry->result = err;
			goto out;
		}
	}

	/* If a SYNC is in progress and we can delay then postpone*/
	if (mutex_is_locked(&znd->mz_io_mutex) &&
	    atomic_read(&znd->gc_throttle) == 0)
		return -EAGAIN;

	if (test_and_clear_bit(DO_GC_NEW, &gc_entry->gc_flags)) {
		u32 wp;

		SpinLock(&wpg->wplck);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		wp |= Z_WP_GC_FULL;
		wpg->wp_alloc[gzoff] = cpu_to_le32(wp);
		set_bit(IS_DIRTY, &wpg->flags);
		clear_bit(IS_FLUSH, &wpg->flags);
		spin_unlock(&wpg->wplck);

		err = _cached_to_tables(znd, gc_entry->z_gc);
		if (err) {
			Z_ERR(znd, "Failed to purge journal: %d", err);
			gc_entry->result = err;
			goto out;
		}

		if (znd->gc_postmap.jcount > 0) {
			Z_ERR(znd, "*** Unexpected data in postmap!!");
			znd->gc_postmap.jcount = 0;
		}

		err = z_zone_gc_metadata_to_ram(gc_entry);
		if (err) {
			Z_ERR(znd,
			      "Pre-load metadata to memory failed!! %d", err);
			gc_entry->result = err;
			goto out;
		}

		do {
			err = memcache_lock_and_sort(znd, &znd->gc_postmap);
		} while (err == -EBUSY);

		set_bit(DO_GC_PREPARE, &gc_entry->gc_flags);

		if (znd->gc_throttle.counter == 0)
			return -EAGAIN;
	}

next_in_queue:
	znd->age = jiffies_64;

	if (test_and_clear_bit(DO_GC_PREPARE, &gc_entry->gc_flags)) {

		MutexLock(&znd->mz_io_mutex);
		mutex_unlock(&znd->mz_io_mutex);

		err = z_zone_gc_read(gc_entry);
		if (err < 0) {
			Z_ERR(znd, "z_zone_gc_chunk issue failure: %d", err);
			gc_entry->result = err;
			goto out;
		}
		if (znd->gc_throttle.counter == 0)
			return -EAGAIN;
	}

	if (test_and_clear_bit(DO_GC_WRITE, &gc_entry->gc_flags)) {
		u32 sid = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_STREAM_MASK;

		MutexLock(&znd->mz_io_mutex);
		mutex_unlock(&znd->mz_io_mutex);

		err = z_zone_gc_write(gc_entry, sid >> 24);
		if (err) {
			Z_ERR(znd, "z_zone_gc_write issue failure: %d", err);
			gc_entry->result = err;
			goto out;
		}
		if (znd->gc_throttle.counter == 0)
			return -EAGAIN;
	}

	if (test_and_clear_bit(DO_GC_META, &gc_entry->gc_flags)) {
		z_do_copy_more(gc_entry);
		goto next_in_queue;
	}

	znd->age = jiffies_64;
	if (test_and_clear_bit(DO_GC_COMPLETE, &gc_entry->gc_flags)) {
		u32 non_seq;
		u32 reclaimed;

		err = z_zone_gc_metadata_update(gc_entry);
		gc_entry->result = err;
		if (err) {
			Z_ERR(znd, "Metadata error ... disable zone: %u",
			      gc_entry->z_gc);
		}
		err = gc_finalize(gc_entry);
		if (err) {
			Z_ERR(znd, "GC: Failed to finalize: %d", err);
			gc_entry->result = err;
			goto out;
		}

		gc_verify_cache(znd, gc_entry->z_gc);

		err = _cached_to_tables(znd, gc_entry->z_gc);
		if (err) {
			Z_ERR(znd, "Failed to purge journal: %d", err);
			gc_entry->result = err;
			goto out;
		}

		err = z_flush_bdev(znd, GFP_KERNEL);
		if (err) {
			gc_entry->result = err;
			goto out;
		}

		/* Release the zones for writing */
		dmz_reset_wp(znd, gc_entry->z_gc);

		SpinLock(&wpg->wplck);
		non_seq = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_NON_SEQ;
		reclaimed = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
		wpg->wp_alloc[gzoff] = cpu_to_le32(non_seq);
		wpg->wp_used[gzoff] = cpu_to_le32(0u);
		wpg->zf_est[gzoff] = cpu_to_le32(Z_BLKSZ);
		znd->discard_count -= reclaimed;
		znd->z_gc_free++;
		if (znd->z_gc_resv & Z_WP_GC_ACTIVE)
			znd->z_gc_resv = gc_entry->z_gc;
		else if (znd->z_meta_resv & Z_WP_GC_ACTIVE)
			znd->z_meta_resv = gc_entry->z_gc;
		set_bit(IS_DIRTY, &wpg->flags);
		clear_bit(IS_FLUSH, &wpg->flags);
		spin_unlock(&wpg->wplck);

		znd->gc_events++;
		update_stale_ratio(znd, gc_entry->z_gc);
		spin_lock_irqsave(&znd->gc_lock, flags);
		znd->gc_backlog--;
		znd->gc_active = NULL;
		spin_unlock_irqrestore(&znd->gc_lock, flags);

		ZDM_FREE(znd, gc_entry, sizeof(*gc_entry), KM_16);

		set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		set_bit(DO_MEMPOOL, &znd->flags);
		set_bit(DO_SYNC, &znd->flags);
	}
out:
	return 0;
}

/**
 * gc_work_task() - Worker thread for GC activity.
 * @work: Work struct holding the ZDM instance to do work on ...
 */
static void gc_work_task(struct work_struct *work)
{
	struct gc_state *gc_entry = NULL;
	unsigned long flags;
	struct zdm *znd;
	int err;

	if (!work)
		return;

	znd = container_of(to_delayed_work(work), struct zdm, gc_work);
	if (!znd)
		return;

	spin_lock_irqsave(&znd->gc_lock, flags);
	if (znd->gc_active)
		gc_entry = znd->gc_active;
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	if (!gc_entry) {
		Z_ERR(znd, "ERROR: gc_active not set!");
		return;
	}

	err = z_zone_gc_compact(gc_entry);
	if (-EAGAIN == err) {
		unsigned long tval = msecs_to_jiffies(10);

		queue_delayed_work(znd->gc_wq, &znd->gc_work, tval);
	} else {
		const int delay = 10;

		on_timeout_activity(znd, delay);
	}
}

/**
 * is_reserved() - Check to see if a zone is 'special'
 * @znd: ZDM Instance
 * @z_pref: Zone to be tested.
 */
static inline int is_reserved(struct zdm *znd, const u32 z_pref)
{
	const u32 gc   = znd->z_gc_resv   & Z_WP_VALUE_MASK;
	const u32 meta = znd->z_meta_resv & Z_WP_VALUE_MASK;

	return (gc == z_pref || meta == z_pref) ? 1 : 0;
}

/**
 * gc_can_cherrypick() - Queue a GC for zone in this bin if ... it will be easy
 * @znd: ZDM Instance
 * @bin: The bin (0 to 255)
 * @delay: Delay metric
 * @gfp: Allocation flags to use.
 */
static int gc_can_cherrypick(struct zdm *znd, u32 bin, int delay, gfp_t gfp)
{
	u32 z_id = bin * znd->stale.binsz;
	u32 s_end = z_id + znd->stale.binsz;

	if (s_end > znd->data_zones)
		s_end = znd->data_zones;

	for (; z_id < s_end; z_id++) {
		u32 gzno  = z_id >> GZ_BITS;
		u32 gzoff = z_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		u32 nfree = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;

		if (wp & Z_WP_RRECALC)
			update_stale_ratio(znd, z_id);

		if (((wp & Z_WP_GC_BITS) == Z_WP_GC_READY) &&
		    ((wp & Z_WP_VALUE_MASK) == Z_BLKSZ) &&
		    (nfree == Z_BLKSZ)) {
			if (z_zone_compact_queue(znd, z_id, delay, gfp))
				return 1;
		}
	}

	return 0;
}

/**
 * gc_queue_with_delay() - Scan to see if a GC can/should be queued.
 * @znd: ZDM Instance
 * @delay: Delay metric
 * @gfp: Allocation flags to use.
 *
 * Return 1 if gc in progress or queued. 0 otherwise.
 */
static int gc_queue_with_delay(struct zdm *znd, int delay, gfp_t gfp)
{
	int gc_idle = 0;
	unsigned long flags;

	spin_lock_irqsave(&znd->gc_lock, flags);
	gc_idle = !znd->gc_active;
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	if (gc_idle) {
		int bin = 0;
		int ratio = 0;
		u32 iter;

		/* Find highest ratio stream */
		for (iter = 0; iter < znd->stale.count; iter++)
			if (znd->stale.bins[iter] > ratio)
				ratio = znd->stale.bins[iter], bin = iter;

		/* Cherrypick a zone in the stream */
		if (gc_idle && gc_can_cherrypick(znd, bin, delay, gfp))
			gc_idle = 0;

		/* Otherwise cherrypick *something* */
		for (iter = 0; gc_idle && (iter < znd->stale.count); iter++)
			if (gc_idle && (bin != iter) &&
			    gc_can_cherrypick(znd, iter, delay, gfp))
				gc_idle = 0;

		/* Otherwise compact a zone in the stream */
		if (gc_idle && gc_request_queued(znd, bin, delay, gfp))
			gc_idle = 0;

		/* Otherwise compact *something* */
		for (iter = 0; gc_idle && (iter < znd->stale.count); iter++)
			if (gc_idle && gc_request_queued(znd, iter, delay, gfp))
				gc_idle = 0;
	}
	return !gc_idle;
}

/**
 * gc_immediate() - Free up some space as soon as possible.
 * @znd: ZDM Instance
 * @gfp: Allocation flags to use.
 */
static int gc_immediate(struct zdm *znd, int wait, gfp_t gfp)
{
	const int delay = 0;
	int can_retry = 0;
	int lock_owner = 0;
	int queued;

	/*
	 * Lock on the first entry. On subsequent entries
	 * only lock of a 'wait' was requested.
	 *
	 * Note: We really should be using a wait queue here but this
	 *       mutex hack should do as a temporary workaround.
	 */
	atomic_inc(&znd->gc_throttle);
	if (atomic_read(&znd->gc_throttle) == 1) {
		mutex_lock(&znd->gc_wait);
		lock_owner = 1;
	} else if (atomic_read(&znd->gc_throttle) > 1) {
		if (wait) {
			mutex_lock(&znd->gc_wait);
			can_retry = 1;
			mutex_unlock(&znd->gc_wait);
		}
		goto out;
	}
	flush_delayed_work(&znd->gc_work);
	queued = gc_queue_with_delay(znd, delay, gfp);
	if (!queued) {
		Z_ERR(znd, " ... GC immediate .. failed to queue GC!!.");
		z_discard_partial(znd, DISCARD_MAX_INGRESS, GFP_KERNEL);
		can_retry = 1;
		goto out;
	}
	can_retry = flush_delayed_work(&znd->gc_work);

out:
	if (lock_owner)
		mutex_unlock(&znd->gc_wait);
	atomic_dec(&znd->gc_throttle);

	return can_retry;
}

/**
 * set_current() - Make zone the preferred zone for allocation.
 * @znd: ZDM Instance
 * @flags: BLock allocation scheme (including stream id)
 * @zone: The zone to make preferred.
 *
 * Once a zone is opened for allocation, future allocations will prefer
 * the same zone, until the zone is full.
 * Each stream id has it's own preferred zone.
 *
 * NOTE: z_current is being deprecated if favor of assuming a default
 *       stream id when nothing is provided.
 */
static inline void set_current(struct zdm *znd, u32 flags, u32 zone)
{
	if (flags & Z_AQ_STREAM_ID) {
		u32 stream_id = flags & Z_AQ_STREAM_MASK;

		znd->bmkeys->stream[stream_id] = cpu_to_le32(zone);
	}
	znd->z_current = zone;
	if (znd->z_gc_free > 0)
		znd->z_gc_free--;
	else
		Z_ERR(znd, "Dec z_gc_free below 0?");
}

/**
 * next_open_zone() - Grab the next available zone
 * @znd: ZDM Instance
 * @z_at: Zone to start scanning from (presumable just filled).
 *
 * Return: NOZONE if no zone exists with space for writing.
 *
 * Scan through the available zones for an empty zone.
 * If no empty zone is available the a zone that is not full is
 * used instead.
 */
static u32 next_open_zone(struct zdm *znd, u32 z_at)
{
	u32 zone = NOZONE;
	u32 z_id;

	if (znd->data_zones < z_at)
		z_at = znd->data_zones;

	/* scan higher lba zones */
	for (z_id = z_at; z_id < znd->data_zones; z_id++) {
		u32 gzno  = z_id >> GZ_BITS;
		u32 gzoff = z_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]);

		if ((wp & Z_WP_VALUE_MASK) == 0) {
			u32 check = gzno << GZ_BITS | gzoff;

			if (!is_reserved(znd, check)) {
				zone = check;
				goto out;
			}
		}
	}

	/* scan lower lba zones */
	for (z_id = 0; z_id < z_at; z_id++) {
		u32 gzno  = z_id >> GZ_BITS;
		u32 gzoff = z_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]);

		if ((wp & Z_WP_VALUE_MASK) == 0) {
			u32 check = gzno << GZ_BITS | gzoff;

			if (!is_reserved(znd, check)) {
				zone = check;
				goto out;
			}
		}
	}

	/* No empty zones .. start co-mingling streams */
	for (z_id = 0; z_id < znd->data_zones; z_id++) {
		u32 gzno  = z_id >> GZ_BITS;
		u32 gzoff = z_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]);

		if ((wp & Z_WP_VALUE_MASK) < Z_BLKSZ) {
			u32 check = gzno << GZ_BITS | gzoff;

			if (!is_reserved(znd, check)) {
				zone = check;
				goto out;
			}
		}
	}

out:
	return zone;
}

/**
 * zone_filled_cleanup() - Update wp_alloc GC Readu flags based on wp_used.
 * @znd: ZDM Instance
 */
static void zone_filled_cleanup(struct zdm *znd)
{
	if (znd->filled_zone != NOZONE) {
		u32 zone = znd->filled_zone;
		u32 gzno;
		u32 gzoff;
		u32 wp;
		u32 used;
		struct meta_pg *wpg;

		znd->filled_zone = NOZONE;

		gzno = zone >> GZ_BITS;
		gzoff = zone & GZ_MMSK;
		wpg = &znd->wp[gzno];

		SpinLock(&wpg->wplck);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		used = le32_to_cpu(wpg->wp_used[gzoff]) & Z_WP_VALUE_MASK;
		if (used == Z_BLKSZ) {
			if (Z_BLKSZ == (wp & Z_WP_VALUE_MASK)) {
				wpg->wp_alloc[gzoff] = cpu_to_le32(wp
						     | Z_WP_GC_READY);
				set_bit(IS_DIRTY, &wpg->flags);
				clear_bit(IS_FLUSH, &wpg->flags);
			} else {
				Z_ERR(znd, "Zone %u seems bogus.", zone);
			}
		}
		spin_unlock(&wpg->wplck);

		dmz_close_zone(znd, zone);
		update_stale_ratio(znd, zone);
	}
}

/**
 * z_acquire() - Allocate blocks for writing
 * @znd: ZDM Instance
 * @flags: Alloc strategy and stream id.
 * @nblks: Number of blocks desired.
 * @nfound: Number of blocks available.
 *
 * Return: Lba for writing.
 */
static u64 z_acquire(struct zdm *znd, u32 flags, u32 nblks, u32 *nfound)
{
	sector_t found = 0;
	u32 z_pref = znd->z_current;
	u32 stream_id = 0;
	u32 z_find;
	const int wait = 1;
	gfp_t gfp = (flags & Z_AQ_NORMAL) ? GFP_ATOMIC : GFP_KERNEL;

	zone_filled_cleanup(znd);

	if (flags & Z_AQ_STREAM_ID) {
		stream_id = flags & Z_AQ_STREAM_MASK;
		z_pref = le32_to_cpu(znd->bmkeys->stream[stream_id]);
	}
	if (z_pref >= znd->data_zones) {
		z_pref = next_open_zone(znd, znd->z_current);
		if (z_pref < znd->data_zones)
			set_current(znd, flags, z_pref);
	}

	if (z_pref < znd->data_zones) {
		found = _blkalloc(znd, z_pref, flags, nblks, nfound);
		if (found || *nfound)
			goto out;
	}

	if (znd->z_gc_free < 5) {
		Z_DBG(znd, "... alloc - gc low on free space.");
		gc_immediate(znd, !wait, gfp);
	}

retry:
	z_find = next_open_zone(znd, znd->z_current);
	if (z_find < znd->data_zones) {
		found = _blkalloc(znd, z_find, flags, nblks, nfound);
		if (found || *nfound) {
			set_current(znd, flags, z_find);
			goto out;
		}
	}

	if (flags & Z_AQ_GC) {
		u32 gresv = znd->z_gc_resv & Z_WP_VALUE_MASK;

		Z_ERR(znd, "Using GC Reserve (%u)", gresv);
		found = _blkalloc(znd, gresv, flags, nblks, nfound);
		znd->z_gc_resv |= Z_WP_GC_ACTIVE;
	}

	if (flags & Z_AQ_META) {
		int can_retry = gc_immediate(znd, wait, gfp);
		u32 mresv = znd->z_meta_resv & Z_WP_VALUE_MASK;

		Z_DBG(znd, "GC: Need META.");
		if (can_retry)
			goto retry;

		Z_ERR(znd, "Using META Reserve (%u)", znd->z_meta_resv);
		found = _blkalloc(znd, mresv, flags, nblks, nfound);
	}

out:
	if (!found && (*nfound == 0)) {
		Z_DBG(znd, "... alloc - out of space?");
		if (gc_immediate(znd, wait, gfp))
			goto retry;

		Z_ERR(znd, "%s: -> Out of space.", __func__);
	}
	return found;
}

/**
 * cmp_current_lba() - Compare map page on lba.
 * @x1: map page
 * @x2: map page
 *
 * Return -1, 0, or 1 if x1 < x2, equal, or >, respectivly.
 */
static int cmp_current_lba(const void *x1, const void *x2)
{
	const struct map_pg *v1 = *(const struct map_pg **)x1;
	const struct map_pg *v2 = *(const struct map_pg **)x2;
	int cmp = (v1->last_write < v2->last_write) ? -1
	        : ((v1->last_write > v2->last_write) ? 1 : 0);

	return cmp;
}

/**
 * compare_lba() - Compare map page on lba.
 * @x1: map page
 * @x2: map page
 *
 * Return -1, 0, or 1 if x1 < x2, equal, or >, respectivly.
 */
static int compare_lba(const void *x1, const void *x2)
{
	const struct map_pg *v1 = *(const struct map_pg **)x1;
	const struct map_pg *v2 = *(const struct map_pg **)x2;
	int cmp = (v1->lba < v2->lba) ? -1 : ((v1->lba > v2->lba) ? 1 : 0);

	return cmp;
}

/**
 * is_dirty() - Test map page if is of bit_type and dirty.
 * @expg: map page
 * @bit_type: map page flag to test for...
 *
 * Return 1 if page had dirty and bit_type flags set.
 *
 * Note: bit_type is IS_CRC and IS_LUT most typically.
 */
static __always_inline int is_dirty(struct map_pg *expg, int bit_type)
{
	return (test_bit(bit_type, &expg->flags) &&
		test_bit(IS_DIRTY, &expg->flags));
}


/**
 * is_old_and_clean() - Test map page if it expired and can be dropped.
 * @expg: map page
 * @bit_type: map page flag to test for...
 *
 * Return 1 if page is clean and old.
 */
static __always_inline int is_old_and_clean(struct map_pg *expg, int bit_type)
{
	int is_match = 0;

	if (!test_bit(IS_DIRTY, &expg->flags) && is_expired(expg->age)) {
		if (getref_pg(expg) == 1)
			is_match = 1;
		else
			pr_err("%"PRIu64": dirty, exp, and elev: %d\n",
				expg->lba, getref_pg(expg));
	}

	(void)bit_type;

	return is_match;
}

/**
 * _pool_write() - Sort/Write and array of ZLT pages.
 * @znd: ZDM Instance
 * @wset: Array of pages to be written.
 * @count: Number of entries.
 *
 *  NOTE: On entry all map_pg entries have elevated refcount from _pool_fill().
 *	write_if_dirty() will dec the refcount when the block hits disk.
 */
static int _pool_write(struct zdm *znd, struct map_pg **wset, int count)
{
	const int use_wq = 0;
	int iter;
	struct map_pg *expg;
	int err = 0;

	/* write dirty table pages */
	if (count <= 0)
		goto out;

	sort(wset, count, sizeof(*wset), compare_lba, NULL);

	for (iter = 0; iter < count; iter++) {
		expg = wset[iter];
		if (expg) {
			if (iter && expg->lba == wset[iter-1]->lba)
				wset[iter] = NULL;
			else
				cache_if_dirty(znd, expg, use_wq);
		}
	}

	for (iter = 0; iter < count; iter++) {
		const int sync = 1;
		/* REDUX: For Async WB:  int sync = (iter == last) ? 1 : 0; */

		expg = wset[iter];
		if (expg) {
			err = write_if_dirty(znd, expg, use_wq, sync);
			if (err) {
				Z_ERR(znd, "Write failed: %d", err);
				goto out;
			}
		}
	}
	err = count;

out:
	return err;
}

/**
 * _pool_read() - Sort/Read an array of ZLT pages.
 * @znd: ZDM Instance
 * @wset: Array of pages to be written.
 * @count: Number of entries.
 *
 *  NOTE: On entry all map_pg entries have elevated refcount from _pool_fill().
 *	write_if_dirty() will dec the refcount when the block hits disk.
 */
static int _pool_read(struct zdm *znd, struct map_pg **wset, int count)
{
	int iter;
	struct map_pg *expg;
	int err = 0;

	/* write dirty table pages */
	if (count <= 0)
		goto out;

	sort(wset, count, sizeof(*wset), cmp_current_lba, NULL);

	for (iter = 0; iter < count; iter++) {
		expg = wset[iter];
		if (expg && test_and_clear_bit(WB_RE_CACHE, &expg->flags)) {
			if (!expg->data.addr) {
				gfp_t gfp = GFP_KERNEL;
				struct mpinfo mpi;
				int rc;

				to_table_entry(znd, expg->lba, &mpi);
				rc = cache_pg(znd, expg, gfp, &mpi);
				if (rc < 0 && rc != -EBUSY)
					znd->meta_result = rc;
			}
			if (expg->last_write && expg->lba != expg->last_write) {
				set_bit(IS_DIRTY, &expg->flags);
				clear_bit(IS_FLUSH, &expg->flags);
			}
		}
	}

out:
	return err;
}

/**
 * journal_alloc() - Grab the next available LBA for the journal.
 * @znd: ZDM Instance
 * @nblks: Number of blocks desired.
 * @num: Number of blocks allocated.
 *
 * Return: lba or 0 on failure.
 */
static u64 journal_alloc(struct zdm *znd, u32 nblks, u32 *num)
{
	struct md_journal *jrnl = &znd->jrnl;
	u64 tlba = 0ul;
	u32 next;
	int retry = 2;

	SpinLock(&jrnl->wb_alloc);
	next = jrnl->wb_next;
	do {
		for (; next < jrnl->size; next++) {
			if (jrnl->wb[next] != MZTEV_UNUSED)
				continue;

			tlba = WB_JRNL_BASE + next;
			jrnl->wb[next] = MZTEV_NF; /* in flight */
			jrnl->in_use++;
			jrnl->wb_next = next + 1;
			*num = 1;
			goto out_unlock;
		}
		next = 0;
	} while (tlba == 0 && --retry > 0);

out_unlock:
	spin_unlock(&jrnl->wb_alloc);

	if ((jrnl->in_use * 100 / jrnl->size) > 50)
		set_bit(DO_SYNC, &znd->flags);

	return tlba;
}

/**
 * z_metadata_lba() - Alloc a block of metadata.
 * @znd: ZDM Instance
 * @map: Block of metadata.
 * @num: Number of blocks allocated.
 *
 * Return: lba or 0 on failure.
 *
 * When map->lba is less than data_lba the metadata is pinned to it's logical
 * location.
 * When map->lba lands in data space it is dynmaically allocated and intermixed
 * within the datapool.
 *
 * [FUTURE: Pick a stream_id for floating metadata]
 */
static u64 z_metadata_lba(struct zdm *znd, struct map_pg *map, u32 *num)
{
	u32 nblks = 1;

	if (map->lba < znd->data_lba) {
		if (test_bit(WB_JRNL_1, &map->flags) ||
		    test_bit(WB_JRNL_2, &map->flags)) {
			u64 jrnl_lba = journal_alloc(znd, nblks, num);

			if (!jrnl_lba) {
				Z_ERR(znd, "Out of MD journal space?");
				jrnl_lba = map->lba;
			}
			return jrnl_lba;
		}
		*num = 1;
		return map->lba;
	}
	return z_acquire(znd, Z_AQ_META_STREAM, nblks, num);
}

/**
 * pg_update_crc() - Update CRC for page pg
 * @znd: ZDM Instance
 * @pg: Entry of lookup table or CRC page
 * @md_crc: 16 bit crc of page.
 *
 * callback from dm_io notify.. cannot hold mutex here
 */
static void pg_update_crc(struct zdm *znd, struct map_pg *pg, __le16 md_crc)
{
	struct mpinfo mpi;

	to_table_entry(znd, pg->lba, &mpi);
	if (pg->crc_pg) {
		struct map_pg *crc_pg = pg->crc_pg;
		int entry = mpi.crc.pg_idx;

		if (crc_pg && crc_pg->data.crc) {
			ref_pg(crc_pg);
			crc_pg->data.crc[entry] = md_crc;
			clear_bit(IS_READA, &crc_pg->flags);
			set_bit(IS_DIRTY, &crc_pg->flags);
			clear_bit(IS_FLUSH, &crc_pg->flags);
			crc_pg->age = jiffies_64
				    + msecs_to_jiffies(crc_pg->hotness);

			if (crc_pg->lba != mpi.crc.lba)
				Z_ERR(znd, "*** BAD CRC PG: %"PRIx64
				      " != %" PRIx64,
				      crc_pg->lba, mpi.crc.lba);

			Z_DBG(znd, "Write if dirty (lut): %"
				   PRIx64" -> %" PRIx64 " crc [%"
				   PRIx64 ".%u] : %04x",
			      pg->lba, pg->last_write,
			      crc_pg->lba, mpi.crc.pg_idx, le16_to_cpu(md_crc));

			deref_pg(crc_pg);
		} else {
			Z_ERR(znd, "**** What CRC Page !?!? %"PRIx64, pg->lba);
		}
		put_map_entry(crc_pg);
		if (getref_pg(crc_pg) == 0)
			pg->crc_pg = NULL;
	} else if (!test_bit(IS_LUT, &pg->flags)) {

		Z_DBG(znd, "Write if dirty (crc): %"
			   PRIx64" -> %" PRIx64 " crc[%u]:%04x",
		      pg->lba, pg->last_write,
		      mpi.crc.pg_idx, le16_to_cpu(md_crc));

		znd->md_crcs[mpi.crc.pg_idx] = md_crc;
	} else {
		Z_ERR(znd, "unexpected state.");
		dump_stack();
	}
}


/**
 * pg_written() - Handle accouting related to lookup table page writes
 * @pg: The page of lookup table [or CRC] that was written.
 * @error: non-zero if an error occurred.
 *
 * callback from dm_io notify.. cannot hold mutex here, cannot sleep.
 *
 */
static int pg_written(struct map_pg *pg, unsigned long error)
{
	int rcode = 0;
	struct zdm *znd = pg->znd;
	__le16 md_crc;

	if (error) {
		Z_ERR(znd, "write_page: %" PRIx64 " -> %" PRIx64
			   " ERR: %ld", pg->lba, pg->last_write, error);
		rcode = -EIO;
		goto out;
	}

	/*
	 * Re-calculate CRC on current memory page. If unchanged then on-disk
	 * is stable and in-memory is not dirty. Otherwise in memory changed
	 * during write back so leave the dirty flag set. For the purpose of
	 * the CRC table we assume that in-memory == on-disk although this
	 * is not strictly true as the page could have updated post disk write.
	 */

	md_crc = crc_md_le16(pg->data.addr, Z_CRC_4K);
	pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
	if (md_crc == pg->md_crc)
		clear_bit(IS_DIRTY, &pg->flags);

	clear_bit(W_IN_FLIGHT, &pg->flags);
	pg_update_crc(znd, pg, md_crc);

/*
 * NOTE: If we reach here it's a problem.
 *       TODO: mutex free path for adding a map entry ...
 */

	if (pg->lba < znd->data_lba) {
		u64 blba = 0ul;

		if (test_bit(WB_JRNL_1, &pg->flags) ||
		    test_bit(WB_JRNL_2, &pg->flags))
			blba = pg->last_write;

		rcode = md_journal_add_map(znd, pg->lba, blba, CRIT);
		if (rcode)
			goto out;
	}

	if (pg->last_write < znd->data_lba)
		goto out;

	/* written lba was allocated from data-pool */
	rcode = z_mapped_addmany(znd, pg->lba, pg->last_write, 1, CRIT);
	if (rcode) {
		Z_ERR(znd, "%s: Journal MANY failed.", __func__);
		goto out;
	}
	increment_used_blks(znd, pg->last_write, 1);

out:
	return rcode;
}


/**
 * on_pg_written() - A block of map table was written.
 * @error: Any error code that occurred during the I/O.
 * @context: The map_pg that was queued/written.
 */
static void on_pg_written(unsigned long error, void *context)
{
	struct map_pg *pg = context;
	int rcode;

	rcode = pg_written(pg, error);
	deref_pg(pg);
	if (rcode < 0)
		pg->znd->meta_result = rcode;
}

/**
 * queue_pg() - Queue a map table page for writeback
 * @znd: ZDM Instance
 * @pg: The target page to ensure the cover CRC blocks is cached.
 * @lba: The address to write the block to.
 */
static int queue_pg(struct zdm *znd, struct map_pg *pg, u64 lba)
{
	const int use_wq = 0;
	sector_t block = lba << Z_SHFT4K;
	unsigned int nDMsect = 1 << Z_SHFT4K;
	int rc;

	pg->znd = znd;
	MutexLock(&pg->md_lock);
	pg->md_crc = crc_md_le16(pg->data.addr, Z_CRC_4K);
	pg->last_write = lba; /* presumably */
	mutex_unlock(&pg->md_lock);

	rc = znd_async_io(znd, DM_IO_KMEM, pg->data.addr, block, nDMsect,
			  REQ_OP_WRITE, 0, use_wq, on_pg_written, pg);
	if (rc) {
		Z_ERR(znd, "queue error: %d Q: %" PRIx64 " [%u dm sect] (Q:%d)",
		      rc, lba, nDMsect, use_wq);
		dump_stack();
	}

	return rc;
}

/**
 * cache_if_dirty() - Load a page of CRC's into memory.
 * @znd: ZDM Instance
 * @pg: The target page to ensure the cover CRC blocks is cached.
 * @wp: If a queue is needed for I/O.
 *
 * The purpose of loading is to ensure the pages are in memory with the
 * async_io (write) completes the CRC accounting doesn't cause a sleep
 * and violate the callback() API rules.
 */
static void cache_if_dirty(struct zdm *znd, struct map_pg *pg, int wq)
{
	if (test_bit(IS_DIRTY, &pg->flags) && test_bit(IS_LUT, &pg->flags) &&
	    pg->data.addr) {
		struct map_pg *crc_pg;
		struct mpinfo mpi;

		to_table_entry(znd, pg->lba, &mpi);
		crc_pg = get_map_entry(znd, mpi.crc.lba, NORMAL);
		if (!crc_pg)
			Z_ERR(znd, "Out of memory. No CRC Pg");
		pg->crc_pg = crc_pg;
	}
}

/**
 * write_if_dirty() - Write old pages (flagged as DIRTY) pages of table map.
 * @znd: ZDM instance.
 * @pg: A page of table map data.
 * @wq: Use worker queue for sync writes.
 * @snc: Performa a Sync or Async write.
 *
 * Return: 0 on success or -errno value
 */
static int write_if_dirty(struct zdm *znd, struct map_pg *pg, int wq, int snc)
{
	u64 dm_s = pg->lba;
	u32 nf;
	u64 lba;
	int rcode = 0;

	if (!pg)
		return rcode;

	if (!test_bit(IS_DIRTY, &pg->flags) && pg->data.addr)
		goto out;

	lba = z_metadata_lba(znd, pg, &nf);
	if (lba && nf) {
		int rcwrt;
		int count = 1;
		__le16 md_crc;

		set_bit(W_IN_FLIGHT, &pg->flags);
		if (!snc) {
			rcode = queue_pg(znd, pg, lba);
			goto out_queued;
		}
		md_crc = crc_md_le16(pg->data.addr, Z_CRC_4K);
		rcwrt = write_block(znd->ti, DM_IO_KMEM,
				    pg->data.addr, lba, count, wq);
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
		pg->last_write = lba;
		pg_update_crc(znd, pg, md_crc);

		if (rcwrt) {
			Z_ERR(znd, "write_page: %" PRIx64 " -> %" PRIx64
				   " ERR: %d", pg->lba, lba, rcwrt);
			rcode = rcwrt;
			goto out;
		}

		if (crc_md_le16(pg->data.addr, Z_CRC_4K) == md_crc)
			clear_bit(IS_DIRTY, &pg->flags);
		clear_bit(W_IN_FLIGHT, &pg->flags);
		if (lba < znd->data_lba) {
			u64 blba = 0ul; /* if not in journal clean map entry */

			if (test_bit(WB_JRNL_1, &pg->flags) ||
			    test_bit(WB_JRNL_2, &pg->flags))
				blba = lba;

			rcwrt = md_journal_add_map(znd, dm_s, blba, NORMAL);
			if (rcwrt) {
				Z_ERR(znd, "%s: MD Journal failed.", __func__);
				rcode = rcwrt;
			}
			goto out;
		}

		rcwrt = z_mapped_addmany(znd, dm_s, lba, nf, NORMAL);
		if (rcwrt) {
			Z_ERR(znd, "%s: Journal MANY failed.",
			      __func__);
			rcode = rcwrt;
			goto out;
		}
		increment_used_blks(znd, lba, nf);
	} else {
		Z_ERR(znd, "%s: Out of space for metadata?", __func__);
		rcode = -ENOSPC;
		goto out;
	}

out:
	deref_pg(pg);

out_queued:
	if (rcode < 0)
		znd->meta_result = rcode;

	return rcode;
}

/**
 * _sync_dirty() - Write all *dirty* ZLT blocks to disk (journal->SYNC->home)
 * @znd: ZDM instance
 * @bit_type: MAP blocks then CRC blocks.
 * @sync: If true write dirty blocks to disk
 * @drop: Number of ZLT blocks to free.
 *
 * Return: 0 on success or -errno value
 */
static int _sync_dirty(struct zdm *znd, int bit_type, int sync, int drop)
{
	int err = 0;
	int entries = 0;
	int want_flush = 0;
	struct map_pg *expg = NULL;
	struct map_pg *_tpg;
	struct map_pg **wset = NULL;
	int dlstsz = 0;
	LIST_HEAD(droplist);

	wset = ZDM_CALLOC(znd, sizeof(*wset), MAX_WSET, KM_19, NORMAL);
	if (!wset)
		return -ENOMEM;

	spin_lock(&znd->zlt_lck);
	if (list_empty(&znd->zltpool))
		goto writeback;

	expg = list_last_entry(&znd->zltpool, typeof(*expg), zltlst);
	if (!expg || &expg->zltlst == (&znd->zltpool))
		goto writeback;

	_tpg = list_prev_entry(expg, zltlst);
	while (&expg->zltlst != &znd->zltpool) {
		ref_pg(expg);

		if (sync && (entries < MAX_WSET) && is_dirty(expg, bit_type)) {
			wset[entries++] = expg;
		} else if ((drop > 0) && is_old_and_clean(expg, bit_type)) {
			int is_lut = test_bit(IS_LUT, &expg->flags);
			spinlock_t *lock;

			lock = is_lut ? &znd->mapkey_lock : &znd->ct_lock;
			spin_lock(lock);
			if (getref_pg(expg) == 1) {
				list_del(&expg->zltlst);
				znd->in_zlt--;
				clear_bit(IN_ZLT, &expg->flags);
				drop--;
				expg->age = jiffies_64;
					  + msecs_to_jiffies(expg->hotness);

				if (test_bit(IS_LAZY, &expg->flags))
					Z_ERR(znd, "** Pg is lazy && zlt %"PRIx64"",
						expg->lba);

				if (!test_bit(IS_LAZY, &expg->flags)) {
					list_add(&expg->lazy, &droplist);
					znd->in_lzy++;
					set_bit(IS_LAZY, &expg->flags);
					set_bit(IS_DROPPED, &expg->flags);
					dlstsz++;
				}
				deref_pg(expg);
				if (getref_pg(expg) > 0)
					Z_ERR(znd, "Moving elv ref: %u",
					      getref_pg(expg));
			}
			spin_unlock(lock);
		} else {
			deref_pg(expg);
		}
		if (entries == MAX_WSET)
			break;

		expg = _tpg;
		_tpg = list_prev_entry(expg, zltlst);
	}

writeback:
	spin_unlock(&znd->zlt_lck);

	if (entries > 0) {
		err = _pool_write(znd, wset, entries);
		if (err < 0)
			goto out;
		if (entries == MAX_WSET)
			err = -EBUSY;
	}

out:
	if (want_flush)
		set_bit(DO_FLUSH, &znd->flags);
	if (!list_empty(&droplist))
		lazy_pool_splice(znd, &droplist);

	if (wset)
		ZDM_FREE(znd, wset, sizeof(*wset) * MAX_WSET, KM_19);

	return err;
}

/**
 * sync_dirty() - Write all *dirty* ZLT blocks to disk (journal->SYNC->home)
 * @znd: ZDM instance
 * @bit_type: MAP blocks then CRC blocks.
 * @sync: Write dirty blocks
 * @drop: IN: # of pages to free.
 *
 * Return: 0 on success or -errno value
 */
static int sync_dirty(struct zdm *znd, int bit_type, int sync, int drop)
{
	int err;

	MutexLock(&znd->pool_mtx);
	do {
		err = _sync_dirty(znd, bit_type, sync, drop);
		drop = 0;
	} while (err == -EBUSY);

	if (err > 0)
		err = 0;
	mutex_unlock(&znd->pool_mtx);

	return err;
}

/**
 * sync_mapped_pages() - Migrate lookup tables and crc pages to disk
 * @znd: ZDM instance
 * @sync: If dirty blocks need to be written.
 * @drop: Number of blocks to drop.
 *
 * Return: 0 on success or -errno value
 */
static int sync_mapped_pages(struct zdm *znd, int sync, int drop)
{
	int err;
	int remove = drop ? 1 : 0;

	err = sync_dirty(znd, IS_LUT, sync, drop);

	/* on error return */
	if (err < 0)
		return err;

	/* TBD: purge CRC's on ref-count? */
	err = sync_dirty(znd, IS_CRC, sync, remove);

	return err;
}

/**
 * dm_s is a logical sector that maps 1:1 to the whole disk in 4k blocks
 * Here the logical LBA and field are calculated for the lookup table
 * where the physical LBA can be read from disk.
 */
static int map_addr_aligned(struct zdm *znd, u64 dm_s, struct map_addr *out)
{
	u64 block	= dm_s >> 10;

	out->zone_id	= block >> 6;
	out->lut_s	= block + znd->s_base;
	out->lut_r	= block + znd->r_base;
	out->pg_idx	= dm_s & 0x3FF;

	return 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/**
 * map_addr_calc()
 * @znd: ZDM instance
 * @origin: address to calc
 * @out: address, zone, crc, lut addr
 *
 * dm_s is a logical sector that maps 1:1 to the whole disk in 4k blocks
 * Here the logical LBA and field are calculated for the lookup table
 * where the physical LBA can be read from disk.
 */
static int map_addr_calc(struct zdm *znd, u64 origin, struct map_addr *out)
{
	u64 offset = ((origin < znd->md_end) ? znd->md_start : znd->md_end);

	out->dm_s = origin;
	return map_addr_aligned(znd, origin - offset, out);
}

/**
 * read_pg() - Read a page of LUT/CRC from disk.
 * @znd: ZDM instance
 * @pg: Page to fill
 * @gfp: Memory allocation rule
 * @mpi: Backing page locations.
 *
 * Load a page of the sector lookup table that maps to pg->lba
 * If pg->lba is not on disk return 0
 *
 * Return: 1 if page exists, 0 if unmodified, else -errno on error.
 */
static int read_pg(struct zdm *znd, struct map_pg *pg, u64 lba48, gfp_t gfp,
		   struct mpinfo *mpi)
{
	int rcode = 0;
	const int count = 1;
	const int wq = 1;
	int rd;
	__le16 check;
	__le16 expect = 0;

	/**
	 * This table entry may be on-disk, if so it needs to
	 * be loaded.
	 * If not it needs to be initialized to 0xFF
	 *
	 * When ! ZF_POOL_FWD and mapped block is FWD && LUT
	 *   the mapped->lba may be pinned in sk_ pool
	 *   if so the sk_ pool entry may need to be pulled
	 *   to resvole the current address of mapped->lba
	 */

/* TODO: Async reads */

	if (warn_bad_lba(znd, lba48))
		Z_ERR(znd, "Bad PAGE %" PRIx64, lba48);

	rd = read_block(znd->ti, DM_IO_KMEM, pg->data.addr, lba48, count, wq);
	if (rd) {
		Z_ERR(znd, "%s: read_block: ERROR: %d", __func__, rd);
		rcode = -EIO;
		goto out;
	}

	/*
	 * Now check block crc
	 */
	check = crc_md_le16(pg->data.addr, Z_CRC_4K);
	if (test_bit(IS_LUT, &pg->flags)) {
		struct map_pg *crc_pg;

		crc_pg = get_map_entry(znd, mpi->crc.lba, gfp);
		if (crc_pg) {
			ref_pg(crc_pg);
			if (crc_pg->data.crc) {
				MutexLock(&crc_pg->md_lock);
				expect = crc_pg->data.crc[mpi->crc.pg_idx];
				mutex_unlock(&crc_pg->md_lock);
				crc_pg->age = jiffies_64
					    + msecs_to_jiffies(crc_pg->hotness);
			}
			deref_pg(crc_pg);
		}

		if (check != expect) {
			Z_ERR(znd, "Corrupt metadata (lut): %"
				PRIx64" -> %" PRIx64 " crc [%"
				PRIx64 ".%u] : %04x != %04x",
				pg->lba, pg->last_write,
				crc_pg->lba, mpi->crc.pg_idx,
				le16_to_cpu(expect), le16_to_cpu(check));
		}



		put_map_entry(crc_pg);
	} else {
		expect = znd->md_crcs[mpi->crc.pg_idx];

		if (check != expect) {
			Z_ERR(znd, "Corrupt metadata (CRC): %"
				PRIx64" -> %" PRIx64 " crc [%u] : %04x != %04x",
				pg->lba, pg->last_write,
				mpi->crc.pg_idx,
				le16_to_cpu(expect), le16_to_cpu(check));
		}

	}

	if (check != expect) {
		znd->meta_result = -ENOSPC;

		Z_ERR(znd,
		      "Corrupt metadata: %" PRIx64 " from %" PRIx64
		      " [%04x != %04x]",
		      pg->lba, lba48,
		      le16_to_cpu(check),
		      le16_to_cpu(expect));
	}
	rcode = 1;

out:
	return rcode;
}

/**
 * cache_pg() - Load a page of LUT/CRC into memory from disk, or default values.
 * @znd: ZDM instance
 * @pg: Page to fill
 * @gfp: Memory allocation rule
 * @mpi: Backing page locations.
 *
 * Return: 1 if page loaded from disk, 0 if empty, else -errno on error.
 */
static int cache_pg(struct zdm *znd, struct map_pg *pg, gfp_t gfp,
		    struct mpinfo *mpi)
{
	int rc = 0;
	int empty_val = test_bit(IS_LUT, &pg->flags) ? 0xff : 0;
	u64 lba48 = current_mapping(znd, pg->lba, gfp);

	ref_pg(pg);
	MutexLock(&pg->md_lock);
	if (!pg->data.addr) {
		pg->data.addr = ZDM_ALLOC(znd, Z_C4K, PG_27, gfp);
		if (pg->data.addr) {
			if (lba48)
				rc = read_pg(znd, pg, lba48, gfp, mpi);
			else
				memset(pg->data.addr, empty_val, Z_C4K);

			if (rc < 0) {
				Z_ERR(znd, "%s: read_pg from %" PRIx64
					   " [to? %x] error: %d", __func__,
				      pg->lba, empty_val, rc);
				ZDM_FREE(znd, pg->data.addr, Z_C4K, PG_27);
			}
		} else {
			Z_ERR(znd, "%s: Out of memory.", __func__);
			rc = -ENOMEM;
		}
		if (pg->data.addr) {
			set_bit(IS_FLUSH, &pg->flags);
			atomic_inc(&znd->incore);
			pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
			rc = pool_add(znd, pg);
			Z_DBG(znd, "Page loaded: lba: %" PRIx64, pg->lba);
		}
	}
	mutex_unlock(&pg->md_lock);

	deref_pg(pg);
	return rc;
}

/**
 * z_lookup_table() - resolve a sector mapping via ZLT mapping
 * @znd: ZDM Instance
 * @addr: Address to resolve (via FWD map).
 * @gfp: Current allocation flags.
 */
static u64 z_lookup_table(struct zdm *znd, u64 addr, gfp_t gfp)
{
	struct map_addr maddr;
	struct map_pg *pg;
	u64 tlba = 0;

	map_addr_calc(znd, addr, &maddr);
	pg = get_map_entry(znd, maddr.lut_s, gfp);
	if (pg) {
		ref_pg(pg);
		if (pg->data.addr) {
			__le32 delta;

			MutexLock(&pg->md_lock);
			delta = pg->data.addr[maddr.pg_idx];
			mutex_unlock(&pg->md_lock);
			tlba = map_value(znd, delta);
			pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
			clear_bit(IS_READA, &pg->flags);
		}
		deref_pg(pg);
		put_map_entry(pg);
	}
	return tlba;
}

/**
 * update_map_entry() - Migrate memcache to lookup table map entries.
 * @znd: ZDM instance
 * @mapped: memcache block.
 * @maddr: map_addr
 * @to_addr: LBA or sector #.
 * @is_fwd: flag forward or reverse lookup table.
 *
 * when is_fwd is 0:
 *  - maddr->dm_s is a sector -> lba.
 *	 in this case the old lba is discarded and scheduled for cleanup
 *	 by updating the reverse map lba tables noting that this location
 *	 is now unused.
 * when is_fwd is 0:
 *  - maddr->dm_s is an lba, lba -> dm_s
 *
 * Return: non-zero on error.
 */
static int update_map_entry(struct zdm *znd, struct map_pg *pg,
			    struct map_addr *maddr, u64 to_addr, int is_fwd)
{
	int err = -ENOMEM;

	if (pg && pg->data.addr) {
		u64 index = maddr->pg_idx;
		__le32 delta;
		__le32 value;
		int was_updated = 0;

		ref_pg(pg);
		MutexLock(&pg->md_lock);
		delta = pg->data.addr[index];
		err = map_encode(znd, to_addr, &value);
		if (!err) {
			/*
			 * if the value is modified update the table and
			 * place it on top of the active [zltlst] list
			 * this will keep the chunk of lookup table in
			 * memory.
			 */
			if (pg->data.addr[index] != value) {
				pg->data.addr[index] = value;
				pg->age = jiffies_64
					    + msecs_to_jiffies(pg->hotness);
				set_bit(IS_DIRTY, &pg->flags);
				clear_bit(IS_FLUSH, &pg->flags);
				clear_bit(IS_READA, &pg->flags);
				was_updated = 1;
			} else if (value != MZTEV_UNUSED) {
				Z_ERR(znd, "*ERR* %" PRIx64
					   " -> data.addr[index] (%x) == (%x)",
				      maddr->dm_s, pg->data.addr[index],
				      value);
				dump_stack();
			}
		} else {
			Z_ERR(znd, "*ERR* Mapping: %" PRIx64 " to %" PRIx64,
			      to_addr, maddr->dm_s);
		}
		mutex_unlock(&pg->md_lock);

		if (was_updated && is_fwd && (delta != MZTEV_UNUSED)) {
			u64 old_phy = map_value(znd, delta);

			/*
			 * add to discard list of the controlling mzone
			 * for the 'delta' physical block
			 */
			Z_DBG(znd, "%s: unused_phy: %" PRIu64
			      " (new lba: %" PRIu64 ")",
			      __func__, old_phy, to_addr);

			WARN_ON(old_phy >= znd->nr_blocks);

			err = unused_phy(znd, old_phy, 0, NORMAL);
			if (err)
				err = -ENOSPC;
		}
		deref_pg(pg);
	}
	return err;
}

/**
 * move_to_map_tables() - Migrate memcache to lookup table map entries.
 * @znd: ZDM instance
 * @mcache: memcache block.
 *
 * Return: non-zero on error.
 */
static int move_to_map_tables(struct zdm *znd, struct map_cache *mcache)
{
	struct map_pg *smtbl = NULL;
	struct map_pg *rmtbl = NULL;
	struct map_addr maddr = { .dm_s = 0ul };
	struct map_addr rev = { .dm_s = 0ul };
	u64 lut_s = BAD_ADDR;
	u64 lut_r = BAD_ADDR;
	int jentry;
	int err = 0;
	int is_fwd = 1;

	/* the journal being move must remain stable so sorting
	 * is disabled. If a sort is desired due to an unsorted
	 * page the search devolves to a linear lookup.
	 */
	mcache_busy(mcache);
	for (jentry = mcache->jcount; jentry > 0;) {
		u64 dm_s = le64_to_lba48(mcache->jdata[jentry].tlba, NULL);
		u64 lba = le64_to_lba48(mcache->jdata[jentry].bval, NULL);

		if (dm_s == Z_LOWER48 || lba == Z_LOWER48) {
			mcache->jcount = --jentry;
			continue;
		}

		map_addr_calc(znd, dm_s, &maddr);
		if (lut_s != maddr.lut_s) {
			put_map_entry(smtbl);
			if (smtbl)
				deref_pg(smtbl);
			smtbl = get_map_entry(znd, maddr.lut_s, NORMAL);
			if (!smtbl) {
				err = -ENOMEM;
				goto out;
			}
			ref_pg(smtbl);
			lut_s = smtbl->lba;
		}

		is_fwd = 1;
		if (lba == 0ul)
			lba = BAD_ADDR;
		err = update_map_entry(znd, smtbl, &maddr, lba, is_fwd);
		if (err < 0)
			goto out;

		/*
		 * In this case the reverse was handled as part of
		 * discarding the forward map entry -- if it was in use.
		 */
		if (lba != BAD_ADDR) {
			map_addr_calc(znd, lba, &rev);
			if (lut_r != rev.lut_r) {
				put_map_entry(rmtbl);
				if (rmtbl)
					deref_pg(rmtbl);
				rmtbl = get_map_entry(znd, rev.lut_r, NORMAL);
				if (!rmtbl) {
					err = -ENOMEM;
					goto out;
				}
				ref_pg(rmtbl);
				lut_r = rmtbl->lba;
			}
			is_fwd = 0;
			err = update_map_entry(znd, rmtbl, &rev, dm_s, is_fwd);
			if (err == 1)
				err = 0;
		}

		if (err < 0)
			goto out;

		mcache->jdata[jentry].tlba = MC_INVALID;
		mcache->jdata[jentry].bval = MC_INVALID;
		if (mcache->jsorted == mcache->jcount)
			mcache->jsorted--;
		mcache->jcount = --jentry;
	}
out:
	if (smtbl)
		deref_pg(smtbl);
	if (rmtbl)
		deref_pg(rmtbl);
	put_map_entry(smtbl);
	put_map_entry(rmtbl);
	set_bit(DO_MEMPOOL, &znd->flags);
	mcache_unbusy(mcache);

	return err;
}

/**
 * unused_phy() - Mark a block as unused.
 * @znd: ZDM instance
 * @lba: Logical LBA of block.
 * @orig_s: Sector being marked.
 * @gfp: Memory allocation rule
 *
 * Return: non-zero on error.
 *
 * Add an unused block to the list of blocks to be discarded during
 * garbage collection.
 */
static int unused_phy(struct zdm *znd, u64 lba, u64 orig_s, gfp_t gfp)
{
	int err = 0;
	struct map_pg *pg;
	struct map_addr reverse;
	int z_off;

	if (lba < znd->data_lba)
		return 0;

	map_addr_calc(znd, lba, &reverse);
	z_off = reverse.zone_id % 1024;
	pg = get_map_entry(znd, reverse.lut_r, gfp);
	if (!pg) {
		err = -EIO;
		Z_ERR(znd, "unused_phy: Reverse Map Entry not found.");
		goto out;
	}

	if (!pg->data.addr) {
		Z_ERR(znd, "Catastrophic missing LUT page.");
		dump_stack();
		err = -EIO;
		goto out;
	}
	ref_pg(pg);

	/*
	 * if the value is modified update the table and
	 * place it on top of the active [zltlst] list
	 */
	if (pg->data.addr[reverse.pg_idx] != MZTEV_UNUSED) {
		u32 gzno  = reverse.zone_id >> GZ_BITS;
		u32 gzoff = reverse.zone_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp;
		u32 zf;
		u32 stream_id;

		if (orig_s) {
			__le32 enc = pg->data.addr[reverse.pg_idx];
			u64 dm_s = map_value(znd, enc);
			int drop_discard = 0;

			if (dm_s < znd->data_lba) {
				drop_discard = 1;
				Z_ERR(znd, "Discard invalid target %"
				      PRIx64" - Is ZDM Meta %"PRIx64" vs %"
				      PRIx64, lba, orig_s, dm_s);
			}
			if (orig_s != dm_s) {
				drop_discard = 1;
				Z_ERR(znd,
				      "Discard %" PRIx64
				      " mismatched src: %"PRIx64 " vs %" PRIx64,
				      lba, orig_s, dm_s);
			}
			if (drop_discard)
				goto out_unlock;
		}
		MutexLock(&pg->md_lock);
		pg->data.addr[reverse.pg_idx] = MZTEV_UNUSED;
		mutex_unlock(&pg->md_lock);
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
		set_bit(IS_DIRTY, &pg->flags);
		clear_bit(IS_READA, &pg->flags);
		clear_bit(IS_FLUSH, &pg->flags);

		SpinLock(&wpg->wplck);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		zf = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
		stream_id = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_STREAM_MASK;
		if (wp > 0 && zf < Z_BLKSZ) {
			zf++;
			wpg->zf_est[gzoff] = cpu_to_le32(zf | stream_id);
			wpg->wp_alloc[gzoff] = cpu_to_le32(wp | Z_WP_RRECALC);
			set_bit(IS_DIRTY, &wpg->flags);
			clear_bit(IS_FLUSH, &wpg->flags);
		}
		spin_unlock(&wpg->wplck);
		if ((wp & Z_WP_VALUE_MASK) == Z_BLKSZ)
			znd->discard_count++;
	} else {
		Z_DBG(znd, "lba: %" PRIx64 " alread reported as free?", lba);
	}

out_unlock:
	deref_pg(pg);

out:
	put_map_entry(pg);

	return err;
}
