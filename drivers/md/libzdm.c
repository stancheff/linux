/*
 * Kernel Device Mapper for abstracting ZAC/ZBC devices as normal
 * block devices for linux file systems.
 *
 * Copyright (C) 2015,2016 Seagate Technology PLC
 *
 * Written by:
 * Shaun Tancheff <shaun.tancheff@seagate.com>
 * 
 * Bio queue support and metadata relocation by:
 * Vineet Agarwal <vineet.agarwal@seagate.com>
 *
 * This file is licensed under  the terms of the GNU General Public
 * License version 2. This program is licensed "as is" without any
 * warranty of any kind, whether express or implied.
 */

#define BUILD_NO		119

#define EXTRA_DEBUG		0

#define MZ_MEMPOOL_SZ		512

#define MAX_PER_PAGE(x)		(PAGE_SIZE / sizeof(*(x)))
#define READ_AHEAD		128  /* Max READA of FWD LUT entries */
#define MEMCACHE_HWMARK		5
#define MEM_PURGE_MSECS		9000
#define MEM_HOT_BOOST_INC	5000
#define DISCARD_IDLE_MSECS	2000
#define DISCARD_MAX_INGRESS	150

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
#define MAX_WSET		4096
#define SYNC_MAX		MAX_WSET

#define MZTEV_UNUSED		(cpu_to_le32(0xFFFFFFFFu))
#define MZTEV_NF		(cpu_to_le32(0xFFFFFFFEu))

#define Z_TABLE_MAGIC		0x123456787654321Eul
#define Z_KEY_SIG		0xFEDCBA987654321Ful

#define Z_CRC_4K		4096
#define MAX_ZONES_PER_MZ	1024

#define GC_READ			(1ul << 15)
#define GC_WROTE		(1ul << 14)
#define GC_DROP			(1ul << 13)

#define BAD_ADDR		(~0ul)
#define MC_INVALID		(cpu_to_le64(BAD_ADDR))
#define NOZONE			(~0u)

#define GZ_BITS			10
#define GZ_MMSK			((1u << GZ_BITS) - 1)

#define CRC_BITS		11
#define CRC_MMSK		((1u << CRC_BITS) - 1)

#define MD_CRC_INIT		(cpu_to_le16(0x5249u))

#define ISCT_BASE		1

#define MC_HEAD			0
#define MC_INTERSECT		1
#define MC_TAIL			2
#define MC_SKIP			2

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
static struct io_4k_block *get_io_vcache(struct zdm *znd, gfp_t gfp);
static int put_io_vcache(struct zdm *znd, struct io_4k_block *cache);
static struct map_pg *gme_noio(struct zdm *znd, u64 lba);
static struct map_pg *get_map_entry(struct zdm *znd, u64 lba,
			     int ahead, int async, int noio, gfp_t gfp);
static struct map_pg *do_gme_io(struct zdm *znd, u64 lba,
				int ahead, int async, gfp_t gfp);
static void put_map_entry(struct map_pg *);
static int cache_pg(struct zdm *znd, struct map_pg *pg, gfp_t gfp,
		    struct mpinfo *mpi);
static int _pool_read(struct zdm *znd, struct map_pg **wset, int count);
static int wait_for_map_pg(struct zdm *znd, struct map_pg *pg, gfp_t gfp);
static int move_to_map_tables(struct zdm *znd, struct map_cache_entry *maps,
			      int count, struct map_pg **pgs, int npgs);
static int move_unused_to_tables(struct zdm *znd, struct map_cache_entry *maps,
				 int count, struct map_pg **pgs, int npgs);
static int unused_add(struct zdm *znd, u64 addr, u64, u32 count, gfp_t gfp);
static int _cached_to_tables(struct zdm *znd, u32 zone);
static void update_stale_ratio(struct zdm *znd, u32 zone);
static int zoned_create_disk(struct dm_target *ti, struct zdm *znd);
static int do_init_zoned(struct dm_target *ti, struct zdm *znd);
static void update_all_stale_ratio(struct zdm *znd);
static int unmap_deref_chunk(struct zdm *znd, u32 blks, int, gfp_t gfp);
static u64 z_lookup_journal_cache(struct zdm *znd, u64 addr);
static u64 z_lookup_ingress_cache(struct zdm *znd, u64 addr);
static int z_lookup_trim_cache(struct zdm *znd, u64 addr);
static u64 z_lookup_table(struct zdm *znd, u64 addr, gfp_t gfp);
static u64 current_mapping(struct zdm *znd, u64 addr, gfp_t gfp);
static u64 current_map_range(struct zdm *znd, u64 addr, u32 *range, gfp_t gfp);
static int do_sort_merge(struct map_pool *to, struct map_pool *src,
			 struct map_cache_entry *chng, int nchgs, int drop);
static int ingress_add(struct zdm *znd, u64 addr, u64 target, u32 count, gfp_t);
static int z_mapped_addmany(struct zdm *znd, u64 addr, u64, u32, gfp_t);
static int md_journal_add_map(struct zdm *znd, u64 addr, u64 lba);
static int md_handle_crcs(struct zdm *znd);
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

static __always_inline void test_and_lock(struct mutex *m, int lineno)
{
	if (!mutex_trylock(m)) {
		pr_debug("mutex stall at %d\n", lineno);
		mutex_lock(m);
	}
}

/**
 * ref_pg() - Decrement refcount on page of ZLT
 * @pg: Page of ZLT map
 */
static __always_inline void deref_pg(struct map_pg *pg)
{
	atomic_dec(&pg->refcount);

#if 0 /* ALLOC_DEBUG */
	if (atomic_dec_return(&pg->refcount) < 0) {
		pr_err("Excessive %"PRIx64": %d who dunnit?\n",
			pg->lba, atomic_read(&pg->refcount));
		dump_stack();
	}
#endif

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
static inline __le32 crc32c_le32(u32 init, void *data, u32 sz)
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
static inline u64 le64_to_lba48(__le64 enc, u32 *flg)
{
	const u64 lba64 = le64_to_cpu(enc);

	if (flg)
		*flg = (lba64 >> LBA48_BITS) & Z_UPPER16;

	return lba64 & Z_LOWER48;
}

/**
 * lba48_to_le64() - Encode 48 bits of lba + 16 bits of flags.
 * @flags: flags to encode.
 * @lba48: LBA to encode
 *
 * Return: Little endian u64.
 */
static inline __le64 lba48_to_le64(u32 flags, u64 lba48)
{
	u64 high_bits = flags;

	return (high_bits << LBA48_BITS) | (lba48 & Z_LOWER48);
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
	int expired = 1;
	if (age) {
		u64 expire_at = age + msecs_to_jiffies(msecs);

		expired = time_after64(jiffies_64, expire_at);
	}
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
 * zlt_pool_splice - Add a list of pages to the zlt pool
 * @znd: ZDM Instance
 * @list: List of map table page to add.
 */
static __always_inline
void zlt_pool_splice(struct zdm *znd, struct list_head *list)
{
	unsigned long flags;

	spin_lock_irqsave(&znd->zlt_lck, flags);
	list_splice_tail(list, &znd->zltpool);
	spin_unlock_irqrestore(&znd->zlt_lck, flags);
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
	if (test_bit(IN_ZLT, &expg->flags)) {
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
 * zlt_pgs_in_use() - Get number of zlt pages in use.
 * @znd: ZDM instance
 *
 * Return: Current in zlt count.
 */
static inline int zlt_pgs_in_use(struct zdm *znd)
{
	return znd->in_zlt;
}

/**
 * low_cache_mem() - test if cache memory is running low
 * @znd: ZDM instance
 *
 * Return: non-zero of cache memory is low.
 */
static inline int low_cache_mem(struct zdm *znd)
{
	return zlt_pgs_in_use(znd) > 2048;
}

/**
 * to_table_entry() - Deconstrct metadata page into mpinfo
 * @znd: ZDM instance
 * @lba: Address (4k resolution)
 * @ra: Is speculative (may be beyond FWD IDX)
 * @mpi: mpinfo describing the entry (OUT, Required)
 *
 * Return: Index into mpinfo.table.
 */
static int to_table_entry(struct zdm *znd, u64 lba, int ra, struct mpinfo *mpi)
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
			if (ra)
				goto done;
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
			if (ra)
				goto done;
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
			if (ra)
				goto done;
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
			if (ra)
				goto done;
			Z_ERR(znd, "%s: CRC BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->crc_count);
			dump_stack();
		}
	} else {
		Z_ERR(znd, "** Corrupt lba %" PRIx64 " not in range.", lba);
		znd->meta_result = -EIO;
		dump_stack();
	}
done:
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
#define GET_PGS		0x020000
#define GET_ZPG		0x040000
#define GET_KM		0x080000
#define GET_VM		0x100000

#define xx_01    (GET_ZPG |  1) /* unused */
#define xx_13    (GET_ZPG | 13) /* unused */
#define xx_17    (GET_ZPG | 17) /* unused */

#define xx_25    (GET_KM  | 25) /* unused */
#define xx_26    (GET_KM  | 26) /* unused */
#define xx_28    (GET_KM  | 28) /* unused */
#define xx_29    (GET_KM  | 29) /* unused */
#define xx_30    (GET_KM  | 30) /* unused */

#define PG_02    (GET_ZPG |  2) /* CoW [RMW] block */
#define PG_05    (GET_ZPG |  5) /* superblock */
#define PG_06    (GET_ZPG |  6) /* WP: Alloc, Used, Shadow */
#define PG_08    (GET_ZPG |  8) /* mcache data block */
#define PG_09    (GET_ZPG |  9) /* mc pg (copy/sync) */
#define PG_10    (GET_ZPG | 10) /* superblock: temporary */
#define PG_11    (GET_ZPG | 11) /* superblock: temporary */
#define PG_27    (GET_ZPG | 27) /* map_pg data block */

#define KM_00    (GET_KM  |  0) /* ZDM: Instance */
#define KM_07    (GET_KM  |  7) /* mcache struct */
#define KM_14    (GET_KM  | 14) /* bio pool: queue */
#define KM_15    (GET_KM  | 15) /* bio pool: chain vec */
#define KM_16    (GET_KM  | 16) /* gc descriptor */
#define KM_18    (GET_KM  | 18) /* wset : sync */
#define KM_19    (GET_KM  | 19) /* wset */
#define KM_20    (GET_KM  | 20) /* map_pg struct */
#define KM_21    (GET_KM  | 21) /* wp array (of ptrs) */

#define VM_01    (GET_VM  |  1) /* wb journal */
#define VM_03    (GET_VM  |  3) /* gc postmap */
#define VM_04    (GET_VM  |  4) /* gc io buffer: 1MiB*/
#define VM_12    (GET_VM  | 12) /* vm io cache */

/* alloc'd page of order > 0 */
#define MP_22    (GET_PGS | 22) /* Metadata CRCs */
#define MP_23    (GET_PGS | 23) /* WB Journal map */

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
	unsigned long flags;
#if ALLOC_DEBUG
	int iter;
	int okay = 0;
#endif

	spin_lock_irqsave(&znd->stats_lock, flags);
	if (sz > znd->memstat)
		Z_ERR(znd, "Free'd more mem than allocated? %d", id);

	if (sz > znd->bins[id]) {
		Z_ERR(znd, "Free'd more mem than allocated? %d", id);
		dump_stack();
	}

#if ALLOC_DEBUG
	for (iter = 0; iter < ADBG_ENTRIES; iter++) {
		if (p == znd->alloc_trace[iter]) {
			znd->alloc_trace[iter] = NULL;
			okay = 1;
			atomic_dec(&znd->allocs);
			break;
		}
	}
	if (!okay) {
		Z_ERR(znd, "Free'd something *NOT* allocated? %d", id);
		dump_stack();
	}
#endif

	znd->memstat -= sz;
	znd->bins[id] -= sz;
	spin_unlock_irqrestore(&znd->stats_lock, flags);
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

	if (!p)
		goto out_invalid;

	if (znd)
		zdm_free_debug(znd, p, sz, id);

#if ALLOC_DEBUG
	memset(p, 0x69, sz); /* DEBUG */
#endif

	switch (code) {
	case PG_08:
	case PG_09:
	case PG_27:
		BUG_ON(!znd->mempool_pages);
		mempool_free(virt_to_page(p), znd->mempool_pages);
		return;

#if ENABLE_BIO_QUEUE
	case KM_14:
		BUG_ON(!znd->bp_q_node);
		BUG_ON(sz != sizeof(struct zdm_q_node));
		mempool_free(p, znd->bp_q_node);
		return;
	case KM_15:
		BUG_ON(!znd->bp_chain_vec);
		BUG_ON(sz != sizeof(struct zdm_bio_chain));
		mempool_free(p, znd->bp_chain_vec);
		return;
#endif

	case KM_18:
	case KM_19:
		BUG_ON(!znd->mempool_wset);
		mempool_free(p, znd->mempool_wset);
		return;
	case KM_20:
		BUG_ON(!znd->mempool_maps);
		BUG_ON(sz != sizeof(struct map_pg));
		mempool_free(p, znd->mempool_maps);
		return;
	default:
		break;
	}

	switch (flag) {
	case GET_ZPG:
		free_page((unsigned long)p);
		break;
	case GET_PGS:
		free_pages((unsigned long)p, ilog2(sz >> PAGE_SHIFT));
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
	return;

out_invalid:
	Z_ERR(znd, "double zdm_free %p [%d]", p, id);
	dump_stack();
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
	unsigned long flags;
#if ALLOC_DEBUG
	int iter;
	int count;
#endif

	spin_lock_irqsave(&znd->stats_lock, flags);

#if ALLOC_DEBUG
	atomic_inc(&znd->allocs);
	count = atomic_read(&znd->allocs);
	if (atomic_read(&znd->allocs) < ADBG_ENTRIES) {
		for (iter = 0; iter < ADBG_ENTRIES; iter++) {
			if (!znd->alloc_trace[iter]) {
				znd->alloc_trace[iter] = p;
				break;
			}
		}
	} else {
		Z_ERR(znd, "Exceeded max debug alloc trace");
	}
#endif

	znd->memstat += sz;
	znd->bins[id] += sz;
	if (znd->bins[id]/sz > znd->max_bins[id]) {
		znd->max_bins[id] = znd->bins[id]/sz;
	}
	spin_unlock_irqrestore(&znd->stats_lock, flags);
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
	struct page *page = NULL;
	void *pmem = NULL;
	int id   = code & 0x00FFFF;
	int flag = code & 0xFF0000;
	gfp_t gfp_mask = GFP_KERNEL;
	int zeroed = 0;

	if (znd && gfp != GFP_KERNEL)
		gfp_mask = GFP_ATOMIC;

	switch (code) {
	case PG_08:
	case PG_09:
	case PG_27:
		BUG_ON(!znd->mempool_pages);
		page = mempool_alloc(znd->mempool_pages, gfp_mask);
		if (page)
			pmem = page_address(page);
		goto out;
#if ENABLE_BIO_QUEUE
	case KM_14:
		BUG_ON(!znd->bp_q_node);
		BUG_ON(sz != sizeof(struct zdm_q_node));
		pmem = mempool_alloc(znd->bp_q_node, gfp_mask);
		goto out;
	case KM_15:
		BUG_ON(!znd->bp_chain_vec);
		BUG_ON(sz != sizeof(struct zdm_bio_chain));
		pmem = mempool_alloc(znd->bp_chain_vec, gfp_mask);
		goto out;
#endif
	case KM_18:
	case KM_19:
		BUG_ON(!znd->mempool_wset);
		pmem = mempool_alloc(znd->mempool_wset, gfp_mask);
		goto out;
	case KM_20:
		BUG_ON(!znd->mempool_maps);
		BUG_ON(sz != sizeof(struct map_pg));
		pmem = mempool_alloc(znd->mempool_maps, gfp_mask);
		goto out;
	default:
		break;
	}

	if (flag == GET_VM)
		might_sleep();

	if (gfp_mask == GFP_KERNEL)
		might_sleep();

	zeroed = 1;
	switch (flag) {
	case GET_ZPG:
		pmem = (void *)get_zeroed_page(gfp_mask);
		if (!pmem && gfp_mask == GFP_ATOMIC) {
			if (znd)
				Z_ERR(znd, "No atomic for %d, try noio.", id);
			pmem = (void *)get_zeroed_page(GFP_NOIO);
		}
		break;
	case GET_PGS:
		zeroed = 0;
		page = alloc_pages(gfp_mask, ilog2(sz >> PAGE_SHIFT));
		if (page)
			pmem = page_address(page);
		break;
	case GET_KM:
		pmem = kzalloc(sz, gfp_mask);
		if (!pmem && gfp_mask == GFP_ATOMIC) {
			if (znd)
				Z_ERR(znd, "No atomic for %d, try noio.", id);
			pmem = kzalloc(sz, GFP_NOIO);
		}
		break;
	case GET_VM:
		pmem = vzalloc(sz);
		break;
	default:
		Z_ERR(znd, "zdm alloc scheme for %u unknown.", code);
		break;
	}

out:
	if (pmem) {
		if (znd)
			zdm_alloc_debug(znd, pmem, sz, id);
		if (!zeroed)
			memset(pmem, 0, sz);
	} else {
		if (znd)
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
	unsigned long flags;

	if (mapped) {
		spin_lock_irqsave(&mapped->md_lock, flags);
		WARN_ON(test_bit(IS_DIRTY, &mapped->flags));
		if (mapped->data.addr) {
			ZDM_FREE(znd, mapped->data.addr, Z_C4K, PG_27);
			atomic_dec(&znd->incore);
		}
		if (mapped->crc_pg) {
			deref_pg(mapped->crc_pg);
			mapped->crc_pg = NULL;
		}
		spin_unlock_irqrestore(&mapped->md_lock, flags);
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
	unsigned long flags;

	set_bit(ZF_FREEZE, &znd->flags);
	atomic_inc(&znd->gc_throttle);

	mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
	flush_delayed_work(&znd->gc_work);

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

	spin_lock_irqsave(&znd->lzy_lck, flags);
	INIT_LIST_HEAD(&znd->lzy_pool);
	spin_unlock_irqrestore(&znd->lzy_lck, flags);

	spin_lock_irqsave(&znd->zlt_lck, flags);
	INIT_LIST_HEAD(&znd->zltpool);
	spin_unlock_irqrestore(&znd->zlt_lck, flags);

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
	unsigned long flags;

	spin_lock_irqsave(&znd->mapkey_lock, flags);
	hash_for_each_safe(znd->fwd_hm, entry, _tmp, expg, hentry) {
		hash_del(&expg->hentry);
		mapped_free(znd, expg);
	}
	hash_for_each_safe(znd->rev_hm, entry, _tmp, expg, hentry) {
		hash_del(&expg->hentry);
		mapped_free(znd, expg);
	}
	spin_unlock_irqrestore(&znd->mapkey_lock, flags);

	spin_lock_irqsave(&znd->ct_lock, flags);
	hash_for_each_safe(znd->fwd_hcrc, entry, _tmp, expg, hentry) {
		hash_del(&expg->hentry);
		mapped_free(znd, expg);
	}
	hash_for_each_safe(znd->rev_hcrc, entry, _tmp, expg, hentry) {
		hash_del(&expg->hentry);
		mapped_free(znd, expg);
	}
	spin_unlock_irqrestore(&znd->ct_lock, flags);
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
	ZDM_FREE(znd, wp, znd->gz_count * sizeof(*wp), KM_21);
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
		ZDM_FREE(znd, jrnl->wb, avail * sizeof(*jrnl->wb), MP_23);
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

	if (znd->dev) {
		dm_put_device(znd->ti, znd->dev);
		znd->dev = NULL;
	}
#if ENABLE_SEC_METADATA
	if (znd->meta_dev) {
		dm_put_device(znd->ti, znd->meta_dev);
		znd->meta_dev = NULL;
	}
#endif
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
		ZDM_FREE(znd, znd->md_crcs, Z_C4K * 2, MP_22);
	if (znd->gc_io_buf)
		ZDM_FREE(znd, znd->gc_io_buf, Z_C4K * GC_MAX_STRIPE, VM_04);
	if (znd->gc_postmap.mcd) {
		size_t sz = sizeof(struct gc_map_cache_data);

		ZDM_FREE(znd, znd->gc_postmap.mcd, sz, VM_03);
	}
	if (znd->jrnl_map.mcd) {
		size_t sz = sizeof(struct jrnl_map_cache_data);

		ZDM_FREE(znd, znd->jrnl_map.mcd, sz, VM_01);
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

	if (znd->cow_block)
		ZDM_FREE(znd, znd->cow_block, Z_C4K, PG_02);

	_free_md_journal(znd);

	if (znd->bio_set) {
		struct bio_set *bs = znd->bio_set;

		znd->bio_set = NULL;
		bioset_free(bs);
	}

	if (znd->mempool_pages)
		mempool_destroy(znd->mempool_pages), znd->mempool_pages = NULL;
	if (znd->mempool_maps)
		mempool_destroy(znd->mempool_maps), znd->mempool_maps = NULL;
	if (znd->mempool_wset)
		mempool_destroy(znd->mempool_wset), znd->mempool_wset = NULL;

#if ENABLE_BIO_QUEUE
	if (znd->bp_q_node)
		mempool_destroy(znd->bp_q_node), znd->bp_q_node = NULL;
	if (znd->bp_chain_vec)
		mempool_destroy(znd->bp_chain_vec), znd->bp_chain_vec = NULL;
#endif

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
	u32 blks = (znd->md_start - WB_JRNL_BASE) >> PAGE_SHIFT;
	u32 avail = (blks << PAGE_SHIFT) / sizeof(*jrnl->wb);
	u32 iter;

	Z_ERR(znd, "MD journal space: %u [%u]", avail, blks);

	spin_lock_init(&znd->jrnl.wb_alloc);
	if (avail > WB_JRNL_MAX)
		avail = WB_JRNL_MAX;

	if (avail > WB_JRNL_MIN)
		avail = WB_JRNL_MIN;

	jrnl->wb = ZDM_ALLOC(znd, avail * sizeof(*jrnl->wb), MP_23, GFP_KERNEL);
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
	const gfp_t gfp = GFP_KERNEL;

	wp = ZDM_CALLOC(znd, znd->gz_count, sizeof(*znd->wp), KM_21, gfp);
	if (!wp)
		goto out;
	for (gzno = 0; gzno < znd->gz_count; gzno++) {
		struct meta_pg *wpg = &wp[gzno];

		spin_lock_init(&wpg->wplck);
		wpg->lba      = 2048ul + (gzno * 2);
		wpg->wp_alloc = ZDM_ALLOC(znd, Z_C4K, PG_06, gfp);
		wpg->zf_est   = ZDM_ALLOC(znd, Z_C4K, PG_06, gfp);
		wpg->wp_used  = ZDM_ALLOC(znd, Z_C4K, PG_06, gfp);
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
	u32 max_count = max_report_entries(bufsz);

	struct bdev_zone_report *report = rpt_in;

	/* ZBC: scsi results are big endian */
	if (max_count > be32_to_cpu(report->descriptor_count))
		report->descriptor_count = cpu_to_be32(max_count);

	count = be32_to_cpu(report->descriptor_count);
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
	return (BLK_ZONE_TYPE_CONVENTIONAL == (dentry->type & 0x0F)) ? 1 : 0;
}

/**
 * is_zone_reset() - Determine if zone is reset / ready for writing.
 * @dentry: Zone descriptor entry.
 *
 * Return: 1 if zone condition is empty or zone type is conventional.
 */
static inline int is_zone_reset(struct bdev_zone_descriptor *dentry)
{
	u8 type = dentry->type & 0x0f;
	u8 cond = (dentry->flags >> 4) & 0x0f;

	return (BLK_ZONE_EMPTY == cond ||
		BLK_ZONE_TYPE_CONVENTIONAL == type) ? 1 : 0;
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
	struct bdev_zone_descriptor *big = dentry_in;

	return be64_to_cpu(big->lba_wptr) - be64_to_cpu(big->lba_start);
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
	u32 est = le32_to_cpu(wpg->zf_est[gzoff]) + lost;

	if (est > Z_BLKSZ)
		est = Z_BLKSZ;
	wpg->zf_est[gzoff] = cpu_to_le32(est);
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
	struct page *pgs = alloc_pages(GFP_KERNEL|GFP_DMA, order);

	if (pgs)
		report = page_address(pgs);

	if (!report) {
		rcode = -ENOMEM;
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
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
	u64 part_sz = i_size_read(get_bdev_bd_inode(znd));
	u64 zone_count = part_sz >> 28;
	u64 gz_count = (zone_count + 1023) >> 10;
	u64 overprovision = znd->mz_provision * gz_count;
	u64 zltblks = (znd->zdstart << 19) - znd->start_sect;
	u64 blocks = (zone_count - overprovision) << 19;
	u64 data_zones = (blocks >> 19) + overprovision;
	u64 mapct;
	u64 crcct;
	u32 mz_min = 0; /* cache */
	int rcode = 0;

	INIT_LIST_HEAD(&znd->zltpool);
	INIT_LIST_HEAD(&znd->lzy_pool);

	spin_lock_init(&znd->zlt_lck);
	spin_lock_init(&znd->lzy_lck);
	spin_lock_init(&znd->stats_lock);
	spin_lock_init(&znd->mapkey_lock);
	spin_lock_init(&znd->ct_lock);
	spin_lock_init(&znd->gc_lock);
	spin_lock_init(&znd->gc_postmap.cached_lock);
	spin_lock_init(&znd->jrnl_map.cached_lock);

	spin_lock_init(&znd->in_rwlck);
	spin_lock_init(&znd->unused_rwlck);
	spin_lock_init(&znd->trim_rwlck);
	spin_lock_init(&znd->wbjrnl_rwlck);

	mutex_init(&znd->pool_mtx);
	mutex_init(&znd->gc_wait);
	mutex_init(&znd->gc_vcio_lock);
	mutex_init(&znd->vcio_lock);
	mutex_init(&znd->mz_io_mutex);

	/*
	 * The space from the start of the partition [znd->start_sect]
	 * to the first zone used for data [znd->zdstart]
	 * is the number of blocks reserved for fLUT, rLUT and CRCs [zltblks]
	 */
	blocks -= zltblks;
	data_zones = (blocks >> 19) + overprovision;

	pr_err("ZDM: Part Sz %llu\n", part_sz);
	pr_err("ZDM: DM Size %llu\n", blocks);
	pr_err("ZDM: Zones %llu -- Reported Data Zones %llu\n", zone_count, blocks >> 19);
	pr_err("ZDM: Data Zones %llu\n", (blocks >> 19) + overprovision);
	pr_err("ZDM: GZones %llu\n", gz_count);


#if ENABLE_SEC_METADATA
	if (znd->meta_dst_flag == DST_TO_SEC_DEVICE) {
		/*
		 * When metadata is *only* on secondary we can reclaim the
		 * space reserved for metadata on the primary.
		 * We can add back upto: zltblks
		 * However data should still be zone aligned so:
		 *     zltblks & ((1ull << 19) - 1)
		 */
		u64 t = (part_sz - (znd->sec_zone_align << Z_SHFT_SEC)) >> 12;

// FIXME: Add back the zones used for SBlock + fLUT rLUT and CRCs ?

		data_zones = t >> Z_BLKBITS;
		gz_count = dm_div_up(data_zones, MAX_ZONES_PER_MZ);
	}
	mapct = dm_div_up(data_zones << Z_BLKBITS, 1024);
	crcct = dm_div_up(mapct, 2048);
#else
	mapct = dm_div_up(data_zones << Z_BLKBITS, 1024);
	crcct = dm_div_up(mapct, 2048);
#endif

	/* pool of single pages (order 0) */
	znd->mempool_pages = mempool_create_page_pool(4096, 0);
	znd->mempool_maps = mempool_create_kmalloc_pool(4096,
						sizeof(struct map_pg));
	znd->mempool_wset = mempool_create_kmalloc_pool(3,
					sizeof(struct map_pg*) * MAX_WSET);

	if (!znd->mempool_pages || !znd->mempool_maps || !znd->mempool_wset) {
		ti->error = "couldn't allocate mempools";
		rcode = -ENOMEM;
		goto out;
	}

#if ENABLE_BIO_QUEUE
	znd->bp_q_node = mempool_create_kmalloc_pool(
				1024, sizeof(struct zdm_q_node));
	znd->bp_chain_vec = mempool_create_kmalloc_pool(
				256, sizeof(struct zdm_bio_chain));
	if (!znd->bp_q_node || !znd->bp_chain_vec) {
		ti->error = "couldn't allocate bio queue mempools";
		rcode = -ENOMEM;
		goto out;
	}
#endif

	znd->in[0].size = ARRAY_SIZE(znd->in[0].mcd.maps);
	znd->in[1].size = ARRAY_SIZE(znd->in[1].mcd.maps);
	znd->in[0].isa = IS_INGRESS;
	znd->in[1].isa = IS_INGRESS;
	znd->ingress = &znd->in[0];

	znd->_use[0].size = ARRAY_SIZE(znd->_use[0].mcd.maps);
	znd->_use[1].size = ARRAY_SIZE(znd->_use[1].mcd.maps);
	znd->_use[0].isa = IS_UNUSED;
	znd->_use[1].isa = IS_UNUSED;
	znd->unused = &znd->_use[0];

	znd->trim_mp[0].size = ARRAY_SIZE(znd->trim_mp[0].mcd.maps);
	znd->trim_mp[1].size = ARRAY_SIZE(znd->trim_mp[1].mcd.maps);
	znd->trim_mp[0].isa = IS_TRIM;
	znd->trim_mp[1].isa = IS_TRIM;
	znd->trim = &znd->trim_mp[0];

	znd->_wbj[0].size = ARRAY_SIZE(znd->trim_mp[0].mcd.maps);
	znd->_wbj[1].size = ARRAY_SIZE(znd->trim_mp[1].mcd.maps);
	znd->_wbj[0].isa = IS_WBJRNL;
	znd->_wbj[1].isa = IS_WBJRNL;
	znd->wbjrnl = &znd->_wbj[0];

	znd->data_zones = data_zones;
	znd->gz_count = gz_count;
	znd->crc_count = crcct;
	znd->map_count = mapct;

	znd->md_start = dm_round_up(znd->start_sect, Z_BLKSZ) - znd->start_sect;
	if (znd->md_start < (WB_JRNL_BASE + WB_JRNL_MIN)) {
		znd->md_start += Z_BLKSZ;
		mz_min++;
	}

	/* md_start - lba of first full zone in partition addr space */
	znd->s_base = znd->md_start;
	mz_min += gz_count;
	if (mz_min < znd->zdstart)
		set_bit(ZF_POOL_FWD, &znd->flags);

	znd->r_base = znd->s_base + (gz_count << Z_BLKBITS);
	mz_min += gz_count;
	if (mz_min < znd->zdstart)
		set_bit(ZF_POOL_REV, &znd->flags);

	znd->c_base = znd->r_base + (gz_count << Z_BLKBITS);
	znd->c_mid  = znd->c_base + (gz_count * 0x20);
	znd->c_end  = znd->c_mid + (gz_count * 0x20);
	mz_min++;

	if (mz_min < znd->zdstart) {
		Z_ERR(znd, "Conv space for CRCs: Seting ZF_POOL_CRCS");
		set_bit(ZF_POOL_CRCS, &znd->flags);
	}

	if (test_bit(ZF_POOL_FWD, &znd->flags)) {
		znd->sk_low = znd->sk_high = 0;
	} else {
		znd->sk_low = znd->s_base;
		znd->sk_high = znd->sk_low + (gz_count * 0x40);
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
	Z_INFO(znd, "  Bio Queue %s", znd->bio_queue ? "enabled" : "disabled");
	Z_INFO(znd, "  Trim %s", znd->enable_trim ? "enabled" : "disabled");

#if ENABLE_SEC_METADATA
	if (znd->meta_dst_flag != DST_TO_PRI_DEVICE)
		Z_INFO(znd, "  Metadata %s secondary %s",
		       znd->meta_dst_flag == DST_TO_BOTH_DEVICE ?
		       "mirrored with" : "only on", znd->bdev_metaname);
#endif

	Z_INFO(znd, "  Starting Zone# %u", znd->zdstart);
	Z_INFO(znd, "  Starting Sector: %" PRIx64, znd->start_sect);
	Z_INFO(znd, "  Metadata range [%" PRIx64 ", %" PRIx64 "]",
	       znd->md_start, znd->md_end);
	Z_INFO(znd, "  Data LBA %" PRIx64, znd->data_lba);
	Z_INFO(znd, "  Size: %" PRIu64" / %" PRIu64,
	       blocks >> 3, part_sz >> 12);
	Z_INFO(znd, "  Zones: %" PRIu64 " total %u data",
	       zone_count, znd->data_zones);

#if ALLOC_DEBUG
	znd->alloc_trace = vzalloc(sizeof(*znd->alloc_trace) * ADBG_ENTRIES);
	if (!znd->alloc_trace) {
		ti->error = "couldn't allocate in-memory mem debug trace";
		rcode = -ENOMEM;
		goto out;
	}
#endif

	znd->z_sballoc = ZDM_ALLOC(znd, Z_C4K, PG_05, GFP_KERNEL);
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

	znd->gc_postmap.mcd = ZDM_ALLOC(znd, sizeof(struct gc_map_cache_data),
					VM_03, GFP_KERNEL);
	znd->md_crcs = ZDM_ALLOC(znd, Z_C4K * 2, MP_22, GFP_KERNEL);
	znd->gc_io_buf = ZDM_CALLOC(znd, GC_MAX_STRIPE, Z_C4K, VM_04, GFP_KERNEL);
	znd->wp = _alloc_wp(znd);
	znd->io_vcache[0] = ZDM_CALLOC(znd, IO_VCACHE_PAGES,
				sizeof(struct io_4k_block), VM_12, GFP_KERNEL);
	znd->io_vcache[1] = ZDM_CALLOC(znd, IO_VCACHE_PAGES,
				sizeof(struct io_4k_block), VM_12, GFP_KERNEL);

	if (!znd->gc_postmap.mcd || !znd->md_crcs || !znd->gc_io_buf ||
	    !znd->wp || !znd->io_vcache[0] || !znd->io_vcache[1]) {
		rcode = -ENOMEM;
		goto out;
	}
	znd->gc_postmap.jsize = Z_BLKSZ;
	znd->gc_postmap.map_content = IS_POST_MAP;
	_init_mdcrcs(znd);
	znd->jrnl_map.mcd = ZDM_ALLOC(znd, sizeof(struct jrnl_map_cache_data),
					VM_01, GFP_KERNEL);
	if (!znd->jrnl_map.mcd) {
		rcode = -ENOMEM;
		goto out;
	}
	znd->jrnl_map.jsize = WB_JRNL_MAX;
	znd->jrnl_map.map_content = IS_JRNL_PG;
	znd->io_client = dm_io_client_create();
	if (!znd->io_client) {
		rcode = -ENOMEM;
		goto out;
	}

	snprintf(znd->meta_wq_name, sizeof(znd->meta_wq_name), "zwq_md_%s",
		 znd->bdev_name);
	znd->meta_wq = alloc_ordered_workqueue(znd->meta_wq_name, WQ_MEM_RECLAIM);
	if (!znd->meta_wq) {
		ti->error = "couldn't start metadata workqueue";
		rcode = -ENOMEM;
		goto out;
	}
	snprintf(znd->gc_wq_name, sizeof(znd->gc_wq_name), "zwq_gc_%s",
		 znd->bdev_name);
	znd->gc_wq = alloc_ordered_workqueue(znd->gc_wq_name, WQ_MEM_RECLAIM);
	if (!znd->gc_wq) {
		ti->error = "couldn't start GC workqueue.";
		rcode = -ENOMEM;
		goto out;
	}
	snprintf(znd->bg_wq_name, sizeof(znd->bg_wq_name), "zwq_bg_%s",
		 znd->bdev_name);
	znd->bg_wq = alloc_ordered_workqueue(znd->bg_wq_name, WQ_MEM_RECLAIM);
	if (!znd->bg_wq) {
		ti->error = "couldn't start background workqueue.";
		rcode = -ENOMEM;
		goto out;
	}
	snprintf(znd->io_wq_name, sizeof(znd->io_wq_name), "zwq_io_%s",
		 znd->bdev_name);
	znd->io_wq = alloc_ordered_workqueue(znd->io_wq_name, WQ_MEM_RECLAIM);
	if (!znd->io_wq) {
		ti->error = "couldn't start DM I/O workqueue";
		rcode = -ENOMEM;
		goto out;
	}
	snprintf(znd->za_wq_name, sizeof(znd->za_wq_name), "zwq_za_%s",
		 znd->bdev_name);
	znd->zone_action_wq = alloc_ordered_workqueue(znd->za_wq_name,
						      WQ_MEM_RECLAIM);
	if (!znd->zone_action_wq) {
		ti->error = "couldn't start zone action workqueue";
		rcode = -ENOMEM;
		goto out;
	}
#if ENABLE_BIO_QUEUE
	init_waitqueue_head(&znd->wait_bio);
#endif

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
 * gc_lba_cmp() - Compare on tlba48 ignoring high 16 bits.
 * @x1: Page of map cache
 * @x2: Page of map cache
 *
 * Return: 1 less than, -1 greater than, 0 if equal.
 */
static int gc_lba_cmp(const void *x1, const void *x2)
{
	const struct map_cache_entry *r1 = x1;
	const struct map_cache_entry *r2 = x2;
	const u64 v1 = le64_to_lba48(r1->tlba, NULL);
	const u64 v2 = le64_to_lba48(r2->tlba, NULL);

	return (v1 < v2) ? -1 : ((v1 > v2) ? 1 : 0);
}

/**
 * mcache_crng() - Compare on tlba48 ignoring high 16 bits.
 * @x1: Page of map cache
 * @x2: Page of map cache
 *
 * Return: -1 less than, 1 greater than, 0 if equal.
 */
static int mcache_crng(const void *x1, const void *x2)
{
	uint32_t rng;
	const struct map_cache_entry *r1 = x1;
	const struct map_cache_entry *r2 = x2;
	const uint64_t v1 = le64_to_lba48(r1->tlba, NULL);
	const uint64_t v2 = le64_to_lba48(r2->tlba, NULL);

	if (v1 < v2)
		return -1;

	(void)le64_to_lba48(r2->bval, &rng);
	if (v1 >= (v2+rng))
		return 1;

	return 0;
}

static int bsrch_n(const void *key, const void *base,
		   size_t nmemb, size_t size,
		   int (*compar) (const void *, const void *))
{
	size_t lower, upper, idx;
	const void *p;
	int comparison;

	lower = 0;
	upper = nmemb;
	while (lower < upper)
	{
		idx = (lower + upper) / 2;
		p = (void *) (((const char *) base) + (idx * size));

		comparison = (*compar) (key, p);
		if (comparison < 0)
			upper = idx;
		else if (comparison > 0)
			lower = idx + 1;
		else
			return idx;
	}

	return -1;
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
 * _current_mapping() - Lookup a logical sector address to find the disk LBA
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
		if (addr < znd->md_start)
			found = z_lookup_journal_cache(znd, addr);
		if (!found)
			found = addr;
		goto out;
	}
	if (!found)
		found = z_lookup_key_range(znd, addr);
	if (!found && !nodisc && z_lookup_trim_cache(znd, addr)) {
		goto out;
	}
	if (!found) {
		found = z_lookup_ingress_cache(znd, addr);
		if (found == ~0ul) {
			found = 0ul;
			goto out;
		}
	}
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

static int _common_intersect(struct map_pool *mcache, u64 key, u32 range,
			     struct map_cache_page *mc_pg);


/**
 * _lookup_table_range() - resolve a sector mapping via ZLT mapping
 * @znd: ZDM Instance
 * @addr: Address to resolve (via FWD map).
 * @gfp: Current allocation flags.
 */
static u64 _lookup_table_range(struct zdm *znd, u64 addr, u32 *range,
			       int noio, gfp_t gfp)
{
	struct map_addr maddr;
	struct map_pg *pg;
	u64 tlba = 0;
	const int ahead = (gfp == GFP_ATOMIC) ? 1 : READ_AHEAD;
	const int async = 0;

	map_addr_calc(znd, addr, &maddr);
	pg = get_map_entry(znd, maddr.lut_s, ahead, async, noio, gfp);
	if (pg) {
		ref_pg(pg);
		wait_for_map_pg(znd, pg, gfp);
		if (pg->data.addr) {
			unsigned long flags;
			u64 tgt;
			__le32 delta;
			u32 limit = *range;
			u32 num;
			u32 idx;

			spin_lock_irqsave(&pg->md_lock, flags);
			delta = pg->data.addr[maddr.pg_idx];
			tlba = map_value(znd, delta);
			for (num = 0; num < limit; num++) {
				idx = maddr.pg_idx + num;
				if (idx >= 1024)
					break;
				tgt = map_value(znd, pg->data.addr[idx]);
				if (tlba && tgt != (tlba + num))
					break;
				if (!tlba && tgt)
					break;
			}
			*range = num;
			spin_unlock_irqrestore(&pg->md_lock, flags);
			pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
			clear_bit(IS_READA, &pg->flags);
		} else {
			Z_ERR(znd, "lookup page has no data. retry.");
			*range = 0;
			tlba = ~0ul;
		}
		deref_pg(pg);
		put_map_entry(pg);
	} else {
		*range = 0;
		tlba = ~0ul;
	}
	return tlba;
}


static u64 __map_rng(struct zdm *znd, u64 addr, u32 *range, int disc,
		     int noio, gfp_t gfp)
{
	struct map_cache_page *wpg = NULL;
	u64 found = 0ul;
	unsigned long flags;
	u32 blks = *range;

	if (addr < znd->data_lba) {
		*range = 1;

		if (addr < znd->md_start)
			found = z_lookup_journal_cache(znd, addr);
		if (!found)
			found = addr;
		goto out;
	}
	if (!found) {
		found = z_lookup_key_range(znd, addr);
		if (found)
			*range = 1;
	}

	if (!found) {
		wpg = ZDM_ALLOC(znd, sizeof(*wpg), PG_09, gfp);
		if (!wpg) {
			Z_ERR(znd, "%s: Out of memory.", __func__);
			goto out;
		}
	}

	if (!found) {
		struct map_cache_entry *maps = wpg->maps;
		u64 iadr;
		u64 itgt;
		u32 num;
		u32 offset;
		int isects = 0;

		if (disc) {
			spin_lock_irqsave(&znd->trim_rwlck, flags);
			isects = _common_intersect(znd->trim, addr, blks, wpg);
			spin_unlock_irqrestore(&znd->trim_rwlck, flags);
		}
		if (isects) {
			iadr = le64_to_lba48(maps[ISCT_BASE].tlba, NULL);
			itgt = le64_to_lba48(maps[ISCT_BASE].bval, &num);
			if (addr == iadr) {
				if (blks > num)
					blks = num;
				*range = blks;
				goto out;
			} else if (addr > iadr) {
				num -= addr - iadr;
				if (blks > num)
					blks = num;
				*range = blks;
				goto out;
			} else if (addr < iadr) {
				blks -= iadr - addr;
				*range = blks;
			}
		}

		memset(wpg, 0, sizeof(*wpg));
		spin_lock_irqsave(&znd->in_rwlck, flags);
		isects = _common_intersect(znd->ingress, addr, blks, wpg);
		spin_unlock_irqrestore(&znd->in_rwlck, flags);
		if (isects) {
			iadr = le64_to_lba48(maps[ISCT_BASE].tlba, NULL);
			itgt = le64_to_lba48(maps[ISCT_BASE].bval, &num);
			if (addr == iadr) {
				if (blks > num)
					blks = num;
				*range = blks;
				found = itgt;
				goto out;
			} else if (addr > iadr) {
				offset = addr - iadr;
				itgt += offset;
				num -= offset;

				if (blks > num)
					blks = num;
				*range = blks;
				found = itgt;
				goto out;
			} else if (addr < iadr) {
				blks -= iadr - addr;
				*range = blks;
			}
		}
	}
	if (!found) {
		found = _lookup_table_range(znd, addr, &blks, noio, gfp);
		*range = blks;
		if (!noio && found == ~0ul) {
			Z_ERR(znd, "%s: invalid addr? %llx", __func__, addr);
			dump_stack();
		}
	}
out:
	if (wpg)
		ZDM_FREE(znd, wpg, Z_C4K, PG_09);

	return found;
}

static u64 current_map_range(struct zdm *znd, u64 addr, u32 *range, gfp_t gfp)
{
	const int disc = 1;
	const int noio = 0;
	u64 lba;

	if (*range == 1)
		return current_mapping(znd, addr, gfp);

	lba = __map_rng(znd, addr, range, disc, noio, GFP_ATOMIC);

	return lba;
}

static u64 backref_cache(struct zdm *znd, u64 blba)
{
	struct map_pool *mcache = znd->ingress;
	struct map_cache_entry *maps = mcache->mcd.maps;
	u64 addr = 0ul;
	int idx;

	for (idx = 0; idx < mcache->count; idx++) {
		u32 count;
		u64 bval = le64_to_lba48(maps[idx].bval, &count);

		if (count && bval <= blba && blba < (bval + count)) {
			addr = le64_to_lba48(maps[idx].tlba, NULL)
			     + (blba - bval);
			break;
		}
	}
	return addr;
}

/**
 * gc_sort_lba() - Sort an unsorted map cache page
 * @znd: ZDM instance
 * @mcache: Map cache page.
 *
 * Sort a map cache entry if is sorted.
 * Lock using mutex if not already locked.
 */
static void gc_sort_lba(struct zdm *znd, struct map_cache *postmap)
{
	if (postmap->jcount > 1 && postmap->jsorted < postmap->jcount) {
		struct gc_map_cache_data *mcd = postmap->mcd;
		struct map_cache_entry *base = &mcd->maps[0];

		sort(base, postmap->jcount, sizeof(*base), gc_lba_cmp, NULL);
		postmap->jsorted = postmap->jcount;
	}
}

/**
 * z_lookup_journal_cache() - Scan wb journal entries for addr
 * @znd: ZDM Instance
 * @addr: Address [tLBA] to find.
 * @type: mcache type (MAP or DISCARD cache).
 */
static u64 z_lookup_journal_cache(struct zdm *znd, u64 addr)
{
	u64 found = 0ul;
	struct map_cache_entry *maps = znd->wbjrnl->mcd.maps;
	struct map_cache_entry find;
	unsigned long flags;
	int count = znd->wbjrnl->count;
	int at;

	spin_lock_irqsave(&znd->wbjrnl_rwlck, flags);
	find.tlba = lba48_to_le64(0, addr);
	at = bsrch_n(&find, maps, count, sizeof(find), mcache_crng);
	if (at != -1) {
		u32 nelem;
		u32 flags;
		u64 tlba = le64_to_lba48(maps[at].tlba, &flags);
		u64 bval = le64_to_lba48(maps[at].bval, &nelem);

		if (bval)
			bval += (addr - tlba);

		found = bval;
	}
	spin_unlock_irqrestore(&znd->wbjrnl_rwlck, flags);

	return found;
}

/**
 * z_lookup_ingress_cache_nlck() - Scan mcache entries for addr
 * @znd: ZDM Instance
 * @addr: Address [tLBA] to find.
 * @type: mcache type (MAP or DISCARD cache).
 */
static inline u64 z_lookup_ingress_cache_nlck(struct zdm *znd, u64 addr)
{
	u64 found = 0ul;
	struct map_cache_entry *maps = znd->ingress->mcd.maps;
	struct map_cache_entry find;
	int count = znd->ingress->count;
	int at;

	if (znd->ingress->count != znd->ingress->sorted)
		Z_ERR(znd, " ** NOT SORTED");

	find.tlba = lba48_to_le64(0, addr);
	at = bsrch_n(&find, maps, count, sizeof(find), mcache_crng);
	if (at != -1) {
		u32 nelem;
		u32 flags;
		u64 tlba = le64_to_lba48(maps[at].tlba, &flags);
		u64 bval = le64_to_lba48(maps[at].bval, &nelem);

		if (flags & MCE_NO_ENTRY)
			return found;

		if (bval)
			bval += (addr - tlba);

		found = bval ? bval : ~0ul;
	}

	return found;
}

static inline int z_lookup_trim_cache_nlck(struct zdm *znd, u64 addr)
{
	int found = 0;
	struct map_cache_entry *maps = znd->trim->mcd.maps;
	struct map_cache_entry find;
	int count = znd->trim->count;
	u32 flags;
	int at;

	find.tlba = lba48_to_le64(0, addr);
	at = bsrch_n(&find, maps, count, sizeof(find), mcache_crng);
	if (at != -1) {
		(void)le64_to_lba48(maps[at].tlba, &flags);
		found = (flags & MCE_NO_ENTRY) ? 0 : 1;
	}

	return at != -1;
}

static u64 z_lookup_unused_cache_nlck(struct zdm *znd, u64 addr)
{
	u64 found = 0ul;
	int count = znd->unused->count;
	struct map_cache_entry *maps = znd->unused->mcd.maps;
	struct map_cache_entry find;
	u32 flags;
	int at;

	find.tlba = lba48_to_le64(0, addr);
	at = bsrch_n(&find, maps, count, sizeof(find), mcache_crng);
	if (at != -1) {
		found = le64_to_lba48(maps[at].tlba, &flags);
		if (flags & MCE_NO_ENTRY)
			return 0ul;
	}

	return found;
}

static u64 z_lookup_ingress_cache(struct zdm *znd, u64 addr)
{
	unsigned long flags;
	u64 found;

	spin_lock_irqsave(&znd->in_rwlck, flags);
	found = z_lookup_ingress_cache_nlck(znd, addr);
	spin_unlock_irqrestore(&znd->in_rwlck, flags);

	return found;
}


static int z_lookup_trim_cache(struct zdm *znd, u64 addr)
{
	unsigned long flags;
	int found;

	spin_lock_irqsave(&znd->trim_rwlck, flags);
	found = z_lookup_trim_cache_nlck(znd, addr);
	spin_unlock_irqrestore(&znd->trim_rwlck, flags);

	return found;
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
	unsigned long flags;
	unsigned long mflgs;
	spinlock_t *lock = is_lut ? &znd->mapkey_lock : &znd->ct_lock;

	if (test_bit(R_IN_FLIGHT, &expg->flags))
		return req_flush;

	if (!spin_trylock_irqsave(lock, flags))
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

		spin_lock_irqsave(&expg->md_lock, mflgs);
		if (expg->data.addr) {
			void *pg = expg->data.addr;

			Z_DBG(znd, "** jrnl pg %"PRIx64" data dropped (%"PRIx64
			           ") %04x",
			      expg->lba, expg->last_write,
			      crc16_md(pg, Z_C4K));

			expg->data.addr = NULL;
			__smp_mb();
			init_completion(&expg->event);
			set_bit(IS_ALLOC, &expg->flags);
			ZDM_FREE(znd, pg, Z_C4K, PG_27);
			req_flush = !test_bit(IS_FLUSH, &expg->flags);
			atomic_dec(&znd->incore);
		}
		spin_unlock_irqrestore(&expg->md_lock, mflgs);
	} else if (test_bit(IS_DROPPED, &expg->flags)) {
		unsigned long zflgs;

		if (!spin_trylock_irqsave(&znd->zlt_lck, zflgs))
			goto out;
		del_htbl_entry(znd, expg);

		spin_lock_irqsave(&expg->md_lock, mflgs);
		if (test_and_clear_bit(IS_LAZY, &expg->flags)) {
			clear_bit(IS_DROPPED, &expg->flags);
			clear_bit(DELAY_ADD, &expg->flags);
			if (expg->data.addr) {
				void *pg = expg->data.addr;

				expg->data.addr = NULL;
				__smp_mb();
				init_completion(&expg->event);
				set_bit(IS_ALLOC, &expg->flags);
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
		if (dropped && expg->crc_pg) {
			deref_pg(expg->crc_pg);
			expg->crc_pg = NULL;
		}
		spin_unlock_irqrestore(&expg->md_lock, mflgs);
		if (dropped)
			ZDM_FREE(znd, expg, sizeof(*expg), KM_20);
		spin_unlock_irqrestore(&znd->zlt_lck, zflgs);
	}

out:
	spin_unlock_irqrestore(lock, flags);
	return req_flush;
}

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
	unsigned long flags;
	LIST_HEAD(movelist);

	wset = ZDM_CALLOC(znd, sizeof(*wset), MAX_WSET, KM_19, GFP_KERNEL);
	if (!wset) {
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
		return -ENOMEM;
	}

	spin_lock_irqsave(&znd->lzy_lck, flags);
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

		if (test_bit(WB_RE_CACHE, &expg->flags)) {
			if (!expg->data.addr) {
				if (entries < MAX_WSET) {
					ref_pg(expg);
					wset[entries] = expg;
					entries++;
					clear_bit(WB_RE_CACHE, &expg->flags);
				}
			} else {
				set_bit(IS_DIRTY, &expg->flags);
				clear_bit(IS_FLUSH, &expg->flags);
				clear_bit(WB_RE_CACHE, &expg->flags);
			}
		}

		/*
		 * Migrage pg to zltlst list
		 */
		if (test_bit(DELAY_ADD, &expg->flags)) {
			if (!test_bit(IN_ZLT, &expg->flags)) {
				list_del(&expg->lazy);
				znd->in_lzy--;
				clear_bit(IS_LAZY, &expg->flags);
				clear_bit(IS_DROPPED, &expg->flags);
				clear_bit(DELAY_ADD, &expg->flags);
				set_bit(IN_ZLT, &expg->flags);
				list_add(&expg->zltlst, &movelist);
				znd->in_zlt++;
			} else {
				Z_ERR(znd, "** ZLT double add? %"PRIx64,
					expg->lba);
			}
		} else {
			/*
			 * Delete page
			 */
			if (!test_bit(IN_ZLT, &expg->flags) &&
			    !test_bit(R_IN_FLIGHT, &expg->flags) &&
			     getref_pg(expg) == 0 &&
			     test_bit(IS_FLUSH, &expg->flags) &&
			     test_bit(IS_DROPPED, &expg->flags) &&
			     is_expired_msecs(expg->age, msecs))
				want_flush |= pg_delete(znd, expg);
		}
		expg = _tpg;
		_tpg = list_next_entry(expg, lazy);
	}

out:
	spin_unlock_irqrestore(&znd->lzy_lck, flags);

	if (entries > 0)
		_pool_read(znd, wset, entries);

	if (!list_empty(&movelist))
		zlt_pool_splice(znd, &movelist);

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
		set_bit(WB_DIRECT, &expg->flags);
		if (test_and_clear_bit(IN_WB_JOURNAL, &expg->flags))
			set_bit(WB_RE_CACHE, &expg->flags);
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
	unsigned long flags;

	spin_lock_irqsave(&znd->zlt_lck, flags);
	znd->flush_age = jiffies_64;
	if (list_empty(&znd->zltpool))
		goto out;

	expg = list_last_entry(&znd->zltpool, typeof(*expg), zltlst);
	if (!expg || &expg->zltlst == (&znd->zltpool))
		goto out;

	_tpg = list_prev_entry(expg, zltlst);
	while (&expg->zltlst != &znd->zltpool) {
		ref_pg(expg);
		if (!test_bit(R_IN_FLIGHT, &expg->flags)) {
			if (!test_bit(IS_DIRTY, &expg->flags))
				set_bit(IS_FLUSH, &expg->flags);
			if (wb_toggle)
				pg_toggle_wb_journal(znd, expg);
		}
		deref_pg(expg);
		expg = _tpg;
		_tpg = list_prev_entry(expg, zltlst);
	}

out:
	spin_unlock_irqrestore(&znd->zlt_lck, flags);
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
	unsigned long flags;

	spin_lock_irqsave(&znd->lzy_lck, flags);
	expg = list_first_entry_or_null(&znd->lzy_pool, typeof(*expg), lazy);
	if (!expg || (&expg->lazy == &znd->lzy_pool))
		goto out;

	_tpg = list_next_entry(expg, lazy);
	while (&expg->lazy != &znd->lzy_pool) {
		ref_pg(expg);
		if (!test_bit(R_IN_FLIGHT, &expg->flags)) {
			if (!test_bit(IS_DIRTY, &expg->flags))
				set_bit(IS_FLUSH, &expg->flags);
			if (wb_toggle)
				pg_toggle_wb_journal(znd, expg);
		}
		deref_pg(expg);
		expg = _tpg;
		_tpg = list_next_entry(expg, lazy);
	}

out:
	spin_unlock_irqrestore(&znd->lzy_lck, flags);
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

	MutexLock(&znd->pool_mtx);
	if (manage_lazy_activity(znd)) {
		if (!test_bit(DO_FLUSH, &znd->flags))
			Z_ERR(znd, "Extra flush [MD Cache too small]");
		set_bit(DO_FLUSH, &znd->flags);
	}
	mutex_unlock(&znd->pool_mtx);

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
	if (test_bit(DO_FLUSH, &znd->flags)) {
		err = z_mapped_sync(znd);
		if (err) {
			Z_ERR(znd, "Uh oh. z_mapped_sync -> %d", err);
			goto out;
		}
		md_handle_crcs(znd);
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

	if (znd->ingress->count > MC_MOVE_SZ || znd->unused->count > MC_MOVE_SZ)
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);

	if (znd->trim->count > MC_HIGH_WM)
		unmap_deref_chunk(znd, MC_HIGH_WM, 0, GFP_KERNEL);

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

	if (test_and_clear_bit(DO_SYNC, &znd->flags))
		sync = 1;
	else if (test_bit(DO_FLUSH, &znd->flags))
		sync = 1;

	if (sync || test_and_clear_bit(DO_MEMPOOL, &znd->flags)) {
		int pool_size = MZ_MEMPOOL_SZ * 5;

		/**
		 * Trust our cache miss algo
		 */
		pool_size = MZ_MEMPOOL_SZ * 4;
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
	if (!err) {
		err = do_move_map_cache_to_table(znd);
		if (err == -EAGAIN || err == -EBUSY) {
			err = 0;
			set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		}
	}

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

static struct bio *next_bio(struct zdm *znd, struct bio *bio, gfp_t gfp,
			unsigned int nr_pages, struct bio_set *bs)
{
	struct bio *new = bio_alloc_bioset(gfp, nr_pages, bs);
#if ENABLE_SEC_METADATA
	sector_t sector;
#endif

	if (bio) {
		bio_chain(bio, new);
#if ENABLE_SEC_METADATA
		if (znd->meta_dst_flag == DST_TO_SEC_DEVICE) {
			sector = bio->bi_iter.bi_sector;
			bio->bi_bdev = znd_get_backing_dev(znd, &sector);
			bio->bi_iter.bi_sector = sector;
		}
#endif
		submit_bio(bio);
	}

	return new;
}

static unsigned int bio_add_km(struct bio *bio, void *kmem, int pgs)
{
	unsigned int added = 0;
	unsigned int len = pgs << PAGE_SHIFT;
	struct page *pg;
	unsigned long addr = (unsigned long)kmem;
	if (addr && bio) {
		pg = virt_to_page((void*)addr);
		if (pg) {
			added = bio_add_page(bio, pg, len, 0);
			if (added != len)
				pr_err("Failed to add %u to bio\n", len);
		} else {
			pr_err("Invalid pg ? \n");
		}
	} else {
		pr_err("Invalid addr / bio ? \n");
	}
	return added;
}

static int bio_add_wp(struct bio *bio, struct zdm *znd, int idx)
{
	int len;
	struct meta_pg *wpg = &znd->wp[idx];

	znd->bmkeys->wp_crc[idx] = crc_md_le16(wpg->wp_alloc, Z_CRC_4K);
	len = bio_add_km(bio, wpg->wp_alloc, 1);
	znd->bmkeys->zf_crc[idx] = crc_md_le16(wpg->zf_est, Z_CRC_4K);
	len += bio_add_km(bio, wpg->zf_est, 1);

	return len;
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


struct on_sync {
	u64 lba;
	struct map_cache_page **pgs;
	int cpg;
	int npgs;
	int cached;
	int off;
	int discards;
	int maps;
	int unused;
	int n_writes;
	int n_blocks;
};

#if ENABLE_SEC_METADATA
void znd_bio_copy(struct zdm *znd, struct bio *bio, struct bio *clone)
{
	struct bio_vec bv;
	struct bvec_iter iter;

	clone->bi_iter.bi_sector = bio->bi_iter.bi_sector;
	clone->bi_bdev = znd->meta_dev->bdev;
	clone->bi_opf = bio->bi_opf;
	clone->bi_iter.bi_size = bio->bi_iter.bi_size;
	clone->bi_vcnt = 0;

	bio_for_each_segment(bv, bio, iter)
		clone->bi_io_vec[clone->bi_vcnt++] = bv;
}
#endif
#if ENABLE_SEC_METADATA
static struct bio *add_mpool(struct zdm *znd, struct bio *bio,
			     struct map_pool *mpool, struct on_sync *osc,
				struct bio *clone)
#else
static struct bio *add_mpool(struct zdm *znd, struct bio *bio,
			     struct map_pool *mpool, struct on_sync *osc)
#endif
{
	const gfp_t gfp = GFP_KERNEL;
	int idx;

	for (idx = 0; idx < dm_div_up(mpool->count, Z_MAP_MAX); idx++) {
		struct map_cache_page *pg;
		int count = mpool->count - (idx * Z_MAP_MAX);

		pg = ZDM_ALLOC(znd, sizeof(*pg), PG_09, gfp);
		if (!pg) {
			Z_ERR(znd, "%s: alloc pool page.", __func__);
			goto out;
		}
		memcpy(pg->maps, mpool->mcd.maps, sizeof(pg->maps));
		pg->header.bval = lba48_to_le64(count, 0);
		pg->header.tlba = 0ul;
		znd->bmkeys->crcs[idx+osc->off] = crc_md_le16(pg, Z_CRC_4K);

		bio_add_km(bio, pg, 1);
#if ENABLE_SEC_METADATA
		if (znd->meta_dst_flag == DST_TO_BOTH_DEVICE)
			znd_bio_copy(znd, bio, clone);
#endif
		if (osc->cpg < osc->npgs) {
			osc->pgs[osc->cpg] = pg;
			osc->cpg++;
		}
		osc->cached++;
		osc->lba++;

		switch (mpool->isa) {
		case IS_TRIM:
			osc->discards++;
			break;
		case IS_INGRESS:
			osc->maps++;
			break;
		case IS_UNUSED:
			osc->unused++;
			break;
		default:
			break;
		}

		if (osc->cached == BIO_MAX_PAGES) {
			bio = next_bio(znd, bio, BIO_MAX_PAGES, gfp,
					znd->bio_set);
			if (!bio) {
				Z_ERR(znd, "%s: alloc bio.", __func__);
				goto out;
			}

			bio->bi_iter.bi_sector = osc->lba << Z_SHFT4K;
			bio->bi_bdev = znd->dev->bdev;
			bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
			bio->bi_iter.bi_size = 0;
#if ENABLE_SEC_METADATA
			if (znd->meta_dst_flag == DST_TO_BOTH_DEVICE) {
				clone = next_bio(znd, clone, BIO_MAX_PAGES, gfp,
						znd->bio_set);
				znd_bio_copy(znd, bio, clone);
			}
#endif

			osc->n_writes++;
			osc->n_blocks += osc->cached;
			osc->cached = 0;
		}
	}
	osc->off += idx;

out:
	return bio;
}


/**
 * z_mapped_sync() - Write cache entries and bump superblock generation.
 * @znd: ZDM instance
 */
static int z_mapped_sync(struct zdm *znd)
{
	struct md_journal *jrnl = &znd->jrnl;
        struct on_sync osc;
	int rc = 1;
	int idx = 0;
	int more_data = 1;
	struct bio *bio = NULL;
	u64 generation = next_generation(znd);
	u64 modulo = CACHE_COPIES;
	u64 incr = MAX_SB_INCR_SZ;
	const gfp_t gfp = GFP_KERNEL;
#if ENABLE_SEC_METADATA
	sector_t sector;
	struct bio *clone = NULL;
#endif

	memset(&osc, 0, sizeof(osc));
	osc.npgs = SYNC_MAX;
	osc.pgs = ZDM_CALLOC(znd, sizeof(*osc.pgs), osc.npgs, KM_18, gfp);
	if (!osc.pgs) {
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
		return -ENOMEM;
	}

	/* write dirty WP/ZF_EST blocks */
	osc.lba = WP_ZF_BASE;
	for (idx = 0; idx < znd->gz_count; idx++) {
		struct meta_pg *wpg = &znd->wp[idx];

		if (test_bit(IS_DIRTY, &wpg->flags)) {
			if (bio)
				osc.n_writes++, osc.n_blocks += 2;

			bio = next_bio(znd, bio, gfp, 2, znd->bio_set);
			if (!bio) {
				Z_ERR(znd, "%s: alloc bio.", __func__);
				rc = -ENOMEM;
				goto out;
			}
			bio->bi_iter.bi_sector = osc.lba << Z_SHFT4K;
			bio->bi_bdev = znd->dev->bdev;
			bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
			bio->bi_iter.bi_size = 0;
			if (!bio_add_wp(bio, znd, idx)) {
				rc = -EIO;
				goto out;
			}
#if ENABLE_SEC_METADATA
			if (znd->meta_dst_flag == DST_TO_BOTH_DEVICE) {
				clone = next_bio(znd, clone, gfp, 2,
								znd->bio_set);
				if (!clone) {
					Z_ERR(znd, "%s: alloc bio.", __func__);
					rc = -ENOMEM;
					bio_put(bio);
					goto out;
				}
				znd_bio_copy(znd, bio, clone);
			}
#endif
			clear_bit(IS_DIRTY, &wpg->flags);

			Z_DBG(znd, "%d# -- WP: %04x | ZF: %04x",
			      idx, znd->bmkeys->wp_crc[idx],
				   znd->bmkeys->zf_crc[idx]);
		}
		osc.lba += 2;
	}
	osc.lba = (generation % modulo) * incr;
	if (osc.lba == 0)
		osc.lba++;
	if (bio)
		osc.n_writes++, osc.n_blocks += 2;

	bio = next_bio(znd, bio, gfp, BIO_MAX_PAGES, znd->bio_set);
	if (!bio) {
		Z_ERR(znd, "%s: alloc bio.", __func__);
		rc = -ENOMEM;
		goto out;
	}
	bio->bi_iter.bi_sector = osc.lba << Z_SHFT4K;
	bio->bi_bdev = znd->dev->bdev;
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_size = 0;

	osc.cached = jrnl->size >> 10;
	if (!bio_add_km(bio, jrnl->wb, osc.cached)) {
		rc = -EIO;
		Z_ERR(znd, "%s: bio_add_km -> %d", __func__, rc);
		goto out;
	}

#if ENABLE_SEC_METADATA
	if (znd->meta_dst_flag == DST_TO_BOTH_DEVICE) {
		clone = next_bio(znd, clone, gfp, BIO_MAX_PAGES, znd->bio_set);
		if (!clone) {
			Z_ERR(znd, "%s: alloc bio.", __func__);
			rc = -ENOMEM;
			bio_put(bio);
			goto out;
		}
		znd_bio_copy(znd, bio, clone);
	}
#endif
	znd->bmkeys->generation = cpu_to_le64(generation);
	znd->bmkeys->wb_blocks = cpu_to_le32(osc.cached);
	znd->bmkeys->wb_next = cpu_to_le32(jrnl->wb_next);
	znd->bmkeys->wb_crc32 = crc32c_le32(~0u, jrnl->wb, jrnl->size);
	znd->bmkeys->gc_resv = cpu_to_le32(znd->z_gc_resv);
	znd->bmkeys->meta_resv = cpu_to_le32(znd->z_meta_resv);

	osc.lba += osc.cached;

	/* for znd->ingress, znd->trim, znd->unused, znd->wbjrnl allocate
	 * pages and copy data ....
	 */
#if ENABLE_SEC_METADATA
	bio = add_mpool(znd, bio, znd->ingress, &osc, clone);
	bio = add_mpool(znd, bio, znd->trim, &osc, clone);
	bio = add_mpool(znd, bio, znd->unused, &osc, clone);
#else
	bio = add_mpool(znd, bio, znd->ingress, &osc);
	bio = add_mpool(znd, bio, znd->trim, &osc);
	bio = add_mpool(znd, bio, znd->unused, &osc);
#endif

	znd->bmkeys->md_crc = crc_md_le16(znd->md_crcs, Z_CRC_4K << 1);
	znd->bmkeys->discards = cpu_to_le16(osc.discards);
	znd->bmkeys->maps = cpu_to_le16(osc.maps);
	znd->bmkeys->unused = cpu_to_le16(osc.unused);
	znd->bmkeys->n_crcs = cpu_to_le16(osc.maps + osc.discards + osc.unused);
	znd->bmkeys->crc32 = 0;
	znd->bmkeys->crc32 = cpu_to_le32(crc32c(~0u, znd->bmkeys, Z_CRC_4K));
	if (osc.cached < (BIO_MAX_PAGES - 3)) {
		bio_add_km(bio, znd->z_sballoc, 1);
		bio_add_km(bio, znd->md_crcs, 2);
		more_data = 0;
	}

#if ENABLE_SEC_METADATA
	if (znd->meta_dst_flag == DST_TO_BOTH_DEVICE)
		znd_bio_copy(znd, bio, clone);
#endif
	if (unlikely(more_data)) {
		bio = next_bio(znd, bio, gfp, 3, znd->bio_set);
		bio->bi_iter.bi_sector = osc.lba << Z_SHFT4K;
		bio->bi_bdev = znd->dev->bdev;
		bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
		bio->bi_iter.bi_size = 0;

		osc.n_writes++, osc.n_blocks += osc.cached;
		osc.cached = 0;

		bio_add_km(bio, znd->z_sballoc, 1);
		bio_add_km(bio, znd->md_crcs, 2);
#if ENABLE_SEC_METADATA
		if (znd->meta_dst_flag == DST_TO_BOTH_DEVICE) {
			clone = next_bio(znd, clone, gfp, 3, znd->bio_set);
			znd_bio_copy(znd, bio, clone);
		}
#endif
	}
	osc.cached += 3;

	if (bio) {
		bio_set_op_attrs(bio, REQ_OP_WRITE, WRITE_FLUSH_FUA);
#if ENABLE_SEC_METADATA
		if (znd->meta_dst_flag == DST_TO_SEC_DEVICE) {
			sector = bio->bi_iter.bi_sector;
			bio->bi_bdev = znd_get_backing_dev(znd, &sector);
			bio->bi_iter.bi_sector = sector;
		}
		if (clone && znd->meta_dst_flag == DST_TO_BOTH_DEVICE) {
			znd_bio_copy(znd, bio, clone);
			rc = submit_bio_wait(clone);
			bio_put(clone);
		}
#endif
		rc = submit_bio_wait(bio);
		osc.n_writes++, osc.n_blocks += osc.cached;
		bio_put(bio);
		clear_bit(DO_FLUSH, &znd->flags);
		mark_clean_flush(znd, 1);
	}

	for (idx = 0; idx < osc.cpg; idx++)
		ZDM_FREE(znd, osc.pgs[idx], Z_C4K, PG_09);

	Z_DBG(znd, "Sync/Flush: %d writes / %d blocks written [gen %"PRIx64"]",
		osc.n_writes, osc.n_blocks, generation);
out:
	if (osc.pgs)
		ZDM_FREE(znd, osc.pgs, sizeof(*osc.pgs) * osc.npgs, KM_18);

	return rc;
}

/**
 * zoned_personality() - Update zdstart value from superblock
 * @znd: ZDM instance
 * @sblock: ZDM superblock.
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
	u64 *data = ZDM_ALLOC(znd, Z_C4K, PG_10, GFP_KERNEL);

	if (!data) {
		Z_ERR(znd, "No memory for finding generation ..");
		return 0;
	}
	if (lba == 0)
		lba++;

	lba += WB_JRNL_BLKS;
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
			if (count > 16)
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
	u64 *data = ZDM_ALLOC(znd, Z_C4K, PG_11, GFP_KERNEL);

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
 * @s_lba: LBA where sync data starts (in front of the super block).
 */
static u64 mcache_greatest_gen(struct zdm *znd, int use_wq, u64 *sb, u64 *s_lba)
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
		if (s_lba)
			*s_lba = gen_lba[pick];
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
 *
 * FIXME ... read into znd->ingress and znd->trim and ...
 *
 * do_load_cache() - Read a series of map cache blocks to restore from disk.
 * @znd: ZDM Instance
 * @type: Map cache list (MAP, DISCARD, JOURNAL .. )
 * @lba: Starting LBA for reading
 * @idx: Saved/Expected CRC of block.
 * @wq: True when I/O needs to use worker thread.
 */
static int do_load_cache(struct zdm *znd, int type, u64 lba, int idx, int wq)
{
	unsigned long flags;
	struct map_cache_page *work_pg = NULL;
	int rc = -ENOMEM;
	const gfp_t gfp = GFP_KERNEL;
	u32 count;
	__le16 crc;
	int blks = 1;

	work_pg = ZDM_ALLOC(znd, sizeof(*work_pg), PG_09, gfp);
	if (!work_pg) {
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
		goto out;
	}

	rc = read_block(znd->ti, DM_IO_KMEM, work_pg, lba, blks, wq);
	if (rc) {
		Z_ERR(znd, "%s: pg -> %" PRIu64
			   " [%d blks] %p -> %d",
		      __func__, lba, blks, work_pg, rc);
		goto out;
	}
	crc = crc_md_le16(work_pg, Z_CRC_4K);
	if (crc != znd->bmkeys->crcs[idx]) {
		rc = -EIO;
		Z_ERR(znd, "%s: bad crc %" PRIu64,  __func__, lba);
		goto out;
	}
	(void)le64_to_lba48(work_pg->header.bval, &count);

	switch (type) {
	case IS_INGRESS:
		spin_lock_irqsave(&znd->in_rwlck, flags);
		if (count) {
			struct map_cache_entry *maps = maps = work_pg->maps;
			struct map_pool *mp = &znd->in[1];

			if (znd->ingress == &znd->in[1])
				mp = &znd->in[0];
			mp->sorted = mp->count = 0;
			rc = do_sort_merge(mp, znd->ingress, maps, count, 0);
			znd->ingress = mp;
			__smp_mb();
			if (rc)
				Z_ERR(znd, "SortMerge failed: %d [%d]", rc,
				      __LINE__);
			if (znd->ingress->count > MC_MOVE_SZ)
				set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		}
		spin_unlock_irqrestore(&znd->in_rwlck, flags);
		break;
	case IS_TRIM:
		spin_lock_irqsave(&znd->trim_rwlck, flags);
		if (count) {
			struct map_cache_entry *maps = maps = work_pg->maps;
			struct map_pool *mp = &znd->trim_mp[1];

			if (znd->trim == &znd->trim_mp[1])
				mp = &znd->trim_mp[0];
			mp->sorted = mp->count = 0;
			rc = do_sort_merge(mp, znd->trim, maps, count, 0);
			znd->trim = mp;
			__smp_mb();
			if (rc)
				Z_ERR(znd, "DSortMerge failed: %d [%d]", rc,
				      __LINE__);

			rc = 0;
		}
		if (znd->trim->count > MC_MOVE_SZ)
			set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		spin_unlock_irqrestore(&znd->trim_rwlck, flags);
		break;
	case IS_UNUSED:
		spin_lock_irqsave(&znd->unused_rwlck, flags);
		if (count) {
			struct map_pool *mp = &znd->_use[1];
			struct map_cache_entry *maps;

			maps = work_pg->maps;
			if (znd->unused == &znd->_use[1])
				mp = &znd->_use[0];
			mp->sorted = mp->count = 0;
			rc = do_sort_merge(mp, znd->unused, maps, count, 0);
			znd->unused = mp;
			__smp_mb();
			if (rc)
				Z_ERR(znd, "USortMerge failed: %d [%d]", rc,
				      __LINE__);
			if (znd->unused->count > MC_MOVE_SZ)
				set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		}
		spin_unlock_irqrestore(&znd->unused_rwlck, flags);
		break;
	default:
		rc = -EFAULT;
	}

out:
	if (work_pg)
		ZDM_FREE(znd, work_pg, Z_C4K, PG_09);
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
	return do_load_cache(znd, IS_INGRESS, lba, idx, wq);
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
	return do_load_cache(znd, IS_TRIM, lba, idx, wq);
}

/**
 * do_load_unused_cache() - Read a series of map cache blocks to restore from disk.
 * @znd: ZDM Instance
 * @lba: Starting LBA for reading
 * @idx: Saved/Expected CRC of block.
 * @wq: True when I/O needs to use worker thread.
 */
static int do_load_unused_cache(struct zdm *znd, u64 lba, int idx, int wq)
{
	return do_load_cache(znd, IS_UNUSED, lba, idx, wq);
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
	gfp_t gfp = GFP_KERNEL;
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

	/* read WB Journal map */
	nblks = le32_to_cpu(znd->bmkeys->wb_blocks);
	jrnl->size = nblks << 10;
	rc = read_block(ti, DM_IO_KMEM, jrnl->wb, lba, nblks, wq);
	if (rc)
		goto out;

	lba += nblks;
	jrnl->wb_next = le32_to_cpu(znd->bmkeys->wb_next);
	if (znd->bmkeys->wb_crc32 != crc32c_le32(~0u, jrnl->wb, jrnl->size)) {
		rc = -EIO;
		Z_ERR(znd, "WB Journal corrupt.");
		goto out;
	}

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

	/* read in unused cache */
	for (idx = 0; idx < le16_to_cpu(znd->bmkeys->unused); idx++) {
		rc = do_load_unused_cache(znd, lba++, jcount++, wq);
		if (rc)
			goto out;
	}

	/* skip re-read of superblock */
	if (lba == sblba)
		lba++;

	/* read in CRC pgs */
	rc = read_block(ti, DM_IO_KMEM, znd->md_crcs, lba, 2, wq);
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

	for (idx = 0; idx < jrnl->size; idx++) {
		u64 blba;
		u64 tlba;

		if (jrnl->wb[idx] == MZTEV_UNUSED)
			continue;
		if (jrnl->wb[idx] == MZTEV_NF)
			continue;

		blba = idx + WB_JRNL_BASE;
		tlba = le32_to_cpu(jrnl->wb[idx]);

		z_mapped_addmany(znd, tlba, blba, 1, gfp);
	}

	for (idx = 0; idx < jrnl->size; idx++) {
		const int async = 0;
		const int ra = READ_AHEAD;
		const int noio = 0;
		static struct map_pg *expg;
		u64 tlba;

		if (jrnl->wb[idx] == MZTEV_UNUSED)
			continue;
		if (jrnl->wb[idx] == MZTEV_NF)
			continue;

		tlba = le32_to_cpu(jrnl->wb[idx]);
		expg = get_map_entry(znd, tlba, ra, async, noio, gfp);
		if (expg) {
			set_bit(IN_WB_JOURNAL, &expg->flags);
			expg->last_write = idx + WB_JRNL_BASE;
			pg_toggle_wb_journal(znd, expg);
			put_map_entry(expg);
		}
	}

out:
	put_io_vcache(znd, io_vcache);
	mutex_unlock(&znd->vcio_lock);
	return rc;
}

static int mpool_try_merge(struct map_pool *mcache, int entry, u64 tlba,
		       u32 num, u64 blba)
{
	struct map_cache_entry *maps = mcache->mcd.maps;
	u32 nelem;
	u32 flags;
	u64 addr = le64_to_lba48(maps[entry].tlba, &flags);
	u64 bval = le64_to_lba48(maps[entry].bval, &nelem);
	int rc = 0;

	if (flags & (MCE_NO_MERGE|MCE_NO_ENTRY))
		goto out;

	if ((num + nelem) < EXTENT_CEILING) {
		u64 extent = (bval ? bval + nelem : 0);

		if ((addr + nelem) == tlba && blba == extent) {
			nelem += num;
			maps[entry].bval = lba48_to_le64(nelem, bval);
			rc = 1;
			goto out;
		}
	}

out:
	return rc;
}

static int mp_insert(struct map_pool *mcache, u64 tlba, u32 flg,
			u32 num, u64 blba)
{
	struct map_cache_entry *maps = mcache->mcd.maps;
	int top = mcache->count;
	u64 prev;
	int rc = 0;

	if (top < mcache->size) {
		int mmrg = 1;

		if (flg & MCE_NO_ENTRY) {
			pr_err("mp_insert() no entry flag invalid!!\n");
			dump_stack();
		}

		if (top == 0 || (flg & MCE_NO_MERGE))
			mmrg = 0;
		if (mmrg && mpool_try_merge(mcache, top - 1, tlba, num, blba)) {
			rc = 1;
			goto out;
		}

		maps[top].tlba = lba48_to_le64(flg, tlba);
		maps[top].bval = lba48_to_le64(num, blba);
		if (top == 0) {
			++mcache->sorted, ++mcache->count;
		} else {
			prev = le64_to_lba48(maps[top - 1].tlba, NULL);
			if (prev > tlba)
				goto out;
			if (mcache->sorted == mcache->count)
				++mcache->sorted, ++mcache->count;
		}
		rc = 1;
		goto out;
	}
	pr_err("* mp_insert failed %d/%d. -- %llx\n",
		mcache->count, mcache->size, tlba);
	dump_stack();

out:
	if (mcache->count != mcache->sorted)
		pr_err(" ** NOT SORTED %d/%d\n", mcache->sorted, mcache->count);

	return rc; /* not added -- out of space */
}

static int mpool_split(struct map_cache_entry *eval, u64 tlba, u32 num,
		       u64 blba, struct map_cache_entry *cache)
{
	u32 nelem;
	u32 flags;
	u64 addr = le64_to_lba48(eval->tlba, &flags);
	u64 bval = le64_to_lba48(eval->bval, &nelem);

	if (num == 0) {
		pr_err("%s: 0'd split? %llx/%u -> %llx", __func__,
			tlba, num, blba);
		dump_stack();
	}


	if (addr < tlba) {
		u32 delta = tlba - addr;

		if (nelem < delta) {
			pr_err("Split does not overlap? E:%u | D:%u\n",
				nelem, delta);
			delta = nelem;
		}

		cache[MC_HEAD].tlba = lba48_to_le64(0, addr);
		cache[MC_HEAD].bval = lba48_to_le64(delta, bval);
		nelem -= delta;
		addr += delta;
		if (bval)
			bval += delta;
	} else if (addr > tlba) {
		u32 delta = addr - tlba;

		if (num < delta) {
			pr_err("Split does not overlap? N:%u / D:%u\n", num, delta);
			delta = num;
		}
		cache[MC_HEAD].tlba = lba48_to_le64(0, tlba);
		cache[MC_HEAD].bval = lba48_to_le64(delta, blba);
		num -= delta;
		tlba += delta;
		if (blba)
			blba += delta;
	}

	if (num >= nelem) {
		num -= nelem;

		/* note: Intersect will be updated by caller */
		cache[MC_INTERSECT].tlba = lba48_to_le64(0, addr);
		cache[MC_INTERSECT].bval = lba48_to_le64(nelem, bval);
		if (num) {
			tlba += nelem;
			if (blba)
				blba += nelem;
			cache[MC_TAIL].tlba = lba48_to_le64(0, tlba);
			cache[MC_TAIL].bval = lba48_to_le64(num, blba);
		}
	} else {
		nelem -= num;
		cache[MC_INTERSECT].tlba = lba48_to_le64(0, addr);
		cache[MC_INTERSECT].bval = lba48_to_le64(num, bval);
		addr += num;
		if (bval)
			bval += num;
		if (nelem) {
			cache[MC_TAIL].tlba = lba48_to_le64(0, addr);
			cache[MC_TAIL].bval = lba48_to_le64(nelem, bval);
		}
	}

	return 0;
}

static int do_sort_merge(struct map_pool *to, struct map_pool *src,
			 struct map_cache_entry *chng, int nchgs, int drop)
{
	struct map_cache_entry *s_map = src->mcd.maps;
	u64 m_addr = ~0u;
	u64 m_lba = 0;
	u32 m_num = 0;
	u32 m_flg = 0;
	int m_idx = 0;
	u64 s_addr = ~0u;
	u64 s_lba = 0;
	u32 s_flg = 0;
	u32 s_num = 0;
	int s_idx = 0;
	int err = 0;

	do {
		while (s_idx < src->count) {
			s_addr = le64_to_lba48(s_map[s_idx].tlba, &s_flg);
			s_lba = le64_to_lba48(s_map[s_idx].bval, &s_num);
			if (s_flg & MCE_NO_ENTRY)
				s_addr = ~0ul;
			else if (s_addr)
				break;
			s_idx++;
		}
		while (m_idx < nchgs) {
			m_addr = le64_to_lba48(chng[m_idx].tlba, &m_flg);
			m_lba = le64_to_lba48(chng[m_idx].bval, &m_num);
			if (m_addr)
				break;
			m_idx++;
		}
		if (s_idx >= src->count)
			s_addr = ~0ul;
		if (m_idx >= nchgs)
			m_addr = ~0ul;

		if (s_addr == ~0ul && m_addr == ~0ul)
			break;

		if (s_addr == m_addr) {
			int add = 1;

			if (m_flg & MCE_NO_ENTRY)
				add = 0;
			if (add && !mp_insert(to, m_addr, m_flg, m_num, m_lba)) {
				pr_err("Failed to (overwrite) insert %llx\n",
				       m_addr);
				err = -EIO;
				goto out;
			}
			m_idx++;
			s_idx++;
		} else if (s_addr < m_addr) {
			if (s_num && !(s_flg & MCE_NO_ENTRY) &&
			    !mp_insert(to, s_addr, s_flg, s_num, s_lba)) {
				pr_err("Failed to (cur) insert %llx\n", m_addr);
				err = -EIO;
				goto out;
			}
			s_idx++;
		} else {
			if (m_num && !(m_flg & MCE_NO_ENTRY) &&
			    !mp_insert(to, m_addr, m_flg, m_num, m_lba)) {
				pr_err("Failed to (new) insert %llx\n", m_addr);
				err = -EIO;
				goto out;
			}
			m_idx++;
		}
	} while (s_idx < src->count || m_idx < nchgs);

out:
	return err;
}

static int _common_intersect(struct map_pool *mcache, u64 key, u32 range,
			     struct map_cache_page *mc_pg)
{
	struct map_cache_entry *maps = mcache->mcd.maps;
	struct map_cache_entry *out = mc_pg->maps;
	const ssize_t mcesz = sizeof(*out);
	int avail = ARRAY_SIZE(mc_pg->maps);
	int count = 0;
	int fill = ISCT_BASE;
	int idx;

	for (idx = 0; idx < mcache->count; idx++) {
		u32 melem;
		u32 flags;
		u64 addr = le64_to_lba48(maps[idx].tlba, &flags);

		if (flags & MCE_NO_ENTRY)
			continue;

		(void)le64_to_lba48(maps[idx].bval, &melem);
		if (key < (addr+melem) && addr < (key + range)) {
			if (fill < avail) {
				flags |= MCE_NO_MERGE;
				maps[idx].tlba = lba48_to_le64(flags, addr);
				memcpy(&out[fill], &maps[idx], mcesz);
			}
			fill += MC_SKIP;
			count++;
		}
	}
	return count;
}

static int journal_intersect(struct zdm *znd, u64 key, u32 range)
{
	return _common_intersect(znd->wbjrnl, key, range, &znd->wbjrnl_pg);
}

static int __mrg_splt(struct zdm *znd, bool issue_unused,
		      struct map_cache_page *mc_pg, int entries,
		      u64 addr, u32 count, u64 target, gfp_t gfp)
{
	int idx;
	u64 lba_new = target;
	u64 unused;
	u64 lba_was;
	int in_use = ISCT_BASE + (entries * MC_SKIP);
	struct map_cache_entry *mce = mc_pg->maps;

	for (idx = ISCT_BASE; idx < in_use; idx += MC_SKIP) {
		struct map_cache_entry cache[3];
		u32 decr = count;

		memset(cache, 0, sizeof(cache));
		mpool_split(&mce[idx], addr, count, target, cache);

		/* head:       *before* addr */
		/* intersect:  *includes* addr */
		/* tail:       *MAYBE* addr *OR* mce */

		/* copy cache* entries back to mc_pg */
		mce[idx - 1] = cache[MC_HEAD];

		/* Check: if addr was before isect entry */
		unused = le64_to_lba48(cache[MC_HEAD].tlba, NULL);
		if (unused == addr) {
			lba_was = le64_to_lba48(cache[MC_HEAD].bval, &decr);
			if (addr)
				addr += decr;
			if (target)
				lba_new = lba_was + decr;
			count -= decr;
		}

		unused = le64_to_lba48(cache[MC_INTERSECT].tlba, NULL);
		lba_was = le64_to_lba48(cache[MC_INTERSECT].bval, &decr);

		if (unused != addr)
			Z_ERR(znd, "FAIL: addr [%llx] != intersect [%llx]",
			      addr, unused);

		if (issue_unused && lba_was != lba_new && decr > 0)
			unused_add(znd, lba_was, unused, decr, GFP_ATOMIC);

		mce[idx].tlba = lba48_to_le64(0, addr);
		mce[idx].bval = lba48_to_le64(decr, target);

		if (addr)
			addr += decr;
		if (target)
			lba_new = lba_was + decr;
		count -= decr;

		mce[idx + 1] = cache[MC_TAIL];
	}
	return 0;
}

static int _common_merges(struct zdm *znd,
			  struct map_cache_page *mc_pg, int entries,
			  u64 addr, u32 count, u64 target, gfp_t gfp)
{
	return __mrg_splt(znd, true, mc_pg, entries, addr, count, target, gfp);
}

static int unused_merges(struct zdm *znd,
			 struct map_cache_page *mc_pg, int entries,
			 u64 addr, u32 count, u64 target, gfp_t gfp)
{
	return __mrg_splt(znd, false, mc_pg, entries, addr, count, target, gfp);
}

static int _common_drop(struct zdm *znd, struct map_cache_page *mc_pg,
			u64 key, u32 range, int entries)
{
	int idx;
	struct map_cache_entry *maps = mc_pg->maps;
	int in_use = 0;

	if (entries)
		in_use = ISCT_BASE + (entries * MC_SKIP);

	for (idx = 0; idx < in_use; idx++) {
		u32 melem;
		u32 flags;
		u64 addr = le64_to_lba48(maps[idx].tlba, &flags);
		u64 bval = le64_to_lba48(maps[idx].bval, &melem);

		if (key < (addr+melem) && addr < (key + range)) {
			flags |= MCE_NO_ENTRY;
			maps[idx].tlba = lba48_to_le64(flags, addr);
			Z_DBG(znd, " .. drop: [%llx, %llx} {+%u}",
			      addr, addr + melem, melem);
		}
		(void) bval;
	}
	return 0;
}

static int unused_update(struct zdm *znd, u64 addr, u64 source, u32 count,
			 bool drop, gfp_t gfp)
{
	struct map_cache_entry *maps = NULL;
	struct map_cache_page *m_pg = NULL;
	unsigned long flags;
	int rc = 0;
	int matches;
	int avail;

	if (addr < znd->data_lba)
		return rc;

	m_pg = ZDM_ALLOC(znd, sizeof(*m_pg), PG_09, gfp);
	if (!m_pg)
		return -ENOMEM;

	source = 0ul; /* this is not needed again */

resubmit:
	rc = 0;
	spin_lock_irqsave(&znd->unused_rwlck, flags);
	maps = m_pg->maps;
	avail = ARRAY_SIZE(m_pg->maps);
	matches = _common_intersect(znd->unused, addr, count, m_pg);
	if (drop && matches == 0)
		goto out_unlock;
	if (matches == 0) {
		const u32 sflg = 0;
		int in = mp_insert(znd->unused, addr, sflg, count, source);

		if (in != 1) {
			maps[ISCT_BASE].tlba = lba48_to_le64(sflg, addr);
			maps[ISCT_BASE].bval = lba48_to_le64(count, source);
			matches = 1;
		}
	}
	if (matches) {
		struct map_pool *mp = &znd->_use[1];
		const int drop = 0;

		unused_merges(znd, m_pg, matches, addr, count, source, gfp);
		if (drop)
			_common_drop(znd, m_pg, addr, count, matches);
		if (znd->unused == &znd->_use[1])
			mp = &znd->_use[0];
		mp->sorted = mp->count = 0;
		rc = do_sort_merge(mp, znd->unused, maps, avail, drop);
		if (unlikely(rc)) {
			Z_ERR(znd, "USortMerge failed: %d [%d]", rc, __LINE__);
			rc = -EBUSY;
		} else {
			znd->unused = mp;
			__smp_mb();
		}
	}
out_unlock:
	spin_unlock_irqrestore(&znd->unused_rwlck, flags);

	if (rc == -EBUSY) {
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		do_move_map_cache_to_table(znd);
		goto resubmit;
	}

	if (znd->unused->count > MC_MOVE_SZ)
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);

	if (m_pg)
		ZDM_FREE(znd, m_pg, sizeof(*m_pg), PG_09);

	return rc;
}

static int journal_merges(struct zdm *znd, u64 addr, u32 count, u64 lba,
			  int entries)
{
	return _common_merges(znd, &znd->wbjrnl_pg, entries, addr, count, lba,
		              GFP_ATOMIC);
}

static int unused_add(struct zdm *znd, u64 addr, u64 from, u32 count, gfp_t gfp)
{
	Z_DBG(znd, "Unused ... add: [%llx, %llx)", addr, addr + count);
	return unused_update(znd, addr, from, count, false, gfp);
}

/*
 *  if there are unprocessed 'unused' blocks pending then we need to
 * punch a hole to keep unused from overwriting the new incoming map
 */
static int unused_reuse(struct zdm *znd, u64 addr, u64 count, gfp_t gfp)
{
	Z_DBG(znd, "Unused ... reuse: [%llx, %llx)", addr, addr + count);
	return unused_update(znd, addr, 0ul, count, true, gfp);
}


/* On Ingress we may need to punch a hole in a discard extent */

static int trim_deref_range(struct zdm *znd, u64 addr, u32 count, u64 lba,
			    gfp_t gfp)
{
	struct map_cache_page *m_pg = NULL;
	struct map_cache_entry *maps = NULL;
	unsigned long flags;
	int err = 0;
	int avail;
	int matches;

	m_pg = ZDM_ALLOC(znd, sizeof(*m_pg), PG_09, gfp);
	if (!m_pg)
		return -ENOMEM;

resubmit:
	spin_lock_irqsave(&znd->trim_rwlck, flags);
	maps = m_pg->maps;
	avail = ARRAY_SIZE(m_pg->maps);
	matches = _common_intersect(znd->trim, addr, count, m_pg);
	if (matches) {
		struct map_pool *mp = &znd->trim_mp[1];
		const int drop = 1;

		if (znd->trim == &znd->trim_mp[1])
			mp = &znd->trim_mp[0];

		_common_merges(znd, m_pg, matches, addr, count, lba, gfp);
		_common_drop(znd, m_pg, addr, count, matches);

		mp->sorted = mp->count = 0;
		err = do_sort_merge(mp, znd->trim, maps, avail, drop);
		if (unlikely(err)) {
			Z_ERR(znd, "DSortMerge failed: %d [%d]", err, __LINE__);
			err = -EBUSY;
		} else {
			znd->trim = mp;
			__smp_mb();
		}
	}
	spin_unlock_irqrestore(&znd->trim_rwlck, flags);

	if (err == -EBUSY) {
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		do_move_map_cache_to_table(znd);
		goto resubmit;
	}

	if (m_pg)
		ZDM_FREE(znd, m_pg, sizeof(*m_pg), PG_09);

	return err;
}

int ref_crc_pgs(struct map_pg **pgs, int cpg, int npgs)
{
	if (pgs) {
		int count = cpg;
		int idx;

		for (idx = 0; idx < count; idx++) {
			if (pgs[idx]->crc_pg && cpg < npgs) {
				if (cpg > 0 && pgs[cpg - 1] == pgs[idx]->crc_pg)
					continue;
				pgs[cpg] = pgs[idx]->crc_pg;
				ref_pg(pgs[idx]->crc_pg);
				cpg++;
			}
		}
	}
	return cpg;
}

void deref_all_pgs(struct map_pg **pgs, int npgs, int fastage)
{
	const u64 decr = msecs_to_jiffies(MEM_PURGE_MSECS - 1);
	int cpg;

	for (cpg = 0; cpg < npgs; cpg++) {
		if (pgs[cpg] == NULL)
			break;
		if (fastage) {
			pgs[cpg]->age = jiffies_64;
			if (pgs[cpg]->age > decr)
				pgs[cpg]->age -= decr;
		}
		deref_pg(pgs[cpg]);
		pgs[cpg] = NULL;
	}
}

static void working_set(struct map_cache_entry *wset,
			struct map_cache_entry *source, int count, int create)
{
	int iter;
	u32 flags;
	u64 addr;
	u64 conf;

	for (iter = 0; iter < count; iter++) {
		addr = le64_to_lba48(source[iter].tlba, &flags);
		if (create) {
			flags |= MCE_NO_MERGE;
			source[iter].tlba = lba48_to_le64(flags, addr);
			memcpy(&wset[iter], &source[iter], sizeof(*wset));
			continue;
		}

		conf = le64_to_lba48(wset[iter].tlba, NULL);
		if (conf == addr && (flags & MCE_NO_MERGE))
			memcpy(&wset[iter], &source[iter], sizeof(*wset));
		else
			memset(&wset[iter], 0, sizeof(*wset));
	}
}

/**
 * unmap_deref_chunk() - Migrate a chunk of discarded blocks to ingress cache
 * znd: ZDM Instance
 * minblks: Minimum number of blocks to move
 * more: Migrate blocks even when trim cache is mostly empty.
 * gfp: GFP allocation mask
 */
static int unmap_deref_chunk(struct zdm *znd, u32 minblks, int more, gfp_t gfp)
{
	struct map_cache_entry *maps;
	u64 lba;
	unsigned long flags;
	int iter;
	int squash = 0;
	int err = 0;
	int moving = 5;
	const int ndisc = 0;
	int noio = 0;

	if (znd->ingress->count > MC_HIGH_WM || znd->unused->count > MC_HIGH_WM)
		goto out;

	if (znd->trim->count < 10 && !more)
		goto out;

	if (gfp == GFP_KERNEL) {
		if (!spin_trylock_irqsave(&znd->gc_postmap.cached_lock, flags))
			goto out;
	} else {
		spin_lock_irqsave(&znd->gc_postmap.cached_lock, flags);
	}
	spin_unlock_irqrestore(&znd->gc_postmap.cached_lock, flags);

	moving = 5;
	for (iter = 0; iter < moving; iter++) {
		u64 tlba, addr, bval;
		u32 flgs, blks, range, decr;

		spin_lock_irqsave(&znd->trim_rwlck, flags);
		maps = znd->trim->mcd.maps;
		if (moving < znd->trim->count) {
			moving = znd->trim->count;
			if (iter >= moving) {
				spin_unlock_irqrestore(&znd->trim_rwlck, flags);
				break;
			}
		}
		tlba = addr = le64_to_lba48(maps[iter].tlba, &flgs);
		if (flgs & MCE_NO_ENTRY) {
			spin_unlock_irqrestore(&znd->trim_rwlck, flags);
			continue;
		}
		bval = le64_to_lba48(maps[iter].bval, &blks);
		addr = tlba;
		decr = blks;
		flgs |= MCE_NO_MERGE;
		maps[iter].tlba = lba48_to_le64(flgs, tlba);
		spin_unlock_irqrestore(&znd->trim_rwlck, flags);

		do {
			range = blks;
			lba = __map_rng(znd, tlba, &range, ndisc,
					noio, GFP_ATOMIC);
			if (lba == ~0ul) {
				Z_DBG(znd, "%s: __map_rng failed [%llx+%u].",
				      __func__, tlba, blks);
				err = -EBUSY;
				goto out;
			}
			if (range) {
				blks -= range;
				tlba += range;
			}
		} while (blks > 0);

		spin_lock_irqsave(&znd->trim_rwlck, flags);
		maps = znd->trim->mcd.maps;
		if (moving < znd->trim->count) {
			moving = znd->trim->count;
			if (iter >= moving) {
				spin_unlock_irqrestore(&znd->trim_rwlck, flags);
				break;
			}
		}

		tlba = addr;
		(void)le64_to_lba48(maps[iter].bval, &blks);
		if (decr != blks ||
		    tlba != le64_to_lba48(maps[iter].tlba, NULL)) {
			spin_unlock_irqrestore(&znd->trim_rwlck, flags);
			goto out;
		}

		noio = 1;
		do {
			range = blks;
			lba = __map_rng(znd, tlba, &range, ndisc,
					noio, GFP_ATOMIC);
			if (lba == ~0ul) {
				err = -EBUSY;
				spin_unlock_irqrestore(&znd->trim_rwlck, flags);
				goto out;
			}
			if (range) {
				if (lba) {
					err = ingress_add(znd, tlba, 0ul, range,
							  GFP_ATOMIC);
					if (err) {
						spin_unlock_irqrestore(
							&znd->trim_rwlck, flags);
						goto out;
					}
				}
				blks -= range;
				tlba += range;
			}
		} while (blks > 0);
		maps[iter].tlba = lba48_to_le64(MCE_NO_ENTRY, tlba);
		squash = 1;
		spin_unlock_irqrestore(&znd->trim_rwlck, flags);

		if (znd->ingress->count > MC_HIGH_WM ||
		    znd->unused->count > MC_HIGH_WM)
			goto out;

		if (minblks < decr)
			break;
		minblks -= decr;
	}

out:
	/* remove the MCE_NO_ENTRY maps from the table */
	if (squash) {
		struct map_cache_page *m_pg = NULL;
		int avail = 0;
		struct map_pool *mp;
		const int drop = 1;
		int rc;

		m_pg = ZDM_ALLOC(znd, sizeof(*m_pg), PG_09, gfp);
		if (!m_pg)
			return -ENOMEM;

resubmit:
		spin_lock_irqsave(&znd->trim_rwlck, flags);

		mp = &znd->trim_mp[1];
		if (znd->trim == &znd->trim_mp[1])
			mp = &znd->trim_mp[0];
		mp->sorted = mp->count = 0;
		rc = do_sort_merge(mp, znd->trim, m_pg->maps, avail, drop);
		if (unlikely(rc)) {
			Z_ERR(znd, "DSortMerge failed: %d [%d]", rc, __LINE__);
			rc = -EBUSY;
		} else {
			znd->trim = mp;
			__smp_mb();
		}
		spin_unlock_irqrestore(&znd->trim_rwlck, flags);

		if (rc == -EBUSY) {
			set_bit(DO_MAPCACHE_MOVE, &znd->flags);
			do_move_map_cache_to_table(znd);
			goto resubmit;
		}

		ZDM_FREE(znd, m_pg, sizeof(*m_pg), PG_09);
	}

	if (znd->trim->count > MC_MOVE_SZ || znd->ingress->count > MC_MOVE_SZ)
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);

	return err;
}

static int gc_map_drop(struct zdm *znd, u64 addr, u32 count)
{
	struct map_cache *post = &znd->gc_postmap;
	struct gc_map_cache_data *data = post->mcd;
	u64 last_addr = addr + count;
	u64 tlba;
	unsigned long flags;
	u32 t_flgs;
	int idx;

	spin_lock_irqsave(&post->cached_lock, flags);
	for (idx = 0; idx < post->jcount; idx++) {
		if (data->maps[idx].tlba == MC_INVALID)
			continue;
		tlba = le64_to_lba48(data->maps[idx].tlba, &t_flgs);
		if (tlba == Z_LOWER48)
			continue;
		if (t_flgs & GC_DROP)
			continue;
		if (tlba >= addr && tlba < last_addr) {
			u64 bval = le64_to_lba48(data->maps[idx].bval, NULL);

			Z_DBG(znd, "GC postmap #%d: %llx in range "
				   "[%llx, %llx) is stale. {%llx}",
			      idx, tlba, addr, addr+count, bval);

			t_flgs |= GC_DROP;
			data->maps[idx].tlba = lba48_to_le64(t_flgs, tlba);
			data->maps[idx].bval = lba48_to_le64(0, bval);
		}
	}
	spin_unlock_irqrestore(&post->cached_lock, flags);

	return 0;
}

static int ingress_add(struct zdm *znd, u64 addr, u64 lba, u32 count,
		       gfp_t gfp)
{
	struct map_cache_entry *maps = NULL;
	struct map_cache_page *m_pg = NULL;
	unsigned long flags;
	int rc = 0;
	int matches;
	int avail;

	m_pg = ZDM_ALLOC(znd, sizeof(*m_pg), PG_09, gfp);
	if (!m_pg)
		return -ENOMEM;

resubmit:
	if (znd->ingress->count > ((MC_POOL_SZ * 3) >> 2))
		do_move_map_cache_to_table(znd);

	spin_lock_irqsave(&znd->in_rwlck, flags);
	maps = m_pg->maps;
	avail = ARRAY_SIZE(m_pg->maps);
	matches = _common_intersect(znd->ingress, addr, count, m_pg);
	if (matches == 0) {
		const u32 sflg = 0;
		int in = mp_insert(znd->ingress, addr, sflg, count, lba);

		if (in != 1) {
			maps[ISCT_BASE].tlba = lba48_to_le64(sflg, addr);
			maps[ISCT_BASE].bval = lba48_to_le64(count, lba);
			matches = 1;
		}
	}

	if (matches) {
		struct map_pool *mp = &znd->in[1];
		const int drop = 0;

		if (znd->ingress == &znd->in[1])
			mp = &znd->in[0];

		_common_merges(znd, m_pg, matches, addr, count, lba, gfp);
		mp->sorted = mp->count = 0;
		rc = do_sort_merge(mp, znd->ingress, maps, avail, drop);
		if (unlikely(rc)) {
			Z_ERR(znd, "SortMerge failed: %d [%d]", rc, __LINE__);
			dump_stack();
			rc = -EBUSY;
		} else {
			znd->ingress = mp;
			__smp_mb();
		}
	}
	spin_unlock_irqrestore(&znd->in_rwlck, flags);

	if (znd->ingress->count > MC_MOVE_SZ)
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);

	if (rc == -EBUSY) {
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		do_move_map_cache_to_table(znd);
		goto resubmit;
	}

	if (m_pg)
		ZDM_FREE(znd, m_pg, sizeof(*m_pg), PG_09);

	return rc;
}

static int unmap_overwritten(struct zdm *znd, u64 addr, u32 count, gfp_t gfp)
{
	u64 lba;
	u64 tlba = addr;
	u32 blks = count;
	u32 range;
	const int disc = 1; /* check discard entries */
	const int noio = 0;
	int err = 0;

	do {
		range = blks;
		lba = __map_rng(znd, tlba, &range, disc, noio, gfp);

		if (lba == ~0ul || range == 0 || lba < znd->data_lba)
			goto out;

		if (lba) {
			u32 zone = _calc_zone(znd, lba);
			u32 gzno  = zone >> GZ_BITS;
			u32 gzoff = zone & GZ_MMSK;
			struct meta_pg *wpg = &znd->wp[gzno];

			if (zone < znd->data_zones) {
				_dec_wp_avail_by_lost(wpg, gzoff, range);
				update_stale_ratio(znd, zone);
				err = unused_add(znd, lba, tlba, range, gfp);
				if (err)
					goto out;
			}
		}
		if (range) {
			blks -= range;
			tlba += range;
		}
	} while (blks > 0);

out:
	return err;
}


/**
 * do_ingress() - Add multiple entries into the map_cache
 * @znd: ZDM instance
 * @dm_s: tLBA
 * @lba: lba on backing device.
 * @count: Number of (contiguous) map entries to add.
 * @gc: drop from gc post map (if true).
 * @gfp: allocation mask.
 */
static int do_add_map(struct zdm *znd, u64 addr, u64 lba, u32 count, bool gc,
		      gfp_t gfp)
{
	int rc = 0;

	Z_DBG(znd, "%s: [%llx,%llx) -> %llx {+%u}",
	      __func__, addr, addr+count, lba, count);

	if (addr < znd->data_lba)
		return rc;

	if (gc)
		gc_map_drop(znd, addr, count);

	/*
	 * When mapping new (non-discard) entries we need to punch out any
	 * entries in the 'trim' table.
	 */
	if (lba) {
		rc = trim_deref_range(znd, addr, count, lba, gfp);
		if (rc) {
			Z_ERR(znd, "trim_deref_range *FAIL* %llx", addr);
			goto out;
		}
		rc = unmap_overwritten(znd, addr, count, gfp);
		if (rc) {
			Z_ERR(znd, "unmap_overwritten *FAIL* %llx", addr);
			goto out;
		}
		rc = unused_reuse(znd, lba, count, gfp);
		if (rc) {
			Z_ERR(znd, "unused_reuse *FAIL* %llx", addr);
			goto out;
		}
	}
	rc = ingress_add(znd, addr, lba, count, gfp);

out:
	return rc;
}

/**
 * z_mapped_addmany() - Add multiple entries into the map_cache
 * @znd: ZDM instance
 * @dm_s: tLBA
 * @lba: lba on backing device.
 * @count: Number of (contiguous) map entries to add.
 * @gfp: allocation mask.
 */
static int z_mapped_addmany(struct zdm *znd, u64 addr, u64 lba, u32 count,
			    gfp_t gfp)
{
	return do_add_map(znd, addr, lba, count, true, gfp);
}


static void predictive_zfree(struct zdm *znd, u64 addr, u32 count, gfp_t gfp)
{
	u64 lba;
	u32 offset;
	u32 num;
	const int nodisc = 1;

	for (offset = 0; offset < count; ) {
		num = count - offset;
		if (num == 0)
			break;
		if (num > 64)
			num = 64;
		lba = _current_mapping(znd, nodisc, addr + offset, gfp);
		if (lba == 0 || lba == ~0ul)
			break;
		if (lba && lba >= znd->data_lba) {
			u32 zone = _calc_zone(znd, lba);
			u32 limit = (lba - znd->data_lba) & ~(Z_BLKSZ - 1);
			u32 gzno  = zone >> GZ_BITS;
			u32 gzoff = zone & GZ_MMSK;
			struct meta_pg *wpg = &znd->wp[gzno];

			if (limit && num > limit)
				num = limit;

			_dec_wp_avail_by_lost(wpg, gzoff, num);
			update_stale_ratio(znd, zone);
		}
		offset += num;
	}
}

/**
 * z_mapped_discard() - Add a discard extent to the mapping cache
 * @znd: ZDM Instance
 * @tlba: Address being discarded.
 * @blks: number of blocks being discard.
 * @lba: Lba for ?
 */
static int z_mapped_discard(struct zdm *znd, u64 addr, u32 count, gfp_t gfp)
{
	const u64 lba = 0ul;
	struct map_cache_page *m_pg = NULL;
	struct map_cache_entry *maps = NULL;
	unsigned long flags;
	int rc = 0;
	int matches;
	int avail;

	m_pg = ZDM_ALLOC(znd, sizeof(*m_pg), PG_09, gfp);
	if (!m_pg)
		return -ENOMEM;

	maps = m_pg->maps;
	avail = ARRAY_SIZE(m_pg->maps);

	predictive_zfree(znd, addr, count, gfp);
	gc_map_drop(znd, addr, count);

resubmit:

	spin_lock_irqsave(&znd->trim_rwlck, flags);
	matches = _common_intersect(znd->trim, addr, count, m_pg);
	if (matches == 0) {
		if (mp_insert(znd->trim, addr, lba, count, 0) != 1) {
			maps[ISCT_BASE].tlba = lba48_to_le64(0, addr);
			maps[ISCT_BASE].bval = lba48_to_le64(count, lba);
			matches = 1;
		}
	}
	if (matches) {
		struct map_pool *mp = &znd->trim_mp[1];
		const int drop = 0;

		if (znd->trim == &znd->trim_mp[1])
			mp = &znd->trim_mp[0];

		_common_merges(znd, m_pg, matches, addr, count, lba, gfp);
		mp->sorted = mp->count = 0;
		rc = do_sort_merge(mp, znd->trim, maps, avail, drop);
		if (unlikely(rc)) {
			Z_ERR(znd, "DSortMerge failed: %d [%d]", rc, __LINE__);
			rc = -EBUSY;
		} else {
			znd->trim = mp;
			__smp_mb();
		}
		rc = 0;
	}
	if (znd->trim->count > MC_MOVE_SZ)
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);
	spin_unlock_irqrestore(&znd->trim_rwlck, flags);

	if (rc == -EBUSY) {
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		do_move_map_cache_to_table(znd);
		goto resubmit;
	}

	if (m_pg)
		ZDM_FREE(znd, m_pg, sizeof(*m_pg), PG_09);

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
	unsigned long flags;

	if (found) {
		found->lba = lba;
		spin_lock_init(&found->md_lock);
		set_bit(mpi->bit_dir, &found->flags);
		set_bit(mpi->bit_type, &found->flags);
		found->age = jiffies_64;
		found->index = entry;
		INIT_LIST_HEAD(&found->zltlst);
		INIT_LIST_HEAD(&found->lazy);
		INIT_HLIST_NODE(&found->hentry);
		found->znd = znd;
		found->crc_pg = NULL; /* redundant */
		ref_pg(found);
		if (ahead)
			set_bit(IS_READA, &found->flags);
		set_bit(WB_JRNL_1, &found->flags);
		init_completion(&found->event);
		set_bit(IS_ALLOC, &found->flags);

		/* allocation done. check and see if there as a
		 * concurrent race
		 */
		spin_lock_irqsave(mpi->lock, flags);
		if (!add_htbl_entry(znd, mpi, found))
			ZDM_FREE(znd, found, sizeof(*found), KM_20);
		spin_unlock_irqrestore(mpi->lock, flags);
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
	unsigned long flags;

	if (test_bit(IS_DROPPED, &pg->flags)) {
		spin_lock_irqsave(&znd->lzy_lck, flags);
		if (test_bit(IS_DROPPED, &pg->flags) &&
		    test_bit(IS_LAZY, &pg->flags)) {
			clear_bit(IS_DROPPED, &pg->flags);
			set_bit(DELAY_ADD, &pg->flags);
		}
		if (pg->data.addr && pg->hotness < MEM_PURGE_MSECS)
			pg->hotness += MEM_HOT_BOOST_INC;
		if (!pg->data.addr) {
			if (!test_bit(IS_ALLOC, &pg->flags)) {
				Z_ERR(znd, "Undrop no pg? %"PRIx64, pg->lba);
				init_completion(&pg->event);
				set_bit(IS_ALLOC, &pg->flags);
			}
		}
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
		undrop = 1;
		spin_unlock_irqrestore(&znd->lzy_lck, flags);
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
	unsigned long flags;
	struct mpinfo mpi;
	struct map_addr maddr;
	struct map_pg *found;

	gfp = GFP_ATOMIC;
	if (lba < znd->data_lba)
		goto out;

	map_addr_calc(znd, lba, &maddr);
	entry = to_table_entry(znd, maddr.lut_s, 0, &mpi);
	if (entry > -1) {
		spin_lock_irqsave(mpi.lock, flags);
		found = get_htbl_entry(znd, &mpi);
		if (found)
			_maybe_undrop(znd, found);
		spin_unlock_irqrestore(mpi.lock, flags);
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
	unsigned long flags;
	struct map_pg *found;
	u64 base = (mpi->bit_dir == IS_REV) ? znd->c_mid : znd->c_base;

	base += mpi->crc.pg_no;
	entry = to_table_entry(znd, base, 0, mpi);

	if (mpi->bit_type != IS_CRC)
		return rc;
	if (entry >= znd->crc_count)
		return rc;

	spin_lock_irqsave(mpi->lock, flags);
	found = get_htbl_entry(znd, mpi);
	if (found)
		_maybe_undrop(znd, found);
	spin_unlock_irqrestore(mpi->lock, flags);
	if (!found)
		found = alloc_pg(znd, entry, base, mpi, raflg, GFP_ATOMIC);
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

static __always_inline bool _io_pending(struct map_pg *pg)
{
	if (!pg->data.addr ||
	    test_bit(IS_ALLOC, &pg->flags) ||
	    test_bit(R_IN_FLIGHT, &pg->flags) ||
	    test_bit(R_SCHED, &pg->flags))
		return true;

	if (test_bit(R_CRC_PENDING, &pg->flags) &&
	    test_bit(IS_LUT, &pg->flags))
		return true;

	return false;
}


static struct map_pg *gme_noio(struct zdm *znd, u64 lba)
{
	struct map_pg *found = NULL;
	struct mpinfo mpi;
	unsigned long flags;
	int entry = to_table_entry(znd, lba, 0, &mpi);

	if (entry < 0)
		goto out;

	spin_lock_irqsave(mpi.lock, flags);
	found = get_htbl_entry(znd, &mpi);
	spin_unlock_irqrestore(mpi.lock, flags);

	if (found) {
		spinlock_t *lock = &found->md_lock;

		spin_lock_irqsave(lock, flags);
		if (_io_pending(found))
			found = NULL;
		else
			ref_pg(found);
		spin_unlock_irqrestore(lock, flags);
	}

out:
	return found;
}

/**
 * do_gme_io() - Find a page of LUT or CRC table map.
 * @znd: ZDM instance
 * @lba: Logical LBA of page.
 * @ra: Number of blocks to read ahead
 * @async: If cache_pg needs to wait on disk
 * @gfp: Memory allocation rule
 *
 * Return: struct map_pg * or NULL on error.
 *
 * Page will be loaded from disk it if is not already in core memory.
 */
static struct map_pg *do_gme_io(struct zdm *znd, u64 lba,
				int ra, int async, gfp_t gfp)
{
	struct mpinfo mpi;
	struct map_pg **ahead;
	struct map_pg *pg = NULL;
	int entry;
	int iter;
	int range;
	u32 count;
	int empty_val;

	entry = to_table_entry(znd, lba, 0, &mpi);
	if (entry < 0) {
		Z_ERR(znd, "%s: bad lba? %llx", __func__, lba);
		dump_stack();
		return NULL;
	}

	ahead = ZDM_ALLOC(znd, PAGE_SIZE, PG_09, gfp);
	if (!ahead)
		return NULL;

	if (ra > MAX_PER_PAGE(ahead))
		ra = MAX_PER_PAGE(ahead);
	if (ra > 16 && !znd->bio_queue)
		ra = 16;
	if (ra > 2 && low_cache_mem(znd))
		ra = 2;

	if (mpi.bit_type == IS_LUT) {
		count = znd->map_count;
		empty_val = 0xff;

		if (mpi.bit_dir == IS_FWD)
			_load_backing_pages(znd, lba, gfp);
		else if (ra > 4)
			ra = 4;

		_load_crc_page(znd, &mpi, gfp);
		range = entry + ra;
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
		unsigned long flags;
		struct map_pg *found;

		entry = to_table_entry(znd, lba, iter, &mpi);
		if (entry < 0)
			break;

		spin_lock_irqsave(mpi.lock, flags);
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
		spin_unlock_irqrestore(mpi.lock, flags);

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
	ra = iter;

	/*
	 * Each entry in ahead has an elevated refcount.
	 * Only allow the target of do_gme_io() to remain elevated.
	 */
	for (iter = 0; iter < ra; iter++) {
		pg = ahead[iter];

		if (pg) {
			if (!pg->data.addr) {
				int rc;

				to_table_entry(znd, pg->lba, iter, &mpi);
				rc = cache_pg(znd, pg, gfp, &mpi);

				if (rc < 0 && rc != -EBUSY) {
					znd->meta_result = rc;
					ahead[iter] = NULL;

if (iter == 0) {
	Z_ERR(znd, "%s: cache_pg failed? %llx", __func__, lba);
	dump_stack();
}
				}
			}
			if (iter > 0)
				deref_pg(pg);
		}
	}

	pg = ahead[0];
	if (pg && !async) {
		/*
		 * if ahead[0] is queued but not yet available ... wait for
		 * the io to complete ..
		 */
		wait_for_map_pg(znd, pg, gfp);
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
	}

	if (ahead)
		ZDM_FREE(znd, ahead, PAGE_SIZE, PG_09);

	return pg;
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
	unsigned long flags;
	int is_flung = 0;
	const int noio = 0;

	/*
	 * When co
	 * nothing in the GC reverse map should point to
	 * a block *before* a data pool block
	 */
	if (dm_s < znd->data_lba)
		return is_flung;

	if (dm_s >= znd->r_base && dm_s < znd->c_end)
		pg = get_map_entry(znd, dm_s, 4, 0, noio, GFP_KERNEL);

	if (pg) {
		spin_lock_irqsave(&pg->md_lock, flags);
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
		clear_bit(IS_READA, &pg->flags);
		set_bit(IS_DIRTY, &pg->flags);
		clear_bit(IS_FLUSH, &pg->flags);
		clear_bit(IS_READA, &pg->flags);
		is_flung = 1;
		spin_unlock_irqrestore(&pg->md_lock, flags);

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
	set_bit(DO_GC_READ, &gc_entry->gc_flags);
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

	if (metadata_dirty_fling(znd, addr))
		return handled;

	Z_DBG(znd, "%s: %llx -> %llx", __func__, addr, lba);

	if (post->jcount < post->jsize) {
		struct gc_map_cache_data *data = post->mcd;

		data->maps[post->jcount].tlba = lba48_to_le64(0, addr);
		data->maps[post->jcount].bval = lba48_to_le64(1, lba);
		post->jcount++;
		handled = 1;
	} else {
		Z_ERR(znd, "*CRIT* post overflow L:%" PRIx64 "-> S:%" PRIx64,
		      lba, addr);
	}
	return handled;
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
	struct map_pg *rev_pg = NULL;
	struct map_pg *fwd_pg = NULL;
	struct map_cache_page *_pg = NULL;
	struct map_addr ori;
	unsigned long flags;
	unsigned long tflgs;
	unsigned long iflgs;
	unsigned long uflgs;
	int idx;
	const int async = 0;
	int noio = 0;
	int cpg = 0;
	int err = 0;
	gfp_t gfp = GFP_KERNEL;
	struct map_pg **pgs = NULL;
	u64 lut_r = BAD_ADDR;
	u64 lut_s = BAD_ADDR;

	pgs = ZDM_CALLOC(znd, sizeof(*pgs), MAX_WSET, KM_19, gfp);
	if (!pgs) {
		err = -ENOMEM;
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
		goto out;
	}

	/* pull all of the affected struct map_pg and crc pages into memory: */
	for (idx = 0; idx < Z_BLKSZ; idx++) {
		__le32 ORencoded;
		u64 blba = from_lba + idx;
		u64 update = backref_cache(znd, blba);
		u64 tlba = 0ul;

		map_addr_calc(znd, blba, &ori);
		if (lut_r != ori.lut_r) {
			if (rev_pg)
				deref_pg(rev_pg);
			put_map_entry(rev_pg);
			rev_pg = get_map_entry(znd, ori.lut_r, 4, async,
					       noio, gfp);
			if (!rev_pg) {
				err = -ENOMEM;
				Z_ERR(znd, "%s: ENOMEM @ %d",
				      __func__, __LINE__);
				goto out;
			}
			if (pgs && cpg < MAX_WSET) {
				pgs[cpg] = rev_pg;
				ref_pg(rev_pg);
				cpg++;
			}
			ref_pg(rev_pg);
			lut_r = rev_pg->lba;
		}

		if (update) {
			tlba = update;
		} else if (rev_pg && rev_pg->data.addr) {
			ref_pg(rev_pg);
			spin_lock_irqsave(&rev_pg->md_lock, flags);
			ORencoded = rev_pg->data.addr[ori.pg_idx];
			spin_unlock_irqrestore(&rev_pg->md_lock, flags);
			tlba = map_value(znd, ORencoded);
			deref_pg(rev_pg);
		}

		if (!tlba)
			continue;

		map_addr_calc(znd, blba, &ori);
		if (lut_s != ori.lut_s) {
			if (fwd_pg)
				deref_pg(fwd_pg);
			put_map_entry(fwd_pg);
			fwd_pg = get_map_entry(znd, ori.lut_r, 4, async,
					       noio, gfp);
			if (!fwd_pg) {
				err = -ENOMEM;
				Z_ERR(znd, "%s: ENOMEM @ %d",
				      __func__, __LINE__);
				goto out;
			}
			if (pgs && cpg < MAX_WSET) {
				pgs[cpg] = fwd_pg;
				ref_pg(fwd_pg);
				cpg++;
			}
			ref_pg(fwd_pg);
			lut_s = fwd_pg->lba;
		}
		metadata_dirty_fling(znd, tlba);
	}


	cpg = ref_crc_pgs(pgs, cpg, MAX_WSET);

	/* pull all of the affected struct map_pg and crc pages into memory: */
	spin_lock_irqsave(&znd->trim_rwlck, tflgs);
	spin_lock_irqsave(&znd->in_rwlck, iflgs);
	spin_lock_irqsave(&znd->unused_rwlck, uflgs);
	spin_lock_irqsave(&znd->gc_postmap.cached_lock, flags);

	noio = 1;
	for (idx = 0; idx < Z_BLKSZ; idx++) {
		__le32 ORencoded;
		u64 blba = from_lba + idx;
		u64 update = backref_cache(znd, blba);
		u64 tlba = 0ul;

		map_addr_calc(znd, blba, &ori);
		rev_pg = get_map_entry(znd, ori.lut_r, 4, async, noio, gfp);
		if (!rev_pg) {
			err = -EAGAIN;
			znd->gc_postmap.jcount = 0;
			znd->gc_postmap.jsorted = 0;
			spin_unlock_irqrestore(
				&znd->gc_postmap.cached_lock, flags);
			spin_unlock_irqrestore(&znd->unused_rwlck, uflgs);
			spin_unlock_irqrestore(&znd->in_rwlck, iflgs);
			spin_unlock_irqrestore(&znd->trim_rwlck, tflgs);
			goto out;
		}

		if (update) {
			u64 conf = z_lookup_ingress_cache_nlck(znd, update);

			if (conf != blba) {
				Z_ERR(znd, "*** BACKREF BAD!!");
				Z_ERR(znd, "*** BREF %llx -> %llx -> %llx",
					blba, update, conf);
				Z_ERR(znd, "*** BACKREF BAD!!");
			}
			tlba = update;
		} else if (rev_pg && rev_pg->data.addr) {
			unsigned long flags;

			ref_pg(rev_pg);
			spin_lock_irqsave(&rev_pg->md_lock, flags);
			ORencoded = rev_pg->data.addr[ori.pg_idx];
			spin_unlock_irqrestore(&rev_pg->md_lock, flags);

			if (ORencoded != MZTEV_UNUSED)
				tlba = map_value(znd, ORencoded);
			deref_pg(rev_pg);
		}
		put_map_entry(rev_pg);

		if (!tlba)
			continue;
		if (z_lookup_trim_cache_nlck(znd, tlba))
			continue;
		update = z_lookup_ingress_cache_nlck(znd, tlba);
		if (update == ~0ul)
			continue;
		if (update && update != blba)
			continue;
		if (z_lookup_unused_cache_nlck(znd, blba))
			continue;
		gc_post_add(znd, tlba, blba);
	}
	gc_sort_lba(znd, &znd->gc_postmap);
	spin_unlock_irqrestore(&znd->gc_postmap.cached_lock, flags);
	spin_unlock_irqrestore(&znd->unused_rwlck, uflgs);
	spin_unlock_irqrestore(&znd->in_rwlck, iflgs);
	spin_unlock_irqrestore(&znd->trim_rwlck, tflgs);

	if (znd->gc_postmap.jcount == Z_BLKSZ) {
		struct map_cache *post = &znd->gc_postmap;
		struct gc_map_cache_data *data = post->mcd;
		u64 addr;
		u64 blba;
		u64 curr;
		u32 count;
		int at;

		_pg = ZDM_ALLOC(znd, sizeof(*_pg), PG_09, gfp);
		err = -EBUSY;
		for (idx = 0; idx < post->jcount; idx++) {
			addr = le64_to_lba48(data->maps[idx].tlba, NULL);
			blba = le64_to_lba48(data->maps[idx].bval, &count);

			if (addr == Z_LOWER48 || addr == 0ul) {
				Z_ERR(znd, "Bad GC Add of bogus source");
				continue;
			}

			curr = current_mapping(znd, addr, gfp);
			if (curr != blba) {
				addr = Z_LOWER48;
				data->maps[idx].tlba = lba48_to_le64(0, addr);
				data->maps[idx].bval = lba48_to_le64(0, 0ul);
				err = 0;
				continue;
			}
			if (_pg) {
				at = _common_intersect(znd->trim, addr, 1, _pg);
				if (at)
					Z_ERR(znd, "GC ADD: TRIM Lookup FAIL:"
						   " found via isect %d", at);
			}
		}
	}

out:
	if (_pg)
		ZDM_FREE(znd, _pg, Z_C4K, PG_09);

	if (pgs) {
		deref_all_pgs(pgs, MAX_WSET, 0);
		ZDM_FREE(znd, pgs, sizeof(*pgs) * MAX_WSET, KM_19);
	}

	if (fwd_pg)
		deref_pg(fwd_pg);
	put_map_entry(fwd_pg);

	if (rev_pg)
		deref_pg(rev_pg);
	put_map_entry(rev_pg);

	return err;
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
	io_vcache = get_io_vcache(znd, GFP_KERNEL);
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

static inline void set_gc_read_flag(struct map_cache_entry *entry)
{
	u32 flgs;
	u64 addr;

	addr = le64_to_lba48(entry->tlba, &flgs);
	flgs |= GC_READ;
	entry->tlba = lba48_to_le64(flgs, addr);
}

static bool is_valid_and_not_dropped(struct map_cache_entry *mce)
{
	u32 num;
	u32 tflg;
	u64 addr = le64_to_lba48(mce->tlba, &tflg);
	u64 targ = le64_to_lba48(mce->bval, &num);

	return (targ != Z_LOWER48 && addr != Z_LOWER48 &&
		num != 0 && !(tflg & GC_DROP));
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
	struct gc_map_cache_data *mcd = post->mcd;
	unsigned long flags;
	unsigned long gflgs;
	u64 start_lba;
	u64 lba;
	u32 num;
	int nblks;
	int rcode = 0;
	int fill = 0;
	int idx;

	spin_lock_irqsave(&znd->gc_lock, flags);
	idx = gc_entry->r_ptr;
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	spin_lock_irqsave(&post->cached_lock, gflgs);
	do {
		nblks = 0;

		while (idx < post->jcount) {
			if (is_valid_and_not_dropped(&mcd->maps[idx]))
				break;
			idx++;
		}
		if (idx >= post->jcount)
			goto out_finished;

		/* schedule the first block */
		start_lba = le64_to_lba48(mcd->maps[idx].bval, &num);
		set_gc_read_flag(&mcd->maps[idx]);
		nblks = 1;
		idx++;

		while (idx < post->jcount && (nblks+fill) < GC_MAX_STRIPE) {
			bool nothing_to_add = true;

			if (is_valid_and_not_dropped(&mcd->maps[idx])) {
				lba = le64_to_lba48(mcd->maps[idx].bval, &num);
				if (lba == (start_lba + nblks)) {
					set_gc_read_flag(&mcd->maps[idx]);
					nblks++;
					idx++;
					nothing_to_add = false;
				}
			}
			if (nothing_to_add)
				break;
		}
		if (nblks) {
			int err;

			spin_lock_irqsave(&znd->gc_lock, flags);
			gc_entry->r_ptr = idx;
			spin_unlock_irqrestore(&znd->gc_lock, flags);

			spin_unlock_irqrestore(&post->cached_lock, gflgs);
			err = append_blks(znd, start_lba, &io_buf[fill], nblks);
			spin_lock_irqsave(&post->cached_lock, gflgs);
			if (err) {
				rcode = err;
				goto out;
			}
			fill += nblks;
		}
	} while (fill < GC_MAX_STRIPE);

out_finished:
	spin_lock_irqsave(&znd->gc_lock, flags);
	gc_entry->nblks = fill;
	gc_entry->r_ptr = idx;
	if (fill > 0)
		set_bit(DO_GC_WRITE, &gc_entry->gc_flags);
	else
		set_bit(DO_GC_MD_SYNC, &gc_entry->gc_flags);
	spin_unlock_irqrestore(&znd->gc_lock, flags);

out:
	spin_unlock_irqrestore(&post->cached_lock, gflgs);

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
	struct gc_map_cache_data *mcd = post->mcd;
	unsigned long flags;
	unsigned long clflgs;
	u32 aq_flags = Z_AQ_GC | Z_AQ_STREAM_ID | stream_id;
	u64 lba;
	u32 nblks;
	u32 n_out = 0;
	u32 updated;
	u32 avail;
	u32 nwrt;
	int err = 0;
	int idx;

	spin_lock_irqsave(&znd->gc_lock, flags);
	idx = gc_entry->w_ptr;
	nblks = gc_entry->nblks;
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	spin_lock_irqsave(&post->cached_lock, clflgs);
	while (n_out < nblks) {
		const enum dm_io_mem_type io = DM_IO_VMA;
		const unsigned int oflg = REQ_PRIO;

		/*
		 * When lba is zero blocks were not allocated.
		 * Retry with the smaller request
		 */
		avail = nblks - n_out;
		do {
			nwrt = 0;
			lba = z_acquire(znd, aq_flags, avail, &nwrt);
			if (!lba && !nwrt) {
				err = -ENOSPC;
				goto out;
			}
			avail = nwrt;
		} while (!lba && nwrt);

		spin_unlock_irqrestore(&post->cached_lock, clflgs);
		err = writef_block(ti, io, &io_buf[n_out], lba, oflg, nwrt, 0);
		spin_lock_irqsave(&post->cached_lock, clflgs);
		if (err) {
			Z_ERR(znd, "Write %d blocks to %"PRIx64". ERROR: %d",
			      nwrt, lba, err);
			goto out;
		}

		/*
		 * nwrt blocks were written starting from lba ...
		 * update the postmap to point to the new lba(s)
		 */
		updated = 0;
		while (idx < post->jcount && updated < nwrt) {
			u32 num;
			u32 rflg;
			u64 addr = le64_to_lba48(mcd->maps[idx].tlba, &rflg);

			(void)le64_to_lba48(mcd->maps[idx].bval, &num);
			if (rflg & GC_READ) {
				rflg |= GC_WROTE;
				mcd->maps[idx].tlba = lba48_to_le64(rflg, addr);
				mcd->maps[idx].bval = lba48_to_le64(num, lba);
				lba++;
				updated++;
			}
			idx++;
		}
		spin_lock_irqsave(&znd->gc_lock, flags);
		gc_entry->w_ptr = idx;
		spin_unlock_irqrestore(&znd->gc_lock, flags);

		n_out += nwrt;

		if (updated < nwrt && idx >= post->jcount) {
			Z_ERR(znd, "GC: Failed accounting! updated %d/%d. Map %d/%d",
				updated, nwrt, idx, post->jcount);
		}
	}
	Z_DBG(znd, "Write %d blocks from %d", gc_entry->nblks, gc_entry->w_ptr);
	set_bit(DO_GC_CONTINUE, &gc_entry->gc_flags);

out:
	spin_lock_irqsave(&znd->gc_lock, flags);
	gc_entry->nblks = 0;
	gc_entry->w_ptr = idx;
	spin_unlock_irqrestore(&znd->gc_lock, flags);
	spin_unlock_irqrestore(&post->cached_lock, clflgs);

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
	unsigned long clflgs;
	struct zdm *znd = gc_entry->znd;
	struct map_cache *post = &znd->gc_postmap;
	struct gc_map_cache_data *mcd = post->mcd;
	u64 addr;
	u64 lba;
	u32 flgs;
	u32 count;
	int err = 0;
	int idx;
	int entries;

	spin_lock_irqsave(&post->cached_lock, clflgs);
	entries = post->jcount;
	post->jcount = 0;
	post->jsorted = 0;
	for (idx = 0; idx < entries; idx++) {
		if (mcd->maps[idx].tlba == MC_INVALID)
			continue;

		addr = le64_to_lba48(mcd->maps[idx].tlba, &flgs);
		lba = le64_to_lba48(mcd->maps[idx].bval, &count);

		if (lba == Z_LOWER48 || count == 0)
			continue;
		if (addr == Z_LOWER48 || (flgs & GC_DROP))
			continue;

		Z_ERR(znd, "GC: Failed to move %"PRIx64" from %"PRIx64
			   " {flgs: %x %s%s%s} [%d]",
		      addr, lba, flgs,
		      flgs & GC_READ ? "r" : "",
		      flgs & GC_WROTE ? "w" : "",
		      flgs & GC_DROP ? "X" : "",
		      idx);
//		err = -EIO;
	}
	spin_unlock_irqrestore(&post->cached_lock, clflgs);

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
	unsigned long flags;
	int z_id;

	for (z_id = 0; z_id < znd->data_zones; z_id++) {
		u32 gzno  = z_id >> GZ_BITS;
		u32 gzoff = z_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp;

		spin_lock_irqsave(&wpg->wplck, flags);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		if (wp & Z_WP_GC_TARGET) {
			wp &= ~Z_WP_GC_TARGET;
			wpg->wp_alloc[gzoff] = cpu_to_le32(wp);
		}
		set_bit(IS_DIRTY, &wpg->flags);
		clear_bit(IS_FLUSH, &wpg->flags);
		spin_unlock_irqrestore(&wpg->wplck, flags);
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
	struct gc_map_cache_data *mcd = post->mcd;
	u64 addr;
	u64 lba;
	u32 num;
	u32 tflg;
	unsigned long clflgs;
	u32 used = post->jcount;
	int err = 0;
	int idx;
	const bool gc = false;
	const gfp_t gfp = GFP_KERNEL;

	for (idx = 0; idx < post->jcount; idx++) {
		int discard = 0;
		int mapping = 0;
		struct map_pg *mapped = NULL;
		u32 num;
		u32 tflg;
		u64 addr = le64_to_lba48(mcd->maps[idx].tlba, &tflg);
		u64 lba = le64_to_lba48(mcd->maps[idx].bval, &num);
		struct mpinfo mpi;

		if ((znd->s_base <= addr) && (addr < znd->md_end)) {
			if (to_table_entry(znd, addr, 0, &mpi) >= 0)
				mapped = get_htbl_entry(znd, &mpi);
			mapping = 1;
		}

		if (mapping && !mapped)
			Z_ERR(znd, "MD: addr: %" PRIx64 " -> lba: %" PRIx64
				   " no mapping in ram.", addr, lba);

		if (mapped) {
			unsigned long flgs;
			u32 in_z;

			ref_pg(mapped);
			spin_lock_irqsave(&mapped->md_lock, flgs);
			in_z = _calc_zone(znd, mapped->last_write);
			if (in_z != gc_entry->z_gc) {
				Z_ERR(znd, "MD: %" PRIx64
				      " Discarded - %" PRIx64
				      " already flown to: %x",
				      addr, mapped->last_write, in_z);
				discard = 1;
			} else if (mapped->data.addr &&
				   test_bit(IS_DIRTY, &mapped->flags)) {
				Z_ERR(znd,
				      "MD: %" PRIx64 " Discarded - %"PRIx64
				      " is in-flight",
				      addr, mapped->last_write);
				discard = 2;
			}
			if (!discard)
				mapped->last_write = lba;
			spin_unlock_irqrestore(&mapped->md_lock, flgs);
			deref_pg(mapped);
		}

		spin_lock_irqsave(&post->cached_lock, clflgs);
		if (discard == 1) {
			Z_ERR(znd, "Dropped: %" PRIx64 " ->  %"PRIx64,
			      addr, lba);
			tflg |= GC_DROP;
			mcd->maps[idx].tlba = lba48_to_le64(tflg, addr);
			mcd->maps[idx].bval = lba48_to_le64(0, lba);
		}
		if (tflg & GC_DROP)
			used--;
		else if (lba && num)
			increment_used_blks(znd, lba, 1);

		spin_unlock_irqrestore(&post->cached_lock, clflgs);
	}

	for (idx = 0; idx < post->jcount; idx++) {
		if (mcd->maps[idx].tlba == MC_INVALID)
			continue;
		addr = le64_to_lba48(mcd->maps[idx].tlba, &tflg);
		lba = le64_to_lba48(mcd->maps[idx].bval, &num);
		if (!num || addr == Z_LOWER48 || lba == Z_LOWER48 ||
		    !(tflg & GC_READ) || !(tflg & GC_WROTE))
			continue;
		if (lba) {
			if (tflg & GC_DROP)
				err = unused_add(znd, lba, addr, num, gfp);
			else
				err = do_add_map(znd, addr, lba, num, gc, gfp);
			if (err)
				Z_ERR(znd, "ReIngress Post GC failure");

			Z_DBG(znd, "GC Add: %llx -> %llx (%u) [%d] %s-> %d",
			      addr, lba, num, idx,
			      (tflg & GC_DROP) ? "U " : "", err);

		}
		/* mark entry as handled */
		mcd->maps[idx].tlba = lba48_to_le64(0, Z_LOWER48);

		if (test_bit(DO_MAPCACHE_MOVE, &znd->flags)) {
			MutexLock(&znd->mz_io_mutex);
			err = do_move_map_cache_to_table(znd);
			mutex_unlock(&znd->mz_io_mutex);
			if (err)
				Z_ERR(znd, "Move to tables post GC failure");
		}
	}
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
	unsigned long flgs;
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

	spin_lock_irqsave(&wpg->wplck, flgs);
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
	spin_unlock_irqrestore(&wpg->wplck, flgs);

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
	unsigned long flgs;
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
			spin_lock_irqsave(&wpg->wplck, flgs);
			wflg = le32_to_cpu(wpg->wp_alloc[gzoff])
			     & ~Z_WP_RRECALC;
			wpg->wp_alloc[gzoff] = cpu_to_le32(wflg);
			spin_unlock_irqrestore(&wpg->wplck, flgs);
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


static void gc_ref(struct gc_state *gc_entry)
{
	if (gc_entry)
		atomic_inc(&gc_entry->refcount);
}

static void gc_deref(struct gc_state *gc_entry)
{
	if (gc_entry) {
		struct zdm *znd = gc_entry->znd;

		atomic_dec(&gc_entry->refcount);
		if (atomic_read(&gc_entry->refcount) == 0) {
			ZDM_FREE(znd, gc_entry, sizeof(*gc_entry), KM_16);
		}
	}
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
int z_zone_compact_queue(struct zdm *znd, u32 z_gc, int delay, int cpick,
			 gfp_t gfp)
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

	gc_ref(gc_entry);
	init_completion(&gc_entry->gc_complete);
	gc_entry->znd = znd;
	gc_entry->z_gc = z_gc;
	gc_entry->is_cpick = cpick;
	set_bit(DO_GC_INIT, &gc_entry->gc_flags);
	znd->gc_backlog++;

	spin_lock_irqsave(&znd->gc_lock, flags);
	if (znd->gc_active) {
		gc_deref(gc_entry);
		znd->gc_backlog--;
	} else {
		znd->gc_active = gc_entry;
		do_queue = 1;
	}
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	if (do_queue) {
		unsigned long tval = msecs_to_jiffies(delay);

		if (queue_delayed_work(znd->gc_wq, &znd->gc_work, tval))
			err = 1;
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
			delay *= 5;
			rc = z_zone_compact_queue(znd, top_roi, delay, 0, gfp);
			if (rc == 1)
				queued = 1;
			else if (rc < 0)
				Z_ERR(znd, "GC: Z#%u !Q: ERR: %d", top_roi, rc);
		}

		if (!delay && !queued) {
			delay *= 5;
			rc = z_zone_compact_queue(znd, top_roi, delay, 0, gfp);
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
	unsigned long wpflgs;
	int err = 0;
	struct zdm *znd = gc_entry->znd;
	u32 z_gc = gc_entry->z_gc;
	u32 gzno  = z_gc >> GZ_BITS;
	u32 gzoff = z_gc & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];

	znd->age = jiffies_64;

	/*
	 * this could be a little smarter ... just check that any
	 * MD mapped to the target zone has it's DIRTY/FLUSH flags clear
	 */
	if (test_and_clear_bit(DO_GC_INIT, &gc_entry->gc_flags)) {
		err = z_flush_bdev(znd, GFP_KERNEL);
		if (err) {
			gc_entry->result = err;
			goto out;
		}
		set_bit(DO_GC_MD_MAP, &gc_entry->gc_flags);
	}

	/* If a SYNC is in progress and we can delay then postpone*/
	if (mutex_is_locked(&znd->mz_io_mutex))
		return -EAGAIN;

	if (test_and_clear_bit(DO_GC_MD_MAP, &gc_entry->gc_flags)) {
		int nak = 0;
		u32 wp;

		spin_lock_irqsave(&wpg->wplck, wpflgs);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		wp |= Z_WP_GC_FULL;
		wpg->wp_alloc[gzoff] = cpu_to_le32(wp);
		set_bit(IS_DIRTY, &wpg->flags);
		clear_bit(IS_FLUSH, &wpg->flags);
		spin_unlock_irqrestore(&wpg->wplck, wpflgs);

		if (znd->gc_postmap.jcount > 0) {
			Z_ERR(znd, "*** Unexpected data in postmap!!");
			znd->gc_postmap.jcount = 0;
			znd->gc_postmap.jsorted = 0;
		}

		err = z_zone_gc_metadata_to_ram(gc_entry);
		if (err) {
			if (err == -EAGAIN) {
				set_bit(DO_GC_MD_MAP, &gc_entry->gc_flags);
				znd->gc_postmap.jcount = 0;
				znd->gc_postmap.jsorted = 0;

				Z_ERR(znd, "*** metadata to ram, again!!");
				return err;
			}
			if (err != -EBUSY) {
				Z_ERR(znd,
				      "Pre-load metadata to memory failed!! %d", err);
				gc_entry->result = err;
				goto out;
			}
		}

		if (err == -EBUSY && znd->gc_postmap.jcount == Z_BLKSZ)
			nak = 1;
		else if (gc_entry->is_cpick && znd->gc_postmap.jcount > 64)
			nak = 1;

		if (nak) {
			u32 non_seq;
			u32 sid;

			Z_DBG(znd, "Schedule 'move %u' aborting GC",
			      znd->gc_postmap.jcount);

			spin_lock_irqsave(&wpg->wplck, wpflgs);
			non_seq = le32_to_cpu(wpg->wp_alloc[gzoff]);
			non_seq &= ~Z_WP_GC_FULL;
			wpg->wp_alloc[gzoff] = cpu_to_le32(non_seq);
			sid = le32_to_cpu(wpg->zf_est[gzoff]);
			sid &= Z_WP_STREAM_MASK;
			sid |= (Z_BLKSZ - znd->gc_postmap.jcount);
			wpg->zf_est[gzoff] = cpu_to_le32(sid);
			set_bit(IS_DIRTY, &wpg->flags);
			clear_bit(IS_FLUSH, &wpg->flags);
			spin_unlock_irqrestore(&wpg->wplck, wpflgs);

			complete_all(&gc_entry->gc_complete);
			update_stale_ratio(znd, gc_entry->z_gc);

			spin_lock_irqsave(&znd->gc_lock, flags);
			if (znd->gc_active && gc_entry == znd->gc_active) {
				set_bit(DO_GC_COMPLETE, &gc_entry->gc_flags);
				znd->gc_active = NULL;
				__smp_mb();
				gc_deref(gc_entry);
			}
			spin_unlock_irqrestore(&znd->gc_lock, flags);
			znd->gc_postmap.jcount = 0;
			znd->gc_postmap.jsorted = 0;
			err = -EBUSY;
			goto out;
		}
		if (znd->gc_postmap.jcount == 0)
			set_bit(DO_GC_DONE, &gc_entry->gc_flags);
		else
			set_bit(DO_GC_READ, &gc_entry->gc_flags);

		if (atomic_read(&znd->gc_throttle) == 0)
			return -EAGAIN;
	}

next_in_queue:
	znd->age = jiffies_64;

	if (test_and_clear_bit(DO_GC_READ, &gc_entry->gc_flags)) {
		err = z_zone_gc_read(gc_entry);
		if (err < 0) {
			Z_ERR(znd, "z_zone_gc_chunk issue failure: %d", err);
			gc_entry->result = err;
			goto out;
		}
		if (atomic_read(&znd->gc_throttle) == 0)
			return -EAGAIN;
	}

	if (test_and_clear_bit(DO_GC_WRITE, &gc_entry->gc_flags)) {
		u32 sid = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_STREAM_MASK;

		err = z_zone_gc_write(gc_entry, sid >> 24);
		if (err) {
			Z_ERR(znd, "z_zone_gc_write issue failure: %d", err);
			gc_entry->result = err;
			goto out;
		}
		if (atomic_read(&znd->gc_throttle) == 0)
			return -EAGAIN;
	}

	if (test_and_clear_bit(DO_GC_CONTINUE, &gc_entry->gc_flags)) {
		z_do_copy_more(gc_entry);
		goto next_in_queue;
	}

	znd->age = jiffies_64;
	if (test_and_clear_bit(DO_GC_MD_SYNC, &gc_entry->gc_flags)) {
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
		set_bit(DO_GC_DONE, &gc_entry->gc_flags);

		/* flush *before* reset wp occurs to avoid data loss */
		err = z_flush_bdev(znd, GFP_KERNEL);
		if (err) {
			gc_entry->result = err;
			goto out;
		}
        }
	if (test_and_clear_bit(DO_GC_DONE, &gc_entry->gc_flags)) {
		u32 non_seq;
		u32 reclaimed;

		/* Release the zones for writing */
		dmz_reset_wp(znd, gc_entry->z_gc);

		spin_lock_irqsave(&wpg->wplck, wpflgs);
		non_seq = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_NON_SEQ;
		reclaimed = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
		wpg->wp_alloc[gzoff] = cpu_to_le32(non_seq);
		wpg->wp_used[gzoff] = cpu_to_le32(0u);
		wpg->zf_est[gzoff] = cpu_to_le32(Z_BLKSZ);
		znd->discard_count -= reclaimed;
		znd->z_gc_free++;

		/*
		 * If we used a 'reserved' zone for GC/Meta then re-purpose
		 * the just emptied zone as the new reserved zone. Releasing
		 * the reserved zone into the normal allocation pool.
		 */
		if (znd->z_gc_resv & Z_WP_GC_ACTIVE)
			znd->z_gc_resv = gc_entry->z_gc;
		else if (znd->z_meta_resv & Z_WP_GC_ACTIVE)
			znd->z_meta_resv = gc_entry->z_gc;
		set_bit(IS_DIRTY, &wpg->flags);
		clear_bit(IS_FLUSH, &wpg->flags);
		spin_unlock_irqrestore(&wpg->wplck, wpflgs);
		complete_all(&gc_entry->gc_complete);

		znd->gc_events++;
		update_stale_ratio(znd, gc_entry->z_gc);
		spin_lock_irqsave(&znd->gc_lock, flags);
		if (znd->gc_active && gc_entry == znd->gc_active) {
			set_bit(DO_GC_COMPLETE, &gc_entry->gc_flags);
			znd->gc_active = NULL;
			__smp_mb();
			gc_deref(gc_entry);
		} else {
			Z_ERR(znd, "GC: FAIL. FAIL.");
		}
		znd->gc_backlog--;
		spin_unlock_irqrestore(&znd->gc_lock, flags);

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
		int requeue = 0;
		unsigned long tval = msecs_to_jiffies(10);

		spin_lock_irqsave(&znd->gc_lock, flags);
		if (znd->gc_active)
			requeue = 1;
		spin_unlock_irqrestore(&znd->gc_lock, flags);

		if (requeue) {
			if (atomic_read(&znd->gc_throttle) > 0)
				tval = 0;
			queue_delayed_work(znd->gc_wq, &znd->gc_work, tval);
		}
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
			if (z_zone_compact_queue(znd, z_id, delay, 1, gfp))
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
	int gc_idle;
	unsigned long flags;

	spin_lock_irqsave(&znd->gc_lock, flags);
	gc_idle = znd->gc_active ? 0 : 1;
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

		if (delay)
			return !gc_idle;

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
	int queued = 0;

	if (wait) {
		struct gc_state *gc_entry = NULL;
		unsigned long flags;

		atomic_inc(&znd->gc_throttle);
		spin_lock_irqsave(&znd->gc_lock, flags);
		if (znd->gc_active) {
			gc_entry = znd->gc_active;
			if (!test_bit(DO_GC_COMPLETE, &gc_entry->gc_flags))
				gc_ref(gc_entry);
			else
				gc_entry = NULL;
		}
		spin_unlock_irqrestore(&znd->gc_lock, flags);

		if (gc_entry) {
//			Z_ERR(znd, "gc_imm: Wait for Compl");
			wait_for_completion_io(&gc_entry->gc_complete);

			spin_lock_irqsave(&znd->gc_lock, flags);
			gc_deref(gc_entry);
			spin_unlock_irqrestore(&znd->gc_lock, flags);

			if (delayed_work_pending(&znd->gc_work)) {
				mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
//				Z_ERR(znd, "gc_imm: flush delayed");
				can_retry = flush_delayed_work(&znd->gc_work);
			}
//			Z_ERR(znd, "%s: wait: %d. flush -> %d (%d)",
//				__func__, wait, can_retry, __LINE__);
		}
	}

	queued = gc_queue_with_delay(znd, delay, gfp);
	if (wait) {
		atomic_inc(&znd->gc_throttle);
// 		Z_ERR(znd, "%s: wait: %d. queued -> %d (%d)",
//			__func__, wait, queued, __LINE__);
		if (!can_retry && delayed_work_pending(&znd->gc_work)) {
			mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
			can_retry = flush_delayed_work(&znd->gc_work);
		}
		atomic_dec(&znd->gc_throttle);
// 		Z_ERR(znd, "%s: wait: %d. retry -> %d (%d)",
//			__func__, wait, can_retry, __LINE__);
	}

	if (!queued || !can_retry) {
		/*
		 * Couldn't find a zone with enough stale blocks,
		 * but we could after we deref some more discard
		 * extents .. so try again later.
		 */
		if (znd->trim->count > 0)
			can_retry = 1;

		unmap_deref_chunk(znd, 2048, 1, gfp);
	}

	if (wait)
		atomic_dec(&znd->gc_throttle);

	return can_retry | queued;
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
//	else
//		Z_ERR(znd, "Dec z_gc_free below 0?");
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
		unsigned long wpflgs;
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

		spin_lock_irqsave(&wpg->wplck, wpflgs);
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
		spin_unlock_irqrestore(&wpg->wplck, wpflgs);

		dmz_close_zone(znd, zone);
		update_stale_ratio(znd, zone);
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);
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

	if (!(flags & Z_AQ_GC))
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
 * wset_cmp_wr() - Compare map page on lba.
 * @x1: map page
 * @x2: map page
 *
 * Return -1, 0, or 1 if x1 < x2, equal, or >, respectivly.
 */
static int wset_cmp_wr(const void *x1, const void *x2)
{
	const struct map_pg *v1 = *(const struct map_pg **)x1;
	const struct map_pg *v2 = *(const struct map_pg **)x2;
	int cmp = (v1->lba < v2->lba) ? -1 : ((v1->lba > v2->lba) ? 1 : 0);

	return cmp;
}

/**
 * wset_cmp_rd() - Compare map page on lba.
 * @x1: map page
 * @x2: map page
 *
 * Return -1, 0, or 1 if x1 < x2, equal, or >, respectivly.
 */
static int wset_cmp_rd(const void *x1, const void *x2)
{
	const struct map_pg *v1 = *(const struct map_pg **)x1;
	const struct map_pg *v2 = *(const struct map_pg **)x2;
	int cmp = (v1->last_write < v2->last_write) ? -1
		: ((v1->last_write > v2->last_write) ? 1 : wset_cmp_wr(x1, x2));

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
 * Return 1 if page is clean, not in flight, and old.
 */
static __always_inline int is_old_and_clean(struct map_pg *expg, int bit_type)
{
	int is_match = 0;

	if (!test_bit(IS_DIRTY, &expg->flags) &&
	    is_expired(expg->age)) {
		if (test_bit(R_IN_FLIGHT, &expg->flags))
			pr_debug("%5"PRIx64": clean, exp, and in flight: %d\n",
				 expg->lba, getref_pg(expg));
		else if (getref_pg(expg) == 1)
			is_match = 1;
#if 1 // EXTRA_DEBUG
		else if (test_bit(IS_LUT, &expg->flags))
			pr_err("%5"PRIx64": clean, exp, and elev: %d\n",
				 expg->lba, getref_pg(expg));
#endif
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

	if (count > 1)
		sort(wset, count, sizeof(*wset), wset_cmp_wr, NULL);

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
			deref_pg(expg);
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
	struct map_pg *prev = NULL;
	int err = 0;

	/* write dirty table pages */
	if (count <= 0)
		goto out;

	if (count > 1)
		sort(wset, count, sizeof(*wset), wset_cmp_rd, NULL);

	for (iter = 0; iter < count; iter++) {
		expg = wset[iter];
		if (expg) {
			bool in_core = true;

			if (!expg->data.addr) {
				gfp_t gfp = GFP_KERNEL;
				struct mpinfo mpi;
				int rc;

				to_table_entry(znd, expg->lba, 0, &mpi);
				rc = cache_pg(znd, expg, gfp, &mpi);
				if (!rc)
					rc = wait_for_map_pg(znd, expg, gfp);
				if (rc < 0 && rc != -EBUSY)
					znd->meta_result = rc;

				if (rc) {
					in_core = false;
				}

			}
			set_bit(IS_DIRTY, &expg->flags);
			clear_bit(IS_FLUSH, &expg->flags);
			clear_bit(IS_READA, &expg->flags);
			deref_pg(expg);
		}
		if (prev && expg == prev)
			Z_ERR(znd, "Dupe %"PRIx64" in pool_read list.",
			      expg->lba);
		prev = expg;
	}

out:
	return err;
}

/**
 * md_journal_add_map() - Add an entry to the map cache block mapping.
 * @znd: ZDM Instance
 * @addr: Address being added to journal.
 * @lba: bLBA addr is being mapped to (0 to delete the map)
 *
 * Add a new journal wb entry.
 */
static int md_journal_add_map(struct zdm *znd, u64 addr, u64 lba)
{
	unsigned long flgs;
	struct map_cache_entry *maps = znd->wbjrnl_pg.maps;
	struct md_journal *jrnl = &znd->jrnl;
	const int avail = ARRAY_SIZE(znd->wbjrnl_pg.maps);
	const u32 count = 1;
	int rc = 0;
	int matches;
	u64 tlba;
	u32 entry;
	int next;

	if (addr >= znd->data_lba)
		return rc;

	tlba = z_lookup_journal_cache(znd, addr);
	if (tlba && tlba < znd->data_lba) {
		entry = tlba - WB_JRNL_BASE;
		if (entry < jrnl->size &&
		    le32_to_cpu(jrnl->wb[entry]) == addr) {

			spin_lock_irqsave(&jrnl->wb_alloc, flgs);
			jrnl->wb[entry] = MZTEV_UNUSED;
			if (jrnl->in_use > 0)
				jrnl->in_use--;
			spin_unlock_irqrestore(&jrnl->wb_alloc, flgs);
		}
	}

	entry = lba - WB_JRNL_BASE;
	spin_lock_irqsave(&jrnl->wb_alloc, flgs);
	for (next = 0; next < jrnl->size; next++) {
		if (entry == next) {
			jrnl->wb[entry] = cpu_to_le32((u32)addr);
		} else if (le32_to_cpu(jrnl->wb[next]) == addr) {
			Z_ERR(znd, "WB DUPE**");
			jrnl->wb[next] = MZTEV_UNUSED;
			if (jrnl->in_use > 0)
				jrnl->in_use--;
		}
	}
	spin_unlock_irqrestore(&jrnl->wb_alloc, flgs);

resubmit:
	spin_lock_irqsave(&znd->wbjrnl_rwlck, flgs);
	memset(&znd->wbjrnl_pg, 0, sizeof(znd->wbjrnl_pg));
	matches = journal_intersect(znd, addr, count);
	if (matches == 0 && addr < znd->md_start) {
		if (mp_insert(znd->wbjrnl, addr, 0, count, lba) != 1) {
			maps[1].tlba = lba48_to_le64(0, addr);
			maps[1].bval = lba48_to_le64(count, lba);
			matches = 1;
		}
	}
	if (matches) {
		struct map_pool *mp = &znd->in[1];

		if (znd->wbjrnl == &znd->_wbj[1])
			mp = &znd->_wbj[0];

		if (addr >= znd->md_start)
			lba = 0ul;

		journal_merges(znd, addr, count, lba, matches);
		mp->sorted = mp->count = 0;
		rc = do_sort_merge(mp, znd->wbjrnl, maps, avail, 1);
		if (unlikely(rc)) {
			Z_ERR(znd, "JSortMerge failed: %d [%d]", rc, __LINE__);
			rc = -EBUSY;
		} else {
			znd->wbjrnl = mp;
			__smp_mb();
		}
		if (znd->wbjrnl->count > MC_MOVE_SZ)
			set_bit(DO_MAPCACHE_MOVE, &znd->flags);
	}
	spin_unlock_irqrestore(&znd->wbjrnl_rwlck, flgs);
	if (rc == -EBUSY) {
		set_bit(DO_MAPCACHE_MOVE, &znd->flags);
		do_move_map_cache_to_table(znd);
		goto resubmit;
	}
	return rc;
}

/**
 * journal_alloc() - Grab the next available LBA for the journal.
 * @znd: ZDM Instance
 * @addr: Address being reserved.
 * @nblks: Number of blocks desired.
 * @num: Number of blocks allocated.
 *
 * Return: lba or 0 on failure.
 */
static u64 journal_alloc(struct zdm *znd, u64 addr, u32 nblks, u32 *num)
{
	struct md_journal *jrnl = &znd->jrnl;
	unsigned long flgs;
	u64 blba = 0ul;
	u32 next;
	int retry = 2;

	spin_lock_irqsave(&jrnl->wb_alloc, flgs);
	next = jrnl->wb_next;
	do {
		for (; next < jrnl->size; next++) {
			if (jrnl->wb[next] != MZTEV_UNUSED)
				continue;

			blba = WB_JRNL_BASE + next;
			jrnl->wb[next] = MZTEV_NF; /* in flight */
			jrnl->in_use++;
			jrnl->wb_next = next + 1;
			*num = 1;
			goto out_unlock;
		}
		next = 0;
	} while (blba == 0 && --retry > 0);

out_unlock:

//	for (next = 0; next < jrnl->size; next++) {
//		if (le32_to_cpu(jrnl->wb[next]) == addr) {
//			Z_ERR(znd, "DUPE: %llx at %lx",
//				addr, next + WB_JRNL_BASE);
//			jrnl->wb[next] = MZTEV_UNUSED;
//		}
//	}

	spin_unlock_irqrestore(&jrnl->wb_alloc, flgs);

	if ((jrnl->in_use * 100 / jrnl->size) > 50) {
		Z_ERR(znd, "Journal in use threshold flush");
		set_bit(DO_SYNC, &znd->flags);
		set_bit(DO_FLUSH, &znd->flags);
		if (!test_bit(DO_METAWORK_QD, &znd->flags) &&
		    !work_pending(&znd->meta_work)) {
			set_bit(DO_METAWORK_QD, &znd->flags);
			queue_work(znd->meta_wq, &znd->meta_work);
		}
	}

	return blba;
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
#if 0
		if (test_bit(WB_JRNL_1, &map->flags) ||
		    test_bit(WB_JRNL_2, &map->flags)) {
			u64 jrnl_lba = journal_alloc(znd, map->lba, nblks, num);

			if (!jrnl_lba) {
				Z_ERR(znd, "Out of MD journal space?");
				jrnl_lba = map->lba;
			}
			return jrnl_lba;
		}
#endif
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

	to_table_entry(znd, pg->lba, 0, &mpi);
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
 * pg_journal_entry() - Add journal entry and flag in journal status.
 * @znd: ZDM Instance
 * @pg: The page of lookup table [or CRC] that was written.
 */
static int pg_journal_entry(struct zdm *znd, struct map_pg *pg)
{
	int rcode = 0;

	if (pg->lba < znd->data_lba) {
		u64 blba = 0ul; /* if not in journal clean map entry */

		if (test_bit(WB_JRNL_1, &pg->flags) ||
		    test_bit(WB_JRNL_2, &pg->flags)) {
			blba = pg->last_write;
			set_bit(IN_WB_JOURNAL, &pg->flags);
		} else {
			clear_bit(IN_WB_JOURNAL, &pg->flags);
		}

		rcode = md_journal_add_map(znd, pg->lba, blba);
		if (rcode)
			Z_ERR(znd, "%s: MD Journal failed.", __func__);
	}
	return rcode;
}

/**
 * pg_written() - Handle accouting related to lookup table page writes
 * @pg: The page of lookup table [or CRC] that was written.
 * @error: non-zero if an error occurred.
 *
 * callback from dm_io notify.. cannot hold mutex here, cannot sleep.
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

	Z_DBG(znd, "write: %" PRIx64 " -> %" PRIx64 " -> %" PRIx64
		   " crc:%04x [async]",
	      pg->lba, pg->lba48_in, pg->last_write, le16_to_cpu(md_crc));

	rcode = pg_journal_entry(znd, pg);
	if (rcode)
		goto out;

/*
 * NOTE: If we reach here it's a problem.
 *       TODO: mutex free path for adding a map entry ...
 */

	if (pg->last_write < znd->data_lba)
		goto out;

	/* written lba was allocated from data-pool */
	rcode = z_mapped_addmany(znd, pg->lba, pg->last_write, 1, GFP_ATOMIC);
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
	unsigned long flgs;
	sector_t block = lba << Z_SHFT4K;
	unsigned int nDMsect = 1 << Z_SHFT4K;
	const int use_wq = 0;
	int rc;

	pg->znd = znd;
	spin_lock_irqsave(&pg->md_lock, flgs);
	pg->md_crc = crc_md_le16(pg->data.addr, Z_CRC_4K);
	pg->last_write = lba; /* presumably */
	spin_unlock_irqrestore(&pg->md_lock, flgs);

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
 * The purpose of loading is to ensure the pages are in memory when the
 * async_io (write) completes the CRC accounting doesn't cause a sleep
 * and violate the callback() API rules.
 */
static void cache_if_dirty(struct zdm *znd, struct map_pg *pg, int wq)
{
	if (test_bit(IS_DIRTY, &pg->flags) && test_bit(IS_LUT, &pg->flags) &&
	    pg->data.addr) {
		unsigned long flgs;
		struct map_pg *crc_pg;
		struct mpinfo mpi;
		const int async = 0; /* can this be async? */

		to_table_entry(znd, pg->lba, 0, &mpi);
		crc_pg = get_map_entry(znd, mpi.crc.lba, 4, async, 0, GFP_ATOMIC);
		if (!crc_pg)
			Z_ERR(znd, "Out of memory. No CRC Pg");

		if (pg->crc_pg)
			return;

		spin_lock_irqsave(&pg->md_lock, flgs);
		if (!pg->crc_pg) {
			ref_pg(crc_pg);
			pg->crc_pg = crc_pg;
			__smp_mb();
		}
		spin_unlock_irqrestore(&pg->md_lock, flgs);

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

	if (!test_bit(IS_DIRTY, &pg->flags) || !pg->data.addr)
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

		Z_DBG(znd, "write: %" PRIx64 " -> %" PRIx64 " -> %" PRIx64
			   " crc:%04x [drty]", pg->lba, pg->lba48_in,
		      pg->last_write, le16_to_cpu(md_crc));

		if (rcwrt) {
			Z_ERR(znd, "write_page: %" PRIx64 " -> %" PRIx64
				   " ERR: %d", pg->lba, lba, rcwrt);
			rcode = rcwrt;
			goto out;
		}

		if (crc_md_le16(pg->data.addr, Z_CRC_4K) == md_crc)
			clear_bit(IS_DIRTY, &pg->flags);
		clear_bit(W_IN_FLIGHT, &pg->flags);

		rcode = pg_journal_entry(znd, pg);
		if (rcode)
			goto out;

		if (pg->lba < znd->data_lba)
			goto out;

		rcwrt = z_mapped_addmany(znd, dm_s, lba, nf, GFP_ATOMIC);
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
	deref_pg(pg); /* ref'd by queue_pg */

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
	unsigned long zflgs;
	struct map_pg *expg = NULL;
	struct map_pg *_tpg;
	struct map_pg **wset = NULL;
	LIST_HEAD(droplist);
	int err = 0;
	int entries = 0;
	int dlstsz = 0;

	wset = ZDM_CALLOC(znd, sizeof(*wset), MAX_WSET, KM_19, GFP_KERNEL);
	if (!wset) {
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
		return -ENOMEM;
	}

	spin_lock_irqsave(&znd->zlt_lck, zflgs);
	if (list_empty(&znd->zltpool))
		goto writeback;

	expg = list_last_entry(&znd->zltpool, typeof(*expg), zltlst);
	if (!expg || &expg->zltlst == (&znd->zltpool))
		goto writeback;

	_tpg = list_prev_entry(expg, zltlst);
	while (&expg->zltlst != &znd->zltpool) {
		ref_pg(expg);

		if (test_and_clear_bit(WB_RE_CACHE, &expg->flags)) {
			set_bit(IS_DIRTY, &expg->flags);
			clear_bit(IS_FLUSH, &expg->flags);
		}

		if (sync && is_dirty(expg, bit_type)) {
			if (entries < MAX_WSET) {
				ref_pg(expg);
				wset[entries] = expg;
				entries++;
			}
		} else if ((drop > 0) && is_old_and_clean(expg, bit_type)) {
			int is_lut = test_bit(IS_LUT, &expg->flags);
			unsigned long flags;
			spinlock_t *lock;

			lock = is_lut ? &znd->mapkey_lock : &znd->ct_lock;
			spin_lock_irqsave(lock, flags);
			if (getref_pg(expg) == 1) {
				list_del(&expg->zltlst);
				znd->in_zlt--;
				clear_bit(IN_ZLT, &expg->flags);
				drop--;

				if (!low_cache_mem(znd))
					expg->age = jiffies_64;

				if (test_bit(IS_LAZY, &expg->flags))
					Z_ERR(znd, "** Pg is lazy && zlt %"
					      PRIx64, expg->lba);

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
			spin_unlock_irqrestore(lock, flags);
		} else {
			deref_pg(expg);
		}
		if (entries == MAX_WSET)
			break;

		expg = _tpg;
		_tpg = list_prev_entry(expg, zltlst);
	}

writeback:
	spin_unlock_irqrestore(&znd->zlt_lck, zflgs);

	if (entries > 0) {
		err = _pool_write(znd, wset, entries);
		if (err < 0)
			goto out;
		if (entries == MAX_WSET)
			err = -EBUSY;
	}

out:
	if (!list_empty(&droplist))
		lazy_pool_splice(znd, &droplist);

	if (wset)
		ZDM_FREE(znd, wset, sizeof(*wset) * MAX_WSET, KM_19);

	return err;
}


/**
 * _pool_handle_crc() - Wait for current map_pg's to be CRC verified.
 * @znd: ZDM Instance
 * @wset: Array of pages to wait on (check CRC's).
 * @count: Number of entries.
 *
 *  NOTE: On entry all map_pg entries have elevated refcount from
 *        md_handle_crcs().
  */
static int _pool_handle_crc(struct zdm *znd, struct map_pg **wset, int count)
{
	int iter;
	struct map_pg *expg;
	int err = 0;

	/* write dirty table pages */
	if (count <= 0)
		goto out;

	if (count > 1)
		sort(wset, count, sizeof(*wset), wset_cmp_rd, NULL);

	for (iter = 0; iter < count; iter++) {
		expg = wset[iter];
		if (expg) {
			wait_for_map_pg(znd, expg, GFP_KERNEL);
			deref_pg(expg);
		}
	}

out:
	return err;
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
static int md_handle_crcs(struct zdm *znd)
{
	int err = 0;
	int entries = 0;
	unsigned long zflgs;
	struct map_pg *expg = NULL;
	struct map_pg *_tpg;
	struct map_pg **wset = NULL;

	wset = ZDM_CALLOC(znd, sizeof(*wset), MAX_WSET, KM_19, GFP_KERNEL);
	if (!wset) {
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
		return -ENOMEM;
	}

	spin_lock_irqsave(&znd->zlt_lck, zflgs);
	if (list_empty(&znd->zltpool))
		goto writeback;

	expg = list_last_entry(&znd->zltpool, typeof(*expg), zltlst);
	if (!expg || &expg->zltlst == (&znd->zltpool))
		goto writeback;

	_tpg = list_prev_entry(expg, zltlst);
	while (&expg->zltlst != &znd->zltpool) {
		ref_pg(expg);
		if (test_bit(R_CRC_PENDING, &expg->flags)) {
			if (entries < MAX_WSET) {
				ref_pg(expg);
				wset[entries] = expg;
				entries++;
			}
		}
		deref_pg(expg);
		if (entries == MAX_WSET)
			break;

		expg = _tpg;
		_tpg = list_prev_entry(expg, zltlst);
	}

writeback:
	spin_unlock_irqrestore(&znd->zlt_lck, zflgs);

	if (entries > 0) {
		err = _pool_handle_crc(znd, wset, entries);
		if (err < 0)
			goto out;
		if (entries == MAX_WSET)
			err = -EBUSY;
	}

out:
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

	if (low_cache_mem(znd) && (!sync || !drop)) {
		sync = 1;
		if (drop < 2048)
			drop = 2048;
	}

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
	u64 offset = (origin < znd->md_end) ? znd->md_start : znd->md_end;

	out->dm_s = origin;
	return map_addr_aligned(znd, origin - offset, out);
}


static int crc_test_pg(struct zdm *znd, struct map_pg *pg, gfp_t gfp,
		       struct mpinfo *mpi)
{
	unsigned long mflgs;
	int rcode = 0;
	__le16 check;
	__le16 expect = 0;

	/*
	 * Now check block crc
	 */
	check = crc_md_le16(pg->data.addr, Z_CRC_4K);
	if (test_bit(IS_LUT, &pg->flags)) {
		struct map_pg *crc_pg;
		const int async = 0;

		crc_pg = get_map_entry(znd, mpi->crc.lba, 1, async, 0, gfp);
		if (crc_pg) {
			ref_pg(crc_pg);
			if (crc_pg->data.crc) {
				spin_lock_irqsave(&crc_pg->md_lock, mflgs);
				expect = crc_pg->data.crc[mpi->crc.pg_idx];
				spin_unlock_irqrestore(&crc_pg->md_lock, mflgs);
				crc_pg->age = jiffies_64
					    + msecs_to_jiffies(crc_pg->hotness);
			}
			if (!pg->crc_pg) {
				spin_lock_irqsave(&pg->md_lock, mflgs);
				if (!pg->crc_pg) {
					ref_pg(crc_pg);
					pg->crc_pg = crc_pg;
				}
				spin_unlock_irqrestore(&pg->md_lock, mflgs);
			}
			deref_pg(crc_pg);
		}

		if (check != expect) {
			Z_ERR(znd, "Corrupt metadata (lut): %"
				PRIx64" -> %" PRIx64 " -> %" PRIx64 " crc [%"
				PRIx64 ".%u] : %04x != %04x",
				pg->lba, pg->last_write, pg->lba48_in,
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
		      " [%04x != %04x (have)] flags: %lx",
		      pg->lba, pg->lba48_in,
		      le16_to_cpu(expect),
		      le16_to_cpu(check),
		      pg->flags);
		dump_stack();
	}
	rcode = 1;

	return rcode;
}


/**
 * wait_for_map_pg() - Wait on bio of map_pg read ...
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
static int wait_for_map_pg(struct zdm *znd, struct map_pg *pg, gfp_t gfp)
{
	int err = 0;

	ref_pg(pg);

	if (!pg->data.addr) {
		unsigned long flags;

		spin_lock_irqsave(&pg->md_lock, flags);
		if (!pg->data.addr && !test_bit(R_SCHED, &pg->flags))
			err = -EBUSY;
		spin_unlock_irqrestore(&pg->md_lock, flags);

		if (err) {
			Z_ERR(znd, "wait_for_map_pg %llx : %lx ... no page?",
			      pg->lba, pg->flags);
			dump_stack();
			goto out;
		}
	}
	if (test_bit(R_IN_FLIGHT, &pg->flags) ||
	    test_bit(R_SCHED, &pg->flags) ||
	    test_bit(IS_ALLOC, &pg->flags)) {
		wait_for_completion_io(&pg->event);
		err = pg->io_error;
		if (err)
			goto out;
	}
	if (test_and_clear_bit(R_CRC_PENDING, &pg->flags)) {
		struct mpinfo mpi;

		to_table_entry(znd, pg->lba, 0, &mpi);
		err = crc_test_pg(znd, pg, gfp, &mpi);
		if (err)
			goto out;

		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
		Z_DBG(znd, "read: %" PRIx64 " -> %" PRIx64 " crc",
		      pg->lba, pg->last_write);
	}
out:
	if (err)
		Z_DBG(znd, "read: %" PRIx64 " -> %" PRIx64 " err: %d",
		      pg->lba, pg->last_write, err);
	deref_pg(pg);

	return err;
}

static void _pg_read_complete(struct map_pg *pg, int err)
{
	set_bit(R_CRC_PENDING, &pg->flags);
	clear_bit(R_IN_FLIGHT, &pg->flags);
	pg->io_error = err;
	pg->age = jiffies_64;
	set_bit(IS_FLUSH, &pg->flags);
	complete_all(&pg->event);
}

static void map_pg_bio_endio(struct bio *bio)
{
	struct map_pg *pg = bio->bi_private;

	ref_pg(pg);
	_pg_read_complete(pg, bio->bi_error);
	deref_pg(pg);
	bio_put(bio);
}

static int read_pg(struct zdm *znd, struct map_pg *pg, gfp_t gfp)
{
	const int count = 1;
	const int wq = 1;
	int rc;

	ref_pg(pg);
	rc = read_block(znd->ti, DM_IO_KMEM,
			pg->data.addr, pg->lba48_in, count, wq);
	if (rc) {
		Z_ERR(znd, "%s: read_block: ERROR: %d", __func__, rc);
		goto out;
	}
	_pg_read_complete(pg, rc);
	pool_add(pg->znd, pg);
	rc = wait_for_map_pg(znd, pg, gfp);
	deref_pg(pg);

out:
	return rc;
}

static int enqueue_pg(struct zdm *znd, struct map_pg *pg, gfp_t gfp)
{
	int rc = -ENOMEM;
	struct bio *bio;
#if ENABLE_SEC_METADATA
	sector_t sector;
#endif

	if (!znd->bio_queue || gfp != GFP_KERNEL) {
		rc = read_pg(znd, pg, gfp);
		goto out;
	}

	bio = bio_alloc_bioset(gfp, 1, znd->bio_set);
	if (bio) {
		int len;

		bio->bi_private = pg;
		bio->bi_end_io = map_pg_bio_endio;

#if ENABLE_SEC_METADATA
		sector = pg->lba48_in << Z_SHFT4K;
		bio->bi_bdev = znd_get_backing_dev(znd, &sector);
		bio->bi_iter.bi_sector = sector;
#else
		bio->bi_iter.bi_sector = pg->lba48_in << Z_SHFT4K;
		bio->bi_bdev = znd->dev->bdev;
#endif
		bio_set_op_attrs(bio, REQ_OP_READ, 0);
		bio->bi_iter.bi_size = 0;
		len = bio_add_km(bio, pg->data.addr, 1);
		if (len) {
			submit_bio(bio);
			pool_add(pg->znd, pg);
			rc = 0;
		}
	}

out:
	return rc;
}

static int empty_pg(struct zdm *znd, struct map_pg *pg)
{
	int empty_val = test_bit(IS_LUT, &pg->flags) ? 0xff : 0;

	memset(pg->data.addr, empty_val, Z_C4K);
	set_bit(R_CRC_PENDING, &pg->flags);
	clear_bit(R_IN_FLIGHT, &pg->flags);

Z_ERR(znd, "Clear PG? lba: %llx, lba48_in: %llx", pg->lba, pg->lba48_in);

	return pool_add(znd, pg);
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
	unsigned long flags;
	u64 lba48 = pg->lba;
	void *kmem;
	int rc = 0;
	int do_get_page = 0;

	ref_pg(pg);
	spin_lock_irqsave(&pg->md_lock, flags);
	__smp_mb();
	if (!pg->data.addr) {
		if (test_and_clear_bit(IS_ALLOC, &pg->flags)) {
			set_bit(R_SCHED, &pg->flags);
			lba48 = current_mapping(znd, pg->lba, gfp);
			pg->lba48_in = lba48;
			do_get_page = 1;
		} else if (!test_bit(R_SCHED, &pg->flags)) {
			Z_ERR(znd, "IS_ALLOC not set and page is empty");
			Z_ERR(znd, "cache_pg %llx : %lx ... no page?",
			      pg->lba, pg->flags);
			dump_stack();
		}
	}
	spin_unlock_irqrestore(&pg->md_lock, flags);

	if (!do_get_page)
		goto out;

	kmem = ZDM_ALLOC(znd, Z_C4K, PG_27, gfp);
	if (kmem) {
		spin_lock_irqsave(&pg->md_lock, flags);
		__smp_mb();
		if (unlikely(pg->data.addr)) {

Z_ERR(znd, "ALLOC/READ racing .. un-possible? PG: %llx", pg->lba);

			ZDM_FREE(znd, kmem, Z_C4K, PG_27);
			spin_unlock_irqrestore(&pg->md_lock, flags);
			goto out;
		}
		pg->data.addr = kmem;
		__smp_mb();
		pg->znd = znd;
		atomic_inc(&znd->incore);
		pg->age = jiffies_64;
		set_bit(R_IN_FLIGHT, &pg->flags);
		clear_bit(R_SCHED, &pg->flags);
		pg->io_error = 0;
		spin_unlock_irqrestore(&pg->md_lock, flags);

		if (lba48)
			rc = enqueue_pg(znd, pg, gfp);
		else
			rc = empty_pg(znd, pg);

		if (rc < 0) {
			Z_ERR(znd, "%s: addr %" PRIx64 " error: %d",
			      __func__, pg->lba, rc);
			complete_all(&pg->event);
			init_completion(&pg->event);
			set_bit(IS_ALLOC, &pg->flags);
			ZDM_FREE(znd, pg->data.addr, Z_C4K, PG_27);
			atomic_dec(&znd->incore);
			goto out;
		}
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
	} else {
		spin_lock_irqsave(&pg->md_lock, flags);
		clear_bit(R_SCHED, &pg->flags);
		complete_all(&pg->event);
		init_completion(&pg->event);
		set_bit(IS_ALLOC, &pg->flags);
		spin_unlock_irqrestore(&pg->md_lock, flags);
		Z_ERR(znd, "%s: Out of memory.", __func__);
		rc = -ENOMEM;
	}

out:
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
	const int async = 0;
	const int noio = 0;
	const int ahead = READ_AHEAD;
	int err;

	map_addr_calc(znd, addr, &maddr);
	pg = get_map_entry(znd, maddr.lut_s, ahead, async, noio, gfp);
	if (pg) {
		ref_pg(pg);
		err = wait_for_map_pg(znd, pg, gfp);
		if (err)
			Z_ERR(znd, "%s: wait_for_map_pg -> %d", __func__, err);
		if (pg->data.addr) {
			unsigned long mflgs;
			__le32 delta;

			spin_lock_irqsave(&pg->md_lock, mflgs);
			delta = pg->data.addr[maddr.pg_idx];
			spin_unlock_irqrestore(&pg->md_lock, mflgs);
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
		unsigned long mflgs;
		__le32 delta;
		__le32 value;
		int was_updated = 0;

		ref_pg(pg);
		spin_lock_irqsave(&pg->md_lock, mflgs);
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
			}
		} else {
			Z_ERR(znd, "*ERR* Mapping: %" PRIx64 " to %" PRIx64,
			      to_addr, maddr->dm_s);
		}
		spin_unlock_irqrestore(&pg->md_lock, mflgs);

		if (was_updated && is_fwd && (delta != MZTEV_UNUSED)) {
			u64 old_phy = map_value(znd, delta);

			err = unused_add(znd, old_phy, to_addr, 1, GFP_ATOMIC);
		}
		deref_pg(pg);
	} else {
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
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
static int __cached_to_tables(struct zdm *znd, u32 zone)
{
	struct map_cache_page *work_pg = NULL;
	struct map_pg **wset = NULL;
	unsigned long iflgs;
	const gfp_t gfp = GFP_KERNEL;
	int err = -ENOMEM;
	int once = 1;

	work_pg = ZDM_ALLOC(znd, sizeof(*work_pg), PG_09, gfp);
	wset = ZDM_CALLOC(znd, sizeof(*wset), MAX_WSET, KM_19, gfp);
	if (!wset || !work_pg) {
		Z_ERR(znd, "%s: ENOMEM @ %d", __func__, __LINE__);
		goto out;
	}

	err = 0;
	once = znd->unused->count;
	while (once || znd->unused->count > 5) {
		struct map_pool *mp = &znd->_use[1];
		struct map_cache_entry *maps = work_pg->maps;
		int moving = ARRAY_SIZE(work_pg->maps);

		spin_lock_irqsave(&znd->unused_rwlck, iflgs);
		if (znd->unused->count < moving)
			moving = znd->unused->count;
		working_set(maps, znd->unused->mcd.maps, moving, 1);
		if (moving > 30)
			moving = 30;
		spin_unlock_irqrestore(&znd->unused_rwlck, iflgs);

		err = move_unused_to_tables(znd, maps, moving, wset, MAX_WSET);
		if (err < 0) {
			deref_all_pgs(wset, MAX_WSET, low_cache_mem(znd));
			if (err == -ENOMEM)
				err = -EBUSY;
			goto out;
		}

		spin_lock_irqsave(&znd->unused_rwlck, iflgs);
		if (znd->unused->count < moving)
			moving = znd->unused->count;
		if (moving > 30)
			moving = 30;
		memset(maps, 0, sizeof(*maps));
		working_set(maps, znd->unused->mcd.maps, moving, 0);
		err = move_unused_to_tables(znd, maps, moving, NULL, 0);
		if (err < 0) {
			if (err == -ENOMEM)
				err = -EBUSY;
			spin_unlock_irqrestore(&znd->unused_rwlck, iflgs);
			deref_all_pgs(wset, MAX_WSET, low_cache_mem(znd));
			goto out;
		}
		if (znd->unused == &znd->_use[1])
			mp = &znd->_use[0];
		mp->sorted = mp->count = 0;
		err = do_sort_merge(mp, znd->unused, maps, moving, 1);
		if (unlikely(err)) {
			Z_ERR(znd, "USortMerge failed: %d [%d]", err, __LINE__);
			err = -EBUSY;
		} else {
			znd->unused = mp;
			__smp_mb();
		}
		spin_unlock_irqrestore(&znd->unused_rwlck, iflgs);
		memset(work_pg, 0, sizeof(*work_pg));
		deref_all_pgs(wset, MAX_WSET, low_cache_mem(znd));
		once = 0;
	}

	once = znd->ingress->count;
	while (once || znd->ingress->count > 80) {
		struct map_pool *mp = &znd->in[1];
		struct map_cache_entry *maps = work_pg->maps;
		int moving = ARRAY_SIZE(work_pg->maps);

		spin_lock_irqsave(&znd->in_rwlck, iflgs);
		if (znd->ingress->count < moving)
			moving = znd->ingress->count;

		if (znd->ingress->count < 2048 && moving > 15)
			moving = 15;
		if (moving > 60)
			moving = 60;
		working_set(maps, znd->ingress->mcd.maps, moving, 1);
		spin_unlock_irqrestore(&znd->in_rwlck, iflgs);

		err = move_to_map_tables(znd, maps, moving, wset, MAX_WSET);
		if (err < 0) {
			if (err == -ENOMEM)
				err = -EBUSY;
			deref_all_pgs(wset, MAX_WSET, low_cache_mem(znd));
			goto out;
		}

		spin_lock_irqsave(&znd->in_rwlck, iflgs);
		if (znd->ingress->count < moving)
			moving = znd->ingress->count;
		memset(maps, 0, sizeof(*maps));

		if (znd->ingress->count < 2048 && moving > 15)
			moving = 15;
		if (moving > 60)
			moving = 60;
		working_set(maps, znd->ingress->mcd.maps, moving, 0);
		err = move_to_map_tables(znd, maps, moving, NULL, 0);
		if (err < 0) {
			if (err == -ENOMEM)
				err = -EBUSY;
			spin_unlock_irqrestore(&znd->in_rwlck, iflgs);
			deref_all_pgs(wset, MAX_WSET, low_cache_mem(znd));
			goto out;
		}
		if (znd->ingress == &znd->in[1])
			mp = &znd->in[0];
		mp->sorted = mp->count = 0;
		err = do_sort_merge(mp, znd->ingress, maps, moving, 1);
		if (unlikely(err)) {
			Z_ERR(znd, "ISortMerge failed: %d [%d]", err, __LINE__);
			err = -EBUSY;
		} else {
			znd->ingress = mp;
			__smp_mb();
		}
		spin_unlock_irqrestore(&znd->in_rwlck, iflgs);
		memset(work_pg, 0, sizeof(*work_pg));
		deref_all_pgs(wset, MAX_WSET, low_cache_mem(znd));
		once = 0;
	}

out:
	if (wset)
		ZDM_FREE(znd, wset, sizeof(*wset) * MAX_WSET, KM_19);
	if (work_pg)
		ZDM_FREE(znd, work_pg, Z_C4K, PG_09);

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
	int err = 0;

	err = __cached_to_tables(znd, zone);
	return err;
}

static struct map_pg *__do_gme_io(struct zdm *znd, u64 lba,
			          int ahead, int async, gfp_t gfp)
{
	struct map_pg *pg;
	int retries = 5;

	do
		pg = do_gme_io(znd, lba, ahead, async, gfp);
	while (!pg && --retries > 0);

	return pg;
}

static struct map_pg *get_map_entry(struct zdm *znd, u64 lba,
				    int ahead, int async, int noio, gfp_t gfp)
{
	struct map_pg *pg;

	if (noio)
		pg = gme_noio(znd, lba);
	else
		pg = __do_gme_io(znd, lba, ahead, async, gfp);

	return pg;
}


/**
 * move_to_map_tables() - Migrate memcache to lookup table map entries.
 * @znd: ZDM instance
 * @mcache: memcache block.
 *
 * Return: non-zero on error.
 */
static int move_to_map_tables(struct zdm *znd, struct map_cache_entry *maps,
			      int count, struct map_pg **pgs, int npgs)
{
	struct map_pg *smtbl = NULL;
	struct map_pg *rmtbl = NULL;
	struct map_addr maddr = { .dm_s = 0ul };
	struct map_addr rev = { .dm_s = 0ul };
	u64 lut_s = BAD_ADDR;
	u64 lut_r = BAD_ADDR;
	int err = 0;
	int is_fwd = 1;
	int idx;
	int cpg = 0;
	const int async = 0;
	const int noio = pgs ? 0 : 1;
	gfp_t gfp = GFP_KERNEL;

	/* the journal being move must remain stable so sorting
	 * is disabled. If a sort is desired due to an unsorted
	 * page the search devolves to a linear lookup.
	 */
	for (idx = 0; idx < count; idx++) {
		int e;
		u32 flags;
		u32 extent;
		u64 addr = le64_to_lba48(maps[idx].tlba, &flags);
		u64 blba = le64_to_lba48(maps[idx].bval, &extent);
		u64 mapto;

		if (maps[idx].tlba == 0 && maps[idx].bval == 0)
			continue;

		if (flags & MCE_NO_ENTRY)
			continue;

		for (e = 0; e < extent; e++) {
			if (addr) {
				map_addr_calc(znd, addr, &maddr);
				if (lut_s != maddr.lut_s) {
					if (smtbl)
						deref_pg(smtbl);
					put_map_entry(smtbl);
					smtbl = get_map_entry(znd, maddr.lut_s, 4,
							   async, noio, gfp);
					if (!smtbl) {
						if (noio)
							break;
						err = -ENOMEM;
						goto out;
					}
					if (pgs && cpg < npgs) {
						pgs[cpg] = smtbl;
						ref_pg(smtbl);
						cpg++;
					}
					ref_pg(smtbl);
					lut_s = smtbl->lba;
				}
				is_fwd = 1;
				mapto = blba ? blba : BAD_ADDR;
				if (noio)
					err = update_map_entry(znd, smtbl,
							       &maddr, mapto,
							       is_fwd);
				if (err < 0)
					goto out;
			}
			if (blba) {
				map_addr_calc(znd, blba, &rev);
				if (lut_r != rev.lut_r) {
					if (rmtbl)
						deref_pg(rmtbl);
					put_map_entry(rmtbl);
					rmtbl = get_map_entry(znd, rev.lut_r, 4,
							   async, noio, gfp);
					if (!rmtbl) {
						if (noio)
							break;
						err = -ENOMEM;
						goto out;
					}
					if (pgs && cpg < npgs) {
						pgs[cpg] = rmtbl;
						ref_pg(rmtbl);
						cpg++;
					}
					ref_pg(rmtbl);
					lut_r = rmtbl->lba;
				}
				is_fwd = 0;
				if (noio)
					err = update_map_entry(znd, rmtbl, &rev,
							       addr, is_fwd);
				if (err == 1)
					err = 0;
				blba++;
			}
			if (addr)
				addr++;
			if (err < 0)
				goto out;
		}
		if (noio && e == extent) {
			u32 count;
			u32 flgs;

			addr = le64_to_lba48(maps[idx].tlba, &flgs);
			blba = le64_to_lba48(maps[idx].bval, &count);
			if (count == extent) {
				flgs = MCE_NO_ENTRY;
				count = 0;
				maps[idx].bval = lba48_to_le64(count, blba);
				maps[idx].tlba = lba48_to_le64(flgs, addr);
			}
		}
	}
out:
	if (!err && !noio)
		cpg = ref_crc_pgs(pgs, cpg, npgs);

	if (smtbl)
		deref_pg(smtbl);
	if (rmtbl)
		deref_pg(rmtbl);
	put_map_entry(smtbl);
	put_map_entry(rmtbl);
	set_bit(DO_MEMPOOL, &znd->flags);

	return err;
}

/**
 * __move_unused() - Discard overwritten blocks ...
 * blba: the LBA to update in the reverse map.
 * plut_r: LBA of current *_pg.
 * _pg: The active map_pg to update (or cache)
 * pgs: array of ref'd pages to build.
 * pcpg: current page
 * npgs: size of pgs array.
 */
static int __move_unused(struct zdm *znd,
			 u64 blba, u64 *plut_r, struct map_pg **_pg,
			 struct map_pg **pgs, int *pcpg, int npgs)
{
	struct map_addr maddr;
	struct map_pg *pg = *_pg;
	u64 lut_r = *plut_r;
	unsigned long mflgs;
	int cpg = *pcpg;
	int err = 0;
	const int async = 0;
	const int noio = pgs ? 0 : 1;
	gfp_t gfp = GFP_KERNEL;

	if (blba < znd->data_lba)
		goto out;

	map_addr_calc(znd, blba, &maddr);
	if (lut_r != maddr.lut_r) {
		put_map_entry(pg);
		if (pg)
			deref_pg(pg);

		pg = get_map_entry(znd, maddr.lut_r, 4, async, noio, gfp);
		if (!pg) {
			err = noio ? -EBUSY : -ENOMEM;
			goto out;
		}
		if (pgs && cpg < npgs) {
			pgs[cpg] = pg;
			ref_pg(pg);
			cpg++;
		}
		ref_pg(pg);
		lut_r = pg->lba;
	}

	/* on i/o pass .. only load and ref the map_pg's */
	if (pgs)
		goto out;

	if (!pg) {
		Z_ERR(znd, "No PG for %llx?", blba);
		Z_ERR(znd, "  ... maddr.lut_r %llx [%llx]", maddr.lut_r, lut_r);
		Z_ERR(znd, "  ... maddr.dm_s  %llx", maddr.dm_s);
		Z_ERR(znd, "  ... maddr.pg_idx %x", maddr.pg_idx);

		err = noio ? -EBUSY : -ENOMEM;
		goto out;
	}

	ref_pg(pg);
	spin_lock_irqsave(&pg->md_lock, mflgs);
	if (!pg->data.addr) {
		Z_ERR(znd, "PG w/o DATA !?!? %llx", pg->lba);
		err = noio ? -EBUSY : -ENOMEM;
		goto out_unlock;
	}
	if (maddr.pg_idx > 1024) {
		Z_ERR(znd, "Invalid pg index? %u", maddr.pg_idx);
		err = noio ? -EBUSY : -ENOMEM;
		goto out_unlock;
	}

	/*
	 * if the value is modified update the table and
	 * place it on top of the active [zltlst] list
	 */
	if (pg->data.addr[maddr.pg_idx] != MZTEV_UNUSED) {
		unsigned long wflgs;
		u32 gzno  = maddr.zone_id >> GZ_BITS;
		u32 gzoff = maddr.zone_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp;
		u32 zf;
		u32 stream_id;

		pg->data.addr[maddr.pg_idx] = MZTEV_UNUSED;
		pg->age = jiffies_64 + msecs_to_jiffies(pg->hotness);
		set_bit(IS_DIRTY, &pg->flags);
		clear_bit(IS_READA, &pg->flags);
		clear_bit(IS_FLUSH, &pg->flags);

		spin_lock_irqsave(&wpg->wplck, wflgs);
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
		spin_unlock_irqrestore(&wpg->wplck, wflgs);
		if ((wp & Z_WP_VALUE_MASK) == Z_BLKSZ)
			znd->discard_count++;
	} else {
		Z_DBG(znd, "lba: %" PRIx64 " already reported as free?", blba);
	}
out_unlock:
	spin_unlock_irqrestore(&pg->md_lock, mflgs);
	deref_pg(pg);

out:
	*plut_r = lut_r;
	*pcpg  = cpg;
	*_pg = pg;

	return err;
}

static int move_unused_to_tables(struct zdm *znd, struct map_cache_entry *maps,
				 int count, struct map_pg **pgs, int npgs)
{
	struct map_pg *pg = NULL;
	u64 lut_r = BAD_ADDR;
	int err = 0;
	int cpg = 0;
	int idx;

	/* the journal being move must remain stable so sorting
	 * is disabled. If a sort is desired due to an unsorted
	 * page the search devolves to a linear lookup.
	 */
	for (idx = 0; idx < count; idx++) {
		int e;
		u32 flags;
		u32 extent;
		u64 blba = le64_to_lba48(maps[idx].tlba, &flags);
		u64 from = le64_to_lba48(maps[idx].bval, &extent);

		if (maps[idx].tlba == 0 && maps[idx].bval == 0)
			continue;

		if (!blba) {
			Z_ERR(znd, "%d - bogus rmap entry", idx);
			continue;
		}

		for (e = 0; e < extent; e++) {
			err = __move_unused(znd, blba + e, &lut_r, &pg,
					    pgs, &cpg, npgs);
			if (err < 0)
				goto out;
		}
		if (!pgs) {
			flags |= MCE_NO_ENTRY;
			extent = 0;
			maps[idx].tlba = lba48_to_le64(flags, blba);
			maps[idx].bval = lba48_to_le64(extent, from);
		}
	}
out:
	cpg = ref_crc_pgs(pgs, cpg, npgs);

	if (pg)
		deref_pg(pg);
	put_map_entry(pg);
	set_bit(DO_MEMPOOL, &znd->flags);

	return err;
}
