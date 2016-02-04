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

#define BUILD_NO		101

#define EXTRA_DEBUG		0

#define MZ_MEMPOOL_SZ		256
#define JOURNAL_MEMCACHE_BLOCKS	4
#define MEM_PURGE_MSECS		10000

/* acceptable 'free' count for MZ free levels */
#define GC_PRIO_DEFAULT		0xFF00
#define GC_PRIO_LOW		0x7FFF
#define GC_PRIO_HIGH		0x0400
#define GC_PRIO_CRIT		0x0040

/* When less than 20 zones are free use aggressive gc in the megazone */
#define GC_COMPACT_AGGRESSIVE	32

/*
 *  For performance tuning:
 *   Q? smaller strips give smoother performance
 *      a single drive I/O is 8 (or 32?) blocks?
 *   A? Does not seem to ...
 */
#define GC_MAX_STRIPE		256
#define REPORT_BUFFER		65 /* 65 -> min # pages for 4096 descriptors */
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

#define GZ_BITS 10
#define GZ_MMSK ((1u << GZ_BITS) - 1)

#define CRC_BITS 11
#define CRC_MMSK ((1u << CRC_BITS) - 1)

#define MD_CRC_INIT		(cpu_to_le16(0x5249u))

static int map_addr_calc(struct zoned *, u64 dm_s, struct map_addr *out);
static int zoned_io_flush(struct zoned *znd);
static int zoned_wp_sync(struct zoned *znd, int reset_non_empty);

static void cache_if_dirty(struct zoned *znd, struct map_pg *pg, int wq);
static int write_if_dirty(struct zoned *, struct map_pg *, int use_wq, int snc);

static void gc_work_task(struct work_struct *work);
static void meta_work_task(struct work_struct *work);
static u64 mcache_greatest_gen(struct zoned *, int, u64 *, u64 *);
static u64 mcache_find_gen(struct zoned *, u64 base, int, u64 *out);
static int find_superblock(struct zoned *znd, int use_wq, int do_init);
static int do_sync_metadata(struct zoned *znd);
static int sync_mapped_pages(struct zoned *znd);
static int sync_crc_pages(struct zoned *znd);
static int unused_phy(struct zoned *znd, u64 lba, u64 orig_s, int gfp);
static struct io_4k_block *get_io_vcache(struct zoned *znd, int gfp);
static int put_io_vcache(struct zoned *znd, struct io_4k_block *cache);
static struct map_pg *get_map_entry(struct zoned *, u64 lba, int gfp);
static void put_map_entry(struct map_pg *);
static int cache_pg(struct zoned *znd, struct map_pg *pg, int, struct mpinfo *);
static int move_to_map_tables(struct zoned *znd, struct map_cache *jrnl);
static int keep_active_pages(struct zoned *znd, int allowed_pages);
static int zoned_create_disk(struct dm_target *ti, struct zoned *znd);
static int do_init_zoned(struct dm_target *ti, struct zoned *znd);
static sector_t jentry_value(struct map_sect_to_lba *e, bool is_block);
static u64 z_lookup_cache(struct zoned *znd, u64 addr);
static u64 z_lookup_table(struct zoned *znd, u64 addr, int gfp);
static u64 current_mapping(struct zoned *znd, u64 addr, int gfp);
static int z_mapped_add_one(struct zoned *znd, u64 dm_s, u64 lba, int gfp);
static int z_mapped_discard(struct zoned *znd, u64 dm_s, u64 lba);
static int z_mapped_addmany(struct zoned *znd, u64 dm_s, u64 lba, u64, int gfp);
static int z_mapped_to_list(struct zoned *znd, u64 dm_s, u64 lba, int gfp);
static int z_mapped_sync(struct zoned *znd);
static int z_mapped_init(struct zoned *znd);
static u64 z_acquire(struct zoned *znd, u32 flags, u32 nblks, u32 *nfound);
static __le32 sb_crc32(struct zdm_superblock *sblock);
static int update_map_entry(struct zoned *, struct map_pg *,
			    struct map_addr *, u64, int);
static int read_block(struct dm_target *, enum dm_io_mem_type,
		      void *, u64, unsigned int, int);
static int write_block(struct dm_target *, enum dm_io_mem_type,
		       void *, u64, unsigned int, int);
static int zoned_init_disk(struct dm_target *ti, struct zoned *znd,
			   int create, int force);

#define MutexLock(m)  test_and_lock((m), __LINE__)
#define SpinLock(s)   test_and_spin((s), __LINE__)

static inline void test_and_lock(struct mutex *m, int lineno)
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
static inline u32 _calc_zone(struct zoned *znd, u64 addr)
{
	u32 znum = ~0u;

	if (addr < znd->data_lba)
		return znum;

	addr -= znd->data_lba;
	znum = addr >> Z_BLKBITS;

	return znum;
}

/**
 * _pool_last() - Retrieve oldest metadata block from inpool
 * @znd: ZDM instance
 *
 * Return: oldest page of metadata or NULL.
 */
static inline struct map_pg *_pool_last(struct zoned *znd)
{
	struct map_pg *expg = NULL;

	if (!list_empty(&znd->smtpool)) {
		expg = list_last_entry(&znd->smtpool, typeof(*expg), inpool);
		if (&expg->inpool == &znd->smtpool)
			expg = NULL;
	}
	if (expg)
		atomic_inc(&expg->refcount);

	return expg;
}

/**
 * _pool_prev() - Retrieve previous metadata block from inpool
 * @znd: ZDM instance
 * @expg: current metadata block in inpool list.
 *
 * Return: next oldest [previous] page of metadata or NULL.
 */
static struct map_pg *_pool_prev(struct zoned *znd, struct map_pg *expg)
{
	struct map_pg *aux = NULL;

	if (expg)
		aux = list_prev_entry(expg, inpool);
	if (aux)
		if (&aux->inpool == &znd->smtpool)
			aux = NULL;
	if (aux)
		atomic_inc(&aux->refcount);

	return aux;
}

/**
 * pool_add() - Add metadata block to inpool
 * @znd: ZDM instance
 * @expg: current metadata block to add to inpool list.
 */
static inline void pool_add(struct zoned *znd, struct map_pg *expg)
{
	if (expg) {
		MutexLock(&znd->map_pool_lock);
		list_add(&expg->inpool, &znd->smtpool);
		mutex_unlock(&znd->map_pool_lock);
	}
}

/**
 * incore_hint() - Make expg first entry in pool list
 * @znd: ZDM instance
 * @expg: active map_pg
 */
static inline void incore_hint(struct zoned *znd, struct map_pg *expg)
{
	MutexLock(&znd->map_pool_lock);
	if (znd->smtpool.next != &expg->inpool)
		list_move(&expg->inpool, &znd->smtpool);
	mutex_unlock(&znd->map_pool_lock);
}

/**
 * to_table_entry() - Deconstrct metadata page into mpinfo
 * @znd: ZDM instance
 * @lba: Address (4k resolution)
 * @expg: current metadata block in inpool list.
 *
 * Return: Index into mpinfo.table.
 */
static int to_table_entry(struct zoned *znd, u64 lba, struct mpinfo *mpi)
{
	int index = -1;

	if (lba >= znd->s_base && lba < znd->r_base) {
		mpi->table	= znd->fwd_tm;
		index		= lba - znd->s_base;
		mpi->bit_type	= IS_LUT;
		mpi->bit_dir	= IS_FWD;
		mpi->crc.table	= znd->fwd_crc;
		mpi->crc.pg_no	= index >> CRC_BITS;
		mpi->crc.pg_idx	= index & CRC_MMSK;
		if (index < 0 ||  index >= znd->map_count) {
			Z_ERR(znd, "%s: FWD BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->map_count);
		}
	} else if (lba >= znd->r_base && lba < znd->c_base) {
		mpi->table	= znd->rev_tm;
		index		= lba - znd->r_base;
		mpi->bit_type	= IS_LUT;
		mpi->bit_dir	= IS_REV;
		mpi->crc.table	= znd->rev_crc;
		mpi->crc.pg_no	= index >> CRC_BITS;
		mpi->crc.pg_idx	= index & CRC_MMSK;
		if (index < 0 ||  index >= znd->map_count) {
			Z_ERR(znd, "%s: REV BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->map_count);
		}
	} else if (lba >= znd->c_base && lba < znd->c_mid) {
		mpi->table	= znd->fwd_crc;
		index		= lba - znd->c_base;
		mpi->bit_type	= IS_CRC;
		mpi->bit_dir	= IS_FWD;
		mpi->crc.table	= NULL;
		mpi->crc.pg_no	= 0;
		mpi->crc.pg_idx	= index & CRC_MMSK;
		if (index < 0 ||  index >= znd->crc_count) {
			Z_ERR(znd, "%s: CRC BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->crc_count);
		}
	} else if (lba >= znd->c_mid && lba < znd->c_end) {
		mpi->table	= znd->rev_crc;
		index		= lba - znd->c_mid;
		mpi->bit_type	= IS_CRC;
		mpi->bit_dir	= IS_REV;
		mpi->crc.table	= NULL;
		mpi->crc.pg_no	= 1;
		mpi->crc.pg_idx	= (1 << CRC_BITS) + (index & CRC_MMSK);
		if (index < 0 ||  index >= znd->crc_count) {
			Z_ERR(znd, "%s: CRC BAD IDX %"PRIx64" %d of %d",
				__func__, lba, index, znd->crc_count);
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
 * is_ready_for_gc() - Test zone flags for GC sanity and ready flag.
 * @znd: ZDM instance
 * @z_id: Address (4k resolution)
 *
 * Return: non-zero if zone is suitable for GC.
 */
static inline int is_ready_for_gc(struct zoned *znd, u32 z_id)
{
	u32 gzno  = z_id >> GZ_BITS;
	u32 gzoff = z_id & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];
	u32 wp = le32_to_cpu(wpg->wp_alloc[gzoff]);

	if (((wp & Z_WP_GC_BITS) == Z_WP_GC_READY) &&
	    ((wp & Z_WP_VALUE_MASK) == Z_BLKSZ))
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
 *        alloc/free count is tracked for dynamic analysis.
 */

#define GET_ZPG       0x040000
#define GET_KM        0x080000
#define GET_VM        0x100000

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
 * zdm_free() - Unified free by allocation 'code'
 * @znd: ZDM instance
 * @p: memory to be released.
 * @sz: allocated size.
 * @code: allocation size
 *
 * This (ugly) unified scheme helps to find leaks and monitor usage
 *   via ioctl tools.
 */
static void zdm_free(struct zoned *znd, void *p, size_t sz, u32 code)
{
	int id    = code & 0x00FFFF;
	int flag  = code & 0xFF0000;

	if (p) {
		if (znd) {
			SpinLock(&znd->stats_lock);
			if (sz > znd->memstat)
				Z_ERR(znd,
				      "Free'd more mem than allocated? %d", id);

			if (sz > znd->bins[id]) {
				Z_ERR(znd,
				      "Free'd more mem than allocated? %d", id);
				dump_stack();
			}
			znd->memstat -= sz;
			znd->bins[id] -= sz;
			spin_unlock(&znd->stats_lock);
		}

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
static void *zdm_alloc(struct zoned *znd, size_t sz, int code, int gfp)
{
	void *pmem = NULL;
	int id    = code & 0x00FFFF;
	int flag  = code & 0xFF0000;
	int gfp_flags;

#if USE_KTHREAD
	gfp_flags = GFP_KERNEL;
#else
	gfp_flags = gfp ? GFP_ATOMIC : GFP_KERNEL;
#endif

	might_sleep();

	switch (flag) {
	case GET_ZPG:
		pmem = (void *)get_zeroed_page(gfp_flags);
		if (!pmem && gfp_flags == GFP_ATOMIC) {
			Z_ERR(znd, "No atomic for %d, try noio.", id);
			pmem = (void *)get_zeroed_page(GFP_NOIO);
		}
		break;
	case GET_KM:
		pmem = kzalloc(sz, gfp_flags);
		if (!pmem && gfp_flags == GFP_ATOMIC) {
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
	if (!pmem) {
		Z_ERR(znd, "Out of memory. %d", id);
		dump_stack();
	}
	if (znd && pmem) {
		SpinLock(&znd->stats_lock);
		znd->memstat += sz;
		znd->bins[id] += sz;
		spin_unlock(&znd->stats_lock);
	}
	return pmem;
}

/**
 * zdm_calloc() - Unified alloc by 'code':
 * @znd: ZDM instance
 * @n: number of elements in array.
 * @sz: allocation size of each element.
 * @code: allocation strategy (VM, KM, PAGE, N-PAGES).
 * @gfp: kernel allocation flags.
 *
 * calloc is just an zeroed memory array alloc.
 * all zdm_alloc schemes are for zeroed memory so no extra memset needed.
 */
static void *zdm_calloc(struct zoned *znd, size_t n, size_t sz, int code, int q)
{
	return zdm_alloc(znd, sz * n, code, q);
}

/**
 * get_io_vcache() - Get a pre-allocated pool of memory for IO.
 * @znd: ZDM instance
 * @gfp: Allocation flags if no pre-allocated pool can be found.
 *
 * Return: Pointer to pool memory or NULL.
 */
static struct io_4k_block *get_io_vcache(struct zoned *znd, int gfp)
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
 * @gfp: Allocation flags if no pre-allocated pool can be found.
 *
 * Return: Pointer to pool memory or NULL.
 */
static int put_io_vcache(struct zoned *znd, struct io_4k_block *cache)
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
static inline u64 map_value(struct zoned *znd, __le32 delta)
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
static int map_encode(struct zoned *znd, u64 to_addr, __le32 *value)
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
static int release_memcache(struct zoned *znd)
{
	int no;

	for (no = 0; no < Z_HASH_SZ; no++) {
		struct list_head *_jhead = &(znd->mclist[no]);
		struct map_cache *jrnl;
		struct map_cache *jtmp;

		if (list_empty(_jhead))
			return 0;

		list_for_each_entry_safe(jrnl, jtmp, _jhead, mclist) {
			/** move all the journal entries into the SLT */
			SpinLock(&znd->mclck[no]);
			list_del(&jrnl->mclist);
			ZDM_FREE(znd, jrnl->jdata, Z_C4K, PG_08);
			ZDM_FREE(znd, jrnl, sizeof(*jrnl), KM_07);
			spin_unlock(&znd->mclck[no]);
		}

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
static inline int warn_bad_lba(struct zoned *znd, u64 lba48)
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
static void mapped_free(struct zoned *znd, struct map_pg *mapped)
{
	if (mapped) {
		MutexLock(&mapped->md_lock);
		WARN_ON(test_bit(IS_DIRTY, &mapped->flags));
		if (mapped->data.addr) {
			ZDM_FREE(znd, mapped->data.addr, Z_C4K, PG_27);
			znd->incore_count--;
		}
		mutex_unlock(&mapped->md_lock);
		ZDM_FREE(znd, mapped, sizeof(*mapped), KM_20);
	}
}

/**
 * flush_map() - write dirty map entres to disk.
 * @znd: ZDM instance
 * @map: Array of mapped pages.
 * @count: number of elements in range.
 * Return: non-zero on error.
 */
static int flush_map(struct zoned *znd, struct map_pg **map, u32 count)
{
	const int use_wq = 1;
	const int sync = 0;
	u32 ii;
	int err = 0;

	if (!map)
		return err;

	for (ii = 0; ii < count; ii++) {
		if (map[ii] && map[ii]->data.addr) {
			cache_if_dirty(znd, map[ii], use_wq);
			err |= write_if_dirty(znd, map[ii], use_wq, sync);
		}
	}

	return err;
}

/**
 * zoned_io_flush() - flush all pending IO.
 * @znd: ZDM instance
 */
static int zoned_io_flush(struct zoned *znd)
{
	int err = 0;

	set_bit(ZF_FREEZE, &znd->flags);
	atomic_inc(&znd->gc_throttle);

	mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
	flush_delayed_work(&znd->gc_work);

	clear_bit(DO_GC_NO_PURGE, &znd->flags);
	set_bit(DO_JOURNAL_MOVE, &znd->flags);
	set_bit(DO_MEMPOOL, &znd->flags);
	set_bit(DO_SYNC, &znd->flags);
	queue_work(znd->meta_wq, &znd->meta_work);
	flush_workqueue(znd->meta_wq);

	mod_delayed_work(znd->gc_wq, &znd->gc_work, 0);
	flush_delayed_work(&znd->gc_work);
	atomic_dec(&znd->gc_throttle);

	INIT_LIST_HEAD(&znd->smtpool);

	err = flush_map(znd, znd->fwd_tm, znd->map_count);
	if (err)
		goto out;

	err = flush_map(znd, znd->rev_tm, znd->map_count);
	if (err)
		goto out;

	err = flush_map(znd, znd->fwd_crc, znd->crc_count);
	if (err)
		goto out;

	err = flush_map(znd, znd->rev_crc, znd->crc_count);
	if (err)
		goto out;

	set_bit(DO_SYNC, &znd->flags);
	queue_work(znd->meta_wq, &znd->meta_work);
	flush_workqueue(znd->meta_wq);

out:
	return err;
}

/**
 * release_table_pages() - flush and free all table map entries.
 * @znd: ZDM instance
 */
static void release_table_pages(struct zoned *znd)
{
	int entry;

	if (znd->fwd_tm) {
		for (entry = 0; entry < znd->map_count; entry++) {
			mapped_free(znd, znd->fwd_tm[entry]);
			znd->fwd_tm[entry] = NULL;
		}
	}
	if (znd->rev_tm) {
		for (entry = 0; entry < znd->map_count; entry++) {
			mapped_free(znd, znd->rev_tm[entry]);
			znd->rev_tm[entry] = NULL;
		}
	}
	if (znd->fwd_crc) {
		for (entry = 0; entry < znd->crc_count; entry++) {
			mapped_free(znd, znd->fwd_crc[entry]);
			znd->fwd_crc[entry] = NULL;
		}
	}
	if (znd->rev_crc) {
		for (entry = 0; entry < znd->crc_count; entry++) {
			mapped_free(znd, znd->rev_crc[entry]);
			znd->rev_crc[entry] = NULL;
		}
	}
}

/**
 * _release_wp() - free all WP alloc/usage/used data.
 * @znd: ZDM instance
 *
 */
static void _release_wp(struct zoned *znd, struct meta_pg *wp)
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


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/**
 *  Teardown a zoned device mapper instance.
 */
static void zoned_destroy(struct zoned *znd)
{
	int purge;
	size_t ppgsz = sizeof(struct map_pg *);
	size_t mapsz = ppgsz * znd->map_count;
	size_t crcsz = ppgsz * znd->crc_count;

	del_timer_sync(&znd->timer);

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
	if (znd->fwd_tm)
		ZDM_FREE(znd, znd->fwd_tm, mapsz, VM_21);
	if (znd->rev_tm)
		ZDM_FREE(znd, znd->rev_tm, mapsz, VM_22);
	if (znd->fwd_crc)
		ZDM_FREE(znd, znd->fwd_crc, crcsz, VM_21);
	if (znd->rev_crc)
		ZDM_FREE(znd, znd->rev_crc, crcsz, VM_22);

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

	ZDM_FREE(NULL, znd, sizeof(*znd), KM_00);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void _init_streams(struct zoned *znd)
{
	u64 stream;

	for (stream = 0; stream < ARRAY_SIZE(znd->bmkeys->stream); stream++)
		znd->bmkeys->stream[stream] = cpu_to_le32(~0u);

	znd->z_meta_resv = cpu_to_le32(znd->data_zones - 2);
	znd->z_gc_resv   = cpu_to_le32(znd->data_zones - 1);
	znd->z_gc_free   = znd->data_zones - 2;
}

static void _init_mdcrcs(struct zoned *znd)
{
	int idx;

	for (idx = 0; idx < Z_C4K; idx++)
		znd->md_crcs[idx] = MD_CRC_INIT;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void _init_wp(struct zoned *znd, u32 gzno, struct meta_pg *wpg)
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
		wpg->wp_used[iter] = wpg->wp_alloc[iter];
	}
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

struct meta_pg *_alloc_wp(struct zoned *znd)
{
	struct meta_pg *wp;
	u32 gzno;

	wp = ZDM_CALLOC(znd, znd->gz_count, sizeof(*znd->wp), VM_21, NORMAL);
	if (!wp)
		goto out;
	for (gzno = 0; gzno < znd->gz_count; gzno++) {
		struct meta_pg *wpg = &wp[gzno];

		mutex_init(&wpg->wplck);
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
	}

out:
	return wp;
}

/**
 * alloc_map_tables() - Allocate map table entries.
 * @znd: ZDM Target
 * @mapct: Number of map entries needed.
 * @crcct: Number of CRC entries needed.
 */
static int alloc_map_tables(struct zoned *znd, u64 mapct, u64 crcct)
{
	const size_t ptrsz = sizeof(struct map_pg *);
	int rcode = 0;

	znd->fwd_tm  = ZDM_CALLOC(znd, mapct, ptrsz, VM_21, NORMAL);
	znd->rev_tm  = ZDM_CALLOC(znd, mapct, ptrsz, VM_22, NORMAL);
	znd->fwd_crc = ZDM_CALLOC(znd, crcct, ptrsz, VM_21, NORMAL);
	znd->rev_crc = ZDM_CALLOC(znd, crcct, ptrsz, VM_22, NORMAL);
	if (!znd->fwd_tm || !znd->rev_tm || !znd->fwd_crc || !znd->rev_crc)
		rcode = -ENOMEM;

	return rcode;
}

/**
 * do_init_zoned() - Initialize a zoned device mapper instance
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
static int do_init_zoned(struct dm_target *ti, struct zoned *znd)
{
	u64 size = i_size_read(get_bdev_bd_inode(znd));
	u64 bdev_nr_sect4k = size / Z_C4K;
	u64 data_zones = (bdev_nr_sect4k >> Z_BLKBITS) - znd->zdstart;
	u64 mzcount = dm_div_up(data_zones, MAX_ZONES_PER_MZ);
	u64 cache = MAX_CACHE_INCR * CACHE_COPIES * MAX_MZ_SUPP;
	u64 z0_lba = dm_round_up(znd->start_sect, Z_BLKSZ);
	u64 mapct = dm_div_up(data_zones << Z_BLKBITS, 1024);
	u64 crcct = dm_div_up(mapct, 2048);
	int no;
	u32 mz_min = 0; /* cache */
	int rcode = 0;

	INIT_LIST_HEAD(&znd->smtpool);

	for (no = 0; no < Z_HASH_SZ; no++) {
		INIT_LIST_HEAD(&znd->mclist[no]);
		spin_lock_init(&znd->mclck[no]);
		mutex_init(&znd->mcmutex[no]);
	}
	mutex_init(&znd->map_pool_lock);

	spin_lock_init(&znd->gc_lock);
	spin_lock_init(&znd->stats_lock);

	mutex_init(&znd->gc_postmap.cached_lock);
	mutex_init(&znd->gc_vcio_lock);
	mutex_init(&znd->vcio_lock);
	mutex_init(&znd->io_lock);
	mutex_init(&znd->mz_io_mutex);
	mutex_init(&znd->mapkey_lock);
	mutex_init(&znd->ct_lock);

	znd->data_zones = data_zones;
	znd->gz_count = mzcount;
	znd->crc_count = crcct;
	znd->map_count = mapct;

	/* z0_lba - lba of first full zone in disk addr space      */

	znd->md_start = z0_lba - znd->start_sect;
	if (znd->md_start < cache) {
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

	znd->z_sballoc = ZDM_ALLOC(znd, Z_C4K, PG_05, NORMAL);
	if (!znd->z_sballoc) {
		ti->error = "couldn't allocate in-memory superblock";
		rcode = -ENOMEM;
		goto out;
	}

	rcode = alloc_map_tables(znd, mapct, crcct);
	if (rcode)
		goto out;

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
		ti->error = "couldn't start header metadata update thread";
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
		ti->error = "couldn't start header metadata update thread";
		rcode = -ENOMEM;
		goto out;
	}

#if USE_KTHREAD
	init_waitqueue_head(&znd->bio_wait);
	spin_lock_init(&znd->bio_qlck);
#endif

	INIT_WORK(&znd->meta_work, meta_work_task);
	INIT_WORK(&znd->bg_work, bg_work_task);
	INIT_DELAYED_WORK(&znd->gc_work, gc_work_task);
	setup_timer(&znd->timer, activity_timeout, (unsigned long)znd);

	znd->incore_count = 0;
	znd->last_w = BAD_ADDR;

	set_bit(DO_SYNC, &znd->flags);

out:
	return rcode;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 * Metadata of zoned device mapper (for future backward compatibility)
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 * CRC check for superblock.
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 * Check the superblock to see if it is valid and not corrupt.
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

/*
 * Initialize the on-disk format of a zoned device mapper.
 */
static int zoned_create_disk(struct dm_target *ti, struct zoned *znd)
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 * Repair an otherwise good device mapper instance that was not cleanly removed.
 */
static int zoned_repair(struct zoned *znd)
{
	Z_INFO(znd, "Is Dirty .. zoned_repair consistency fixer TODO!!!.");
	return -ENOMEM;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/*
 * Locate the existing SB on disk and re-load or create the device-mapper
 * instance based on the existing disk state.
 */
static int zoned_init_disk(struct dm_target *ti, struct zoned *znd,
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
		set_bit(DO_JOURNAL_LOAD, &znd->flags);
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline sector_t jentry_value(struct map_sect_to_lba *e, bool is_block)
{
	sector_t value = 0;

	if (is_block)
		value = le64_to_lba48(e->physical, NULL);
	else
		value = le64_to_lba48(e->logical, NULL);

	return value;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int compare_logical_sectors(const void *x1, const void *x2)
{
	const struct map_sect_to_lba *r1 = x1;
	const struct map_sect_to_lba *r2 = x2;
	const u64 v1 = le64_to_lba48(r1->logical, NULL);
	const u64 v2 = le64_to_lba48(r2->logical, NULL);

	return (v1 < v2) ? -1 : ((v1 > v2) ? 1 : 0);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int __find_sector_entry_chunk(struct map_sect_to_lba *data,
				     s32 count, sector_t find, bool is_block)
{
	int at = -1;
	s32 first = 0;
	s32 last = count - 1;
	s32 middle = (first + last) / 2;

	while ((-1 == at) && (first <= last)) {
		sector_t logical = BAD_ADDR;

		if (count > middle && middle >= 0)
			logical = jentry_value(&data[middle], is_block);

		if (logical < find)
			first = middle + 1;
		else if (logical > find)
			last = middle - 1;
		else
			at = middle;

		middle = (first + last) / 2;
	}
	return at;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u64 z_lookup_key_range(struct zoned *znd, u64 addr)
{
	u64 found = 0ul;

	if (test_bit(ZF_POOL_FWD, &znd->flags))
		return found;

	if ((znd->sk_low <= addr) && (addr < znd->sk_high))
		found = FWD_TM_KEY_BASE + (addr - znd->sk_low);

	return found;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void _inc_zone_used(struct zoned *znd, u64 lba)
{
	u32 zone = _calc_zone(znd, lba);

	if (zone < znd->data_zones) {
		u32 gzno  = zone >> GZ_BITS;
		u32 gzoff = zone & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 used;
		u32 wp;

		MutexLock(&wpg->wplck);
		used = le32_to_cpu(wpg->wp_used[gzoff]) + 1;
		if (used == Z_BLKSZ) {
			wp = le32_to_cpu(wpg->wp_alloc[gzoff]) | Z_WP_GC_READY;
			wpg->wp_alloc[gzoff] = cpu_to_le32(wp);
		}
		wpg->wp_used[gzoff] = cpu_to_le32(used);
		set_bit(IS_DIRTY, &wpg->flags);
		mutex_unlock(&wpg->wplck);
	}
}


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_mapped_add_one(struct zoned *znd, u64 dm_s, u64 lba, int gfp)
{
	int err = 0;

	if (dm_s < znd->data_lba)
		return err;

	do {
		err = z_mapped_to_list(znd, dm_s, lba, gfp);
	} while (-EBUSY == err);

	if (lba)
		_inc_zone_used(znd, lba);

	return err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_mapped_discard(struct zoned *znd, u64 dm_s, u64 lba)
{
	int err;

	do {
		err = z_mapped_to_list(znd, dm_s, 0ul, CRIT);
	} while (-EBUSY == err);

	return err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static struct map_cache *jalloc(struct zoned *znd, int gfp)
{
	struct map_cache *jrnl_first;

	jrnl_first = ZDM_ALLOC(znd, sizeof(*jrnl_first), KM_07, gfp);
	if (jrnl_first) {
		mutex_init(&jrnl_first->cached_lock);
		jrnl_first->jcount = 0;
		jrnl_first->jsorted = 0;
		jrnl_first->jdata = ZDM_CALLOC(znd, Z_UNSORTED,
			sizeof(*jrnl_first->jdata), PG_08, gfp);

		if (jrnl_first->jdata) {
			u64 logical = Z_LOWER48;
			u64 physical = Z_LOWER48;

			jrnl_first->jdata[0].logical = cpu_to_le64(logical);
			jrnl_first->jdata[0].physical = cpu_to_le64(physical);
			jrnl_first->jsize = Z_UNSORTED - 1;

		} else {
			Z_ERR(znd, "%s: in memory journal is out of space.",
			      __func__);
			ZDM_FREE(znd, jrnl_first, sizeof(*jrnl_first), KM_07);
			jrnl_first = NULL;
		}
	}
	return jrnl_first;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static struct map_cache *jfirst_entry(struct zoned *znd, int n)
{
	unsigned long flags;
	struct map_cache *jrnl;

	spin_lock_irqsave(&znd->mclck[n], flags);
	jrnl = list_first_entry_or_null(&znd->mclist[n], typeof(*jrnl), mclist);
	if (jrnl && (&jrnl->mclist != &znd->mclist[n]))
		atomic_inc(&jrnl->refcount);
	else
		jrnl = NULL;
	spin_unlock_irqrestore(&znd->mclck[n], flags);

	return jrnl;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline void mclist_add(struct zoned *znd, int n, struct map_cache *mc)
{
	unsigned long flags;

	spin_lock_irqsave(&znd->mclck[n], flags);
	list_add(&mc->mclist, &znd->mclist[n]);
	spin_unlock_irqrestore(&znd->mclck[n], flags);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline void jderef(struct zoned *znd, int n, struct map_cache *mcache)
{
	unsigned long flags;

	spin_lock_irqsave(&znd->mclck[n], flags);
	atomic_dec(&mcache->refcount);
	spin_unlock_irqrestore(&znd->mclck[n], flags);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline struct map_cache *jnext_entry(struct zoned *znd, int n,
					    struct map_cache *mcache)
{
	unsigned long flags;
	struct map_cache *next;

	spin_lock_irqsave(&znd->mclck[n], flags);
	next = list_next_entry(mcache, mclist);
	if (next && (&next->mclist != &znd->mclist[n]))
		atomic_inc(&next->refcount);
	else
		next = NULL;
	atomic_dec(&mcache->refcount);
	spin_unlock_irqrestore(&znd->mclck[n], flags);

	return next;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void memcache_sort(struct zoned *znd, struct map_cache *mcache, int no)
{
	if (mcache->jsorted < mcache->jcount) {
		int locked = mutex_is_locked(&znd->mcmutex[no]);

		if (!locked)
			MutexLock(&znd->mcmutex[no]);

		sort(&mcache->jdata[1], mcache->jcount,
		     sizeof(*mcache->jdata),
		     compare_logical_sectors, NULL);
		mcache->jsorted = mcache->jcount;

		if (!locked)
			mutex_unlock(&znd->mcmutex[no]);
	}
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int memcache_lock_and_sort(struct zoned *znd, struct map_cache *mcache,
				  int no)
{
	if (mcache->jsorted < mcache->jcount &&
	    mcache->busy_locked.counter == 0) {
		mutex_lock_nested(&mcache->cached_lock, SINGLE_DEPTH_NESTING);
		memcache_sort(znd, mcache, no);
		mutex_unlock(&mcache->cached_lock);
		return 0;
	}
	return -EBUSY;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int jlinear_find(struct map_cache *mcache, u64 dm_s)
{
	int at = -1;
	int jentry;

	for (jentry = mcache->jcount; jentry > 0; jentry--) {
		u64 logi = le64_to_lba48(mcache->jdata[jentry].logical, NULL);

		if (logi == dm_s) {
			at = jentry - 1;
			goto out;
		}
	}

out:
	return at;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u64 z_lookup_cache(struct zoned *znd, u64 addr)
{
	struct map_cache *mcache;
	u64 found = 0ul;
	int no = addr % Z_HASH_SZ;

	mcache = jfirst_entry(znd, no);
	while (mcache) {
		int at;
		int err;

		/* sort, if needed. only err is -EBUSY so do a linear find. */
		err = memcache_lock_and_sort(znd, mcache, no);
		if (!err)
			at = __find_sector_entry_chunk(&mcache->jdata[1],
				mcache->jcount, addr, 0);
		else
			at = jlinear_find(mcache, addr);

		if (at != -1) {
			struct map_sect_to_lba *data = &mcache->jdata[at + 1];

			found = le64_to_lba48(data->physical, NULL);
		}
		if (found) {
			jderef(znd, no, mcache);
			goto out;
		}

		mcache = jnext_entry(znd, no, mcache);
	}
out:
	return found;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int lba_in_zone(struct zoned *znd, struct map_cache *mcache, u32 zone)
{
	int jentry;

	if (zone >= znd->data_zones)
		goto out;

	for (jentry = mcache->jcount; jentry > 0; jentry--) {
		u64 lba = le64_to_lba48(mcache->jdata[jentry].physical, NULL);

		if (lba && _calc_zone(znd, lba) == zone)
			return 1;
	}
out:
	return 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int gc_verify_cache(struct zoned *znd, u32 zone)
{
	struct map_cache *mcache = NULL;
	int err = 0;
	int no;

	for (no = 0; no < Z_HASH_SZ; no++) {
		mcache = jfirst_entry(znd, no);
		while (mcache) {
			MutexLock(&mcache->cached_lock);
			if (lba_in_zone(znd, mcache, zone)) {
				Z_ERR(znd, "GC: **ERR** %" PRIx32
				      " LBA in cache <= Corrupt", zone);
				err = 1;
				znd->meta_result = -ENOSPC;
			}
			mutex_unlock(&mcache->cached_lock);
			mcache = jnext_entry(znd, no, mcache);
		}
	}

	return err;
}


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int __cached_to_tables(struct zoned *znd, int no, u32 zone)
{
	struct map_cache *jrnl = NULL;
	int err = 0;

	jrnl = jfirst_entry(znd, no);
	while (jrnl) {
		int try_free = 0;
		struct map_cache *jskip;

		atomic_inc(&jrnl->busy_locked);
		MutexLock(&jrnl->cached_lock);
		if (jrnl->jcount == jrnl->jsize) {
			memcache_sort(znd, jrnl, no);
			err = move_to_map_tables(znd, jrnl);
			if (!err && (jrnl->jcount == 0))
				try_free = 1;
		} else {
			if (lba_in_zone(znd, jrnl, zone)) {
				Z_ERR(znd,
					"Moving %d Runts because z: %u",
					jrnl->jcount, zone);

				memcache_sort(znd, jrnl, no);
				err = move_to_map_tables(znd, jrnl);
			}
		}
		mutex_unlock(&jrnl->cached_lock);
		atomic_dec(&jrnl->busy_locked);

		if (err) {
			jderef(znd, no, jrnl);
			Z_ERR(znd, "%s: Sector map failed.", __func__);
			goto out;
		}

		jskip = jnext_entry(znd, no, jrnl);
		if (try_free && mutex_trylock(&znd->mcmutex[no])) {
			if (jrnl->refcount.counter == 0) {
				unsigned long flags;
				size_t sz = Z_UNSORTED * sizeof(*jrnl->jdata);

				spin_lock_irqsave(&znd->mclck[no], flags);
				list_del(&jrnl->mclist);
				spin_unlock_irqrestore(&znd->mclck[no], flags);

				ZDM_FREE(znd, jrnl->jdata, sz, PG_08);
				ZDM_FREE(znd, jrnl, sizeof(*jrnl), KM_07);
				jrnl = NULL;

				znd->mc_entries--;
			}
			mutex_unlock(&znd->mcmutex[no]);
		}
		jrnl = jskip;
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
static int _cached_to_tables(struct zoned *znd, u32 zone)
{
	int err = 0;
	int no;

	for (no = 0; no < Z_HASH_SZ; no++) {
		int sub = __cached_to_tables(znd, no, zone);

		if (sub)
			err = sub;
	}
	return err;
}


/**
 * z_flush_bdev() - Request backing device flushed to disk.
 * @param znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int z_flush_bdev(struct zoned *znd)
{
	int err;
	sector_t bi_done;

	err = blkdev_issue_flush(znd->dev->bdev, GFP_KERNEL, &bi_done);
	if (err)
		Z_ERR(znd, "%s: flush failing sector %lu!", __func__, bi_done);

	return err;
}

/**
 * _meta_sync_to_disk() - Write ZDM state to disk.
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int _meta_sync_to_disk(struct zoned *znd)
{
	int err = 0;

	err = do_sync_metadata(znd);
	if (err) {
		Z_ERR(znd, "Uh oh: do_sync_metadata -> %d", err);
		goto out;
	}

	err = z_mapped_sync(znd);
	if (err) {
		Z_ERR(znd, "Uh oh. z_mapped_sync -> %d", err);
		goto out;
	}

out:
	return err;
}

/**
 * do_init_from_journal() - Restore ZDM state from disk.
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int do_init_from_journal(struct zoned *znd)
{
	int err = 0;

	if (test_and_clear_bit(DO_JOURNAL_LOAD, &znd->flags))
		err = z_mapped_init(znd);

	return err;
}

/**
 * do_journal_to_table() - Migrate memcache entries to lookup tables
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int do_journal_to_table(struct zoned *znd)
{
	int err = 0;

	if (test_and_clear_bit(DO_JOURNAL_MOVE, &znd->flags))
		err = _cached_to_tables(znd, znd->data_zones);

	return err;
}

/**
 * do_free_pages() - Release some pages to RAM
 * @znd: ZDM instance.
 *
 * Return: 0 on success or -errno value
 */
static int do_free_pages(struct zoned *znd)
{
	int err = 0;

	if (test_and_clear_bit(DO_MEMPOOL, &znd->flags)) {
		int pool_size = MZ_MEMPOOL_SZ * 4;

		if (is_expired_msecs(znd->age, MEM_PURGE_MSECS))
			pool_size = MZ_MEMPOOL_SZ;

		if (is_expired_msecs(znd->age, 5000))
			pool_size = 3;

		err = keep_active_pages(znd, pool_size);
	}
	return err;
}

/**
 * do_sync_to_disk() - Write ZDM state to disk.
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int do_sync_to_disk(struct zoned *znd)
{
	int err = 0;

	if (test_and_clear_bit(DO_SYNC, &znd->flags))
		err = _meta_sync_to_disk(znd);
	return err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void meta_work_task(struct work_struct *work)
{
	int err = 0;
	struct zoned *znd;

	if (!work)
		return;

	znd = container_of(work, struct zoned, meta_work);
	if (!znd)
		return;

	MutexLock(&znd->mz_io_mutex);
	err = do_init_from_journal(znd);

	/*
	 * Reduce memory pressure on journal list of arrays
	 * by pushing them into the sector map lookup tables
	 */
	if (!err)
		err = do_journal_to_table(znd);

	/*
	 *  Reduce memory pressure on sector map lookup tables
	 * by pushing them onto disc
	 */
	if (!err)
		err = do_free_pages(znd);

	/* force a consistent set of meta data out to disk */
	if (!err)
		err = do_sync_to_disk(znd);

	znd->age = jiffies_64;
	if (err < 0)
		znd->meta_result = err;

	clear_bit(DO_METAWORK_QD, &znd->flags);

	mutex_unlock(&znd->mz_io_mutex);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_mapped_to_list(struct zoned *znd, u64 dm_s, u64 lba, int gfp)
{
	struct map_cache *jrnl = NULL;
	struct map_cache *jrnl_first = NULL;
	int handled = 0;
	int list_count = 0;
	int err = 0;
	int no = dm_s % Z_HASH_SZ;

	jrnl = jfirst_entry(znd, no);
	while (jrnl) {
		struct map_cache *jskip;
		int try_free = 0;
		int at;

		MutexLock(&jrnl->cached_lock);
		memcache_sort(znd, jrnl, no);
		at = __find_sector_entry_chunk(&jrnl->jdata[1], jrnl->jcount,
					       dm_s, 0);
		if (at != -1) {
			struct map_sect_to_lba *data;
			u64 lba_was;
			u64 physical;

			MutexLock(&znd->mcmutex[no]);
			data = &jrnl->jdata[at + 1];
			lba_was = le64_to_lba48(data->physical, NULL);
			physical = lba & Z_LOWER48;

			if (lba != lba_was) {
				Z_DBG(znd, "Remap %" PRIx64 " -> %" PRIx64
					   " (was %" PRIx64 "->%" PRIx64 ")",
				      dm_s, lba,
				      le64_to_lba48(data->logical, NULL),
				      le64_to_lba48(data->physical, NULL));
				err = unused_phy(znd, lba_was, 0, gfp);
				if (err == 1)
					err = 0;
			}
			data->physical = cpu_to_le64(physical);
			handled = 1;
			mutex_unlock(&znd->mcmutex[no]);
		} else if (!jrnl_first) {
			if (jrnl->jcount < jrnl->jsize)
				jrnl_first = jrnl;
		} else if (jrnl->jcount == 0 && jrnl->refcount.counter == 0) {
			try_free = 1;
		}
		mutex_unlock(&jrnl->cached_lock);
		if (handled) {
			jderef(znd, no, jrnl);
			goto out;
		}

		jskip = jnext_entry(znd, no, jrnl);
		if (try_free) {
			unsigned long flags;
			size_t sz;

			MutexLock(&znd->mcmutex[no]);
			if (!jrnl->refcount.counter == 0) {

				sz = Z_UNSORTED * sizeof(*jrnl->jdata);
				spin_lock_irqsave(&znd->mclck[no], flags);
				list_del(&jrnl->mclist);
				spin_unlock_irqrestore(&znd->mclck[no], flags);

				ZDM_FREE(znd, jrnl->jdata, sz, PG_08);
				ZDM_FREE(znd, jrnl, sizeof(*jrnl), KM_07);
				jrnl = NULL;

				znd->mc_entries--;
			}
			mutex_unlock(&znd->mcmutex[no]);
		}
		jrnl = jskip;
		list_count++;
	}

	/* ------------------------------------------------------------------ */
	/* ------------------------------------------------------------------ */
	if (jrnl_first) {
		atomic_inc(&jrnl_first->refcount);
	} else {
		jrnl_first = jalloc(znd, gfp);
		if (jrnl_first) {
			atomic_inc(&jrnl_first->refcount);
			mclist_add(znd, no, jrnl_first);
		} else {
			Z_ERR(znd, "%s: in memory journal is out of space.",
			      __func__);
			err = -ENOMEM;
			goto out;
		}

		if (list_count > JOURNAL_MEMCACHE_BLOCKS)
			set_bit(DO_JOURNAL_MOVE, &znd->flags);
		znd->mc_entries = list_count + 1;
	}

	/* ------------------------------------------------------------------ */
	/* ------------------------------------------------------------------ */

	if (jrnl_first) {
		MutexLock(&znd->mcmutex[no]);
		MutexLock(&jrnl_first->cached_lock);
		if (jrnl_first->jcount < jrnl_first->jsize) {
			u16 idx = ++jrnl_first->jcount;

			WARN_ON(jrnl_first->jcount > jrnl_first->jsize);

			jrnl_first->jdata[idx].logical = lba48_to_le64(0, dm_s);
			jrnl_first->jdata[idx].physical = lba48_to_le64(0, lba);
		} else {
			Z_ERR(znd, "%s: cached bin out of space!", __func__);
			err = -EBUSY;
		}
		mutex_unlock(&jrnl_first->cached_lock);
		jderef(znd, no, jrnl_first);
		mutex_unlock(&znd->mcmutex[no]);
	}
out:
	return err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline u64 next_generation(struct zoned *znd)
{
	u64 generation = le64_to_cpu(znd->bmkeys->generation);

	if (generation == 0)
		generation = 2;

	generation++;
	if (generation == 0)
		generation++;

	return generation;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_mapped_sync(struct zoned *znd)
{
	struct dm_target *ti = znd->ti;
	struct map_cache *jrnl;
	int nblks = 1;
	int use_wq = 0;
	int rc = 1;
	int jwrote = 0;
	int cached = 0;
	int idx = 0;
	int no;
	int need_sync_io = 1;
	u64 lba = LBA_SB_START;
	u64 generation = next_generation(znd);
	u64 modulo = CACHE_COPIES;
	u64 incr = MAX_CACHE_INCR;
	struct io_4k_block *io_vcache;

	MutexLock(&znd->vcio_lock);
	io_vcache = get_io_vcache(znd, NORMAL);
	if (!io_vcache) {
		Z_ERR(znd, "%s: FAILED to get IO CACHE.", __func__);
		rc = -ENOMEM;
		goto out;
	}


/* TIME TO DO SOME WORK!! */

/* dirty wp blocks (zone pointers) [allow for 64*4 blocks] */
/* md_crcs-> 0x2148/9 */

	lba = 0x2048;
	for (idx = 0; idx < znd->gz_count; idx++) {
		struct meta_pg *wpg = &znd->wp[idx];

		if (test_bit(IS_DIRTY, &wpg->flags)) {
			znd->bmkeys->wp_crc[idx] = crc_md_le16(wpg->wp_alloc,
				Z_CRC_4K);
			znd->bmkeys->zf_crc[idx] = crc_md_le16(wpg->zf_est,
				Z_CRC_4K);
			if (test_bit(IS_DIRTY, &wpg->flags)) {
				memcpy(io_vcache[0].data, wpg->wp_alloc, Z_C4K);
				memcpy(io_vcache[1].data, wpg->zf_est, Z_C4K);

				rc = write_block(ti, DM_IO_VMA, io_vcache, lba,
					cached, use_wq);
				if (rc)
					goto out;
			}
			clear_bit(IS_DIRTY, &wpg->flags);
		}
		lba += 2;
	}

	lba += (generation % modulo) * incr;
	if (lba == 0)
		lba++;

	znd->bmkeys->generation = cpu_to_le64(generation);
	znd->bmkeys->gc_resv = cpu_to_le32(znd->z_gc_resv);
	znd->bmkeys->meta_resv = cpu_to_le32(znd->z_meta_resv);

	idx = 0;
	for (no = 0; no < Z_HASH_SZ; no++) {
		jrnl = jfirst_entry(znd, no);
		while (jrnl) {
			u64 phy = le64_to_lba48(jrnl->jdata[0].physical, NULL);
			u16 jcount = jrnl->jcount & 0xFFFF;

			jrnl->jdata[0].physical = lba48_to_le64(jcount, phy);
			znd->bmkeys->crcs[idx] =
				crc_md_le16(jrnl->jdata, Z_CRC_4K);
			idx++;

			memcpy(io_vcache[cached].data, jrnl->jdata, Z_C4K);
			cached++;

			if (cached == IO_VCACHE_PAGES) {
				rc = write_block(ti, DM_IO_VMA, io_vcache, lba,
						 cached, use_wq);
				if (rc) {
					Z_ERR(znd, "%s: cache-> %" PRIu64
					      " [%d blks] %p -> %d",
					      __func__, lba, nblks,
					      jrnl->jdata, rc);
					jderef(znd, no, jrnl);
					goto out;
				}
				lba    += cached;
				jwrote += cached;
				cached  = 0;
			}
			jrnl = jnext_entry(znd, no, jrnl);
		}
		jwrote += cached;
	}
	if (jwrote > 20)
		Z_ERR(znd, "**WARNING** large map cache %d", jwrote);

	znd->bmkeys->md_crc = crc_md_le16(znd->md_crcs, 2 * Z_CRC_4K);
	znd->bmkeys->n_crcs = cpu_to_le16(jwrote);
	znd->bmkeys->crc32 = 0;
	znd->bmkeys->crc32 = cpu_to_le32(crc32c(~0u, znd->bmkeys, Z_CRC_4K));
	if (cached < (IO_VCACHE_PAGES - 3)) {
		memcpy(io_vcache[cached].data, znd->bmkeys, Z_C4K);
		cached++;
		memcpy(io_vcache[cached].data, znd->md_crcs, Z_C4K * 2);
		cached++;
		need_sync_io = 0;
	}

	do {
		if (cached > 0) {
			rc = write_block(ti, DM_IO_VMA, io_vcache, lba, cached,
					 use_wq);
			if (rc) {
				Z_ERR(znd, "%s: Jrnl-> %" PRIu64
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
			need_sync_io = 0;
		}
	} while (cached > 0);

out:
	put_io_vcache(znd, io_vcache);
	mutex_unlock(&znd->vcio_lock);
	return rc;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline void zoned_personality(struct zoned *znd,
				     struct zdm_superblock *sblock)
{
	znd->zdstart = le32_to_cpu(sblock->zdstart);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int find_superblock_at(struct zoned *znd, u64 lba, int use_wq,
			      int do_init)
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int find_superblock(struct zoned *znd, int use_wq, int do_init)
{
	int found = 0;
	u64 lba;

	for (lba = 0; lba < 0x800; lba += MAX_CACHE_INCR) {
		found = find_superblock_at(znd, lba, use_wq, do_init);
		if (found)
			break;
	}
	return found;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u64 mcache_find_gen(struct zoned *znd, u64 lba, int use_wq,
				    u64 *sb_lba)
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
			Z_ERR(znd, "%s: Jrnl-> %" PRIu64 " [%d blks] %p -> %d",
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
		if (data[0] == 0 && data[1] == 0) {
			/* No SB here... */
			Z_DBG(znd, "FGen: Invalid block %" PRIx64 "?", lba);
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline int cmp_gen(u64 left, u64 right)
{
	int result = 0;

	if (left != right) {
		u64 delta = (left > right) ? left - right : right - left;

		result = -1;
		if (delta > 1) {
			if (left == BAD_ADDR)
				result = 1;
		} else {
			if (right > left)
				result = 1;
		}
	}

	return result;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u64 mcache_greatest_gen(struct zoned *znd, int use_wq, u64 *sb,
				u64 *at_lba)
{
	u64 lba = LBA_SB_START;
	u64 gen_no[CACHE_COPIES] = { 0ul, 0ul, 0ul };
	u64 gen_lba[CACHE_COPIES] = { 0ul, 0ul, 0ul };
	u64 gen_sb[CACHE_COPIES] = { 0ul, 0ul, 0ul };
	u64 incr = MAX_CACHE_INCR;
	int locations = ARRAY_SIZE(gen_lba);
	int pick = 0;
	int idx;

	for (idx = 0; idx < locations; idx++) {
		u64 *pAt = &gen_sb[idx];

		gen_lba[idx] = lba;
		gen_no[idx] = mcache_find_gen(znd, lba, use_wq, pAt);
		if (gen_no[idx])
			pick = idx;
		lba += incr;
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u64 count_stale_blocks(struct zoned *znd, u32 gzno, struct meta_pg *wpg)
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_mapped_init(struct zoned *znd)
{
	struct dm_target *ti = znd->ti;
	int nblks = 1;
	int wq = 0;
	int rc = 1;
	int done = 0;
	int jfound = 0;
	int idx = 0;
	struct list_head hjload;
	u64 lba = 0;
	u64 generation;
	__le32 crc_chk;
	struct io_4k_block *io_vcache;

	MutexLock(&znd->vcio_lock);
	io_vcache = get_io_vcache(znd, NORMAL);

	if (!io_vcache) {
		Z_ERR(znd, "%s: FAILED to get SYNC CACHE.", __func__);
		rc = -ENOMEM;
		goto out;
	}

	/*
	 * Read write printers
	 */
	lba = 0x2048;
	for (idx = 0; idx < znd->gz_count; idx++) {
		struct meta_pg *wpg = &znd->wp[idx];

		rc = read_block(ti, DM_IO_KMEM, wpg->wp_alloc, lba, 1, wq);
		if (rc)
			goto out;
		rc = read_block(ti, DM_IO_KMEM, wpg->zf_est,   lba + 1, 1, wq);
		if (rc)
			goto out;
		lba += 2;
	}

	INIT_LIST_HEAD(&hjload);

	generation = mcache_greatest_gen(znd, wq, NULL, &lba);
	if (generation == 0) {
		rc = -ENODATA;
		goto out;
	}

	if (lba == 0)
		lba++;

	do {
		struct map_cache *jrnl = jalloc(znd, NORMAL);

		if (!jrnl) {
			rc = -ENOMEM;
			goto out;
		}

		rc = read_block(ti, DM_IO_KMEM,
				jrnl->jdata, lba, nblks, wq);
		if (rc) {
			Z_ERR(znd, "%s: Jrnl-> %" PRIu64 " [%d blks] %p -> %d",
			      __func__, lba, nblks, jrnl->jdata, rc);

			goto out;
		}
		lba++;

		if (is_key_page(jrnl->jdata)) {
			size_t sz = Z_UNSORTED * sizeof(*jrnl->jdata);

			memcpy(znd->bmkeys, jrnl->jdata, Z_C4K);
			jrnl->jcount = 0;
			done = 1;
			ZDM_FREE(znd, jrnl->jdata, sz, PG_08);
			ZDM_FREE(znd, jrnl, sizeof(*jrnl), KM_07);
			jrnl = NULL;
		} else {
			u16 jcount;

			(void)le64_to_lba48(jrnl->jdata[0].physical, &jcount);
			jrnl->jcount = jcount;

			list_add(&(jrnl->mclist), &hjload);
			jfound++;
		}

		if (jfound > MAX_CACHE_SYNC) {
			rc = -EIO;
			goto out;
		}
	} while (!done);

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
	}

	if (jfound != le16_to_cpu(znd->bmkeys->n_crcs)) {
		Z_ERR(znd, " ... mcache entries: found = %u, expected = %u",
		      jfound, le16_to_cpu(znd->bmkeys->n_crcs));
		rc = -EIO;
	}
	if ((crc_chk == znd->bmkeys->crc32) && !list_empty(&hjload)) {
		struct map_cache *jrnl;
		struct map_cache *jsafe;

		idx = 0;
		list_for_each_entry_safe(jrnl, jsafe, &hjload, mclist) {
			__le16 crc = crc_md_le16(jrnl->jdata, Z_CRC_4K);

			Z_DBG(znd, "JRNL CRC: %u: %04x [vs %04x] (c:%d)",
			      idx, le16_to_cpu(crc),
			      le16_to_cpu(znd->bmkeys->crcs[idx]),
			      jrnl->jcount);

			if (crc == znd->bmkeys->crcs[idx]) {
				int no = le64_to_lba48(jrnl->jdata[1].logical,
						NULL) % Z_HASH_SZ;

				mclist_add(znd, no, jrnl);
			} else {
				Z_ERR(znd, ".. %04x [vs %04x] (c:%d)",
				      le16_to_cpu(crc),
				      le16_to_cpu(znd->bmkeys->crcs[idx]),
				      jrnl->jcount);
				rc = -EIO;
			}
			idx++;
		}
	}
	crc_chk = crc_md_le16(znd->md_crcs, Z_CRC_4K * 2);
	if (crc_chk != znd->bmkeys->md_crc) {
		Z_ERR(znd, "CRC of CRC PGs: Ex %04x vs %04x  <- calculated",
		      le16_to_cpu(znd->bmkeys->md_crc),
		      le16_to_cpu(crc_chk));
	}

	znd->discard_count = 0;
	for (idx = 0; idx < znd->gz_count; idx++) {
		struct meta_pg *wpg = &znd->wp[idx];
		__le16 crc_wp = crc_md_le16(wpg->wp_alloc, Z_CRC_4K);
		__le16 crc_zf = crc_md_le16(wpg->zf_est, Z_CRC_4K);

		if (znd->bmkeys->wp_crc[idx] == crc_wp &&
		    znd->bmkeys->zf_crc[idx] == crc_zf) {
			znd->discard_count += count_stale_blocks(znd, idx, wpg);
		}
	}

	znd->z_gc_resv   = le32_to_cpu(znd->bmkeys->gc_resv);
	znd->z_meta_resv = le32_to_cpu(znd->bmkeys->meta_resv);

out:
	put_io_vcache(znd, io_vcache);
	mutex_unlock(&znd->vcio_lock);
	return rc;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_mapped_addmany(struct zoned *znd, u64 dm_s, u64 lba, u64 c, int fp)
{
	int rc = 0;
	sector_t blk;

	for (blk = 0; blk < c; blk++) {
		rc = z_mapped_add_one(znd, dm_s + blk, lba + blk, fp);
		if (rc)
			goto out;
	}
out:
	return rc;
}

/**
 * current_mapping() - Lookup a logical sector address to find the disk LBA
 * @param znd: ZDM instance
 * @param addr: Logical LBA of page.
 * @param gfp: Memory allocation rule
 *
 * Return: Disk LBA or 0 if not found.
 */
static u64 current_mapping(struct zoned *znd, u64 addr, int gfp)
{
	u64 found = 0ul;

	if (addr < znd->data_lba)
		found = addr;
	if (!found)
		found = z_lookup_key_range(znd, addr);
	if (!found)
		found = z_lookup_cache(znd, addr);
	if (!found)
		found = z_lookup_table(znd, addr, gfp);

	return found;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static struct map_pg *map_entry_for(struct zoned *znd, u64 lba, int gfp,
				    struct mpinfo *mpi)
{
	int entry;
	struct map_pg *found = NULL;
	struct mutex *lock;

	entry = to_table_entry(znd, lba, mpi);
	lock = mpi->bit_type == IS_LUT ? &znd->mapkey_lock : &znd->ct_lock;
	MutexLock(lock);
	if (entry > -1)
		found = mpi->table[entry];
	if (!found) {
		/* if we didn't find one .. create it */
		found = ZDM_ALLOC(znd, sizeof(*found), KM_20, gfp);
		if (found) {
			found->lba = lba;
			mutex_init(&found->md_lock);

			set_bit(mpi->bit_dir, &found->flags);
			set_bit(mpi->bit_type, &found->flags);

			mpi->table[entry] = found;
		} else {
			Z_ERR(znd, "NO MEM for mapped_t !!!");
		}
	}
	mutex_unlock(lock);
	return found;
}


/**
 * put_map_entry() - Decrement refcount of mapped page.
 * @mapped: mapped page
 */
static inline void put_map_entry(struct map_pg *mapped)
{
	if (mapped)
		atomic_dec(&mapped->refcount);
}

/**
 * get_map_entry() - Find page of LUT or CRC table map.
 * @znd: ZDM instance
 * @lba: Logical LBA of page.
 * @gfp: Memory allocation rule
 *
 * Return: struct map_pg * or NULL on error.
 *
 * Page will be loaded from disk it if is not already in core memory.
 */
static struct map_pg *get_map_entry(struct zoned *znd, u64 lba, int gfp)
{
	struct mpinfo mpi;
	struct map_pg *mapped;

	mapped = map_entry_for(znd, lba, gfp, &mpi);
	if (mapped) {
		atomic_inc(&mapped->refcount);
		if (!mapped->data.addr) {
			int rc;

			rc = cache_pg(znd, mapped, gfp, &mpi);
			if (rc < 0) {
				znd->meta_result = rc;
				mapped = NULL;
			}
		}
	} else {
		Z_ERR(znd, "%s: No table for %" PRIx64, __func__, lba);
	}
	return mapped;
}


/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int metadata_dirty_fling(struct zoned *znd, u64 dm_s)
{
	struct map_pg *Smap = NULL;
	int is_flung = 0;

	/*
	 *  not sure why this would ever happen during GC
	 * nothing in the GC reverse map should point to
	 * a block *before* a data pool block
	 */
	if (dm_s < znd->data_lba)
		return is_flung;

	Smap = NULL;
	if (dm_s >= znd->r_base && dm_s < znd->c_end) {
		Smap = get_map_entry(znd, dm_s, NORMAL);
		if (!Smap)
			Z_ERR(znd, "Failed to fling: %" PRIx64, dm_s);
	}
	if (Smap) {
		MutexLock(&Smap->md_lock);
		Smap->age = jiffies_64;
		set_bit(IS_DIRTY, &Smap->flags);
		is_flung = 1;
		mutex_unlock(&Smap->md_lock);

		if (Smap->lba != dm_s)
			Z_ERR(znd, "Excess churn? lba %"PRIx64
				   " [last: %"PRIx64"]", dm_s, Smap->lba);
		put_map_entry(Smap);
	}
	return is_flung;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline void z_do_copy_more(struct gc_state *gc_entry)
{
	unsigned long flags;
	struct zoned *znd = gc_entry->znd;

	spin_lock_irqsave(&znd->gc_lock, flags);
	set_bit(DO_GC_PREPARE, &gc_entry->gc_flags);
	spin_unlock_irqrestore(&znd->gc_lock, flags);
}

/**
 * gc_post() - Add a tLBA and current bLBA origin.
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
static int gc_post(struct zoned *znd, u64 dm_s, u64 lba)
{
	struct map_cache *post = &znd->gc_postmap;
	int handled = 0;

	if (post->jcount < post->jsize) {
		u16 idx = ++post->jcount;

		WARN_ON(post->jcount > post->jsize);

		post->jdata[idx].logical = lba48_to_le64(0, dm_s);
		post->jdata[idx].physical = lba48_to_le64(0, lba);
		handled = 1;
	} else {
		Z_ERR(znd, "*CRIT* post overflow L:%" PRIx64 "-> S:%" PRIx64,
		      lba, dm_s);
	}
	return handled;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_zone_gc_metadata_to_ram(struct gc_state *gc_entry)
{
	struct zoned *znd = gc_entry->znd;
	u64 from_lba = (gc_entry->z_gc << Z_BLKBITS) + znd->md_end;
	struct map_pg *ORmap;
	struct map_pg *Smap;
	struct map_addr ORaddr;
	struct map_addr Saddr;
	int count;
	int rcode = 0;

	/* pull all of the affected struct map_pg and crc pages into memory: */
	for (count = 0; count < Z_BLKSZ; count++) {
		__le32 ORencoded;
		u64 ORlba = from_lba + count;

		map_addr_calc(znd, ORlba, &ORaddr);
		ORmap = get_map_entry(znd, ORaddr.lut_r, NORMAL);
		if (ORmap && ORmap->data.addr) {
			atomic_inc(&ORmap->refcount);
			MutexLock(&ORmap->md_lock);
			ORencoded = ORmap->data.addr[ORaddr.pg_idx];
			mutex_unlock(&ORmap->md_lock);
			if (ORencoded != MZTEV_UNUSED) {
				u64 dm_s = map_value(znd, ORencoded);
				u64 lut_s;

				if (dm_s < znd->nr_blocks) {
					map_addr_calc(znd, dm_s, &Saddr);
					lut_s = Saddr.lut_s;
					Smap = get_map_entry(znd, lut_s,
							     NORMAL);
					if (!Smap)
						rcode = -ENOMEM;
					put_map_entry(Smap);
				} else {
					Z_ERR(znd, "Invalid rmap entry: %x.",
					      ORencoded);
				}
				if (!metadata_dirty_fling(znd, dm_s))
					gc_post(znd, dm_s, ORlba);
			}
			atomic_dec(&ORmap->refcount);
		}
		put_map_entry(ORmap);
	}

	return rcode;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int append_blks(struct zoned *znd, u64 lba,
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_zone_gc_read(struct gc_state *gc_entry)
{
	struct zoned *znd = gc_entry->znd;
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
			le64_to_lba48(post->jdata[jentry].physical, NULL))) {
		jentry++;
	}
	/* nothing left to move */
	if (jentry > post->jcount)
		goto out_finished;

	/* skip over any discarded blocks */
	if (jstart != jentry)
		jstart = jentry;

	start_lba = le64_to_lba48(post->jdata[jentry].physical, NULL);
	post->jdata[jentry].physical = lba48_to_le64(GC_READ, start_lba);
	nblks = 1;
	jentry++;

	while (jentry <= post->jcount && (nblks+fill) < GC_MAX_STRIPE) {
		u64 dm_s = le64_to_lba48(post->jdata[jentry].logical, NULL);
		u64 lba = le64_to_lba48(post->jdata[jentry].physical, NULL);

		if (Z_LOWER48 == dm_s || Z_LOWER48 == lba) {
			jentry++;
			continue;
		}

		post->jdata[jentry].physical = lba48_to_le64(GC_READ, lba);

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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_zone_gc_write(struct gc_state *gc_entry, u32 stream_id)
{
	struct zoned *znd = gc_entry->znd;
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
					post->jdata[jentry].physical, &rflg);
			u64 dm_s = le64_to_lba48(
					post->jdata[jentry].logical, NULL);

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
			post->jdata[jentry].physical = lba48_to_le64(rflg, lba);
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

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int gc_finalize(struct gc_state *gc_entry)
{
	int err = 0;
	struct zoned *znd = gc_entry->znd;
	struct map_cache *post = &znd->gc_postmap;
	int jentry;

	MutexLock(&post->cached_lock);
	for (jentry = post->jcount; jentry > 0; jentry--) {
		u64 dm_s = le64_to_lba48(post->jdata[jentry].logical, NULL);
		u64 lba = le64_to_lba48(post->jdata[jentry].physical, NULL);

		if (dm_s != Z_LOWER48 || lba != Z_LOWER48) {
			Z_ERR(znd, "GC: Failed to move %" PRIx64
			      " from %"PRIx64" [%d]", dm_s, lba, jentry);
			err = -EIO;
		}
	}
	mutex_unlock(&post->cached_lock);
	post->jcount = jentry;

	return err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void clear_gc_target_flag(struct zoned *znd)
{
	int z_id;

	for (z_id = 0; z_id < znd->data_zones; z_id++) {
		u32 gzno  = z_id >> GZ_BITS;
		u32 gzoff = z_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp;

		MutexLock(&wpg->wplck);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		if (wp & Z_WP_GC_TARGET) {
			wp &= ~Z_WP_GC_TARGET;
			wpg->wp_alloc[gzoff] = cpu_to_le32(wp);
		}
		set_bit(IS_DIRTY, &wpg->flags);
		mutex_unlock(&wpg->wplck);
	}
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int z_zone_gc_metadata_update(struct gc_state *gc_entry)
{
	struct zoned *znd = gc_entry->znd;
	struct map_cache *post = &znd->gc_postmap;
	u32 used = post->jcount;
	int err = 0;
	int jentry;

	for (jentry = post->jcount; jentry > 0; jentry--) {
		int discard = 0;
		int mapping = 0;
		struct map_pg *mapped = NULL;
		u64 dm_s = le64_to_lba48(post->jdata[jentry].logical, NULL);
		u64 lba = le64_to_lba48(post->jdata[jentry].physical, NULL);

		if ((znd->r_base <= dm_s) && dm_s < (znd->r_base + Z_BLKSZ)) {
			u64 off = dm_s - znd->r_base;

			mapped = znd->rev_tm[off];
			mapping = 1;
		} else if ((znd->s_base <= dm_s) &&
			   (dm_s < (znd->s_base + Z_BLKSZ))) {
			u64 off = dm_s - znd->s_base;

			mapped = znd->fwd_tm[off];
			mapping = 1;
		}

		if (mapping && !mapped)
			Z_ERR(znd, "MD: dm_s: %" PRIx64 " -> lba: %" PRIx64
				   " no mapping in ram.", dm_s, lba);

		if (mapped) {
			u32 in_z;

			atomic_inc(&mapped->refcount);
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
			atomic_dec(&mapped->refcount);
		}

		MutexLock(&post->cached_lock);
		if (discard == 1) {
			Z_ERR(znd, "Dropped: %" PRIx64 " ->  %"PRIx64,
			      le64_to_cpu(post->jdata[jentry].logical),
			      le64_to_cpu(post->jdata[jentry].physical));

			post->jdata[jentry].logical = MC_INVALID;
			post->jdata[jentry].physical = MC_INVALID;
		}
		if (post->jdata[jentry].logical  == MC_INVALID &&
		    post->jdata[jentry].physical == MC_INVALID) {
			used--;
		} else if (lba) {
			_inc_zone_used(znd, lba);
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
 *         available blocks remaining.
 */
static sector_t _blkalloc(struct zoned *znd, u32 z_at, u32 flags,
			  u32 nblks, u32 *nfound)
{
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

	MutexLock(&wpg->wplck);
	wp      = le32_to_cpu(wpg->wp_alloc[gzoff]);
	gc_tflg = wp & (Z_WP_GC_TARGET|Z_WP_NON_SEQ);
	wptr    = wp & ~(Z_WP_GC_TARGET|Z_WP_NON_SEQ);
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
	}
	mutex_unlock(&wpg->wplck);

	if (do_open_zone)
		dmz_open_zone(znd, z_at);

	return found;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u16 _gc_tag = 1;

static void dbg_queued(struct zoned *znd, u32 z_gc, u16 tag)
{
	u32 gzno  = z_gc >> GZ_BITS;
	u32 gzoff = z_gc & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];
	u32 zf_est = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
	u32 wptr = le32_to_cpu(wpg->wp_alloc[gzoff]);

	Z_ERR(znd, "Queue GC: Z# %x, wp: %x, free %x - tag %u",
	      z_gc, wptr, zf_est, tag);
}

/**
 * z_zone_compact_queue() - Queue zone compaction.
 * @znd: ZDM instance
 * @z_gc: Zone to queue.
 * @delay: Delay queue metric
 * @gfp: Allocation scheme.
 */
static int z_zone_compact_queue(struct zoned *znd, u32 z_gc, int delay, int gfp)
{
	unsigned long flags;
	int do_queue = 0;
	int err = 0;
	struct gc_state *gc_entry =
		ZDM_ALLOC(znd, sizeof(*gc_entry), KM_16, gfp);

	if (!gc_entry) {
		Z_ERR(znd, "No Memory for compact!!");
		return -ENOMEM;
	}
	gc_entry->znd = znd;
	gc_entry->z_gc = z_gc;
	gc_entry->tag = _gc_tag++;
	set_bit(DO_GC_NEW, &gc_entry->gc_flags);
	znd->gc_backlog++;

	spin_lock_irqsave(&znd->gc_lock, flags);
	if (!znd->gc_active) {
		znd->gc_active = gc_entry;
		do_queue = 1;
	}
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	if (do_queue) {
		unsigned long tval = msecs_to_jiffies(delay);

		queue_delayed_work(znd->gc_wq, &znd->gc_work, tval);
		dbg_queued(znd, z_gc, gc_entry->tag);
	} else {
		Z_ERR(znd, "Queue GC: Z# %x ** fail: active", z_gc);
		ZDM_FREE(znd, gc_entry, sizeof(*gc_entry), KM_16);
		znd->gc_backlog--;
	}

	return err;
}

/**
 * zone_zfest() - Queue zone compaction.
 * @znd: ZDM instance
 * @z_id: Zone to queue.
 */
static u32 zone_zfest(struct zoned *znd, u32 z_id)
{
	u32 gzno  = z_id >> GZ_BITS;
	u32 gzoff = z_id & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];

	return le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
}


/**
 * gc_compact_check() - Called periodically to initiate GC
 *
 * @znd: ZDM instance
 * @bin: Bin with stale zones to scan for GC
 * @delay: Metric for delay queuing.
 * @gfp: Default memory allocation scheme.
 *
 */
static int gc_compact_check(struct zoned *znd, int bin, int delay, int gfp)
{
#define GC_NFOUND	(~0u)
	unsigned long flags;
	int queued = 0;
	u16 gc_tag = 0;
	u32 top_roi = GC_NFOUND;
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
	if (znd->gc_active) {
		queued = 1;
		gc_tag = znd->gc_active->tag;
	}
	spin_unlock_irqrestore(&znd->gc_lock, flags);

	if (queued) {
		Z_ERR(znd, "GC is active: %u", gc_tag);
		goto out;
	}

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
				if (top_roi == GC_NFOUND)
					top_roi = z_gc;
				else if (nfree > zone_zfest(znd, top_roi))
					top_roi = z_gc;
			}
		}
	}

	/* determine the cut-off for GC based on MZ overall staleness */
	if (top_roi != GC_NFOUND) {
		u32 state_metric = GC_PRIO_DEFAULT;
		u32 n_filled = znd->data_zones - znd->z_gc_free;
		u32 n_empty = znd->data_zones - n_filled;
		int pctfree = n_empty * 100 / znd->data_zones;

		/*
		 * -> at less than 5 zones free switch to critical
		 * -> at less than 5% zones free switch to HIGH
		 * -> at less than 25% free switch to LOW
		 * -> high level is 'cherry picking' near empty zones
		 */
		if (znd->z_gc_free < 5)
			state_metric = GC_PRIO_CRIT;
		else if (pctfree < 5)
			state_metric = GC_PRIO_HIGH;
		else if (pctfree < 25)
			state_metric = GC_PRIO_LOW;

		if (zone_zfest(znd, top_roi) > state_metric)
			if (z_zone_compact_queue(znd, top_roi, delay * 5, gfp))
				queued = 1;
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
	int do_meta_flush = 0;
	struct zoned *znd = gc_entry->znd;
	u32 z_gc = gc_entry->z_gc;
	u32 gzno  = z_gc >> GZ_BITS;
	u32 gzoff = z_gc & GZ_MMSK;
	struct meta_pg *wpg = &znd->wp[gzno];

	znd->age = jiffies_64;
	if (test_and_clear_bit(DO_GC_NEW, &gc_entry->gc_flags)) {
		u32 wp;

		err = z_flush_bdev(znd);
		if (err) {
			gc_entry->result = err;
			goto out;
		}
		MutexLock(&wpg->wplck);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]);
		wp |= Z_WP_GC_FULL;
		wpg->wp_alloc[gzoff] = cpu_to_le32(wp);
		set_bit(IS_DIRTY, &wpg->flags);
		mutex_unlock(&wpg->wplck);

		MutexLock(&znd->mz_io_mutex);
		err = _cached_to_tables(znd, gc_entry->z_gc);
		set_bit(DO_GC_NO_PURGE, &znd->flags);
		mutex_unlock(&znd->mz_io_mutex);
		if (err) {
			Z_ERR(znd, "Failed to purge journal: %d", err);
			gc_entry->result = err;
			goto out;
		}

		if (znd->gc_postmap.jcount > 0) {
			Z_ERR(znd, "*** Unexpected data in postmap!!");
			znd->gc_postmap.jcount = 0;
		}

		MutexLock(&znd->mz_io_mutex);
		err = z_zone_gc_metadata_to_ram(gc_entry);
		mutex_unlock(&znd->mz_io_mutex);
		if (err) {
			Z_ERR(znd,
			      "Pre-load metadata to memory failed!! %d", err);
			gc_entry->result = err;
			goto out;
		}
		set_bit(DO_GC_PREPARE, &gc_entry->gc_flags);

		if (znd->gc_throttle.counter == 0)
			return -EAGAIN;
	}

next_in_queue:
	znd->age = jiffies_64;
	if (test_and_clear_bit(DO_GC_PREPARE, &gc_entry->gc_flags)) {
		MutexLock(&znd->mz_io_mutex);
		err = z_zone_gc_read(gc_entry);
		mutex_unlock(&znd->mz_io_mutex);
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
		err = z_zone_gc_write(gc_entry, sid >> 24);
		mutex_unlock(&znd->mz_io_mutex);
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

		MutexLock(&znd->mz_io_mutex);
		err = z_zone_gc_metadata_update(gc_entry);
		gc_entry->result = err;
		mutex_unlock(&znd->mz_io_mutex);
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

		MutexLock(&znd->mz_io_mutex);
		err = _cached_to_tables(znd, gc_entry->z_gc);
		mutex_unlock(&znd->mz_io_mutex);
		if (err) {
			Z_ERR(znd, "Failed to purge journal: %d", err);
			gc_entry->result = err;
			goto out;
		}

		err = z_flush_bdev(znd);
		if (err) {
			gc_entry->result = err;
			goto out;
		}

		/* Release the zones for writing */
		dmz_reset_wp(znd, gc_entry->z_gc);

		MutexLock(&wpg->wplck);
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
		mutex_unlock(&wpg->wplck);

		Z_ERR(znd, "GC %d: z#0x%x, wp:%08x, free:%lx finished.",
		      gc_entry->tag, gc_entry->z_gc, non_seq, Z_BLKSZ);

		spin_lock_irqsave(&znd->gc_lock, flags);
		znd->gc_backlog--;
		znd->gc_active = NULL;
		spin_unlock_irqrestore(&znd->gc_lock, flags);

		ZDM_FREE(znd, gc_entry, sizeof(*gc_entry), KM_16);

		set_bit(DO_JOURNAL_MOVE, &znd->flags);
		set_bit(DO_MEMPOOL, &znd->flags);
		set_bit(DO_SYNC, &znd->flags);
		do_meta_flush = 1;
	}
out:
	clear_bit(DO_GC_NO_PURGE, &znd->flags);
	if (do_meta_flush) {
		if (!work_pending(&znd->meta_work))
			queue_work(znd->meta_wq, &znd->meta_work);
	}
	return 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void gc_work_task(struct work_struct *work)
{
	struct gc_state *gc_entry = NULL;
	unsigned long flags;
	struct zoned *znd;
	int err;

	if (!work)
		return;

	znd = container_of(to_delayed_work(work), struct zoned, gc_work);
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
		on_timeout_activity(znd, 0, 10);
	}
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static inline int is_reserved(struct zoned *znd, const u32 z_pref)
{
	const u32 gc   = znd->z_gc_resv   & Z_WP_VALUE_MASK;
	const u32 meta = znd->z_meta_resv & Z_WP_VALUE_MASK;

	return (gc == z_pref || meta == z_pref) ? 1 : 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int gc_can_cherrypick(struct zoned *znd, u32 bin, int delay, int gfp)
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

		if (((wp & Z_WP_GC_BITS) == Z_WP_GC_READY) &&
		    ((wp & Z_WP_VALUE_MASK) == Z_BLKSZ) &&
		    (nfree == Z_BLKSZ)) {
			if (z_zone_compact_queue(znd, z_id, delay, gfp))
				return 1;
		}
	}

	return 0;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int gc_queue_with_delay(struct zoned *znd, int delay, int gfp)
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
		for (iter = 0; iter < znd->stale.count; iter++)
			if (gc_idle && (bin != iter) &&
			    gc_can_cherrypick(znd, iter, delay, gfp))
				gc_idle = 0;

		/* Otherwise compact a zone in the stream */
		if (gc_idle && gc_compact_check(znd, bin, delay, gfp))
			gc_idle = 0;

#if 0
		/* Otherwise compact *something* */
		for (iter = 0; iter < znd->stale.count; iter++)
			if (gc_idle && gc_compact_check(znd, iter, delay, gfp))
				gc_idle = 0;
#endif

	}
	return !gc_idle;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int gc_immediate(struct zoned *znd, int gfp)
{
	const int delay = 0;
	int can_retry = 0;
	int queued;
	int is_locked;

	atomic_inc(&znd->gc_throttle);

	if (znd->gc_throttle.counter > 1)
		goto out;

	is_locked = mutex_is_locked(&znd->mz_io_mutex);
	if (is_locked)
		mutex_unlock(&znd->mz_io_mutex);
	flush_delayed_work(&znd->gc_work);
	if (is_locked)
		MutexLock(&znd->mz_io_mutex);

	queued = gc_queue_with_delay(znd, delay, gfp);
	if (!queued) {
		Z_ERR(znd, " ... GC immediate .. failed to queue GC!!.");
		dump_stack();
		goto out;
	}

	if (is_locked)
		mutex_unlock(&znd->mz_io_mutex);
	can_retry = flush_delayed_work(&znd->gc_work);
	if (is_locked)
		MutexLock(&znd->mz_io_mutex);

out:
	atomic_dec(&znd->gc_throttle);

	return can_retry;
}

/**
 * update_stale_ratio() - Update the stale ratio for the finished bin.
 * @znd: ZDM instance
 * @zone: Zone that needs update.
 */
static void update_stale_ratio(struct zoned *znd, u32 zone)
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

		if (wp == Z_BLKSZ)
			total_stale += stale;
		else
			free_zones++;
	}

	total_stale /= free_zones;
	znd->stale.bins[bin] = (total_stale > ~0u) ? ~0u : total_stale;
}

static inline void set_current(struct zoned *znd, u32 flags, u32 zone)
{
	if (flags & Z_AQ_STREAM_ID) {
		u32 stream_id = flags & Z_AQ_STREAM_MASK;

		znd->bmkeys->stream[stream_id] = cpu_to_le32(zone);
	}
	znd->z_current = zone;
	znd->z_gc_free--;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static u32 next_open_zone(struct zoned *znd, u32 z_at)
{
	u32 zone = ~0u;
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


static u64 z_acquire(struct zoned *znd, u32 flags, u32 nblks, u32 *nfound)
{
	sector_t found = 0;
	u32 z_pref = znd->z_current;
	u32 stream_id = 0;
	u32 z_find;
	int gfp = flags & Z_AQ_NORMAL ? CRIT : NORMAL;

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

		/* no space left in zone .. explicitly close it */
		dmz_close_zone(znd, z_pref);
		update_stale_ratio(znd, z_pref);
	}

	if (znd->z_gc_free < 5) {
		Z_ERR(znd, "... alloc - gc low on free space.");
		gc_immediate(znd, gfp);
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
		int can_retry = gc_immediate(znd, gfp);
		u32 mresv = znd->z_meta_resv & Z_WP_VALUE_MASK;

		if (can_retry)
			goto retry;

		Z_ERR(znd, "Using META Reserve (%u)", znd->z_meta_resv);
		found = _blkalloc(znd, mresv, flags, nblks, nfound);
	}

out:
	if (!found && (*nfound == 0)) {
		if (gc_immediate(znd, gfp))
			goto retry;

		Z_ERR(znd, "%s: -> Out of space.", __func__);
	}
	return found;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void do_free_mapped(struct zoned *znd, struct map_pg *expg)
{
	struct mpinfo mpi;
	int entry;
	struct map_pg *found = NULL;
	struct mutex *lock;

	entry = to_table_entry(znd, expg->lba, &mpi);
	lock = mpi.bit_type == IS_LUT ? &znd->mapkey_lock : &znd->ct_lock;
	MutexLock(lock);
	if (entry > -1)
		found = mpi.table[entry];
	if (found) {
		int drop = 0;

		if (!mutex_trylock(&expg->md_lock))
			goto out;

		if (expg->refcount.counter == 0) {
			list_del(&expg->inpool);
			drop = 1;
			if (expg->data.addr) {
				ZDM_FREE(znd, expg->data.addr, Z_C4K, PG_27);
				znd->incore_count--;
			}
		}
		mutex_unlock(&expg->md_lock);

		if (drop) {
			ZDM_FREE(znd, found, sizeof(*found), KM_20);
			mpi.table[entry] = NULL;
		}
	} else {
		Z_ERR(znd, " *** purge: %"PRIx64 " @ %d ????",
		      expg->lba, entry);
	}

out:
	mutex_unlock(lock);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int compare_lba(const void *x1, const void *x2)
{
	const struct map_pg *v1 = *(const struct map_pg **)x1;
	const struct map_pg *v2 = *(const struct map_pg **)x2;
	int cmp = (v1->lba < v2->lba) ? -1 : ((v1->lba > v2->lba) ? 1 : 0);

	return cmp;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int is_old_and_dirty(struct map_pg *expg, int bit_type)
{
	return (is_expired(expg->age) &&
		test_bit(bit_type, &expg->flags) &&
		test_bit(IS_DIRTY, &expg->flags));
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int is_dirty(struct map_pg *expg, int bit_type)
{
	return (test_bit(bit_type, &expg->flags) &&
		test_bit(IS_DIRTY, &expg->flags));
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int is_old_del(struct map_pg *expg, int bit_type)
{
	int inc = 0;

	if (!test_bit(IS_DIRTY, &expg->flags) && is_expired(expg->age))
		if (expg->refcount.counter == 1)
			inc = 1;

	(void)bit_type;

	return inc;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int _pool_fill(struct zoned *znd,
		      struct map_pg **stale,
		      int count,
		      int (*match)(struct map_pg *, int),
		      int bit_type)
{
	int stale_count = 0;
	struct map_pg *expg;

	MutexLock(&znd->map_pool_lock);
	stale_count = 0;
	expg = _pool_last(znd);
	while (expg && stale_count < count) {
		struct map_pg *aux = _pool_prev(znd, expg);

		if (match(expg, bit_type))
			stale[stale_count++] = expg;

		atomic_dec(&expg->refcount);
		expg = aux;
	}
	if (expg)
		atomic_dec(&expg->refcount);
	mutex_unlock(&znd->map_pool_lock);

	return stale_count;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int _pool_write(struct zoned *znd, struct map_pg **stale, int count)
{
	const int use_wq = 0;
	int err;

	/* write dirty table pages */
	if (count > 0) {
		int iter;
		int last = count - 1;

		sort(stale, count, sizeof(*stale), compare_lba, NULL);

		for (iter = 0; iter < count; iter++)
			cache_if_dirty(znd, stale[iter], use_wq);

		for (iter = 0; iter < count; iter++) {
			int sync = (iter == last) ? 1 : 0;

			err = write_if_dirty(znd, stale[iter], use_wq, sync);
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
 * expire_old_pool() - Write old pages to disk and purge from RAM
 * @znd: ZDM instance.
 * @count: Goal number of LUT pages to write/purge [capped at 1024].
 *
 * Return: >= 0 on success or -errno value
 */
static int expire_old_pool(struct zoned *znd, int count)
{
	int iter;
	int err = 0;
	int stale_count = 0;
	struct map_pg **stale = NULL;

	if (count > 1024)
		count = 1024;

	if (!count)
		goto out;

	/* fill stale with OLD dirty lookup-table pages [Ignore CRC pgs] */
	stale = ZDM_CALLOC(znd, sizeof(*stale), count, KM_19, NORMAL);
	stale_count = _pool_fill(znd, stale, count, is_old_and_dirty, IS_LUT);
	err = _pool_write(znd, stale, stale_count);
	if (err < 0)
		goto out;

	sync_crc_pages(znd);

	/* Stage 2: Drop clean table pages from ram */
	if (test_bit(DO_GC_NO_PURGE, &znd->flags))
		goto out;

	stale_count = _pool_fill(znd, stale, count, is_old_del, 0);
	for (iter = 0; iter < stale_count; iter++) {
		if (stale[iter])
			do_free_mapped(znd, stale[iter]);
	}

out:
	if (stale)
		ZDM_FREE(znd, stale, sizeof(*stale) * count, KM_19);

	return err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/**
 * z_metadata_lba() - Alloc a block of metadata.
 * @znd: ZDM Target
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
static u64 z_metadata_lba(struct zoned *znd, struct map_pg *map, u32 *num)
{
	u32 nblks = 1;
	u64 lba = map->lba;

	if (lba < znd->data_lba) {
		*num = 1;
		return map->lba;
	}
	return z_acquire(znd, Z_AQ_META, nblks, num);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/**
 * callback from dm_io notify.. cannot hold mutex here
 */
static void pg_crc(struct zoned *znd, struct map_pg *pg, __le16 md_crc)
{
	struct mpinfo mpi;

	to_table_entry(znd, pg->lba, &mpi);
	if (pg->crc_pg) {
		int entry = mpi.crc.pg_idx;
		struct map_pg *crc_pg = pg->crc_pg;

		if (crc_pg && crc_pg->data.crc) {
			atomic_inc(&crc_pg->refcount);
			crc_pg->data.crc[entry] = md_crc;
			set_bit(IS_DIRTY, &crc_pg->flags);
			crc_pg->age = jiffies_64;
			atomic_dec(&crc_pg->refcount);
		}
		put_map_entry(crc_pg);
		if (crc_pg->refcount.counter == 0)
			pg->crc_pg = NULL;
	} else if (!test_bit(IS_LUT, &pg->flags)) {
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
	struct zoned *znd = pg->znd;
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
	pg->age = jiffies_64;
	if (md_crc == pg->md_crc)
		clear_bit(IS_DIRTY, &pg->flags);

	pg_crc(znd, pg, md_crc);

	if (pg->last_write < znd->data_lba)
		goto out;

/*
 * NOTE: If we reach here it's a problem.
 *       TODO: mutex free path for adding a map entry ...
 */

	/* written lba was allocated from data-pool */
	rcode = z_mapped_addmany(znd, pg->lba, pg->last_write, 1, CRIT);
	if (rcode) {
		Z_ERR(znd, "%s: Journal MANY failed.", __func__);
		goto out;
	}

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
	atomic_dec(&pg->refcount);
	if (rcode < 0)
		pg->znd->meta_result = rcode;
}

/**
 * queue_pg() - Queue a map table page for writeback
 * @znd: ZDM Instance
 * @pg: The target page to ensure the cover CRC blocks is cached.
 * @lba: The address to write the block to.
 */
static int queue_pg(struct zoned *znd, struct map_pg *pg, u64 lba)
{
	const int use_wq = 0;
	sector_t block = lba << Z_SHFT4K;
	unsigned int nDMsect = 1 << Z_SHFT4K;
	int rc;

	atomic_inc(&pg->refcount);
	pg->znd = znd;

	MutexLock(&pg->md_lock);
	pg->md_crc = crc_md_le16(pg->data.addr, Z_CRC_4K);
	pg->last_write = lba; /* presumably */
	mutex_unlock(&pg->md_lock);

	rc = znd_async_io(znd, DM_IO_KMEM, pg->data.addr, block, nDMsect, WRITE,
			  use_wq, on_pg_written, pg);
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
static void cache_if_dirty(struct zoned *znd, struct map_pg *pg, int wq)
{
	atomic_inc(&pg->refcount);
	if (test_bit(IS_DIRTY, &pg->flags) && test_bit(IS_LUT, &pg->flags) &&
	    pg->data.addr) {
		u64 base = znd->c_base;
		struct map_pg *crc_pg;
		struct mpinfo mpi;

		if (test_bit(IS_REV, &pg->flags))
			base = znd->c_mid;

		to_table_entry(znd, pg->lba, &mpi);
		base += mpi.crc.pg_no;
		crc_pg = get_map_entry(znd, base, NORMAL);
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
static int write_if_dirty(struct zoned *znd, struct map_pg *pg, int wq, int snc)
{
	int rcode = 0;

	if (!pg)
		return rcode;

	if (test_bit(IS_DIRTY, &pg->flags) && pg->data.addr) {
		u64 dm_s = pg->lba;
		u32 nf;
		u64 lba = z_metadata_lba(znd, pg, &nf);

		Z_DBG(znd, "Write if dirty: %" PRIx64" -> %" PRIx64, dm_s, lba);

		if (lba && nf) {
			int rcwrt;
			int count = 1;
			__le16 md_crc;

			if (!snc) {
				rcode = queue_pg(znd, pg, lba);
				goto out;
			}

			md_crc = crc_md_le16(pg->data.addr, Z_CRC_4K);
			rcwrt = write_block(znd->ti, DM_IO_KMEM,
					    pg->data.addr, lba, count, wq);
			pg->age = jiffies_64;
			pg->last_write = lba;
			pg_crc(znd, pg, md_crc);

			if (rcwrt) {
				Z_ERR(znd, "write_page: %" PRIx64 " -> %" PRIx64
				      " ERR: %d", pg->lba, lba, rcwrt);
				rcode = rcwrt;
				goto out;
			}

			if (crc_md_le16(pg->data.addr, Z_CRC_4K) == md_crc)
				clear_bit(IS_DIRTY, &pg->flags);

			if (lba < znd->data_lba)
				goto out;

			rcwrt = z_mapped_addmany(znd, dm_s, lba, nf, NORMAL);
			if (rcwrt) {
				Z_ERR(znd, "%s: Journal MANY failed.",
				      __func__);
				rcode = rcwrt;
				goto out;
			}
		} else {
			Z_ERR(znd, "%s: Out of space for metadata?", __func__);
			rcode = -ENOSPC;
			goto out;
		}
	}
out:
	atomic_dec(&pg->refcount);

	if (rcode < 0)
		znd->meta_result = rcode;

	return rcode;
}

/**
 * keep_active_pages() - Write old pages to disk and purge from RAM
 * @znd: ZDM instance.
 * @allowed_pages: Goal number of pages to retain.
 *
 * Return: 0 on success or -errno value
 */
static int keep_active_pages(struct zoned *znd, int allowed_pages)
{
	int err = 0;

	if (znd->incore_count > allowed_pages) {
		int count = znd->incore_count - allowed_pages;
		int rc = expire_old_pool(znd, count);

		if (rc < 0) {
			err = rc;
			Z_ERR(znd, "%s: Failed to remove oldest pages!",
				__func__);
		}
	}

	return err;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static int sync_dirty(struct zoned *znd, int bit_type, int count)
{
	int err = 0;
	int stale_count;
	struct map_pg **stale = NULL;

	if (count > 1024)
		count = 1024;

	if (!count)
		goto out;

	stale = ZDM_CALLOC(znd, sizeof(*stale), count, KM_19, NORMAL);
	stale_count = _pool_fill(znd, stale, count, is_dirty, bit_type);
	err = _pool_write(znd, stale, stale_count);
	if (err < 0)
		goto out;
out:
	if (stale)
		ZDM_FREE(znd, stale, sizeof(*stale) * count, KM_19);

	return err;
}

/**
 * sync_crc_pages() - Migrate crc pages to disk
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int sync_crc_pages(struct zoned *znd)
{
	const int max = 1024;
	int stale;

	do {
		stale = sync_dirty(znd, IS_CRC, max);
	} while (stale > 0);

	return stale;
}

/**
 * sync_mapped_pages() - Migrate lookup tables and crc pages to disk
 * @znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int sync_mapped_pages(struct zoned *znd)
{
	const int max = 1024;
	int stale;

	do {
		stale = sync_dirty(znd, IS_LUT, max);
	} while (stale > 0);

	/* on error return */
	if (stale < 0)
		return stale;

	stale = sync_crc_pages(znd);

	return stale;
}

/**
 * do_sync_metadata() - Migrate memcache entries through lookup tables to disk
 * @param znd: ZDM instance
 *
 * Return: 0 on success or -errno value
 */
static int do_sync_metadata(struct zoned *znd)
{
	int err = _cached_to_tables(znd, znd->data_zones);

	if (err)
		goto out;

	err = sync_mapped_pages(znd);
out:
	return err;
}

/**
 * dm_s is a logical sector that maps 1:1 to the whole disk in 4k blocks
 * Here the logical LBA and field are calculated for the lookup table
 * where the physical LBA can be read from disk.
 */
static int map_addr_aligned(struct zoned *znd, u64 dm_s, struct map_addr *out)
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
 * dm_s is a logical sector that maps 1:1 to the whole disk in 4k blocks
 * Here the logical LBA and field are calculated for the lookup table
 * where the physical LBA can be read from disk.
 */
static int map_addr_calc(struct zoned *znd, u64 origin, struct map_addr *out)
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
static int read_pg(struct zoned *znd, struct map_pg *pg, u64 lba48, int gfp,
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
		u64 base = znd->c_base;

		if (test_bit(IS_REV, &pg->flags))
			base = znd->c_mid;

		crc_pg = get_map_entry(znd, mpi->crc.pg_no + base, gfp);
		if (crc_pg) {
			atomic_inc(&crc_pg->refcount);
			if (crc_pg->data.crc) {
				MutexLock(&crc_pg->md_lock);
				expect = crc_pg->data.crc[mpi->crc.pg_idx];
				incore_hint(znd, crc_pg);
				mutex_unlock(&crc_pg->md_lock);
				crc_pg->age = jiffies_64;
			}
			atomic_dec(&crc_pg->refcount);
		}
		put_map_entry(crc_pg);
	} else {
		expect = znd->md_crcs[mpi->crc.pg_idx];
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
static int cache_pg(struct zoned *znd, struct map_pg *pg, int gfp,
		    struct mpinfo *mpi)
{
	int rc = 0;
	int empty_val = 0;
	u64 lba48 = current_mapping(znd, pg->lba, gfp);

	if (test_bit(IS_LUT, &pg->flags))
		empty_val = 0xff;

	atomic_inc(&pg->refcount);

	MutexLock(&pg->md_lock);
	pg->data.addr = ZDM_ALLOC(znd, Z_C4K, PG_27, gfp);
	if (pg->data.addr) {
		if (lba48)
			rc = read_pg(znd, pg, lba48, gfp, mpi);
		else
			memset(pg->data.addr, empty_val, Z_C4K);
	}
	mutex_unlock(&pg->md_lock);

	if (!pg->data.addr) {
		Z_ERR(znd, "%s: Out of memory.", __func__);
		goto out;
	}

	if (rc < 0) {
		Z_ERR(znd, "%s: read_pg from %" PRIx64" [to? %d] error: %d",
		      __func__, pg->lba, test_bit(IS_FWD, &pg->flags), rc);
		ZDM_FREE(znd, pg->data.addr, Z_C4K, PG_27);
		goto out;
	}
	znd->incore_count++;
	pg->age = jiffies_64;
	pool_add(znd, pg);

	Z_DBG(znd, "Page loaded: lba: %" PRIx64, pg->lba);
out:
	atomic_dec(&pg->refcount);
	return rc;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

/* resolve a sector mapping to the on-block-device lba via the lookup table */
static u64 z_lookup_table(struct zoned *znd, u64 addr, int gfp)
{
	struct map_addr maddr;
	struct map_pg *mapped;
	u64 old_phy = 0;

	map_addr_calc(znd, addr, &maddr);
	mapped = get_map_entry(znd, maddr.lut_s, gfp);
	if (mapped) {
		atomic_inc(&mapped->refcount);
		if (mapped->data.addr) {
			__le32 delta;

			MutexLock(&mapped->md_lock);
			delta = mapped->data.addr[maddr.pg_idx];
			incore_hint(znd, mapped);
			mutex_unlock(&mapped->md_lock);
			old_phy = map_value(znd, delta);
			mapped->age = jiffies_64;
		}
		atomic_dec(&mapped->refcount);
		put_map_entry(mapped);
	}
	return old_phy;
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
 *         in this case the old lba is discarded and scheduled for cleanup
 *         by updating the reverse map lba tables noting that this location
 *         is now unused.
 * when is_fwd is 0:
 *  - maddr->dm_s is an lba, lba -> dm_s
 *
 * Return: non-zero on error.
 */
static int update_map_entry(struct zoned *znd, struct map_pg *mapped,
			    struct map_addr *maddr, u64 to_addr, int is_fwd)
{
	int err = -ENOMEM;

	if (mapped && mapped->data.addr) {
		u64 index = maddr->pg_idx;
		__le32 delta;
		__le32 value;
		int was_updated = 0;

		atomic_inc(&mapped->refcount);
		MutexLock(&mapped->md_lock);
		delta = mapped->data.addr[index];
		err = map_encode(znd, to_addr, &value);
		if (!err) {
			/*
			 * if the value is modified update the table and
			 * place it on top of the active [inpool] list
			 * this will keep the chunk of lookup table in
			 * memory.
			 */
			if (mapped->data.addr[index] != value) {
				mapped->data.addr[index] = value;
				mapped->age = jiffies_64;
				set_bit(IS_DIRTY, &mapped->flags);
				was_updated = 1;
				incore_hint(znd, mapped);
			} else {
				Z_ERR(znd, "*ERR* %" PRIx64
					   " -> data.addr[index] (%x) == (%x)",
				      maddr->dm_s, mapped->data.addr[index],
				      value);
				dump_stack();
			}
		} else {
			Z_ERR(znd, "*ERR* Mapping: %" PRIx64 " to %" PRIx64,
			      to_addr, maddr->dm_s);
		}
		mutex_unlock(&mapped->md_lock);

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
		atomic_dec(&mapped->refcount);
	}
	return err;
}

/**
 * move_to_map_tables() - Migrate memcache to lookup table map entries.
 * @znd: ZDM instance
 * @jrnl: memcache block.
 *
 * Return: non-zero on error.
 */
static int move_to_map_tables(struct zoned *znd, struct map_cache *jrnl)
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
	atomic_inc(&jrnl->busy_locked);

	for (jentry = jrnl->jcount; jentry > 0;) {
		u64 dm_s = le64_to_lba48(jrnl->jdata[jentry].logical, NULL);
		u64 lba = le64_to_lba48(jrnl->jdata[jentry].physical, NULL);

		if (dm_s == Z_LOWER48 || lba == Z_LOWER48) {
			jrnl->jcount = --jentry;
			continue;
		}

		map_addr_calc(znd, dm_s, &maddr);
		if (lut_s != maddr.lut_s) {
			put_map_entry(smtbl);
			if (smtbl)
				atomic_dec(&smtbl->refcount);
			smtbl = get_map_entry(znd, maddr.lut_s, NORMAL);
			if (!smtbl) {
				err = -ENOMEM;
				goto out;
			}
			atomic_inc(&smtbl->refcount);
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
					atomic_dec(&rmtbl->refcount);
				rmtbl = get_map_entry(znd, rev.lut_r, NORMAL);
				if (!rmtbl) {
					err = -ENOMEM;
					goto out;
				}
				atomic_inc(&rmtbl->refcount);
				lut_r = rmtbl->lba;
			}
			is_fwd = 0;
			err = update_map_entry(znd, rmtbl, &rev, dm_s, is_fwd);
			if (err == 1)
				err = 0;
		}

		if (err < 0)
			goto out;

		jrnl->jdata[jentry].logical = MC_INVALID;
		jrnl->jdata[jentry].physical = MC_INVALID;
		jrnl->jcount = --jentry;
	}
out:
	if (smtbl)
		atomic_dec(&smtbl->refcount);
	if (rmtbl)
		atomic_dec(&rmtbl->refcount);
	put_map_entry(smtbl);
	put_map_entry(rmtbl);
	set_bit(DO_MEMPOOL, &znd->flags);

	atomic_dec(&jrnl->busy_locked);

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
static int unused_phy(struct zoned *znd, u64 lba, u64 orig_s, int gfp)
{
	int err = 0;
	struct map_pg *mapped;
	struct map_addr reverse;
	int z_off;

	if (lba < znd->data_lba)
		return 0;

	map_addr_calc(znd, lba, &reverse);
	z_off = reverse.zone_id % 1024;
	mapped = get_map_entry(znd, reverse.lut_r, gfp);
	if (!mapped) {
		err = -EIO;
		Z_ERR(znd, "unused_phy: Reverse Map Entry not found.");
		goto out;
	}

	if (!mapped->data.addr) {
		Z_ERR(znd, "Catastrophic missing LUT page.");
		dump_stack();
		err = -EIO;
		goto out;
	}
	atomic_inc(&mapped->refcount);

	/*
	 * if the value is modified update the table and
	 * place it on top of the active [inpool] list
	 */
	if (mapped->data.addr[reverse.pg_idx] != MZTEV_UNUSED) {
		u32 gzno  = reverse.zone_id >> GZ_BITS;
		u32 gzoff = reverse.zone_id & GZ_MMSK;
		struct meta_pg *wpg = &znd->wp[gzno];
		u32 wp;
		u32 zf;
		u32 stream_id;

		if (orig_s) {
			__le32 enc = mapped->data.addr[reverse.pg_idx];
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
		MutexLock(&mapped->md_lock);
		mapped->data.addr[reverse.pg_idx] = MZTEV_UNUSED;
		mutex_unlock(&mapped->md_lock);

		mapped->age = jiffies_64;
		set_bit(IS_DIRTY, &mapped->flags);
		incore_hint(znd, mapped);

		MutexLock(&wpg->wplck);
		wp = le32_to_cpu(wpg->wp_alloc[gzoff]) & Z_WP_VALUE_MASK;
		zf = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_VALUE_MASK;
		stream_id = le32_to_cpu(wpg->zf_est[gzoff]) & Z_WP_STREAM_MASK;
		if (wp > 0 && zf < Z_BLKSZ) {
			zf++;
			wpg->zf_est[gzoff] = cpu_to_le32(zf | stream_id);
			set_bit(IS_DIRTY, &wpg->flags);
		}
		mutex_unlock(&wpg->wplck);
		if (wp == Z_BLKSZ)
			znd->discard_count++;
	} else {
		Z_DBG(znd, "lba: %" PRIx64 " alread reported as free?", lba);
	}

out_unlock:
	atomic_dec(&mapped->refcount);

out:
	put_map_entry(mapped);

	return err;
}
