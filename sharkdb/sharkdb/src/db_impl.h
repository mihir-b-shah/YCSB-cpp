
#ifndef _DB_IMPL_H_
#define _DB_IMPL_H_

#define TBB_PREVIEW_CONCURRENT_ORDERED_CONTAINERS 1

#include <sharkdb.h>

#include "consts.h"
#include "filter.h"
#include "wal.h"

#include <cassert>
#include <vector>
#include <utility>
#include <cstring>
#include <pthread.h>
#include <tbb/concurrent_map.h>

struct cmp_keys_lt {
    bool operator()(const char* k1, const char* k2) const {
        return memcmp(k1, k2, SHARKDB_KEY_BYTES) < 0;
    }
};

struct fence_ptr_t {
    const char* k;
    size_t blk_num;
};

static constexpr size_t calc_n_blocks(size_t level) {
    return MEM_TABLE_BYTES * GROWTH_POWERS[level] / BLOCK_BYTES;
}

static constexpr size_t calc_n_filter_bits(size_t level) {
    return calc_n_blocks(level) * FILTER_BITS_PER_BLOCK;
}

static constexpr size_t calc_n_fence_ptrs(size_t level) {
    return calc_n_blocks(level) / BLOCKS_PER_FENCE;
}

//  Let's keep functions free, just attach constructor/destructor.
struct level_t {
    size_t level_;
    int fd_;
    bloom_filter_t filter_;
    std::vector<fence_ptr_t> fence_ptrs_;

    level_t(size_t level);
    ~level_t();
};

struct mem_entry_t {
    // SplinterDB uses a distributed RW lock- maybe worth trying?
    pthread_rwlock_t lock_;
    const char* v_;

    mem_entry_t(const char* v) : lock_(PTHREAD_RWLOCK_INITIALIZER), v_(v) {
		assert(pthread_rwlock_wrlock(&lock_) == 0);
	}
    ~mem_entry_t() {
        assert(pthread_rwlock_destroy(&lock_) == 0);
    }
};

struct db_t {
	typedef tbb::concurrent_map<const char*, mem_entry_t, cmp_keys_lt> mem_table_t;
    mem_table_t mem_table_;
    std::vector<level_t> disk_levels_;
    wal_t wal_;

    db_t();
    ~db_t();
};

#endif
