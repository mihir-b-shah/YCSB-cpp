
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
    char k_[SHARKDB_KEY_BYTES];
    size_t blk_num_;

	fence_ptr_t(const char* k, size_t blk_num) : blk_num_(blk_num) {
		memcpy(&k_[0], k, SHARKDB_KEY_BYTES);
	} 
};

static constexpr size_t calc_n_blocks(size_t level) {
    return MEM_TABLE_MAX_ENTRIES * GROWTH_POWERS[level] / N_ENTRIES_PER_BLOCK;
}

static constexpr size_t calc_n_filter_bits(size_t level) {
    return calc_n_blocks(level) * FILTER_BITS_PER_BLOCK;
}

//  Let's keep functions free, just attach constructor/destructor.
struct ss_table_t {
    size_t level_;
    int fd_;
    bloom_filter_t filter_;
    std::vector<fence_ptr_t> fence_ptrs_;

	ss_table_t(size_t level) : level_(level), filter_(calc_n_filter_bits(level)) {}
	~ss_table_t() {}
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

struct level_0_t {
	typedef tbb::concurrent_map<const char*, mem_entry_t, cmp_keys_lt> mem_table_t;

    mem_table_t mem_table_;
	wal_t wal_;
	size_t id_;

	level_0_t(size_t id) : mem_table_{cmp_keys_lt()}, id_(id) {}
};

void* flush_thr_body(void* arg);

struct partition_t {
	size_t tid_;
	level_0_t* l0_;
	level_0_t* l0_swp_;
	//	Keep level 0 empty to allow intuitive indices.
    std::vector<std::vector<ss_table_t*>> disk_levels_;

	pthread_t flush_thr_;
	pthread_t compact_thr_;
	pthread_rwlock_t namespace_lock_;
	bool stop_flush_thr_;

	partition_t(size_t tid) : tid_(tid), l0_swp_(nullptr), namespace_lock_(PTHREAD_RWLOCK_INITIALIZER), disk_levels_(1+N_DISK_LEVELS), stop_flush_thr_(false) {
		l0_ = new level_0_t(1);
		assert(pthread_create(&flush_thr_, nullptr, flush_thr_body, this) == 0);
	}
    ~partition_t() {
		stop_flush_thr_ = true;

		void* res;
		assert(pthread_join(flush_thr_, &res) == 0);
		__sync_synchronize();

		delete l0_;
		if (l0_swp_ != nullptr) {
			delete l0_swp_;
		}
		for (std::vector<ss_table_t*>& v : disk_levels_) {
			for (ss_table_t* table : v) {
				delete table;
			}
		}
	}
};

struct db_t {
	std::vector<partition_t> partitions_;

	db_t() {
		//	Necessary, since we're not implementing rule of 5...
		partitions_.reserve(N_PARTITIONS);
		for (size_t i = 0; i<N_PARTITIONS; ++i) {
			partitions_.emplace_back(i);
		}
	}

	~db_t() {}
};

#endif
