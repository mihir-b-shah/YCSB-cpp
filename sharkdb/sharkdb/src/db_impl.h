
#ifndef _DB_IMPL_H_
#define _DB_IMPL_H_

#define TBB_PREVIEW_CONCURRENT_ORDERED_CONTAINERS 1

#include <sharkdb.h>

#include "consts.h"
#include "filter.h"
#include "wal.h"

#include <cstdlib>
#include <cassert>
#include <vector>
#include <queue>
#include <utility>
#include <cstring>
#include <pthread.h>
#include <errno.h>
#include <tbb/concurrent_map.h>

struct fence_ptr_t {
    char k_[SHARKDB_KEY_BYTES];
    size_t blk_num_;

	fence_ptr_t(const char* k, size_t blk_num) : blk_num_(blk_num) {
		memcpy(&k_[0], k, SHARKDB_KEY_BYTES);
	} 
};

struct version_ptr_t {
	uint64_t lclk_;
	uint32_t buf_pos_;

	// if lclk_ == 0, then doesn't matter what buf_pos_ is.
	version_ptr_t() : lclk_(0), buf_pos_(0) {}
};

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
	//	Is a spinlock good idea? Maybe a cheaper mechanism?
    pthread_spinlock_t lock_;
    const char* v_;
	version_ptr_t p_ucommit_;
	version_ptr_t p_commit_;

    mem_entry_t(const char* v) : v_(v) {
		assert(pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE) == 0);
		assert(pthread_spin_lock(&lock_) == 0);
	}
    ~mem_entry_t() {
        assert(pthread_spin_destroy(&lock_) == 0);
    }
};

typedef tbb::concurrent_map<const char*, mem_entry_t, cmp_keys_lt> mem_table_t;

struct level_0_t {
    mem_table_t mem_table_;
	wal_t wal_;
	size_t id_;
	kv_pair_t* log_buffer_;
	uint32_t buf_idx_;

	level_0_t(size_t id) : mem_table_{cmp_keys_lt()}, id_(id), buf_idx_(0) {
		void* buf_p;
		posix_memalign(&buf_p, SECTOR_BYTES, sizeof(kv_pair_t)*LOG_BUF_MAX_ENTRIES);
		log_buffer_ = (kv_pair_t*) buf_p;
	}
	~level_0_t() {
		free(log_buffer_);
	}
};

void* flush_thr_body(void* arg);

struct partition_t {
	size_t tid_;
	level_0_t* l0_;
	level_0_t* l0_swp_;
	//	Keep level 0 empty to allow intuitive indices.
    std::vector<std::vector<ss_table_t*>> disk_levels_;
	// inclusive- i.e. lclk_visible_ == 1 means all lclk <= 1 are visible.
	uint64_t lclk_visible_;
	uint64_t lclk_next_;
	pthread_t flush_thr_;
	pthread_rwlock_t namespace_lock_;
	bool stop_flush_thr_;

	partition_t(size_t tid) : tid_(tid), l0_swp_(nullptr), disk_levels_(1+N_DISK_LEVELS), lclk_visible_(0), lclk_next_(1), namespace_lock_(PTHREAD_RWLOCK_INITIALIZER), stop_flush_thr_(false) {
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

void* io_mgmt_thr_body(void* arg);

struct db_t {
	std::vector<partition_t> partitions_;
	pthread_t io_mgmt_thr_;

	db_t() {
		//	Necessary, since we're not implementing rule of 5...
		partitions_.reserve(N_PARTITIONS);
		for (size_t i = 0; i<N_PARTITIONS; ++i) {
			partitions_.emplace_back(i);
		}
		assert(pthread_create(&io_mgmt_thr_, nullptr, io_mgmt_thr_body, this) == 0);
	}

	~db_t() {}
};

struct cqe_t {
	partition_t* part_;
	uint64_t lclk_visible_;
	sharkdb_cqev ev_;

	cqe_t(partition_t* part, uint64_t lclk, sharkdb_cqev ev) 
		: part_(part), lclk_visible_(lclk), ev_(ev) {}
};
typedef std::queue<cqe_t> cq_t;

#endif
