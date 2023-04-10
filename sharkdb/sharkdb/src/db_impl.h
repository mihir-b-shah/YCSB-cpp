
#ifndef _DB_IMPL_H_
#define _DB_IMPL_H_

#define TBB_PREVIEW_CONCURRENT_ORDERED_CONTAINERS 1

#include <sharkdb.h>

#include "consts.h"
#include "filter.h"
#include "free_list.h"

#include <cstdlib>
#include <cassert>
#include <vector>
#include <queue>
#include <utility>
#include <cstring>
#include <pthread.h>
#include <errno.h>
#include <liburing.h>
#include <sys/uio.h>
#include <tbb/concurrent_map.h>

struct db_t;

struct wal_t {
    struct wal_block_t {
        kv_pair_t kvs_[N_ENTRIES_PER_BLOCK];
        char pad_[BLOCK_BYTES - sizeof(kv_pair_t)*N_ENTRIES_PER_BLOCK];
    };
    static_assert(sizeof(wal_block_t) == BLOCK_BYTES);

	int idx_;
    int fd_;
	wal_block_t* log_buffer_;
	uint32_t buf_p_ucommit_;
	uint32_t buf_p_commit_;
    db_t* db_ref_;

    wal_t(db_t* ref);
    ~wal_t();
};

struct fence_ptr_t {
    char k_[SHARKDB_KEY_BYTES];
    size_t blk_num_;

	fence_ptr_t(const char* k, size_t blk_num);
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

    mem_entry_t(const char* v);
    ~mem_entry_t() {}
};

typedef tbb::concurrent_map<const char*, mem_entry_t, cmp_keys_lt> mem_table_t;

struct level_0_t {
    mem_table_t mem_table_;
	wal_t wal_;
	size_t version_;
    db_t* db_ref_;

	level_0_t(db_t* ref);
	~level_0_t() {}
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
	bool stop_flush_thr_;
	pthread_rwlock_t namespace_lock_;
    pthread_rwlockattr_t namespace_lock_attrs_;
    db_t* db_ref_;
    pthread_spinlock_t lclk_lock_;

	partition_t(size_t tid, db_t* ref);
    ~partition_t();
};

struct wal_resources_t {
    int wal_fds_[N_PARTITIONS * 2];
    struct iovec log_buffers_[N_PARTITIONS * 2];
    pthread_spinlock_t wal_assn_lock_;
    struct io_uring log_ring_;

    wal_resources_t();
    ~wal_resources_t();
};

void* log_thr_body(void* arg);
void* io_thr_body(void* arg);

struct cqe_t {
	partition_t* part_;
	uint64_t lclk_visible_;
	sharkdb_cqev ev_;

	cqe_t(partition_t* part, uint64_t lclk, sharkdb_cqev ev) 
		: part_(part), lclk_visible_(lclk), ev_(ev) {}
};
typedef std::queue<cqe_t> cq_t;

/*  User-thread private ring, since io_uring is intended to be each thread-private */
struct read_ring_t {
    struct __attribute__((packed)) buffer_t {
        char buf_[BLOCKS_PER_FENCE][BLOCK_BYTES];
    };
    static_assert(sizeof(buffer_t) == BLOCKS_PER_FENCE * BLOCK_BYTES);

	struct progress_t {
		size_t level_;
		size_t ss_table_id_;
		char key_[SHARKDB_KEY_BYTES];
		size_t blk_fill_id_;
		char* user_buf_;
		sharkdb_cqev cqev_;
	};

    buffer_t* buffers_;
	progress_t* progress_states_;
    io_uring read_ring_;
    db_t* db_ref_;
    free_list_t free_list_;

    read_ring_t(db_t* ref);
    ~read_ring_t();
};

std::pair<size_t, size_t> get_ss_blk_range(const char* k, ss_table_t* ss_table);
void submit_read_io(read_ring_t::progress_t* prog_state);

//  Put stuff here I want, to coordinate io's.
struct io_manager_t {
};

struct db_t {
    /*  Important wal_resources_ is constructed before partitions_, and destructed afterward-
        as such, by C++ spec, we place it before partitions_ */
    wal_resources_t wal_resources_;
	std::vector<partition_t> partitions_;
	pthread_t log_thr_;
    uint32_t l0_version_ctr_;
	bool stop_thrs_;
	pthread_t io_thr_;
    io_manager_t io_manager_;

	db_t();
	~db_t();
};

#endif
