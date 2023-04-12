
#include "db_impl.h"
#include <thread>

//  Just fill out constructors, little methods, etc. here from db_impl.h/sharkdb.h

fence_ptr_t::fence_ptr_t(const char* k, size_t blk_num) : blk_num_(blk_num) {
    memcpy(&k_[0], k, SHARKDB_KEY_BYTES);
}

mem_entry_t::mem_entry_t(const char* v) : v_(v) {
    int rc;
    rc = pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
    assert(rc == 0);
    rc = pthread_spin_lock(&lock_);
    assert(rc == 0);
}

level_0_t::level_0_t(db_t* ref) : mem_table_{cmp_keys_lt()}, wal_(ref), db_ref_(ref) {
    //  TODO do I need sequential consistency here?
    version_ = __atomic_add_fetch(&db_ref_->l0_version_ctr_, 1, __ATOMIC_SEQ_CST);
}

partition_t::partition_t(size_t tid, db_t* ref) : tid_(tid), l0_swp_(nullptr), disk_levels_(1+N_DISK_LEVELS), lclk_visible_(0), lclk_next_(1), db_ref_(ref) {
    int rc;
    rc = pthread_spin_init(&lclk_lock_, PTHREAD_PROCESS_PRIVATE);
    assert(rc == 0);

    l0_ = new level_0_t(db_ref_);
    /*  Tells pthread we don't recursively acquire read lock, and to give writers priority (since
        this is a queued lock (I think?). This should prevent livelock of the flushing thread */
    rc = pthread_rwlockattr_setkind_np(&namespace_lock_attrs_, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    assert(rc == 0);

    rc = pthread_rwlock_init(&namespace_lock_, &namespace_lock_attrs_);
    assert(rc == 0);

    rc = pthread_create(&flush_thr_, nullptr, flush_thr_body, this);
    assert(rc == 0);
}

partition_t::~partition_t() {
    void* res;
    int rc = pthread_join(flush_thr_, &res);
    assert(rc == 0);
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

/*	[s,e) range if found, if not [0,0)
	Fence pointers are good enough to store for <10 blocks at a time. As such,
	Just linearly stream the blocks in. */
std::pair<size_t, size_t> get_ss_blk_range(const char* k, ss_table_t* ss_table) {
	if (ss_table->filter_.test(k)) {
		// use fence pointers to find blocks to check.
		fence_ptr_t dummy(k, 0);
		auto it = std::upper_bound(ss_table->fence_ptrs_.begin(), ss_table->fence_ptrs_.end(), dummy, [](const fence_ptr_t& f1, const fence_ptr_t& f2){
			return memcmp(&f1.k_[0], &f2.k_[0], SHARKDB_KEY_BYTES) < 0;
		});
		//	because fence pointers are left-justified.
		assert(it != ss_table->fence_ptrs_.begin());
		size_t blk_range_end = it->blk_num_;
		it--;
		size_t blk_range_start = it->blk_num_;
		assert(blk_range_end - blk_range_start <= BLOCKS_PER_FENCE);
		return {blk_range_start, blk_range_end};
	} else {
		return {0, 0};
	}
}

db_t::db_t() : l0_version_ctr_(0), stop_thrs_(false) {
	int rc;

    //	Necessary to avoid copy constructor, since we're not implementing rule of 5...
    partitions_.reserve(N_PARTITIONS);
    for (size_t i = 0; i<N_PARTITIONS; ++i) {
        partitions_.emplace_back(i, this);
    }

    rc = pthread_create(&log_thr_, nullptr, log_thr_body, this);
    assert(rc == 0);
    rc = pthread_create(&io_thr_, nullptr, io_thr_body, this);
    assert(rc == 0);
}

db_t::~db_t() {
    stop_thrs_ = true;
	int rc;
    void* res;

    rc = pthread_join(log_thr_, &res);
    assert(rc == 0);
    rc = pthread_join(io_thr_, &res);
    assert(rc == 0);
}

static db_t* db_instance = nullptr;
static void free_db_instance() {
	delete db_instance;
}

sharkdb_t* sharkdb_init() {
    static std::once_flag init_flag;
    static uint32_t init_ct = 0;
    int rc;

	std::call_once(init_flag, [&](){
		atexit(free_db_instance);
		db_instance = new db_t();
	});
    cq_t* cq = new cq_t();
    read_ring_t* rd_ring = new read_ring_t(db_instance);

    rc = pthread_mutex_lock(&db_instance->io_manager_.lock_);
    assert(rc == 0);

    assert(++init_ct <= N_USER_THREADS);
    db_instance->io_manager_.cq_refs_.push_back(cq);
    db_instance->io_manager_.rd_ring_refs_.push_back(rd_ring);

    rc = pthread_mutex_unlock(&db_instance->io_manager_.lock_);
    assert(rc == 0);

    return new sharkdb_t(db_instance, cq, rd_ring);
}

void sharkdb_drain(sharkdb_t* db) {
    read_ring_t* ring = (read_ring_t*) db->rd_ring_impl_;
    size_t space = io_uring_sq_space_left(&ring->ring_);
    if (space < READ_SQ_DEPTH) {
        int n_submitted = io_uring_submit(&ring->ring_);
        assert(n_submitted == (int) (READ_SQ_DEPTH-space));
    }
}

sharkdb_cqev sharkdb_cpoll_cq(sharkdb_t* db) {
	cq_t* cq = (cq_t*) db->cq_impl_;
	cqe_t cqe = cq->front();
	if (cqe.lclk_visible_ <= cqe.part_->lclk_visible_) {
        cq->pop();
        return cqe.ev_;
    } else {
        return SHARKDB_CQEV_FAIL;
    }
}

void sharkdb_free(sharkdb_t* db) {
	delete db;
}
