
#include "db_impl.h"

//  Just fill out constructors, etc. here from db_impl.h

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

partition_t::partition_t(size_t tid, db_t* ref) : tid_(tid), l0_swp_(nullptr), disk_levels_(1+N_DISK_LEVELS), lclk_visible_(0), lclk_next_(1), stop_flush_thr_(false), db_ref_(ref) {
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
    stop_flush_thr_ = true;

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

db_t::db_t() : stop_log_thr_(false), l0_version_ctr_(0) {
    //	Necessary, since we're not implementing rule of 5...
    partitions_.reserve(N_PARTITIONS);
    for (size_t i = 0; i<N_PARTITIONS; ++i) {
        partitions_.emplace_back(i, this);
    }
    int rc = pthread_create(&log_thr_, nullptr, log_thr_body, this);
    assert(rc == 0);
}

db_t::~db_t() {
    stop_log_thr_ = true;
    void* res;
    int rc = pthread_join(log_thr_, &res);
    assert(rc == 0);
}
