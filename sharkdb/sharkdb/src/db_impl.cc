
#include "db_impl.h"
#include <thread>
#include <algorithm>

/*	TODO need to implement some form of draining:
	1) we buffer calls before doing io_uring_submit.
	2) need to finish polling read completion queue.
	3) need to persist current in-memory changes to log. */

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
		//	Possible for key not existing in this table.
		if (it == ss_table->fence_ptrs_.begin()) {
			return {0,0};
		}
		//	because fence pointers are left-justified.
		assert(it != ss_table->fence_ptrs_.end());
		size_t blk_range_end = it->blk_num_;
		it--;
		size_t blk_range_start = it->blk_num_;

		assert(blk_range_end - blk_range_start <= BLOCKS_PER_FENCE);
		return {blk_range_start, blk_range_end};
	} else {
		return {0, 0};
	}
}

db_t::db_t() : l0_version_ctr_(0), stop_thrs_(false), do_writes_(true) {
	int rc;

    //	Necessary to avoid copy constructor, since we're not implementing rule of 5...
    partitions_.reserve(N_PARTITIONS);
    for (size_t i = 0; i<N_PARTITIONS; ++i) {
        partitions_.emplace_back(i, this);
    }

    rc = pthread_create(&log_thr_, nullptr, log_thr_body, this);
    assert(rc == 0);
}

db_t::~db_t() {
    stop_thrs_ = true;
	int rc;
    void* res;

    rc = pthread_join(log_thr_, &res);
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
    get_stats()->thr_name_ = "user_thread";

	//	Use a mutex since we might want to add more stuff in here.
    rc = pthread_mutex_lock(&db_instance->io_manager_.lock_);
    assert(rc == 0);
    assert(++init_ct <= N_USER_THREADS);
    rc = pthread_mutex_unlock(&db_instance->io_manager_.lock_);
    assert(rc == 0);

    //  Set high io priority.

    return new sharkdb_t(db_instance, cq, rd_ring);
}

std::pair<bool, sharkdb_cqev> sharkdb_cpoll_cq(sharkdb_t* db) {
	cq_t* cq = (cq_t*) db->cq_impl_;
	if (!cq->empty() && cq->top().lclk_visible_ <= cq->top().part_->lclk_visible_) {
	    cqe_t cqe = cq->top();
        #if defined(MEASURE)
        get_stats()->t_write_visible_ns_.push_back(get_ts_nsecs() - cqe.submit_ts_);
        #endif

        cq->pop();
        return {cqe.success_, cqe.ev_};
    } else {
        return {true, SHARKDB_CQEV_FAIL};
    }
}

void sharkdb_free(sharkdb_t* db) {
	delete (cq_t*) db->cq_impl_;
	delete (read_ring_t*) db->rd_ring_impl_;
	delete db;
}

//	Hacky- just for ending, to avoid complex logic
size_t sharkdb_drain(sharkdb_t* db) {
	cq_t* cq = (cq_t*) db->cq_impl_;
    size_t ct = 0;
	while (!cq->empty()) {
		cq->pop();
        ct += 1;
	}
    return ct;
}

void sharkdb_nowrites(sharkdb_t* db) {
    db_t* p_db = (db_t*) db->db_impl_;
    p_db->do_writes_ = false;
}

//  Stats code
stats_t::stats_t() : thr_name_(nullptr), n_reads_(0), n_reads_io_(0), n_flush_syncs_(0), n_log_syncs_(0) {
    #if defined(MEASURE)
    t_read_io_ns_.reserve(2000000);
    t_read_io_sync_ns_.reserve(2000000);
    t_read_io_single_ns_.reserve(2000000);
    t_write_visible_ns_.reserve(2000000);
    t_contend_namesp_ns_.reserve(2000000);
    t_backpres_inflight_ns_.reserve(2000000);
    t_backpres_mt_space_ns_.reserve(2000000);
    t_backpres_mt_space_ns_.reserve(2000000);
    #endif
}

static constexpr int PRBUF_BYTES = 200;

static void fill_latency_stats(std::vector<uint64_t>& times, char* fill) {
    int rc;
    if (times.size() == 0) {
        rc = sprintf(fill, "n: 0");
    } else {
        std::sort(times.begin(), times.end());
        uint64_t time_sum = 0;
        uint64_t time_sum_1ms = 0;
        for (uint64_t time : times) {
            time_sum += time;
            if (time < 1000000) {
                time_sum_1ms += time;
            }
        }

        rc = sprintf(fill, "n: %lu, sum: %lu, sum_1ms: %lu, [min: %lu, 1%%: %lu, 10%%: %lu, 50%%: %lu, 90%%: %lu, 99%%: %lu, 99.9%%: %lu, 99.99%%: %lu, max: %lu]", times.size(), time_sum, time_sum_1ms, times[0], times[times.size()/100], times[times.size()/10], times[times.size()/2], times[times.size()*9/10], times[times.size()*99/100], times[times.size()*999/1000], times[times.size()*9999/10000], times[times.size()-1]);
    }
    assert(rc < PRBUF_BYTES);
}

stats_t::~stats_t() {
    #if defined(MEASURE)
    char t_io_sync_buf[PRBUF_BYTES];
    fill_latency_stats(t_read_io_sync_ns_, &t_io_sync_buf[0]);
    char t_io_buf[PRBUF_BYTES];
    fill_latency_stats(t_read_io_ns_, &t_io_buf[0]);
    char t_io_single_buf[PRBUF_BYTES];
    fill_latency_stats(t_read_io_single_ns_, &t_io_single_buf[0]);
    char t_write_visible_buf[PRBUF_BYTES];
    fill_latency_stats(t_write_visible_ns_, &t_write_visible_buf[0]);
    char t_contend_namesp_buf[PRBUF_BYTES];
    fill_latency_stats(t_contend_namesp_ns_, &t_contend_namesp_buf[0]);
    char t_backpres_inflight_buf[PRBUF_BYTES];
    fill_latency_stats(t_backpres_inflight_ns_, &t_backpres_inflight_buf[0]);
    char t_backpres_mt_space_buf[PRBUF_BYTES];
    fill_latency_stats(t_backpres_mt_space_ns_, &t_backpres_mt_space_buf[0]);

    fprintf(stderr,
        "thr_name: %s\n"
        "n_reads: %u\n"
        "n_reads_io: %u\n"
        "n_flush_syncs: %u\n"
        "n_log_syncs: %u\n"
        "t_read_io_sync_ns: %s\n"
        "t_read_io_ns: %s\n"
        "t_read_io_single_ns: %s\n"
        "t_write_visible_ns: %s\n"
        "t_contend_namesp_ns: %s\n"
        "t_backpres_inflight_ns: %s\n"
        "t_backpres_mt_space_ns: %s\n"
        "\n",
        thr_name_, n_reads_, n_reads_io_, n_flush_syncs_, n_log_syncs_, &t_io_sync_buf[0], &t_io_buf[0], &t_io_single_buf[0], &t_write_visible_buf[0], &t_contend_namesp_buf[0], &t_backpres_inflight_buf[0], &t_backpres_mt_space_buf[0]);

    #endif
}

stats_t* get_stats() {
    static thread_local stats_t stats_;
    return &stats_;
}
