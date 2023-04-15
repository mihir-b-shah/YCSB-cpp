
#include "consts.h"
#include "db_impl.h"

#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <liburing.h>
#include <sys/uio.h>
#include <errno.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <bitset>

wal_resources_t::wal_resources_t() {
    int rc;

    rc = pthread_spin_init(&wal_assn_lock_, PTHREAD_PROCESS_PRIVATE);
    assert(rc == 0);

    size_t log_size = (LOG_BUF_MAX_ENTRIES + N_ENTRIES_PER_BLOCK - 1) / N_ENTRIES_PER_BLOCK * BLOCK_BYTES;
	for (size_t i = 0; i<2*N_PARTITIONS; ++i) {
		char path[40];
		sprintf(&path[0], "/tmp/sharkdb/wal_%lu", i);
		wal_fds_[i] = open((const char*) path, O_CREAT | O_WRONLY | O_SYNC | O_DIRECT, S_IRUSR | S_IWUSR);
		assert(wal_fds_[i] >= 0);
        rc = posix_fallocate(wal_fds_[i], 0, log_size);
        assert(rc == 0);

        void* buf_p;
        rc = posix_memalign(&buf_p, BLOCK_BYTES, log_size);
        assert(rc == 0);
        log_buffers_[i] = (struct iovec) {buf_p, log_size};
	}

    //  TODO should we do fadvise for the log_ring?
    rc = io_uring_queue_init(16+2*N_PARTITIONS, &log_ring_, 0);
    assert(rc == 0);
}

wal_resources_t::~wal_resources_t() {
    int rc;

	// just a formality, process end should close them anyway.
	for (size_t i = 0; i<2*N_PARTITIONS; ++i) {
		assert(wal_fds_[i] >= 0);
        rc = close(wal_fds_[i]);
        assert(rc == 0);

	    free((void*) log_buffers_[i].iov_base);
	}

    io_uring_queue_exit(&log_ring_);
}

wal_t::wal_t(db_t* ref) : buf_p_ucommit_(0), buf_p_commit_(0), db_ref_(ref) {
    int rc;
    wal_resources_t* wres = &db_ref_->wal_resources_;

    rc = pthread_spin_lock(&wres->wal_assn_lock_);
    assert(rc == 0);

	size_t i;
	for (i = 0; i<2*N_PARTITIONS; ++i) {
		if (wres->wal_fds_[i] >= 0) {
			break;
		}
	}
	assert(i < 2*N_PARTITIONS);
    
    int fd = wres->wal_fds_[i];
	wres->wal_fds_[i] = -1;
    rc = pthread_spin_unlock(&wres->wal_assn_lock_);
    assert(rc == 0);

	fd_ = fd;
	idx_ = i;
    //  TODO Is this safe, if the file is also being touched by io_uring?
	rc = lseek(fd_, 0, SEEK_SET);
    assert(rc == 0);

    log_buffer_ = (block_t*) wres->log_buffers_[i].iov_base;
}

wal_t::~wal_t() {
    int rc;
    wal_resources_t* wres = &db_ref_->wal_resources_;

    rc = pthread_spin_lock(&wres->wal_assn_lock_);
    assert(rc == 0);
	wres->wal_fds_[idx_] = fd_;
    rc = pthread_spin_unlock(&wres->wal_assn_lock_);
    assert(rc == 0);
}

/*  Single logging thread
    1)  Scan through partitions, and see if any logs need flushing. Note buf_p_commit_ is only read,
        written by this thread- so no reason for it to be atomic.

        Log writes do not need to hold namespace lock. Reasoning is our swapping
        mechanism means the pointer it is trying to sync to the wal is valid until
        the backing in memory log is deleted along with the l0. But this would happen
        strictly after the sstable is created as such the wal serves no use. And, the
        sstable will have our (uncommitted) changes, since it doesn't read timestamps.
        However, ASAN might flag this? If so, I maybe need to extract some code elsewhere,
        and turn off asan for that file? */

struct sync_state_t {
    bool valid_;
    size_t part_idx_;
    uint32_t l0_version_;
    uint64_t new_lclk_visible_;
    size_t n_written_;

    sync_state_t() : valid_(false) {}
};

void* log_thr_body(void* arg) {
    int rc;
    db_t* db = (db_t*) arg;
    struct io_uring* log_ring = &db->wal_resources_.log_ring_;
    sync_state_t sync_state[N_PARTITIONS];
    bool do_backpressure = false;
    bool did_backpressure_locks = false;

    get_stats()->thr_name_ = "log_thread";

    while (!db->stop_thrs_) {
        _loop_head:
        if (do_backpressure) {
            if (!did_backpressure_locks) {
                for (size_t i = 0; i<N_PARTITIONS; ++i) {
                    partition_t* part = &db->partitions_[i];

                    pthread_rwlock_lock_wrap<TEMPL_IS_WRITE>(&part->namespace_lock_, get_stats()->t_contend_namesp_ns_);
                }
                did_backpressure_locks = true;
            }
        } else {
            for (size_t i = 0; i<N_PARTITIONS; ++i) {
                partition_t* part = &db->partitions_[i];

                pthread_rwlock_lock_wrap<TEMPL_IS_READ>(&part->namespace_lock_, get_stats()->t_contend_namesp_ns_);

                //  check if we need to sync to log...
                uint32_t ucommit_ld = __atomic_load_n(&part->l0_->wal_.buf_p_ucommit_, __ATOMIC_SEQ_CST);
                if (ucommit_ld - part->l0_->wal_.buf_p_commit_ >= LOG_BUF_SYNC_INTV) {
                    wal_t* wal_p = &part->l0_->wal_;
                    rc = pthread_rwlock_unlock(&part->namespace_lock_);
                    assert(rc == 0);

                    if (sync_state[i].valid_) {
                        assert(!do_backpressure && !did_backpressure_locks);
                        do_backpressure = true;
                        goto _loop_head;
                    }

                    /*  Pthread rw lock cannot be upgraded, so release/try again.

                        Acquiring the write lock is VERY convenient- it forces all reads
                        to finish- meaning the buf_p_ucommit_ counter is consistent with
                        the contents of the buffer- since the memcpy actions have happened,
                        and by ACQ_REL semantics of a rdlock, they have been flushed to memory. */

                    pthread_rwlock_lock_wrap<TEMPL_IS_WRITE>(&part->namespace_lock_, get_stats()->t_contend_namesp_ns_);
                    uint64_t lclk = part->lclk_next_;
                    uint32_t until = part->l0_->wal_.buf_p_ucommit_;
                    uint32_t p_commit = part->l0_->wal_.buf_p_commit_;

                    rc = pthread_rwlock_unlock(&part->namespace_lock_);
                    assert(rc == 0);

                    pthread_rwlock_lock_wrap<TEMPL_IS_READ>(&part->namespace_lock_, get_stats()->t_contend_namesp_ns_);

                    //  the index of file and buffer we want to write.
                    //  Ensure we only log whole blocks.
                    until /= N_ENTRIES_PER_BLOCK;
                    p_commit /= N_ENTRIES_PER_BLOCK; 

                    char* p = (char*) &part->l0_->wal_.log_buffer_[p_commit];
                    size_t len = (until - p_commit) * BLOCK_BYTES;
                    size_t f_offs = p_commit * BLOCK_BYTES;

                    if (wal_p != &part->l0_->wal_) {
                        pthread_rwlock_unlock(&part->namespace_lock_);
                        goto _loop_head;
                    }

                    struct io_uring_sqe* sqe = io_uring_get_sqe(log_ring);

                    if (!part->db_ref_->do_writes_) {
                        len = BLOCK_BYTES;
                    }

                    io_uring_prep_write(sqe, part->l0_->wal_.fd_, p, len, f_offs);

                    sync_state[i].valid_ = true;
                    sync_state[i].part_idx_ = i;
                    sync_state[i].l0_version_ = part->l0_->version_;
                    sync_state[i].n_written_ = len;
                    sync_state[i].new_lclk_visible_ = lclk-1;
                    io_uring_sqe_set_data(sqe, (void*) &sync_state[i]);

                    //  safe, since only this thread writes to this ring.
                    int n_submitted = io_uring_submit(log_ring);
                    assert(n_submitted == 1);

                    //  advance p_commit now, so we can proceed...
                    part->l0_->wal_.buf_p_commit_ = until * N_ENTRIES_PER_BLOCK;
                }

                rc = pthread_rwlock_unlock(&part->namespace_lock_);
                assert(rc == 0);
            }
        }
        
        //  this wastes CPU, ideally we would wait for a completion, but simpler.
        struct io_uring_cqe* cqe;
        rc = io_uring_peek_cqe(log_ring, &cqe);
        if (rc == 0) {
            /*  Do the following when we know a sync succeeded.
                1) Check the l0/wal I am modifying is the same as the one that synced- by id.
                2) Increment lclk_visible_ on the partition. */
            sync_state_t* state = (sync_state_t*) io_uring_cqe_get_data(cqe);
            assert(state->valid_);

            partition_t* part = &db->partitions_[state->part_idx_];
            part->lclk_visible_ = state->new_lclk_visible_;

            state->valid_ = false;
            assert(cqe->res == (int) state->n_written_);
            io_uring_cqe_seen(log_ring, cqe);

            if (did_backpressure_locks) {
                for (size_t i = 0; i<N_PARTITIONS; ++i) {
                    partition_t* part = &db->partitions_[i];
                    rc = pthread_rwlock_unlock(&part->namespace_lock_);
                    assert(rc == 0);
                }
                did_backpressure_locks = false;
                do_backpressure = false;
            }
        } else {
            assert(rc == -EAGAIN);
        }
        __builtin_ia32_pause();
    }
	return nullptr;
}
