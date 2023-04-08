
#include "consts.h"
#include "db_impl.h"

#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <liburing.h>
#include <sys/uio.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>

wal_resources_t::wal_resources_t() {
    int rc;

    rc = pthread_spin_init(&wal_assn_lock_, PTHREAD_PROCESS_PRIVATE);
    assert(rc == 0);

    struct iovec reg_bufs[2*N_PARTITIONS];
    int reg_fds[2*N_PARTITIONS];

    size_t log_size = (LOG_BUF_MAX_ENTRIES + N_ENTRIES_PER_BLOCK - 1) / N_ENTRIES_PER_BLOCK * BLOCK_BYTES;

	for (size_t i = 0; i<2*N_PARTITIONS; ++i) {
		char path[40];
		sprintf(&path[0], "/tmp/sharkdb/wal_%lu", i);
		wal_fds_[i] = open((const char*) path, O_CREAT | O_APPEND | O_WRONLY | O_SYNC | O_DIRECT, S_IWUSR);
		assert(wal_fds_[i] >= 0);
        rc = posix_fallocate(wal_fds_[i], 0, log_size);
        assert(rc == 0);

        void* buf_p;
        rc = posix_memalign(&buf_p, SECTOR_BYTES, log_size);
        assert(rc == 0);
        log_buffers_[i] = (kv_pair_t*) buf_p;

        reg_fds[i] = wal_fds_[i];
        reg_bufs[i] = (struct iovec) {buf_p, log_size};
	}

    rc = io_uring_queue_init(16+2*N_PARTITIONS, &log_ring_, 0);
    assert(rc == 0);

    // register log buffers/files with io_uring.
    rc = io_uring_register_buffers(&log_ring_, &reg_bufs[0], 2*N_PARTITIONS);
    assert(rc == 0);
    rc = io_uring_register_files(&log_ring_, &reg_fds[0], 2*N_PARTITIONS);
    assert(rc == 0);
}

wal_resources_t::~wal_resources_t() {
    int rc;

	// just a formality, process end should close them anyway.
	for (size_t i = 0; i<2*N_PARTITIONS; ++i) {
		assert(wal_fds_[i] >= 0);
        rc = close(wal_fds_[i]);
        assert(rc == 0);

	    free((void*) log_buffers_[i]);
	}

    rc = pthread_spin_destroy(&wal_assn_lock_);
    assert(rc == 0);
    rc = io_uring_unregister_files(&log_ring_);
    assert(rc == 0);
    rc = io_uring_unregister_buffers(&log_ring_);
    assert(rc == 0);
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
    
    int fd = wres->wal_fds_[i];
	wres->wal_fds_[i] = -1;
    rc = pthread_spin_unlock(&wres->wal_assn_lock_);
    assert(rc == 0);

	assert(i < 2*N_PARTITIONS);
	fd_ = fd;
	idx_ = i;
	rc = lseek(fd_, 0, SEEK_SET);
    assert(rc == 0);

    log_buffer_ = wres->log_buffers_[i];
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

/*  Single I/O mgmt thread, does a couple of important things:
    1)  Scan through partitions, and see if any logs need flushing. Note buf_p_commit_ is only read,
        written by this thread- so no reason for it to be atomic.
    2)  Manage block reads (main function) */

void* io_mgmt_thr_body(void* arg) {
    /*
    int rc;
    db_t* db = (db_t*) arg;

    while (!db->stop_io_thr_) {
        //  TODO I want to not keep acquiring the namespace lock- give some time.
        for (size_t i = 0; i<N_PARTITIONS; ++i) {
            partition_t* part = db->partitions_[i];

            rc = pthread_rwlock_rdlock(&part->namespace_lock_);
            assert(rc == 0);

            uint32_t ucommit_ld = __atomic_load_n(&part->l0_->buf_p_ucommit_, __ATOMIC_SEQ_CST);
            if (ucommit_ld - part->l0_->buf_p_commit_ >= LOG_BUF_FLUSH_INTV) {
                    Log writes do not need to hold namespace lock. Reasoning is our swapping
                    mechanism means the pointer it is trying to sync to the wal is valid until
                    the backing in memory log is deleted along with the l0. But this would happen
                    strictly after the sstable is created as such the wal serves no use. However,
                    ASAN might flag this? If so, I maybe need to extract some code elsewhere, and turn
                    off asan for that file?


            }

            rc = pthread_rwlock_unlock(&part->namespace_lock_);
            assert(rc == 0);
        }
        __builtin_ia32_pause();
    }

    */
	return nullptr;
}
