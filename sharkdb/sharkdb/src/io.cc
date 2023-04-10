
#include "db_impl.h"

#include <sys/resource.h>
#include <liburing.h>

static constexpr size_t SQ_DEPTH = 16;

read_ring_t::read_ring_t(db_t* ref) : db_ref_(ref) {
    int rc;

    struct rlimit mlock_lim;
    rc = getrlimit(RLIMIT_MEMLOCK, &mlock_lim);
    assert(rc == 0);
    size_t total_lk_mem_avail = (mlock_lim.rlim_cur * PERC_LOCKED_MEM_USE) / 100;
    size_t n_buffers = total_lk_mem_avail / (BLOCKS_PER_FENCE * BLOCK_BYTES * N_USER_THREADS);

	//	Note for each read that requires I/O, we need one progress 
    buffers_ = new buffer_t[n_buffers];
	progress_states_ = new progress_t[n_buffers];
	free_list_.init(n_buffers);

    rc = io_uring_queue_init(SQ_DEPTH, &read_ring_, IORING_SETUP_IOPOLL);
    assert(rc == 0);

    struct iovec ivec = (struct iovec) {buffers_, n_buffers * BLOCK_BYTES * BLOCKS_PER_FENCE};
    rc = io_uring_register_buffers(&read_ring_, &ivec, 1);
    assert(rc == 0);
}

read_ring_t::~read_ring_t() {
    int rc;
    
    rc = io_uring_unregister_buffers(&read_ring_);
    assert(rc == 0);

    io_uring_queue_exit(&read_ring_);
    
    delete[] buffers_;
	delete[] progress_states_;
}

void submit_read_io(read_ring_t::progress_t* prog_state) {
	/*
	rc = pread(ss_table->fd_, &pread_buf[0], BLOCK_BYTES*(blk_range_end-blk_range_start), BLOCK_BYTES*blk_range_start);
	assert(rc == BLOCK_BYTES*BLOCKS_PER_FENCE);

	for (size_t b = 0; b<blk_range_end-blk_range_start; ++b) {
		for (size_t j = 0; j<N_ENTRIES_PER_BLOCK; ++j) {
			char* kr = &pread_buf[b*BLOCK_BYTES + j*(SHARKDB_KEY_BYTES+SHARKDB_VAL_BYTES)];
			if (memcmp(kr, k, SHARKDB_KEY_BYTES) == 0) {
				v_found = kr+SHARKDB_KEY_BYTES;
				goto search_done;
			}
		}
	}
	*/
}

void* io_thr_body(void* arg) {
	return nullptr;
}

