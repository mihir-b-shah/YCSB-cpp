
#include "db_impl.h"

#include <sys/resource.h>
#include <algorithm>
#include <liburing.h>

read_ring_t::read_ring_t(db_t* ref) : db_ref_(ref), n_progress_io_(0) {
    int rc;

    struct rlimit mlock_lim;
    rc = getrlimit(RLIMIT_MEMLOCK, &mlock_lim);
    assert(rc == 0);
    size_t total_lk_mem_avail = (mlock_lim.rlim_cur * PERC_LOCKED_MEM_USE) / 100;
    size_t n_buffers = std::min(total_lk_mem_avail / (BLOCKS_PER_FENCE * BLOCK_BYTES * N_USER_THREADS), N_IN_FLIGHT / N_USER_THREADS);

	//	Note for each read that requires I/O, we need one progress_state tracker.
    void* buffers_base;
    rc = posix_memalign(&buffers_base, BLOCK_BYTES, n_buffers * sizeof(buffer_t));
    assert(rc == 0);

    buffers_ = (buffer_t*) buffers_base;
	progress_states_ = new progress_t[n_buffers];
	free_list_.init(n_buffers);

    //  TODO Try IOPOLL, https://github.com/axboe/liburing/issues/385
    rc = io_uring_queue_init(READ_SQ_DEPTH, &ring_, 0);
    assert(rc == 0);

    struct iovec ivec = (struct iovec) {buffers_, n_buffers * BLOCK_BYTES * BLOCKS_PER_FENCE};
    rc = io_uring_register_buffers(&ring_, &ivec, 1);
    assert(rc == 0);
}

read_ring_t::~read_ring_t() {
    int rc = io_uring_unregister_buffers(&ring_);
    assert(rc == 0);

    io_uring_queue_exit(&ring_);
    
    free(buffers_);
	delete[] progress_states_;
}

void submit_read_io(read_ring_t* ring, read_ring_t::progress_t* prog_state) {
    #if defined(INSTR)
    get_stats()->n_reads_io_ += 1;
    #endif

    #if defined(INSTR)
    prog_state->submit_ts_single_ns_ = get_ts_nsecs();
    #endif

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring->ring_);
    
    void* p = (void*) &ring->buffers_[prog_state->blk_fill_id_];
    size_t len = (prog_state->blk_range_end_ - prog_state->blk_range_start_) * BLOCK_BYTES;
    size_t f_offs = prog_state->blk_range_start_ * BLOCK_BYTES;

    io_uring_prep_read_fixed(sqe, prog_state->fd_, p, len, f_offs, 0);
    io_uring_sqe_set_data(sqe, (void*) prog_state);
    if (io_uring_sq_space_left(&ring->ring_) == 0) {
        int n_submitted = io_uring_submit(&ring->ring_);
        assert(n_submitted == READ_SQ_DEPTH);
    }
}

void drain_sq_ring(read_ring_t* ring) {
    int n_submitted = io_uring_submit(&ring->ring_);
    assert(n_submitted >= 0);
}

static char* search_buffer(read_ring_t* ring, read_ring_t::progress_t* state) {
    for (size_t b = 0; b<state->blk_range_end_ - state->blk_range_start_; ++b) {
        for (size_t j = 0; j<N_ENTRIES_PER_BLOCK; ++j) {
            kv_pair_t* kv = &ring->buffers_[state->blk_fill_id_].buf_[b].kvs_[j];
            if (memcmp(&kv->key_[0], &state->key_[0], SHARKDB_KEY_BYTES) == 0) {
                return &kv->val_[0];
            }
        }
    }
    return nullptr;
}

//	Done within the namespace lock, in the read path.
void check_io(sharkdb_t* arg) {
    int rc;
	cq_t* cq = (cq_t*) arg->cq_impl_;
	read_ring_t* ring = (read_ring_t*) arg->rd_ring_impl_;

    struct io_uring_cqe* cqe;
	rc = io_uring_peek_cqe(&ring->ring_, &cqe);

	if (rc == 0) {
		read_ring_t::progress_t* state = (read_ring_t::progress_t*) io_uring_cqe_get_data(cqe);

        #if defined(INSTR)
        get_stats()->t_read_io_single_ns_.push_back(get_ts_nsecs() - state->submit_ts_single_ns_);
        #endif

		size_t n_read = (state->blk_range_end_ - state->blk_range_start_) * BLOCK_BYTES;
		assert(cqe->res == (int) n_read);
		char* res = search_buffer(ring, state);

		if (res != nullptr) {
            #if defined(INSTR)
            get_stats()->t_read_io_ns_.push_back(get_ts_nsecs() - state->submit_ts_ns_);
            #endif

			memcpy(state->user_buf_, res, SHARKDB_VAL_BYTES);
			//  For reads, lclk=0 (i.e. always visible) is fine.
			cq->emplace(state->part_, 0, state->cqev_, true);
			ring->free_list_.free(state->blk_fill_id_);
		} else {
			//  Value not found- continue scanning.
			assert(state->level_ == 1 && "No compaction for now");
			ssize_t j;
			for (j = (ssize_t) state->ss_table_id_-1; j>=0; --j) {
				ss_table_t* ss_table = state->part_->disk_levels_[1][j];
				std::pair<size_t, size_t> range = get_ss_blk_range(&state->key_[0], ss_table);
				if (range != std::pair<size_t, size_t>(0,0)) {
					state->ss_table_id_ = j;
					state->fd_ = ss_table->fd_;
					state->blk_range_start_ = range.first;
					state->blk_range_end_ = range.second;
					break;
				}
			}

			if (j == -1) {
                cq->emplace(state->part_, 0, state->cqev_, false);
			    ring->free_list_.free(state->blk_fill_id_);
            } else {
                submit_read_io(ring, state);
            }
		}
		io_uring_cqe_seen(&ring->ring_, cqe);
	}
}
