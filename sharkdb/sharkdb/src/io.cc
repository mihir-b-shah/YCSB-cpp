
#include "db_impl.h"

#include <sys/resource.h>
#include <liburing.h>

static constexpr size_t SQ_DEPTH = 16;

read_ring_t::free_list_t::free_list_t(size_t n_blocks) : head_(0), TAIL_(n_blocks) {
    tlist_ = new size_t[n_blocks];
    for (size_t i = 0; i<n_blocks-1; ++i) {
        tlist_[i] = i+1;
    }
    tlist_[n_blocks-1] = TAIL_;
}

read_ring_t::read_ring_t(db_t* ref) : db_ref_(ref) {
    int rc;

    struct rlimit mlock_lim;
    rc = getrlimit(RLIMIT_MEMLOCK, &mlock_lim);
    assert(rc == 0);
    size_t total_lk_mem_avail = (mlock_lim.rlim_cur * PERC_LOCKED_MEM_USE) / 100;
    size_t n_blocks = total_lk_mem_avail / (BLOCK_BYTES * N_USER_THREADS);
    blocks_ = new block_t[n_blocks];

    rc = io_uring_queue_init(SQ_DEPTH, &read_ring_, IORING_SETUP_IOPOLL);
    assert(rc == 0);

    struct iovec ivec = (struct iovec) {blocks_, n_blocks * BLOCK_BYTES};
    rc = io_uring_register_buffers(&read_ring_, &ivec, 1);
    assert(rc == 0);
}

read_ring_t::~read_ring_t() {
    int rc;
    
    rc = io_uring_unregister_buffers(&read_ring_);
    assert(rc == 0);

    rc = io_uring_queue_exit(&read_ring_);
    assert(rc == 0);
    
    delete[] blocks_;
}
