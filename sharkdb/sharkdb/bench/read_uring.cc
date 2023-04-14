
#include <liburing.h>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <cstddef>
#include <cassert>
#include <cstdlib>
#include <sys/stat.h>

static uint64_t get_micros(struct timespec ts) {
    return ((uint64_t) ts.tv_sec) * 1000000 + (ts.tv_nsec / 1000);
}

static constexpr size_t BLOCK_BYTES = 4096;
static constexpr size_t N_OPS = 100000;
static constexpr size_t WINDOW_SIZE = 256;

struct free_list_t {
	const size_t TAIL_;
	size_t head_;
	//  Just implement in a threaded fashion to avoid allocations.
	size_t* tlist_;

	free_list_t(size_t pool_size) : TAIL_(pool_size), head_(0) {
		tlist_ = new size_t[pool_size];
		for (size_t i = 0; i<pool_size-1; ++i) {
			tlist_[i] = i+1;
		}
		tlist_[pool_size-1] = TAIL_;
    }

	~free_list_t() {
		if (tlist_ != nullptr) {
			delete[] tlist_;
		}
	}

	size_t alloc() {
        size_t ret;
		if (head_ == TAIL_) {
			//	out of slots
			ret = TAIL_;
		} else {
            size_t next = tlist_[head_];
            ret = head_;
            head_ = next;
        }
		return ret;
	}

	void free(size_t slot) {
		tlist_[slot] = head_;
		head_ = slot;
	}
};

int main() {
    int rc;

	int fd = open("/tmp/sharkdb/ss_table_0_1_0", O_RDONLY | O_DIRECT, S_IRUSR);
	assert(fd >= 0);

    struct stat stat_buf;
    rc = fstat(fd, &stat_buf);
    assert(rc == 0);
    uint64_t n_blocks = stat_buf.st_size / BLOCK_BYTES;

	//	Note for each read that requires I/O, we need one progress_state tracker.
    void* buffers_base;
    rc = posix_memalign(&buffers_base, BLOCK_BYTES, BLOCK_BYTES);
    assert(rc == 0);

    struct io_uring ring;
    //  For now, just one syscall per io.
    rc = io_uring_queue_init(1, &ring, 0);
    assert(rc == 0);

    //  Just pass same buffer to avoid a free_list.
    struct iovec ivec = (struct iovec) {buffers_base, BLOCK_BYTES};
    rc = io_uring_register_buffers(&ring, &ivec, 1);
    assert(rc == 0);

    //  Submission, completion ptrs.
    //  For now, let's submit every i/o in a separate syscall.
    size_t sp = 0;
    size_t cp = 0;
    std::vector<uint64_t> times(N_OPS);

    while (cp < N_OPS) {
        //  Check if there is work left and space in window.
        if (sp < N_OPS && sp-cp < WINDOW_SIZE) {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            uint64_t offset = (rand() % n_blocks) * BLOCK_BYTES;
            io_uring_prep_read_fixed(sqe, fd, buffers_base, BLOCK_BYTES, offset, 0);
            io_uring_sqe_set_data64(sqe, sp);

            struct timespec ts_start;
            rc = clock_gettime(CLOCK_MONOTONIC, &ts_start);
            assert(rc == 0);
            times[sp] = get_micros(ts_start);

            int n_submitted = io_uring_submit(&ring);
            assert(n_submitted == 1);
            sp += 1;
        }

        struct io_uring_cqe* cqe;
        rc = io_uring_peek_cqe(&ring, &cqe);
        if (rc == 0) {
            assert(cqe->res == BLOCK_BYTES);
            size_t slot = io_uring_cqe_get_data64(cqe);

            struct timespec ts_end;
            rc = clock_gettime(CLOCK_MONOTONIC, &ts_end);
            assert(rc == 0);
            times[slot] = get_micros(ts_end) - times[slot];

            io_uring_cqe_seen(&ring, cqe);
            cp += 1;
        }
    }

    std::sort(times.begin(), times.end());
    printf("1%%: %lu, 10%%: %lu, 50%%: %lu, 90%%: %lu, 99%%: %lu, 99.9%%: %lu, 99.99%%: %lu, 99.999%%: %lu\n", times[N_OPS/100], times[N_OPS/10], times[N_OPS/2], times[9*N_OPS/10], times[99*N_OPS/100], times[999*N_OPS/1000], times[9999*N_OPS/10000], times[99999*N_OPS/100000]);

    return 0;
}
