
#include <liburing.h>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <sys/stat.h>

static uint64_t get_micros(struct timespec ts) {
    return ((uint64_t) ts.tv_sec) * 1000000 + (ts.tv_nsec / 1000);
}

static constexpr size_t BLOCK_BYTES = 4096;
static constexpr size_t N_OPS = 100000;

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
    rc = io_uring_queue_init(16, &ring, IORING_SETUP_IOPOLL);
    assert(rc == 0);

    struct iovec ivec = (struct iovec) {buffers_base, BLOCK_BYTES};
    rc = io_uring_register_buffers(&ring, &ivec, 1);
    assert(rc == 0);

    std::vector<uint64_t> times(N_OPS);
    for (size_t i = 0; i<N_OPS; ++i) {
        struct timespec ts_start;
        rc = clock_gettime(CLOCK_MONOTONIC, &ts_start);
        assert(rc == 0);

        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        uint64_t offset = (rand() % n_blocks) * BLOCK_BYTES;

        io_uring_prep_read_fixed(sqe, fd, buffers_base, BLOCK_BYTES, offset, 0);
        int n_submitted = io_uring_submit(&ring);
        assert(n_submitted == 1);

        struct io_uring_cqe* cqe;
        /*
        // TODO try out io_uring_wait_cqe_nr(..., 0) to use wait api but not block.

        do {
            rc = io_uring_peek_cqe(&ring, &cqe);
        } while (rc != 0);
        */
        rc = io_uring_wait_cqe(&ring, &cqe);
        printf("res: %d\n", cqe->res);
        assert(cqe->res == BLOCK_BYTES);
        io_uring_cqe_seen(&ring, cqe);

        struct timespec ts_end;
        rc = clock_gettime(CLOCK_MONOTONIC, &ts_end);
        assert(rc == 0);

        times[i] = get_micros(ts_end) - get_micros(ts_start);
    }

    std::sort(times.begin(), times.end());
    printf("1%%: %lu, 10%%: %lu, 50%%: %lu, 90%%: %lu, 99%%: %lu, 99.9%%: %lu, 99.99%%: %lu, 99.999%%: %lu\n", times[N_OPS/100], times[N_OPS/10], times[N_OPS/2], times[9*N_OPS/10], times[99*N_OPS/100], times[999*N_OPS/1000], times[9999*N_OPS/10000], times[99999*N_OPS/100000]);

    return 0;
}
