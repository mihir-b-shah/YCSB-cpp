
#include <liburing.h>
#include <vector>
#include <utility>
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
static constexpr double TGT_LATENCY = 250; //microseconds

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
    std::vector<std::pair<uint64_t, uint64_t>> times(N_OPS);

    struct timespec ts_begin;
    rc = clock_gettime(CLOCK_MONOTONIC, &ts_begin);
    assert(rc == 0);

    std::vector<double> moving_avg;
    constexpr size_t AVG_WINDOW = 4;
    moving_avg.reserve(N_OPS+AVG_WINDOW);

    std::vector<uint64_t> times2;
    times2.reserve(1+N_OPS);

    size_t WINDOW_SIZE = 1;
    size_t ctr = 0;

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
            times[sp].first = offset;
            times[sp].second = get_micros(ts_start);

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
            times[slot].second = get_micros(ts_end) - times[slot].second;
            times2.push_back(times[slot].second);

            if (times2.size() > AVG_WINDOW) {
                ssize_t diff = ((ssize_t) times2[times2.size()-1]) - ((ssize_t) times2[times2.size()-AVG_WINDOW-1]);
                moving_avg.push_back((moving_avg.back()*AVG_WINDOW + diff) / AVG_WINDOW);
            } else if (times2.size() == 1) {
                moving_avg.push_back(times2.back());
            } else {
                moving_avg.push_back(((double) (moving_avg.back()*moving_avg.size() + times2.back())) / (moving_avg.size()+1));
            }

            if (++ctr % WINDOW_SIZE == 0) {
                if (moving_avg.back() <= TGT_LATENCY) {
                    WINDOW_SIZE *= 2;
                } else {
                    if (WINDOW_SIZE > 1) {
                        WINDOW_SIZE /= 2;
                    }
                }
                ctr = 0;
            }

            io_uring_cqe_seen(&ring, cqe);
            cp += 1;
        }
    }

    struct timespec ts_final;
    rc = clock_gettime(CLOCK_MONOTONIC, &ts_final);
    assert(rc == 0);

    printf("total time: %lu\n", get_micros(ts_final) - get_micros(ts_begin));
    
    /*
    for (double avg : moving_avg) {
        fprintf(stderr, "avg: %f\n", avg);
    }
    */

    std::sort(times2.begin(), times2.end());
    printf("1%%: %lu, 10%%: %lu, 50%%: %lu, 90%%: %lu, 99%%: %lu, 99.9%%: %lu, 99.99%%: %lu, 99.999%%: %lu\n", times2[N_OPS/100], times2[N_OPS/10], times2[N_OPS/2], times2[9*N_OPS/10], times2[99*N_OPS/100], times2[999*N_OPS/1000], times2[9999*N_OPS/10000], times2[99999*N_OPS/100000]);

    return 0;
}
