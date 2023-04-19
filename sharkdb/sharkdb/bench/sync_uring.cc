
#include <unistd.h>
#include <fcntl.h>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <liburing.h>

static constexpr size_t N_SQ_ENTRIES = 128;
static constexpr size_t LOG_SIZE = 16000;

int main() {
    int rc;

	int fd = open("/tmp/wal", O_CREAT | O_WRONLY | O_DIRECT | O_SYNC, S_IRUSR | S_IWUSR);
	assert(fd >= 0);

    struct io_uring ring;
    rc = io_uring_queue_init(N_SQ_ENTRIES, &ring, 0);
    assert(rc == 0);

    void* buf_p;
    rc = posix_memalign(&buf_p, 4096, LOG_SIZE * 4096);
    assert(rc == 0);

    size_t n_syncs = 0;
    while (n_syncs < 1) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);

        io_uring_prep_write(sqe, fd, buf_p, 4096 * LOG_SIZE, 0);
        int n_submitted = io_uring_submit(&ring);
        assert(n_submitted == 1);

        struct io_uring_cqe* cqe;
        int rc;
        while ((rc = io_uring_peek_cqe(&ring, &cqe)) != 0) {
            assert(rc == -EAGAIN);
            __builtin_ia32_pause();
        }

        assert(cqe->res == (int) LOG_SIZE*4096);
        io_uring_cqe_seen(&ring, cqe);
        n_syncs += 1;
    }

    io_uring_queue_exit(&ring);
	return 0;
}
