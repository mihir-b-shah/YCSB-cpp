
#include <liburing.h>
#include <cstdio>
#include <cassert>

int main() {
	int fd = open("/tmp/sharkdb/ss_table_0_1_0", O_RDONLY, S_IRUSR);
	assert(fd >= 0);

    struct io_uring ring;
    int rc = io_uring_queue_init(16, &ring, 0);
    char buf[4096];

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    io_uring_prep_read(sqe, fd, &buf[0], 4096, 0);
    int n_submitted = io_uring_submit(&ring);
    assert(n_submitted == 1);

    struct io_uring_cqe* cqe;
    do {
        rc = io_uring_peek_cqe(&ring, &cqe);
    } while (rc != 0);

    printf("%d\n", cqe->res);
    assert(cqe->res == 4096);
    return 0;
}
