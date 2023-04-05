
#include <unistd.h>
#include <fcntl.h>
#include <cassert>
#include <liburing.h>

char buf[4096];

static constexpr size_t N_SQ_ENTRIES = 128;

int main() {
	int fd = open("wal", O_CREAT | O_APPEND | O_WRONLY | O_SYNC, S_IWUSR);
	assert(fd >= 0);
	assert(unlink("wal") == 0);

    struct io_uring ring;
    assert(io_uring_queue_init(N_SQ_ENTRIES, &ring, 0) == 0);

    

	for (size_t i = 0; i<1000; ++i) {
		assert(write(fd, &buf[0], 4096) == 4096);
	}

    io_uring_queue_exit(&ring);
	return 0;
}
