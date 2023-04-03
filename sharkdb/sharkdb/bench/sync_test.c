
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>

char buf[4096];

int main() {
	int fd = open("wal", O_CREAT | O_APPEND | O_WRONLY | O_SYNC, S_IWUSR);
	assert(fd >= 0);
	assert(unlink("wal") == 0);

	for (size_t i = 0; i<1000; ++i) {
		assert(write(fd, &buf[0], 4096) == 4096);
	}
	return 0;
}
