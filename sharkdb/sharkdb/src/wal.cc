
#include "consts.h"
#include "wal.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>

static int wal_fds[N_PARTITIONS * 2];

static void close_wals() {
	// just a formality, process end should close them anyway.
	for (size_t i = 0; i<2*N_PARTITIONS; ++i) {
		// TODO add this assert back- just doesn't matter much, so leaving it this way.
		// assert(wal_fds[i] >= 0);
		if (wal_fds[i] >= 0) {
			assert(close(wal_fds[i]) == 0);
		}
	}
}

__attribute__((constructor))
static void open_wals() {
	atexit(close_wals);
	for (size_t i = 0; i<2*N_PARTITIONS; ++i) {
		char path[40];
		sprintf(&path[0], "/tmp/sharkdb/wal_%lu", i);
		//	TODO fallocate here?
		//	TODO O_DIRECT here?
		wal_fds[i] = open((const char*) path, O_CREAT | O_APPEND | O_WRONLY, S_IRUSR | S_IWUSR);
		assert(wal_fds[i] >= 0);
	}
}

wal_t::wal_t() {
	size_t i;
	for (i = 0; i<2*N_PARTITIONS; ++i) {
		if (wal_fds[i] >= 0) {
			break;
		}
	}
	fd_ = wal_fds[i];
	fd_array_idx_ = i;
	assert(lseek(fd_, 0, SEEK_SET) == 0);
	wal_fds[i] = -1;
	zero_padding_ = (char*) calloc(WAL_BLOCK_PAD_BYTES, 1);
}

wal_t::~wal_t() {
	free(zero_padding_);
	printf("Called destructor.\n");
	wal_fds[fd_array_idx_] = fd_;
}

void log_write(wal_t* wal, wal_block_winfo_t* to_write) {
	struct iovec iovs[2*N_ENTRIES_PER_BLOCK+1];
	for (size_t i = 0; i<N_ENTRIES_PER_BLOCK; ++i) {
		iovs[2*i] = (struct iovec) {(void*) to_write->keys[i], SHARKDB_KEY_BYTES};
		iovs[2*i+1] = (struct iovec) {(void*) to_write->vals[i], SHARKDB_VAL_BYTES};
	}
	iovs[2*N_ENTRIES_PER_BLOCK] = (struct iovec) {wal->zero_padding_, WAL_BLOCK_PAD_BYTES};
	assert(writev(wal->fd_, &iovs[0], 2*N_ENTRIES_PER_BLOCK+1) == BLOCK_BYTES);
}

void log_commit(wal_t* wal) {
    assert(fsync(wal->fd_) == 0);
}
