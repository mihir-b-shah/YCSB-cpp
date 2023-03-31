
#include "consts.h"
#include "wal.h"

#include <cassert>
#include <cstdlib>

#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>

wal_t::wal_t() {
	//	TODO fallocate here?
    fd_ = open("/tmp/sharkdb/wal", O_CREAT | O_APPEND | O_DIRECT | O_SYNC, S_IRUSR | S_IWUSR);
    assert(fd_ >= 0);
	zero_padding_ = (char*) calloc(WAL_BLOCK_PAD_BYTES, 1);
}

wal_t::~wal_t() {
}

void log_write(wal_t* wal, wal_block_winfo_t* to_write) {
	struct iovec iovs[2*N_ENTRIES_WAL_BLOCK+1];
	for (size_t i = 0; i<N_ENTRIES_WAL_BLOCK; ++i) {
		iovs[2*i] = {&to_write->keys[i], SHARKDB_KEY_BYTES};
		iovs[2*i+1] = {&to_write->vals[i], SHARKDB_VAL_BYTES};
	}
	iovs[2*N_ENTRIES_WAL_BLOCK] = {wal->zero_padding_, WAL_BLOCK_PAD_BYTES};
	assert(writev(wal->fd_, &iovs[0], 2*N_ENTRIES_WAL_BLOCK+1) == BLOCK_BYTES);
}
