
#ifndef _WAL_H_
#define _WAL_H_

#include <sharkdb.h>
#include "consts.h"

struct wal_block_winfo_t {
	const char* keys[N_ENTRIES_PER_BLOCK];
	const char* vals[N_ENTRIES_PER_BLOCK];
};
static constexpr size_t WAL_BLOCK_PAD_BYTES = 
	BLOCK_BYTES - N_ENTRIES_PER_BLOCK * (SHARKDB_KEY_BYTES + SHARKDB_VAL_BYTES);

struct wal_t {
	int fd_array_idx_;
    int fd_;
	char* zero_padding_;

    wal_t();
    ~wal_t();
};

void log_write(wal_t* wal, wal_block_winfo_t* to_write);

#endif
