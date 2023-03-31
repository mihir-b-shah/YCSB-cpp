
#ifndef _WAL_H_
#define _WAL_H_

#include <sharkdb.h>
#include "consts.h"

static constexpr size_t N_ENTRIES_WAL_BLOCK = 1;
struct wal_block_winfo_t {
	const char* keys[N_ENTRIES_WAL_BLOCK];
	const char* vals[N_ENTRIES_WAL_BLOCK];
};
static constexpr size_t WAL_BLOCK_PAD_BYTES = 
	BLOCK_BYTES - N_ENTRIES_WAL_BLOCK * (SHARKDB_KEY_BYTES + SHARKDB_VAL_BYTES);

struct wal_t {
    int fd_;
	char* zero_padding_;

    wal_t();
    ~wal_t();
};

void log_write(wal_t* wal, wal_block_winfo_t* to_write);

#endif
