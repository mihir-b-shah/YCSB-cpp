
#ifndef _CONSTS_H_
#define _CONSTS_H_

#include "sharkdb.h"

#include <cstdio>
#include <cstddef>
#include <cstring>
#include <cassert>

static constexpr size_t N_ENTRIES_PER_BLOCK = 4;
static constexpr size_t N_DISK_LEVELS = 2;
static constexpr size_t BLOCK_BYTES = 4096;
static constexpr size_t FILTER_BITS_PER_BLOCK = 8;
static constexpr size_t BLOCKS_PER_FENCE = 1;
static constexpr size_t N_USER_THREADS = 9;
static constexpr size_t READ_SQ_DEPTH = 1;
static constexpr size_t PERC_LOCKED_MEM_USE = 80;
static constexpr size_t N_IN_FLIGHT = 64;
static constexpr size_t N_PARTITIONS = 1;
static constexpr size_t LOG_BUF_SYNC_INTV = 768;
static constexpr size_t LOG_FULL_THR = 90;
static constexpr size_t LOG_BUF_MAX_ENTRIES = 80000;
static constexpr size_t MEM_TABLE_FLUSH_INTV = 1000;
static constexpr size_t SIZE_RATIO = 8;
static constexpr size_t GROWTH_POWERS[] = {1, 8, 64};
static constexpr const char* KEY_PREFIX = "user";
static constexpr const char* ORDER_PREFIXES[N_PARTITIONS] = {
//    "49999999999999999999",
    "99999999999999999999",
};

//	Assume k is in the form 'user[0-9]+'
static inline size_t get_partition(const char* k) {
	assert(memcmp(k, KEY_PREFIX, strlen(KEY_PREFIX)) == 0);
	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		if (memcmp(k+strlen(KEY_PREFIX), ORDER_PREFIXES[i], strlen(ORDER_PREFIXES[0])) <= 0) {
            return i;
		}
	}
	assert(false && "Should have matched a memtable.");
    return 0;
}

static_assert(GROWTH_POWERS[1] == SIZE_RATIO);
static_assert(1+N_DISK_LEVELS == sizeof(GROWTH_POWERS)/sizeof(GROWTH_POWERS[0]));

struct cmp_keys_lt {
    bool operator()(const char* k1, const char* k2) const {
        return memcmp(k1, k2, SHARKDB_KEY_BYTES) < 0;
    }
};

static constexpr size_t calc_n_filter_bits(size_t level) {
    return (LOG_BUF_MAX_ENTRIES * GROWTH_POWERS[level] / N_ENTRIES_PER_BLOCK) * FILTER_BITS_PER_BLOCK;
}

struct __attribute__((packed)) kv_pair_t {
	char key_[SHARKDB_KEY_BYTES];
	char val_[SHARKDB_VAL_BYTES];
};

static inline void print_key(void* p, char* s) {
	char buf[1+SHARKDB_KEY_BYTES];
	memcpy(&buf[0], s, SHARKDB_KEY_BYTES);
	buf[SHARKDB_KEY_BYTES] = '\0';
	fprintf(stderr, "p: %p, key: %s\n", p, (const char*) &buf[0]);
}

#endif
