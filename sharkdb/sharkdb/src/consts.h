
#ifndef _CONSTS_H_
#define _CONSTS_H_

#include "sharkdb.h"

#include <cstddef>
#include <cstring>

static constexpr size_t N_ENTRIES_PER_BLOCK = 1;
static constexpr size_t MEM_TABLE_FULL_THR = 90;
static constexpr size_t N_DISK_LEVELS = 2;
static constexpr size_t BLOCK_BYTES = 4096;
static constexpr size_t SECTOR_BYTES = 512;
static constexpr size_t FILTER_BITS_PER_BLOCK = 8;
static constexpr size_t BLOCKS_PER_FENCE = 1;
static constexpr size_t N_PARTITIONS = 2;
static constexpr size_t MEM_TABLE_MAX_ENTRIES = 500; // per partition
static constexpr size_t LOG_BUF_MAX_ENTRIES = 5000; // per partition- TODO change
static constexpr size_t SIZE_RATIO = 8;
static constexpr size_t GROWTH_POWERS[] = {1, 8, 64};
static constexpr const char* KEY_PREFIX = "user";
static constexpr const char* ORDER_PREFIXES[N_PARTITIONS] = {"49", "99"};

static_assert(GROWTH_POWERS[1] == SIZE_RATIO);
static_assert(1+N_DISK_LEVELS == sizeof(GROWTH_POWERS)/sizeof(GROWTH_POWERS[0]));

struct cmp_keys_lt {
    bool operator()(const char* k1, const char* k2) const {
        return memcmp(k1, k2, SHARKDB_KEY_BYTES) < 0;
    }
};

static constexpr size_t calc_n_blocks(size_t level) {
    return MEM_TABLE_MAX_ENTRIES * GROWTH_POWERS[level] / N_ENTRIES_PER_BLOCK;
}

static constexpr size_t calc_n_filter_bits(size_t level) {
    return calc_n_blocks(level) * FILTER_BITS_PER_BLOCK;
}

struct __attribute__((packed)) kv_pair_t {
	char key[SHARKDB_KEY_BYTES];
	char val[SHARKDB_VAL_BYTES];
};

#endif
