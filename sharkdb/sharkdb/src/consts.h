
#ifndef _CONSTS_H_
#define _CONSTS_H_

#include <cstddef>

static constexpr size_t N_ENTRIES_PER_BLOCK = 1;
static constexpr size_t MEM_TABLE_FULL_THR = 90;
static constexpr size_t N_DISK_LEVELS = 2;
static constexpr size_t BLOCK_BYTES = 4096;
static constexpr size_t FILTER_BITS_PER_BLOCK = 8;
static constexpr size_t BLOCKS_PER_FENCE = 1;
static constexpr size_t N_PARTITIONS = 4;
static constexpr size_t MEM_TABLE_MAX_ENTRIES = 30; // per partition
static constexpr size_t SIZE_RATIO = 8;
static constexpr size_t GROWTH_POWERS[] = {1, 8, 64};
static constexpr const char* KEY_PREFIX = "user";
static constexpr const char* ORDER_PREFIXES[N_PARTITIONS] = {"24", "49", "74", "99"};

static_assert(GROWTH_POWERS[1] == SIZE_RATIO);
static_assert(1+N_DISK_LEVELS == sizeof(GROWTH_POWERS)/sizeof(GROWTH_POWERS[0]));

#endif
