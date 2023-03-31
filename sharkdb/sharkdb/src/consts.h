
#ifndef _CONSTS_H_
#define _CONSTS_H_

#include <cstddef>

static constexpr size_t MEM_TABLE_BYTES = 1000000;
static constexpr size_t N_DISK_LEVELS = 2;
static constexpr size_t BLOCK_BYTES = 4096;
static constexpr size_t FILTER_BITS_PER_BLOCK = 8;
static constexpr size_t BLOCKS_PER_FENCE = 1;

static constexpr size_t GROWTH_POWERS[] = {1, 8, 64};
static_assert(1+N_DISK_LEVELS == sizeof(GROWTH_POWERS)/sizeof(GROWTH_POWERS[0]));

#endif
