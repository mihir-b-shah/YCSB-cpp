
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>

#include <liburing.h>
#include <tbb/concurrent_map.h>

level_t::level_t(size_t level) : level_(level), filter_(calc_n_filter_bits(level)) {
}

level_t::~level_t() {
}

db_t::db_t() : mem_table_(cmp_keys_lt()) {
    for (size_t i = 0; i<N_DISK_LEVELS; ++i) {
        disk_levels_.emplace_back(i);
    }
}

db_t::~db_t() {
}
