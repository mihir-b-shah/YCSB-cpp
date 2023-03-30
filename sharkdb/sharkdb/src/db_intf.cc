
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>

#include <liburing.h>
#include <tbb/concurrent_map.h>

sharkdb_p sharkdb_init() {
    return new db_t();
}

void sharkdb_multiread(sharkdb_p db, std::vector<const char*>& ks, std::vector<char*>& fill_vs) {
    db_t* p_db = (db_t*) db;
}

//  XXX if we use std::strings, we can do std::string v2 = std::move(v);
void sharkdb_multiwrite(sharkdb_p db, std::vector<const char*>& ks, std::vector<const char*>& vs) {
}

void sharkdb_free(sharkdb_p db) {
    delete ((db_t*) db);
}
