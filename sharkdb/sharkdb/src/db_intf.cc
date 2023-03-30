
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>

#include <liburing.h>
#include <tbb/concurrent_map.h>

sharkdb_p sharkdb_init() {
    return new db_t();
}

void sharkdb_read(sharkdb_p db, const std::string& k, std::string& v_fill) {
    db_t* p_db = (db_t*) db;
}

void sharkdb_update(sharkdb_p db, const std::string& k, std::string&& v) {
    std::string v2 = std::move(v);
}

void sharkdb_insert(sharkdb_p db, const std::string& k, std::string&& v) {
}

void sharkdb_delete(sharkdb_p db, const std::string& k) {
}

void sharkdb_free(sharkdb_p db) {
    delete ((db_t*) db);
}
