
#include <sharkdb.h>
#include "db_impl.h"

#include <cstdlib>

#include <liburing.h>

sharkdb_p sharkdb_init() {
    return new db_t();
}

void sharkdb_read(sharkdb_p db, const char* k, char* v_fill) {
    db_t* p_db = (db_t*) db;
}

void sharkdb_update(sharkdb_p db, const char* k, const char* v) {
}

void sharkdb_insert(sharkdb_p db, const char* k, const char* v) {
}

void sharkdb_delete(sharkdb_p db, const char* k) {
}

void sharkdb_scan(sharkdb_p db, const char* k, size_t len, char* buf) {
}

void sharkdb_free(sharkdb_p db) {
    delete ((db_t*) db);
}
