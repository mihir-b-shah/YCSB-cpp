
#ifndef _KV_H_
#define _KV_H_

#include <cstddef>

#define SHARKDB_KEY_SIZE 24
#define SHARKDB_VAL_SIZE 1000

typedef void* sharkdb_p;

// maybe add return codes later?
sharkdb_p sharkdb_init();
void sharkdb_read(sharkdb_p db, const char* k, char* v_fill);
void sharkdb_update(sharkdb_p db, const char* k, const char* v);
void sharkdb_insert(sharkdb_p db, const char* k, const char* v);
void sharkdb_delete(sharkdb_p db, const char* k);
void sharkdb_scan(sharkdb_p db, const char* k, size_t len, char* buf);
void sharkdb_free(sharkdb_p db);

#endif
