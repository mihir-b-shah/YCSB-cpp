
#ifndef _KV_H_
#define _KV_H_

#include <stdint.h>

#define KEY_SIZE 24
#define VAL_SIZE 100

typedef struct sharkdb_key {
    uint8_t buf[KEY_SIZE];
} sharkdb_key_t;

typedef struct sharkdb_val {
    uint8_t buf[VAL_SIZE];
} sharkdb_val_t;

typedef struct sharkdb_pair {
    sharkdb_key_t k;
    sharkdb_val_t v;
} sharkdb_pair_t;

typedef struct sharkdb {
    void* impl;
} sharkdb_t;

// maybe add return codes later?
void sharkdb_init(sharkdb_t* db);
void sharkdb_read(sharkdb_t* db, const sharkdb_key_t* k, sharkdb_val_t* v_fill);
void sharkdb_write(sharkdb_t* db, const sharkdb_key_t* k, const sharkdb_val_t* v_fill);
void sharkdb_scan(sharkdb_t* db, const sharkdb_key_t* k_low, const sharkdb_key_t* k_high, sharkdb_pair_t** pairs_fill);
void sharkdb_free(sharkdb_t* db);

#endif
