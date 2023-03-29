
#include "sharkdb.h"
#include "lsm.h"

#include <stdlib.h>

void sharkdb_init(sharkdb_t* db) {
    db->impl = malloc(sizeof(struct lsm_t));
}

void sharkdb_read(sharkdb_t* db, const sharkdb_key_t* k, sharkdb_val_t* v_fill) {
}

void sharkdb_write(sharkdb_t* db, const sharkdb_key_t* k, const sharkdb_val_t* v_fill) {
}

void sharkdb_scan(sharkdb_t* db, const sharkdb_key_t* k_low, const sharkdb_key_t* k_high, sharkdb_pair_t** pairs_fill) {
}

void sharkdb_free(sharkdb_t* db) {
    free(db->impl);
}
