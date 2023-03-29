
#include "sharkdb.h"

#include <stdio.h>

int main() {
    sharkdb_t db;
    sharkdb_init(&db);

    sharkdb_key_t k1;
    sharkdb_val_t v1;
    sharkdb_write(&db, &k1, &v1);

    sharkdb_free(&db);
    printf("Done!\n");
    return 0;
}
