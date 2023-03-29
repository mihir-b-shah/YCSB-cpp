
#include "sharkdb.h"

#include <cstdio>
#include <cstdlib>

int main() {
    sharkdb_p p_db = sharkdb_init();
    const char* k = "012345678901234567890123";
    const char* v = (const char*) malloc(1000);

    sharkdb_insert(p_db, k, v);
    sharkdb_free(p_db);
    printf("Done!\n");
    return 0;
}
