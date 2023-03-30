
#include "sharkdb.h"

#include <cstdio>
#include <cstdlib>

int main() {
    sharkdb_p p_db = sharkdb_init();
    std::vector<const char*> ks = {"012345678901234567890123"};
    std::vector<const char*> vs = {(const char*) malloc(1000)};

    sharkdb_multiwrite(p_db, ks, vs);
    sharkdb_free(p_db);
    printf("Done!\n");
    return 0;
}
