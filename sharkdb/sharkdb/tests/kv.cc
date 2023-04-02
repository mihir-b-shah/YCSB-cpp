
#include "sharkdb.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

int main() {
    sharkdb_p p_db = sharkdb_init();
    std::vector<const char*> ks = {"user01234567890123456789"};
	void* v = malloc(1000);
	char* vc = (char*) v;
	for (size_t i = 0; i<1000; ++i) {
		vc[i] = 'A';
	}

    std::vector<const char*> vs = {(const char*) v};
    std::vector<const char*> vs_rd = {nullptr};

	sharkdb_multiwrite(p_db, ks, vs);
    sharkdb_multiread(p_db, ks, vs_rd);

	for (size_t i = 0; i<1000; ++i) {
		assert(vs[0][i] == vs_rd[0][i]);
	}

    sharkdb_free(p_db);
	free(v);

    printf("Done!\n");
    return 0;
}
