
#include "sharkdb.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

int main() {
    sharkdb_t* p_db = sharkdb_init();

	const char* k = "user01234567890123456789";
	void* v = malloc(SHARKDB_VAL_BYTES);
	char* vc = (char*) v;
	for (size_t i = 0; i<SHARKDB_VAL_BYTES; ++i) {
		vc[i] = 'A';
	}
	char buf[SHARKDB_VAL_BYTES];
	sharkdb_write_async(p_db, k, vc);

    sharkdb_read_async(p_db, k, &buf[0]);

	for (size_t i = 0; i<SHARKDB_VAL_BYTES; ++i) {
		assert(vc[i] == buf[i]);
	}

    sharkdb_free(p_db);
	free(v);

    printf("Done!\n");
    return 0;
}
