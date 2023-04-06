
#include "sharkdb.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

int main() {
    sharkdb_t* p_db = sharkdb_init();

	const char* k = "user01234567890123456789";
	void* v = malloc(1000);
	char* vc = (char*) v;
	for (size_t i = 0; i<1000; ++i) {
		vc[i] = 'A';
	}
	char buf[1000];
	sharkdb_write_async(p_db, k, vc);

    sharkdb_read_async(p_db, k, &buf[0]);

	for (size_t i = 0; i<1000; ++i) {
		assert(vc[i] == buf[i]);
	}

    sharkdb_free(p_db);
	free(v);

    printf("Done!\n");
    return 0;
}
