
#include "sharkdb.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>

static uint64_t rand64() {
	return (((uint64_t) rand()) << 32) | rand();
}

static constexpr size_t N_WRITES = 10000;

int main() {
    sharkdb_t* p_db = sharkdb_init();

	std::vector<std::string> ks_strs;
	for (size_t i = 0; i<N_WRITES; ++i) {
		ks_strs.push_back("user" + std::to_string(rand64()));
	}

	std::vector<const char*> vs_v;
	for (size_t i = 0; i<N_WRITES; ++i) {
		void* v = malloc(1000);
		assert(v != nullptr);
		char* vc = (char*) v;
		for (size_t j = 0; j<1000; ++j) {
			vc[j] = 'A';
		}
		vs_v.push_back((const char*) vc);
	}

	for (size_t i = 0; i<N_WRITES; ++i) {
		if (i % 1000 == 0) {
			printf("Writing %lu\n", i);
		}
		sharkdb_write_async(p_db, ks_strs[i].c_str(), vs_v[i]);
	}
	printf("After multiwrite.\n");

    sharkdb_free(p_db);
	for (const char* v : vs_v) {
		free((void*) v);
	}

    printf("Done!\n");
    return 0;
}
