
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
    sharkdb_p p_db = sharkdb_init();

	std::vector<std::string> ks_strs;
	for (size_t i = 0; i<N_WRITES; ++i) {
		ks_strs.push_back("user" + std::to_string(rand64()));
	}

	std::vector<std::vector<const char*>> ks_v;
	std::vector<std::vector<const char*>> vs_v;
	
	for (size_t e = 0; e<N_WRITES/10; ++e) {
		ks_v.emplace_back();
		vs_v.emplace_back();

		for (size_t i = 0; i<10; ++i) {
			ks_v.back().push_back(ks_strs[e*10+i].c_str());

			void* v = malloc(1000);
			char* vc = (char*) v;
			for (size_t j = 0; j<1000; ++j) {
				vc[j] = 'A';
			}
			vs_v.back().push_back(vc);
		}
	}

	printf("Before multiwrites.\n");
	for (size_t i = 0; i<N_WRITES/10; ++i) {
		if (i % 100 == 0) {
			printf("Writing %lu\n", i*10);
		}
		sharkdb_multiwrite(p_db, ks_v[i], vs_v[i]);
	}
	printf("After multiwrite.\n");

    sharkdb_free(p_db);
	for (const std::vector<const char*>& vs : vs_v) {
		for (const char* v : vs) {
			free((void*) v);
		}
	}

    printf("Done!\n");
    return 0;
}
