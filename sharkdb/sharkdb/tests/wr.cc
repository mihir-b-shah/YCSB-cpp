
#include "sharkdb.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>

static constexpr size_t N_WRITES = 1000000;
static constexpr size_t N_MAX_LOG_PENDING = 20000;

int main() {
    sharkdb_t* p_db = sharkdb_init();

	std::vector<std::string> ks_strs;
	for (size_t i = 0; i<N_WRITES; ++i) {
        std::string k = "user";
        for (size_t j = 0; j<20; ++j) {
            k.push_back('0' + (rand() % 10));
        }
		ks_strs.push_back(k);
	}

	std::vector<const char*> vs_v;
	for (size_t i = 0; i<N_WRITES/100; ++i) {
		void* v = malloc(1000);
		assert(v != nullptr);
		char* vc = (char*) v;
		vs_v.push_back((const char*) vc);
	}

    for (size_t i = 0; i<N_WRITES; ++i) {
        sharkdb_write_async(p_db, ks_strs[i].c_str(), vs_v[i % (N_WRITES/100)]);
    } 

    size_t cq_received = 0;
    while (cq_received < N_WRITES-N_MAX_LOG_PENDING) {
        sharkdb_cqev ev = sharkdb_cpoll_cq(p_db);
        if (ev != SHARKDB_CQEV_FAIL) {
            cq_received += 1;
        }
    }

	printf("After multiwrite.\n");

    for (size_t i = 0; i<N_WRITES; ++i) {
        sharkdb_read_async(p_db, ks_strs[i].c_str(), (char*) vs_v[i % (N_WRITES/100)]);
    } 

    cq_received = 0;
    while (cq_received < N_WRITES-N_MAX_LOG_PENDING) {
        sharkdb_cqev ev = sharkdb_cpoll_cq(p_db);
        if (ev != SHARKDB_CQEV_FAIL) {
            cq_received += 1;
        }
    }

    sharkdb_free(p_db);
	for (const char* v : vs_v) {
		free((void*) v);
	}

    printf("Done!\n");
    return 0;
}
