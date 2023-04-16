
#include "sharkdb.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>

static constexpr size_t N_WRITES = 1000000;

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
		void* v = malloc(SHARKDB_VAL_BYTES);
		memset(v, 'A', SHARKDB_VAL_BYTES);
		assert(v != nullptr);
		char* vc = (char*) v;
		vs_v.push_back((const char*) vc);
	}

    for (size_t i = 0; i<N_WRITES; ++i) {
        sharkdb_write_async(p_db, ks_strs[i].c_str(), vs_v[i % (N_WRITES/100)]);
    } 

    size_t cq_received = 0;
    while (true) {
        std::pair<bool, sharkdb_cqev> ev = sharkdb_cpoll_cq(p_db);
        assert(ev.first);
        if (ev.second == SHARKDB_CQEV_FAIL) {
            break;
        }
        cq_received += 1;
    }

	printf("After multiwrite, cq_received=%lu.\n", cq_received);
	sharkdb_drain(p_db);
	sharkdb_nowrites(p_db);

    for (size_t i = 0; i<N_WRITES; ++i) {
        sharkdb_read_async(p_db, ks_strs[i].c_str(), (char*) vs_v[i % (N_WRITES/100)]);
    } 
	printf("After reads.\n");

    cq_received = 0;
    while (true) {
        std::pair<bool, sharkdb_cqev> ev = sharkdb_cpoll_cq(p_db);
        assert(ev.first);
        if (ev.second == SHARKDB_CQEV_FAIL) {
            break;
        }
        cq_received += 1;
    }

    sharkdb_free(p_db);
	for (const char* v : vs_v) {
		free((void*) v);
	}

    printf("Done! cq_received: %lu\n", cq_received);
    return 0;
}
