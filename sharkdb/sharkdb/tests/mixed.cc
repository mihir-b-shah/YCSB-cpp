
#include "sharkdb.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>

static constexpr size_t N_OPS = 1000000;

int main() {
    sharkdb_t* p_db = sharkdb_init();

	std::vector<std::string> ks_strs;
	for (size_t i = 0; i<2*N_OPS; ++i) {
        std::string k = "user";
        for (size_t j = 0; j<20; ++j) {
            k.push_back('0' + (rand() % 10));
        }
		ks_strs.push_back(k);
	}

	std::vector<const char*> vs_v;
	for (size_t i = 0; i<N_OPS/100; ++i) {
		void* v = malloc(SHARKDB_VAL_BYTES);
		memset(v, 'A', SHARKDB_VAL_BYTES);
		assert(v != nullptr);
		char* vc = (char*) v;
		vs_v.push_back((const char*) vc);
	}
    fprintf(stderr, "setup done.\n");

    for (size_t i = 0; i<N_OPS; ++i) {
        sharkdb_write_async(p_db, ks_strs[i].c_str(), vs_v[i % (N_OPS/100)]);
    }
    fprintf(stderr, "did writes.\n");

    size_t cq_received = 0;
    while (cq_received < N_OPS-20000) {
        std::pair<bool, sharkdb_cqev> ev = sharkdb_cpoll_cq(p_db);
        assert(ev.first);
        if (ev.second != SHARKDB_CQEV_FAIL) {
            cq_received += 1;
        }
    }
	sharkdb_drain(p_db);
    // sharkdb_nowrites(p_db);

    for (size_t i = 0; i<N_OPS; ++i) {
        sharkdb_write_async(p_db, ks_strs[i+N_OPS].c_str(), vs_v[i % (N_OPS/100)]);
        sharkdb_read_async(p_db, ks_strs[i].c_str(), (char*) vs_v[i % (N_OPS/100)]);

        while (true) {
            std::pair<bool, sharkdb_cqev> ev = sharkdb_cpoll_cq(p_db);
            if (ev.second == SHARKDB_CQEV_FAIL) {
                break;
            }
        }
    } 
    fprintf(stderr, "raar!\n");

    sharkdb_free(p_db);
	for (const char* v : vs_v) {
		free((void*) v);
	}

    printf("Done!\n\n");
    return 0;
}
