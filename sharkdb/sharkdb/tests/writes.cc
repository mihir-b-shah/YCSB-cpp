
#include "sharkdb.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>

static constexpr size_t N_WRITES = 10000;

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
	for (size_t i = 0; i<N_WRITES; ++i) {
		void* v = malloc(1000);
		assert(v != nullptr);
		char* vc = (char*) v;
		for (size_t j = 0; j<1000; ++j) {
			vc[j] = 'A';
		}
		vs_v.push_back((const char*) vc);
	}

    size_t cq_received = 0;
    for (size_t T = 0; T<100; ++T) {
        for (size_t i = 0; i<N_WRITES; ++i) {
            sharkdb_write_async(p_db, ks_strs[i].c_str(), vs_v[i]);

            std::pair<bool, sharkdb_cqev> ev;
            while (true) {
                ev = sharkdb_cpoll_cq(p_db);
                assert(ev.first);
                if (ev.second != SHARKDB_CQEV_FAIL) {
                    cq_received += 1;
                } else {
                    break;
                }
            }
        }
    }
    size_t n_drained = sharkdb_drain(p_db);
    printf("n_drained: %lu\n", n_drained);

    sharkdb_free(p_db);
	for (const char* v : vs_v) {
		free((void*) v);
	}

    printf("Done!\n\n");
    return 0;
}
