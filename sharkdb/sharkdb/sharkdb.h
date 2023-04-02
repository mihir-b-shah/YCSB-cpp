
#ifndef _KV_H_
#define _KV_H_

#include <cstddef>
#include <vector>

#define SHARKDB_KEY_BYTES 24
#define SHARKDB_VAL_BYTES 1000

typedef void* sharkdb_p;

// maybe add return codes later?
sharkdb_p sharkdb_init();
void sharkdb_multiread(sharkdb_p db, std::vector<const char*>& ks, std::vector<char*>& fill_vs);
void sharkdb_multiwrite(sharkdb_p db, std::vector<const char*>& ks, std::vector<const char*>& vs);
// TODO add scan!
void sharkdb_free(sharkdb_p db);

#endif
