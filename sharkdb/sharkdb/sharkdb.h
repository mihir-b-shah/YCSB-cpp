
#ifndef _KV_H_
#define _KV_H_

#include <cstddef>
#include <string>

#define SHARKDB_KEY_SIZE 24
#define SHARKDB_VAL_SIZE 1000

typedef void* sharkdb_p;

// maybe add return codes later?
sharkdb_p sharkdb_init();
void sharkdb_read(sharkdb_p db, const std::string& k, std::string& v_fill);
void sharkdb_update(sharkdb_p db, const std::string& k, std::string&& v);
void sharkdb_insert(sharkdb_p db, const std::string& k, std::string&& v);
void sharkdb_delete(sharkdb_p db, const std::string& k);
// TODO add scan!
void sharkdb_free(sharkdb_p db);

#endif
