
#ifndef _KV_H_
#define _KV_H_

#include <cstddef>
#include <vector>
#include <queue>
#include <unordered_map>
#include <utility>

#define SHARKDB_KEY_BYTES 24
#define SHARKDB_VAL_BYTES 1000

/*	For simplicity, let's keep YCSB benchmark in batch-sync mode- b/c async is a pain
	with the way they've impl'ed it. */

typedef int sharkdb_cqev;
static constexpr sharkdb_cqev SHARKDB_CQEV_FAIL = -1;

struct sharkdb_t {
	void* db_impl_;
	sharkdb_cqev next_cqev_;
	std::unordered_map<sharkdb_cqev, char*> bufs_;
	std::queue<sharkdb_cqev> cq_;

	sharkdb_t(void* impl) : db_impl_(impl), next_cqev_(0) {}
};

// maybe add return codes later?
sharkdb_t* sharkdb_init();
sharkdb_cqev sharkdb_read_async(sharkdb_t* db, const char* k, char* v);
sharkdb_cqev sharkdb_write_async(sharkdb_t* db, const char* k, const char* v);
void sharkdb_free(sharkdb_t* db);

#endif
