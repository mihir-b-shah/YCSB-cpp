
#ifndef _KV_H_
#define _KV_H_

#include <cstddef>
#include <vector>
#include <unordered_map>

#define SHARKDB_KEY_BYTES 24
#define SHARKDB_VAL_BYTES 1000

/*	For simplicity, let's keep YCSB benchmark in batch-sync mode- b/c async is a pain
	with the way they've impl'ed it. */

typedef int sharkdb_cqev;
static constexpr sharkdb_cqev SHARKDB_CQEV_FAIL = -1;

struct sharkdb_t {
	void* db_impl_;
	sharkdb_cqev next_cqev_;
	void* cq_impl_;
    void* rd_ring_impl_;

	sharkdb_t(void* db_impl, void* cq_impl, void* rd_ring_impl) : db_impl_(db_impl), next_cqev_(0), cq_impl_(cq_impl), rd_ring_impl_(rd_ring_impl) {}
};

// maybe add return codes later?
sharkdb_t* sharkdb_init();
sharkdb_cqev sharkdb_read_async(sharkdb_t* db, const char* k, char* v);
sharkdb_cqev sharkdb_write_async(sharkdb_t* db, const char* k, const char* v);
sharkdb_cqev sharkdb_cpoll_cq(sharkdb_t* db);
void sharkdb_free(sharkdb_t* db);

#endif
