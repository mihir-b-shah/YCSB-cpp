
#ifndef _KV_H_
#define _KV_H_

#include <cstddef>
#include <vector>
#include <utility>

#define SHARKDB_KEY_BYTES 24
#define SHARKDB_VAL_BYTES 1000

typedef int sharkdb_cqev;
static constexpr sharkdb_cqev SHARKDB_CQEV_FAIL = -1;

struct sharkdb_t {
	void* db_impl_;
	sharkdb_cqev next_cqev_;
	void* cq_impl_;
    void* rd_ring_impl_;

	sharkdb_t(void* db_impl, void* cq_impl, void* rd_ring_impl) : db_impl_(db_impl), next_cqev_(0), cq_impl_(cq_impl), rd_ring_impl_(rd_ring_impl) {}
};

sharkdb_t* sharkdb_init();
sharkdb_cqev sharkdb_read_async(sharkdb_t* db, const char* k, char* v);
sharkdb_cqev sharkdb_write_async(sharkdb_t* db, const char* k, const char* v);
std::pair<bool, sharkdb_cqev> sharkdb_cpoll_cq(sharkdb_t* db);
void sharkdb_free(sharkdb_t* db);
// a hack that empties the completion queue, regardless of timestamps.
size_t sharkdb_drain(sharkdb_t* db);
void sharkdb_nowrites(sharkdb_t* db);

#endif
