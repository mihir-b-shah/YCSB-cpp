
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>
#include <thread>

#include <liburing.h>
#include <tbb/concurrent_map.h>

thread_local char pread_buf[BLOCK_BYTES * BLOCKS_PER_FENCE];

static db_t* db_instance = nullptr;

static void free_db_instance() {
	delete db_instance;
}

static std::once_flag init_flag;
sharkdb_t* sharkdb_init() {
	std::call_once(init_flag, [&](){
		atexit(free_db_instance);
		setup_logs();
		db_instance = new db_t();
	});
    return new sharkdb_t(db_instance);
}

//	Assume k is in the form 'user[0-9]+'
static partition_t* get_partition(db_t* db, const char* k) {
	assert(memcmp(k, KEY_PREFIX, strlen(KEY_PREFIX)) == 0);
	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		if (memcmp(k+strlen(KEY_PREFIX), ORDER_PREFIXES[i], strlen(ORDER_PREFIXES[0])) <= 0) {
			return &db->partitions_[i];
		}
	}
	assert(false && "Should have matched a memtable.");
}

sharkdb_cqev sharkdb_read_async(sharkdb_t* db, const char* k, char* fill_v) {
    db_t* p_db = (db_t*) db->db_impl_;
	partition_t* part = get_partition(p_db, k);
	sharkdb_cqev cqev = db->next_cqev_++;

	assert(pthread_rwlock_rdlock(&part->namespace_lock_) == 0);
	level_0_t::mem_table_t::iterator it = part->l0_->mem_table_.find(k);
	if (it != part->l0_->mem_table_.end()) {
		assert(pthread_rwlock_rdlock(&it->second.lock_) == 0);
		memcpy(fill_v, (char*) it->second.v_, SHARKDB_VAL_BYTES);
		db->cq_.emplace(cqev);
		assert(pthread_rwlock_unlock(&it->second.lock_) == 0);
		return cqev;
	}

	char* v_found = nullptr;
	for (std::vector<ss_table_t*>& ss_level : part->disk_levels_) {
		for (ss_table_t* ss_table : ss_level) {
			if (ss_table->filter_.test(k)) {
				// use fence pointers to find blocks to check.
				fence_ptr_t dummy(k, 0);
				auto it = std::upper_bound(ss_table->fence_ptrs_.begin(), ss_table->fence_ptrs_.end(), dummy, [](const fence_ptr_t& f1, const fence_ptr_t& f2){
					return memcmp(&f1.k_[0], &f2.k_[0], SHARKDB_KEY_BYTES) < 0;
				});
				//	because fence pointers are left-justified.
				assert(it != ss_table->fence_ptrs_.begin());
				size_t blk_range_end = it->blk_num_;
				it--;
				size_t blk_range_start = it->blk_num_;
				assert(blk_range_end - blk_range_start <= BLOCKS_PER_FENCE);

				/*	Fence pointers are good enough to store for <10 blocks at a time. As such,
					just linearly stream the blocks in. */
				assert(pread(ss_table->fd_, &pread_buf[0], BLOCK_BYTES*(blk_range_end-blk_range_start), BLOCK_BYTES*blk_range_start) == BLOCK_BYTES*BLOCKS_PER_FENCE);

				for (size_t b = 0; b<blk_range_end-blk_range_start; ++b) {
					for (size_t i = 0; i<N_ENTRIES_PER_BLOCK; ++i) {
						char* kr = &pread_buf[b*BLOCK_BYTES + i*(SHARKDB_KEY_BYTES+SHARKDB_VAL_BYTES)];
						if (memcmp(kr, k, SHARKDB_KEY_BYTES) == 0) {
							v_found = kr+SHARKDB_KEY_BYTES;
							goto search_done;
						}
					}
				}
			}
		}
	}

	search_done:
	assert(pthread_rwlock_unlock(&part->namespace_lock_) == 0);
	db->cq_.emplace(cqev);
	memcpy(fill_v, v_found, SHARKDB_VAL_BYTES);
	return cqev;
}

/*  XXX if we use std::string's, we can do std::string v2 = std::move(v) to get zero-copy.
    Also, who cares- we're writing them to the *log* anyway?- scatter-gathering from here
    feels sketchy... */
sharkdb_cqev sharkdb_write_async(sharkdb_t* db, const char* k, const char* v) {
    db_t* p_db = (db_t*) db->db_impl_;
	sharkdb_cqev cqev = db->next_cqev_++;

	partition_t* part = get_partition(p_db, k);
	level_0_t::mem_table_t* mem_table = &part->l0_->mem_table_;
	assert(pthread_rwlock_rdlock(&part->namespace_lock_) == 0);

	//	TODO copy v in!!
	std::pair<level_0_t::mem_table_t::iterator, bool> r = mem_table->emplace(k, v);
	if (!r.second) {
		// already existed, need to lock.
		assert(pthread_rwlock_wrlock(&r.first->second.lock_) == 0);
	}

	// First WAL log it.
	wal_block_winfo_t winfo;
	winfo.keys[0] = k;
	winfo.vals[0] = v;
	log_write(&part->l0_->wal_, &winfo);
	log_commit(&part->l0_->wal_);

	// Now update in the memtable, if this was an insert, a wasted write, who cares?
	r.first->second.v_ = v;
	assert(pthread_rwlock_unlock(&r.first->second.lock_) == 0);
	assert(pthread_rwlock_unlock(&part->namespace_lock_) == 0);

	db->cq_.emplace(cqev);
	return cqev;
}

void sharkdb_free(sharkdb_t* db) {
	delete db;
}
