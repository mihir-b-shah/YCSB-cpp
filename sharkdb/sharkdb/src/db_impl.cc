
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
    return new sharkdb_t(db_instance, new cq_t());
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
	/*
    db_t* p_db = (db_t*) db->db_impl_;
	partition_t* part = get_partition(p_db, k);
	sharkdb_cqev cqev = db->next_cqev_++;

	assert(pthread_rwlock_rdlock(&part->namespace_lock_) == 0);
	mem_table_t::iterator it = part->l0_->mem_table_.find(k);
	if (it != part->l0_->mem_table_.end()) {
		assert(pthread_rwlock_rdlock(&it->second.lock_) == 0);
		memcpy(fill_v, (char*) it->second.v_, SHARKDB_VAL_BYTES);
		db->cq_impl_->emplace(cqev);
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

				//	Fence pointers are good enough to store for <10 blocks at a time. As such,
				//	just linearly stream the blocks in.
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
	db->cq_impl->emplace(cqev);
	memcpy(fill_v, v_found, SHARKDB_VAL_BYTES);
	return cqev;
	*/
	return SHARKDB_CQEV_FAIL;
}

sharkdb_cqev sharkdb_write_async(sharkdb_t* db, const char* k, const char* v) {
    db_t* p_db = (db_t*) db->db_impl_;
	sharkdb_cqev cqev = db->next_cqev_++;

	partition_t* part = get_partition(p_db, k);
	assert(pthread_rwlock_rdlock(&part->namespace_lock_) == 0);
	// TODO Can I get away with relaxed atomic here?
	uint64_t my_lclk = __atomic_fetch_add(&part->lclk_next_, 1, __ATOMIC_SEQ_CST);

	mem_table_t* mem_table = &part->l0_->mem_table_;
	// TODO Can I get away with relaxed atomic here? probably?
	uint32_t buf_spot = __atomic_fetch_add(&part->l0_->buf_idx_, 1, __ATOMIC_SEQ_CST);
	assert(buf_spot <= LOG_BUF_MAX_ENTRIES);

	kv_pair_t* kv_fill = &part->l0_->log_buffer_[buf_spot];
	memcpy(&kv_fill->key[0], k, SHARKDB_KEY_BYTES);
	memcpy(&kv_fill->val[0], v, SHARKDB_VAL_BYTES);

	std::pair<mem_table_t::iterator, bool> r = mem_table->emplace(&kv_fill->key[0], &kv_fill->val[0]);
	if (!r.second) {
		// already existed, need to lock.
		assert(pthread_spin_lock(&r.first->second.lock_) == 0);
	}

	// update ptrs to in-memory log-structured data.
	mem_entry_t& entry = r.first->second;
	if (entry.p_ucommit_.lclk_ != 0 && entry.p_ucommit_.lclk_ <= part->lclk_visible_) {
		entry.p_commit_ = entry.p_ucommit_;
	}
	entry.p_ucommit_.lclk_ = my_lclk;
	entry.p_ucommit_.buf_pos_ = buf_spot;
	assert(pthread_spin_unlock(&r.first->second.lock_) == 0);

	cq_t* cq = (cq_t*) db->cq_impl_;
	cq->emplace(part, my_lclk, cqev);

	assert(pthread_rwlock_unlock(&part->namespace_lock_) == 0);
	return cqev;
}

sharkdb_cqev sharkdb_cpoll_cq(sharkdb_t* db) {
	cq_t* cq = (cq_t*) db->cq_impl_;
	cqe_t& cqe = cq->front();
	return cqe.lclk_visible_ <= cqe.part_->lclk_visible_ ? cqe.ev_ : SHARKDB_CQEV_FAIL;
}

//	don't delete the database, maybe reference count it via a std::shared_ptr?
void sharkdb_free(sharkdb_t* db) {
	delete (cq_t*) db->cq_impl_;
	delete db;
}
