
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>

#include <liburing.h>
#include <tbb/concurrent_map.h>

thread_local char pread_buf[BLOCK_BYTES * BLOCKS_PER_FENCE];

sharkdb_p sharkdb_init() {
    return new db_t();
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

void sharkdb_multiread(sharkdb_p db, std::vector<const char*>& ks, std::vector<char*>& fill_vs) {
    db_t* p_db = (db_t*) db;
    assert(ks.size() == fill_vs.size());

	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		assert(pthread_rwlock_rdlock(&p_db->partitions_[i].namespace_lock_) == 0);
	}

    for (size_t i = 0; i<ks.size(); ++i) {
		partition_t* part = get_partition(p_db, ks[i]);

		level_0_t::mem_table_t::iterator it = part->l0_->mem_table_.find(ks[i]);
		if (it != part->l0_->mem_table_.end()) {
			assert(pthread_rwlock_rdlock(&it->second.lock_) == 0);
			fill_vs[i] = (char*) it->second.v_;
			assert(pthread_rwlock_unlock(&it->second.lock_) == 0);
		} else {
			for (std::vector<ss_table_t*>& ss_level : part->disk_levels_) {
				for (ss_table_t* ss_table : ss_level) {
					if (ss_table->filter_.test(ks[i])) {
						// use fence pointers to find blocks to check.
						fence_ptr_t dummy(ks[i], 0);
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
						//	TODO scan the blocks to find me!

						char* v = nullptr;
						for (size_t b = 0; b<blk_range_end-blk_range_start; ++b) {
							for (size_t i = 0; i<N_ENTRIES_PER_BLOCK; ++i) {
								char* k = &pread_buf[b*BLOCK_BYTES + i*(SHARKDB_KEY_BYTES+SHARKDB_VAL_BYTES)];
								if (memcmp(ks[i], k, SHARKDB_KEY_BYTES) == 0) {
									v = k+SHARKDB_KEY_BYTES;
									memcpy(fill_vs[i], v, SHARKDB_VAL_BYTES);
									goto search_done;
								}
							}
						}
					}
				}
			}
		}
	}

	search_done:
	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		assert(pthread_rwlock_unlock(&p_db->partitions_[i].namespace_lock_) == 0);
	}
}

/*  XXX if we use std::string's, we can do std::string v2 = std::move(v) to get zero-copy.
    Also, who cares- we're writing them to the *log* anyway?- scatter-gathering from here
    feels sketchy... */
void sharkdb_multiwrite(sharkdb_p db, std::vector<const char*>& ks, std::vector<const char*>& vs) {
    db_t* p_db = (db_t*) db;
    assert(ks.size() == vs.size());

	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		assert(pthread_rwlock_rdlock(&p_db->partitions_[i].namespace_lock_) == 0);
	}

    for (size_t i = 0; i<ks.size(); ++i) {
		// fprintf(stderr, "Wrote i: %lu\n", i);
		partition_t* part = get_partition(p_db, ks[i]);
		level_0_t::mem_table_t* mem_table = &part->l0_->mem_table_;

        std::pair<level_0_t::mem_table_t::iterator, bool> r = mem_table->emplace(ks[i], vs[i]);
        if (!r.second) {
            // already existed, need to lock.
            assert(pthread_rwlock_wrlock(&r.first->second.lock_) == 0);
        }

        // First WAL log it.
		wal_block_winfo_t winfo;
		winfo.keys[0] = ks[i];
		winfo.vals[0] = vs[i];
        log_write(&part->l0_->wal_, &winfo);
	}

	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		log_commit(&p_db->partitions_[i].l0_->wal_);
	}

	for (size_t i = 0; i<ks.size(); ++i) {
		partition_t* part = get_partition(p_db, ks[i]);
		level_0_t::mem_table_t* mem_table = &part->l0_->mem_table_;
        level_0_t::mem_table_t::iterator r = mem_table->find(ks[i]);

		// Now update in the memtable, if this was an insert, a wasted write, who cares?
		r->second.v_ = vs[i];
        assert(pthread_rwlock_unlock(&r->second.lock_) == 0);
    }

	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		assert(pthread_rwlock_unlock(&p_db->partitions_[i].namespace_lock_) == 0);
	}
}

void sharkdb_free(sharkdb_p db) {
    delete ((db_t*) db);
}
