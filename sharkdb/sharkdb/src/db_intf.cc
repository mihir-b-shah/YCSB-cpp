
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>

thread_local char pread_buf[BLOCK_BYTES * BLOCKS_PER_FENCE];

static void update_mem_entry(mem_entry_t* entry, uint64_t curr_clk) {
	if (entry->p_ucommit_.lclk_ != 0 && entry->p_ucommit_.lclk_ <= curr_clk) {
		entry->p_commit_ = entry->p_ucommit_;
	}
}

static kv_pair_t* get_kv_pair_at(partition_t* part, uint32_t buf_spot) {
    uint32_t block_spot = buf_spot / N_ENTRIES_PER_BLOCK;
    uint32_t block_idx = buf_spot % N_ENTRIES_PER_BLOCK;
	return &part->l0_->wal_.log_buffer_[block_spot].kvs_[block_idx];
}

sharkdb_cqev sharkdb_read_async(sharkdb_t* db, const char* k, char* fill_v) {
    int rc;

    db_t* p_db = (db_t*) db->db_impl_;
	partition_t* part = &p_db->partitions_[get_partition(k)];
	sharkdb_cqev cqev = db->next_cqev_++;

	rc = pthread_rwlock_rdlock(&part->namespace_lock_);
    assert(rc == 0);

    //  Do memory reads synchronously, and kick off first I/O here.
	mem_table_t::iterator it = part->l0_->mem_table_.find(k);
	if (it != part->l0_->mem_table_.end()) {
        /*  Acquiring the spinlock here shouldn't degrade perf/is correct:
            1)  In write_async, we do the memcpy outside the spinlock region- so perf wise,
                spinlock shouldn't be held too long.
            2)  Correctness- when we do memcpy followed by add to the memtable, there is no
                need to place a membarrier between them, since for reads, that data will not become
                visible for the duration of the method (since data needs to be synced to be read),
                since our syncing impl acquires an exclusive namespace lock, and thus pushes
                everyone out of their operations. */

        mem_entry_t& entry = it->second;
		rc = pthread_spin_lock(&entry.lock_);
        assert(rc == 0);

        update_mem_entry(&entry, part->lclk_visible_);
        assert(entry.p_commit_.lclk_ < part->lclk_visible_);
        char* pv = &get_kv_pair_at(part, entry.p_commit_.buf_pos_)->val_[0]

		rc = pthread_spin_unlock(&entry.lock_);
        assert(rc == 0);

		memcpy(fill_v, pv, SHARKDB_VAL_BYTES);
        cq_t* cq = (cq_t*) db->cq_impl_;
        cq->emplace(part, my_lclk, cqev);
		return cqev;
	}

    /*
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
				rc = pread(ss_table->fd_, &pread_buf[0], BLOCK_BYTES*(blk_range_end-blk_range_start), BLOCK_BYTES*blk_range_start);
                assert(rc == BLOCK_BYTES*BLOCKS_PER_FENCE);

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
	rc = pthread_rwlock_unlock(&part->namespace_lock_);
    assert(rc == 0);
	db->cq_impl->emplace(cqev);
	memcpy(fill_v, v_found, SHARKDB_VAL_BYTES);
	return cqev;
    */
    return SHARKDB_CQEV_FAIL;
}

sharkdb_cqev sharkdb_write_async(sharkdb_t* db, const char* k, const char* v) {
    db_t* p_db = (db_t*) db->db_impl_;
	sharkdb_cqev cqev = db->next_cqev_++;
    int rc;

	partition_t* part = &p_db->partitions_[get_partition(k)];
	rc = pthread_rwlock_rdlock(&part->namespace_lock_);
    assert(rc == 0);

    rc = pthread_spin_lock(&part->lclk_lock_);
    assert(rc == 0);
    uint64_t my_lclk = part->lclk_next_++;
    uint32_t buf_spot = part->l0_->wal_.buf_p_ucommit_++;
    rc = pthread_spin_unlock(&part->lclk_lock_);
    assert(rc == 0);

	assert(buf_spot < LOG_BUF_MAX_ENTRIES && "Ran out of write space, shouldn't happen");

	mem_table_t* mem_table = &part->l0_->mem_table_;

	kv_pair_t* kv_fill = get_kv_pair_at(part, buf_spot);
	memcpy(&kv_fill->key_[0], k, SHARKDB_KEY_BYTES);
	memcpy(&kv_fill->val_[0], v, SHARKDB_VAL_BYTES);

	std::pair<mem_table_t::iterator, bool> r = mem_table->emplace(&kv_fill->key_[0], &kv_fill->val_[0]);
	if (!r.second) {
		// already existed, need to lock.
		rc = pthread_spin_lock(&r.first->second.lock_);
        assert(rc == 0);
    }

	// update ptrs to in-memory log-structured data.
	mem_entry_t& entry = r.first->second;
    update_mem_entry(&entry, part->lclk_visible_);
	entry.p_ucommit_.lclk_ = my_lclk;
	entry.p_ucommit_.buf_pos_ = buf_spot;
	rc = pthread_spin_unlock(&r.first->second.lock_);
    assert(rc == 0);

	cq_t* cq = (cq_t*) db->cq_impl_;
	cq->emplace(part, my_lclk, cqev);

	rc = pthread_rwlock_unlock(&part->namespace_lock_);
    assert(rc == 0);
	return cqev;
}
