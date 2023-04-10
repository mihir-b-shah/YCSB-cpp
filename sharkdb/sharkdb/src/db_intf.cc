
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

template <bool IS_WRITE>
static std::pair<uint64_t, uint32_t> lclk_advance(partition_t* part) {
	std::pair<uint64_t, uint32_t> ret;
	int rc;
    rc = pthread_spin_lock(&part->lclk_lock_);
    assert(rc == 0);

    ret.first = part->lclk_next_++;
	if (IS_WRITE) {
		ret.second = part->l0_->wal_.buf_p_ucommit_++;
	}

    rc = pthread_spin_unlock(&part->lclk_lock_);
    assert(rc == 0);
	return ret;
}

sharkdb_cqev sharkdb_read_async(sharkdb_t* db, const char* k, char* fill_v) {
    int rc;

    db_t* p_db = (db_t*) db->db_impl_;
	partition_t* part = &p_db->partitions_[get_partition(k)];
	sharkdb_cqev cqev = db->next_cqev_++;

	/*	Acquires a spinlock, is this scalable? */
	std::pair<uint64_t, uint32_t> la_res = lclk_advance<false>(part);
	uint64_t my_lclk = la_res.first;

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
        assert(entry.p_commit_.lclk_ <= part->lclk_visible_);
        char* pv = &get_kv_pair_at(part, entry.p_commit_.buf_pos_)->val_[0];

		rc = pthread_spin_unlock(&entry.lock_);
        assert(rc == 0);

		memcpy(fill_v, pv, SHARKDB_VAL_BYTES);
        cq_t* cq = (cq_t*) db->cq_impl_;
        cq->emplace(part, my_lclk, cqev);
		return cqev;
	}

    /*  Scan bloom filters in order, until I find a match.
        Issue a read for the range of blocks the fence ptr tells me about.
        In the read, provide info in user_data field about until where we searched,
        so on completion, we can leave off from where we started.

        It is safe to release namespace locks between such scans, since namespace
        modifications have the property that they take from earlier (earlier levels,
        or earlier occurring in the vector for a level), and place in later- observe
        this is true for flush/compaction. Then, if it is guaranteed that even if
        the namespace changes, nothing we should have searched would come up behind us. 
        Of course, namespace change might mean what we searched from, say L1, and issued
        read for, is now deleted and moved to an L2- in this case, the read will fail
        with EBADF. */
    
    assert((part->disk_levels_[0].size() == 0) && "L0 is not on disk");
    for (size_t l = 2; l<=N_DISK_LEVELS; ++l) {
        assert((part->disk_levels_[l].size() == 0) && "Not doing compaction right now");
    }

	bool submitted = false;
	for (size_t i = 0; i<part->disk_levels_[1].size(); ++i) {
		ss_table_t* ss_table = part->disk_levels_[1][i];

		std::pair<size_t, size_t> range = get_ss_blk_range(k, ss_table);
		if (range == std::pair<size_t, size_t>(0,0)) {
			continue;
		}

		read_ring_t* rd_ring = (read_ring_t*) db->rd_ring_impl_;

		/*	A very poor form of backpressure, if we keep # in-flight requests correct,
			shouldn't happen much. Problematic, since we do this while holding the
			namespace lock though. */
		size_t alloc_idx;
		while (true) {
			alloc_idx = rd_ring->free_list_.alloc();
			if (alloc_idx != rd_ring->free_list_.TAIL_) {
				break;
			} else {
				__builtin_ia32_pause();
			}
		}

		read_ring_t::progress_t* prog_state = &rd_ring->progress_states_[alloc_idx];
		prog_state->level_ = 1;
		prog_state->ss_table_id_ = i;
		memcpy(&prog_state->key_[0], k, SHARKDB_KEY_BYTES);
		prog_state->blk_fill_id_ = alloc_idx;
		prog_state->user_buf_ = fill_v;
		prog_state->cqev_ = cqev;

		submit_read_io(prog_state);
		submitted = true;
		break;
	}

	//	If matched nothing- note bloom filters have no false negatives, so really nothing.
	assert(submitted && "We do not tolerate reads for keys not existing in DB, for now");

	rc = pthread_rwlock_unlock(&part->namespace_lock_);
    assert(rc == 0);
	return cqev;
}

sharkdb_cqev sharkdb_write_async(sharkdb_t* db, const char* k, const char* v) {
    db_t* p_db = (db_t*) db->db_impl_;
	sharkdb_cqev cqev = db->next_cqev_++;
    int rc;

	partition_t* part = &p_db->partitions_[get_partition(k)];
	rc = pthread_rwlock_rdlock(&part->namespace_lock_);
    assert(rc == 0);

	std::pair<uint64_t, uint32_t> la_res = lclk_advance<true>(part);
	uint64_t my_lclk = la_res.first;
	uint32_t buf_spot = la_res.second;
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
