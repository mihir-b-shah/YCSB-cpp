
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>

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

template <templ_rw_arg_e OP_TYPE>
static std::pair<uint64_t, uint32_t> lclk_advance(partition_t* part) {
	std::pair<uint64_t, uint32_t> ret;
	int rc;
    rc = pthread_spin_lock(&part->lclk_lock_);
    assert(rc == 0);

    ret.first = part->lclk_next_++;
	if (OP_TYPE == TEMPL_IS_WRITE) {
		ret.second = part->l0_->wal_.buf_p_ucommit_++;
	}

    rc = pthread_spin_unlock(&part->lclk_lock_);
    assert(rc == 0);
	return ret;
}

static char* do_memtable_read(partition_t* part, level_0_t* l0, const char* k) {
	mem_table_t::iterator it = l0->mem_table_.find(k);
	char* ret;
	int rc;

	if (it != l0->mem_table_.end()) {
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
        ret = &get_kv_pair_at(part, entry.p_commit_.buf_pos_)->val_[0];

		rc = pthread_spin_unlock(&entry.lock_);
        assert(rc == 0);
	} else {
		ret = nullptr;
	}
	return ret;
}

sharkdb_cqev sharkdb_read_async(sharkdb_t* db, const char* k, char* fill_v) {
    int rc;

    #if defined(INSTR)
    get_stats()->n_reads_ += 1;
    #endif

    db_t* p_db = (db_t*) db->db_impl_;
	size_t part_idx = get_partition(k);
	partition_t* part = &p_db->partitions_[part_idx];
	sharkdb_cqev cqev = db->next_cqev_++;
    cq_t* cq = (cq_t*) db->cq_impl_;

	//	Acquires a spinlock, is this scalable?
    //  I don't actually need the result here...
	lclk_advance<TEMPL_IS_READ>(part);

    pthread_rwlock_lock_wrap<TEMPL_IS_READ>(&part->namespace_lock_, get_stats()->t_contend_namesp_ns_);

	//	See my past completions- requires namespace lock to be held, for simplicity.
	check_io(db);

    //  Do memory reads synchronously, and kick off first I/O here.
	char* v_found = do_memtable_read(part, part->l0_, k);
	if (v_found == nullptr && part->l0_swp_ != nullptr) {
		v_found = do_memtable_read(part, part->l0_swp_, k);
	}
	if (v_found != nullptr) {
		memcpy(fill_v, v_found, SHARKDB_VAL_BYTES);
        //  lclk=0 suffices here, reads should become visible as soon as appear in queue.
        cq->emplace(part, 0, cqev, true);
	} else {
        /*  Scan bloom filters in order, until I find a match.
            Issue a read for the range of blocks the fence ptr tells me about.
            In the read, provide info in user_data field about until where we searched,
            so on completion, we can leave off from where we started.

            It is safe to release namespace locks between such scans, since namespace
            modifications (i.e. just flushing for now) have the property, they move memtable
            to disk. But since we already know which memtable we start on, and work backwards,
            the additions are happening to our right, and as such we view a consistent prefix
            of sstables. */

        assert((part->disk_levels_[0].size() == 0) && "L0 is not on disk");
        for (size_t l = 2; l<=N_DISK_LEVELS; ++l) {
            assert((part->disk_levels_[l].size() == 0) && "Not doing compaction right now");
        }

        ssize_t i;
        for (i = (ssize_t) part->disk_levels_[1].size()-1; i>=0; --i) {
            ss_table_t* ss_table = part->disk_levels_[1][i];
            std::pair<size_t, size_t> range = get_ss_blk_range(k, ss_table);
            if (range == std::pair<size_t, size_t>(0,0)) {
                continue;
            }
            read_ring_t* rd_ring = (read_ring_t*) db->rd_ring_impl_;

            /*	A very poor form of backpressure, if we keep # in-flight requests correct,
                shouldn't happen much. Problematic, since we do this while holding the
                namespace lock though- shouldn't be too bad though? */
            size_t alloc_idx;
            bool first_loop = true; 
            while (true) {
                alloc_idx = rd_ring->free_list_.alloc();
                if (alloc_idx != rd_ring->free_list_.TAIL_) {
                    break;
                } else {
                    if (first_loop) {
                        drain_sq_ring(rd_ring);
                        first_loop = false;
                    }
                    check_io(db);
                    __builtin_ia32_pause();
                }
            }

            read_ring_t::progress_t* prog_state = &rd_ring->progress_states_[alloc_idx];
            prog_state->part_ = part;
            prog_state->level_ = 1;
            prog_state->ss_table_id_ = i;
            memcpy(&prog_state->key_[0], k, SHARKDB_KEY_BYTES);
            prog_state->blk_fill_id_ = alloc_idx;
            prog_state->user_buf_ = fill_v;
            prog_state->cqev_ = cqev;
            prog_state->fd_ = ss_table->fd_;
            prog_state->blk_range_start_ = range.first; 
            prog_state->blk_range_end_ = range.second;

            #if defined(INSTR)
            prog_state->submit_ts_ns_ = get_ts_nsecs();
            #endif

            //  Take the hit of the first I/O submit on the user thread
            submit_read_io(rd_ring, prog_state);
            break;
        }

        //	If matched nothing- note bloom filters have no false negatives, so really nothing.
        if (i == -1) {
            cq->emplace(part, 0, cqev, false);
        }
    }

    rc = pthread_rwlock_unlock(&part->namespace_lock_);
    assert(rc == 0);
	return cqev;
}

sharkdb_cqev sharkdb_write_async(sharkdb_t* db, const char* k, const char* v) {
    db_t* p_db = (db_t*) db->db_impl_;
	sharkdb_cqev cqev = db->next_cqev_++;
    int rc;

	partition_t* part = &p_db->partitions_[get_partition(k)];

    pthread_rwlock_lock_wrap<TEMPL_IS_READ>(&part->namespace_lock_, get_stats()->t_contend_namesp_ns_);

    uint64_t my_lclk;
    uint32_t buf_spot;
    while (true) {
        //  Again, poor man's backpressure
        std::pair<uint64_t, uint32_t> la_res = lclk_advance<TEMPL_IS_WRITE>(part);
        my_lclk = la_res.first;
        buf_spot = la_res.second;
        if (buf_spot >= LOG_BUF_MAX_ENTRIES) {
            level_0_t* l0_old_ = part->l0_;
            rc = pthread_rwlock_unlock(&part->namespace_lock_);
            assert(rc == 0);

            //  Wait for the l0_ to change (i.e. it is swapped out)
            while (part->l0_ == l0_old_) {
                __builtin_ia32_pause();
            }

            //  Note this is a write operation, but we are only reading the namespace.
            pthread_rwlock_lock_wrap<TEMPL_IS_READ>(&part->namespace_lock_, get_stats()->t_contend_namesp_ns_);
        } else {
            break;
        }
    }

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
	cq->emplace(part, my_lclk, cqev, true);

	rc = pthread_rwlock_unlock(&part->namespace_lock_);
    assert(rc == 0);
	return cqev;
}
