
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>

#include <liburing.h>
#include <tbb/concurrent_map.h>

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

void sharkdb_multiread(sharkdb_p db, std::vector<const char*>& ks, std::vector<const char*>& fill_vs) {
    db_t* p_db = (db_t*) db;
    assert(ks.size() == fill_vs.size());

	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		assert(pthread_rwlock_rdlock(&p_db->partitions_[i].namespace_lock_) == 0);
	}

    for (size_t i = 0; i<ks.size(); ++i) {
		partition_t* part = get_partition(p_db, ks[i]);

		level_0_t::mem_table_t::iterator it = part->l0_->mem_table_.find(ks[i]);
		assert(it != part->l0_->mem_table_.end());

		assert(pthread_rwlock_rdlock(&it->second.lock_) == 0);
		fill_vs[i] = it->second.v_;
		assert(pthread_rwlock_unlock(&it->second.lock_) == 0);
    }

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

    //  In real version, we'll use io_uring to overlap latencies here.
    for (size_t i = 0; i<ks.size(); ++i) {
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

		// Now update in the memtable, if this wasn't an insert.
		if (!r.second) {
			r.first->second.v_ = vs[i];
		}

        assert(pthread_rwlock_unlock(&r.first->second.lock_) == 0);
    }

	for (size_t i = 0; i<N_PARTITIONS; ++i) {
		assert(pthread_rwlock_unlock(&p_db->partitions_[i].namespace_lock_) == 0);
	}
}

void sharkdb_free(sharkdb_p db) {
    delete ((db_t*) db);
}
