
#include <sharkdb.h>
#include "db_impl.h"

#include <string>
#include <cstdlib>

#include <liburing.h>
#include <tbb/concurrent_map.h>

sharkdb_p sharkdb_init() {
    db_t* db = new db_t();
    return db;
}

void sharkdb_multiread(sharkdb_p db, std::vector<const char*>& ks, std::vector<const char*>& fill_vs) {
    db_t* p_db = (db_t*) db;
    assert(ks.size() == fill_vs.size());

    for (size_t i = 0; i<ks.size(); ++i) {
		db_t::mem_table_t::iterator it = p_db->mem_table_.find(ks[i]);
		assert(it != p_db->mem_table_.end());

		assert(pthread_rwlock_rdlock(&it->second.lock_) == 0);
		fill_vs[i] = it->second.v_;
		assert(pthread_rwlock_unlock(&it->second.lock_) == 0);
    }
}

/*  XXX if we use std::string's, we can do std::string v2 = std::move(v) to get zero-copy.
    Also, who cares- we're writing them to the *log* anyway?- scatter-gathering from here
    feels sketchy... */
void sharkdb_multiwrite(sharkdb_p db, std::vector<const char*>& ks, std::vector<const char*>& vs) {
    db_t* p_db = (db_t*) db;
    assert(ks.size() == vs.size());
    //  In real version, we'll use io_uring to overlap latencies here.
    for (size_t i = 0; i<ks.size(); ++i) {
        std::pair<db_t::mem_table_t::iterator, bool> r = p_db->mem_table_.emplace(ks[i], vs[i]);
        if (!r.second) {
            // already existed, need to lock.
            assert(pthread_rwlock_wrlock(&r.first->second.lock_) == 0);
        }

        // First WAL log it.
		wal_block_winfo_t winfo;
		winfo.keys[0] = ks[i];
		winfo.vals[0] = vs[i];
        log_write(&p_db->wal_, &winfo);

		// Now update in the memtable, if this wasn't an insert.
		if (!r.second) {
			r.first->second.v_ = vs[i];
		}

        assert(pthread_rwlock_unlock(&r.first->second.lock_) == 0);
    }
}

void sharkdb_free(sharkdb_p db) {
    delete ((db_t*) db);
}
