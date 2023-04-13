
#include "consts.h"
#include "db_impl.h"

#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>

static const char* make_ss_table_path(const char* prefix, char* buf, size_t part, size_t level, size_t num) {
	sprintf(buf, "%sss_table_%lu_%lu_%lu", prefix, part, level, num);
	return (const char*) buf;
}

//	bss is zero-init.
static const char zero_buf[BLOCK_BYTES] = {};

/*	Steps:
	1)	l0=A,	l0_swp=null,	L1={X,Y}
		Memtable is in W mode by users, and not touched by me.
	2)	l0=B,	l0_swp=A,		L1={X,Y}
		Memtable is swapped, so in R mode by users, and R mode by me (to flush).
	3)	l0=B,	l0_swp=null,	L1={X,Y,A}
		L1 version is done, so memtable is gone from users, and W mode for me- to dealloc. */
void* flush_thr_body(void* arg) {
    int rc;
	partition_t* part = (partition_t*) arg;

	while (!part->db_ref_->stop_thrs_) {
		//	No need to read while locked, it's ok if we read an old (smaller) value?
		uint32_t log_entries_used = part->l0_->wal_.buf_p_ucommit_;

        //  Maintain size on our own, since regular size() is not safe- not monotonic, etc.
		if (log_entries_used >= (LOG_BUF_MAX_ENTRIES * LOG_FULL_THR)/100) {
			level_0_t* l0_new = new level_0_t(part->db_ref_);
			level_0_t* l0_flush = part->l0_;

            /*  we can safely read l0_ before acquiring the lock here, since THIS THREAD is the only
                one that can swap l0_'s, etc */
			rc = pthread_rwlock_wrlock(&part->namespace_lock_);
            assert(rc == 0);

			part->l0_swp_ = l0_flush;
			part->l0_ = l0_new;
			size_t ss_table_id = part->disk_levels_[1].size();

			rc = pthread_rwlock_unlock(&part->namespace_lock_);
            assert(rc == 0);

			// level=1, since we are flushing.
			char ss_table_name[40];
			const char* ss_table_path = make_ss_table_path("/tmp/sharkdb/", &ss_table_name[0], part->tid_, 1, ss_table_id);
			ss_table_t* ss_table = new ss_table_t(1);
			ss_table->fd_ = open(ss_table_path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
            assert(ss_table->fd_ >= 0);

			mem_table_t* mem_table = &l0_flush->mem_table_;

			size_t entries_wr = 0;
			for (auto it = mem_table->begin(); it != mem_table->end(); ++it) {
				ss_table->filter_.set(it->first);

                rc = write(ss_table->fd_, it->first, SHARKDB_KEY_BYTES);
                assert(rc == SHARKDB_KEY_BYTES);
				
                /*  Completely ignore the version numbers- if it's in the memtable, flush it-
                    since the sstable is now our persistence mechanism. */
                rc = write(ss_table->fd_, it->second.v_, SHARKDB_VAL_BYTES);
                assert(rc == SHARKDB_VAL_BYTES);

				if (entries_wr % (BLOCKS_PER_FENCE * N_ENTRIES_PER_BLOCK) == 0) {
					ss_table->fence_ptrs_.emplace_back(it->first, entries_wr / N_ENTRIES_PER_BLOCK);
				}
				entries_wr += 1;
			}

			/*	Make sure:
				1)	There is a fence pointer for a "top" key, so that std::upper_bound requests
					against the fence_ptrs succeed.
				2)	The last block is padded, if need be. */

			const char* TOP_KEY_STEM = "vser00000000000000000000";
			assert(strcmp(TOP_KEY_STEM, KEY_PREFIX) > 0 && strlen(TOP_KEY_STEM) == SHARKDB_KEY_BYTES);

			if (entries_wr % N_ENTRIES_PER_BLOCK != 0) {
				char key_buf[SHARKDB_KEY_BYTES+1];
				strcpy(&key_buf[0], TOP_KEY_STEM);

				for (size_t i = (entries_wr % N_ENTRIES_PER_BLOCK); i<N_ENTRIES_PER_BLOCK; ++i) {
					key_buf[SHARKDB_KEY_BYTES-1] = i + '0';
					rc = write(ss_table->fd_, &key_buf[0], SHARKDB_KEY_BYTES);
					assert(rc == SHARKDB_KEY_BYTES);
					rc = write(ss_table->fd_, &zero_buf[0], SHARKDB_VAL_BYTES);
					assert(rc == SHARKDB_VAL_BYTES);
				}
			}
			ss_table->fence_ptrs_.emplace_back(TOP_KEY_STEM, (entries_wr + N_ENTRIES_PER_BLOCK - 1) / N_ENTRIES_PER_BLOCK);

            rc = fsync(ss_table->fd_);
            assert(rc == 0);
			rc = close(ss_table->fd_);
            assert(rc == 0);

			ss_table->fd_ = open(ss_table_path, O_RDONLY | O_DIRECT, S_IRUSR);
			assert(ss_table->fd_ >= 0);

			rc = pthread_rwlock_wrlock(&part->namespace_lock_);
            assert(rc == 0);

			part->l0_swp_ = nullptr;
			part->disk_levels_[1].push_back(ss_table);

			rc = pthread_rwlock_unlock(&part->namespace_lock_);
            assert(rc == 0);

			delete l0_flush;
			__sync_synchronize();
		}
        __builtin_ia32_pause();
	}
	return nullptr;
}
