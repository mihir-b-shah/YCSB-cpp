
#include "consts.h"
#include "db_impl.h"

#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>

static const char* make_ss_table_path(const char* prefix, char* buf, size_t part, size_t level, size_t num) {
	sprintf(buf, "%sss_table_%lu_%lu_%lu", prefix, part, level, num);
	return (const char*) buf;
}

/*	Steps:
	1)	l0=A,	l0_swp=null,	L1={X,Y}
		Memtable is in W mode by users, and not touched by me.
	2)	l0=B,	l0_swp=A,		L1={X,Y}
		Memtable is swapped, so in R mode by users, and R mode by me (to flush).
	3)	l0=B,	l0_swp=null,	L1={X,Y,A}
		L1 version is done, so memtable is gone from users, and W mode for me- to dealloc. */
void* flush_thr_body(void* arg) {
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, nullptr);
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, nullptr);

	partition_t* part = (partition_t*) arg;

	while (true) {
		/*	Safe to check outside, since small race in read is not a problem. Size is presumably
			<64 bits, so reads are atomic. */
		if (part->l0_->mem_table_.size() >= MEM_TABLE_MAX_SIZE * MEM_TABLE_FULL_THR/100) {
			level_0_t* l0_new = new level_0_t(1+part->l0_->id_);
			level_0_t* l0_flush = part->l0_;

			assert(pthread_rwlock_wrlock(&part->namespace_lock_) == 0);
			part->l0_swp_ = l0_flush;
			part->l0_ = l0_new;
			size_t ss_table_id = part->disk_levels_[1].size();
			assert(pthread_rwlock_unlock(&part->namespace_lock_) == 0);

			// level=1, since we are flushing.
			char ss_table_name[40];
			const char* ss_table_path = make_ss_table_path("/tmp/sharkdb/", &ss_table_name[0], part->tid_, 1, ss_table_id);
			ss_table_t* ss_table = new ss_table_t(1);
			ss_table->fd_ = open(ss_table_path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

			level_0_t::mem_table_t* mem_table = &part->l0_swp_->mem_table_;

			size_t entries_wr = 0;
			for (auto it = mem_table->begin(); it != mem_table->end(); ++it) {
				ss_table->filter_.set(it->first);
				assert(write(ss_table->fd_, it->first, SHARKDB_KEY_BYTES) == SHARKDB_KEY_BYTES);
				assert(write(ss_table->fd_, it->second.v_, SHARKDB_VAL_BYTES) == SHARKDB_VAL_BYTES);
				entries_wr += 1;

				if (entries_wr % (BLOCKS_PER_FENCE * N_ENTRIES_PER_BLOCK) == 0) {
					//	TODO copy it->first!!
					ss_table->fence_ptrs_.emplace_back(it->first, entries_wr / (BLOCKS_PER_FENCE * N_ENTRIES_PER_BLOCK));
				}
			}

			assert(close(ss_table->fd_) == 0);
			ss_table->fd_ = open(ss_table_path, O_RDONLY, S_IRUSR);
			assert(ss_table->fd_ >= 0);

			level_0_t* l0_free = part->l0_swp_;
			assert(pthread_rwlock_wrlock(&part->namespace_lock_) == 0);
			part->l0_swp_ = nullptr;
			part->disk_levels_[1].push_back(ss_table);
			assert(pthread_rwlock_unlock(&part->namespace_lock_) == 0);

			delete l0_free;
		}
	}
	
	return nullptr;
}
