
#ifndef _FREE_LIST_H_
#define _FREE_LIST_H_

#include <cstddef>
#include <pthread.h>

struct free_list_t {
	size_t TAIL_;
	size_t head_;
	//  Just implement in a threaded fashion to avoid allocations.
	size_t* tlist_;
    //  Could be implemented lock-free (is SPSC), but for simplicity...
    pthread_spinlock_t lock_;

	free_list_t() : TAIL_(0), head_(0), tlist_(nullptr) {
        int rc = pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
        assert(rc == 0);
    }
	~free_list_t() {
		if (tlist_ != nullptr) {
			delete[] tlist_;
		}
	}

	void init(size_t pool_size) {
		TAIL_ = pool_size;
		head_ = 0;

		tlist_ = new size_t[pool_size];
		for (size_t i = 0; i<pool_size-1; ++i) {
			tlist_[i] = i+1;
		}
		tlist_[pool_size-1] = TAIL_;
	}

	size_t alloc() {
        size_t ret;
        int rc;

        rc = pthread_spin_lock(&lock_);
        assert(rc == 0);

		if (head_ == TAIL_) {
			//	out of slots
			ret = TAIL_;
		} else {
            size_t next = tlist_[head_];
            ret = head_;
            head_ = next;
        }

        rc = pthread_spin_unlock(&lock_);
        assert(rc == 0);
		return ret;
	}

	void free(size_t slot) {
        int rc;
        rc = pthread_spin_lock(&lock_);
        assert(rc == 0);

		tlist_[slot] = head_;
		head_ = slot;

        rc = pthread_spin_unlock(&lock_);
        assert(rc == 0);
	}
};

#endif
