
#ifndef _FREE_LIST_H_
#define _FREE_LIST_H_

struct free_list_t {
	size_t TAIL_;
	size_t head_;
	//  Just implement in a threaded fashion to avoid allocations.
	size_t* tlist_;

	free_list_t() : TAIL_(0), head_(0), tlist_(nullptr) {}
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
		if (head_ == TAIL_) {
			//	out of slots
			return TAIL_;
		}
		size_t next = tlist_[head_];
		size_t ret = head_;
		head_ = next;
		return ret;
	}

	void free(size_t slot) {
		tlist_[slot] = head_;
		head_ = slot;
	}
};

#endif
