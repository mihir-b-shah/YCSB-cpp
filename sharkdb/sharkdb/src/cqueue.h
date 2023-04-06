
#ifndef _CQUEUE_H_
#define _CQUEUE_H_

/*	Spliceable single-threaded completion queue */

#include <list>
#include <vector>

template <typename T>
class cqueue_t {
public:
	cqueue_t() : pos_(0) {}

	void push_bulk(std::vector<T>&& vec) {
		q_.emplace_back(std::move(vec));
	}

	bool is_empty() {
		return q_.size() > 0;
	}

	T& front() {
		return q_.front()[pos_];
	}

	void pop() {
		if (pos_+1 == q_.front().size()) {
			q_.pop_front();
			pos_ = 0;
		} else {
			pos_ += 1;
		}
	}

private:
	std::list<std::vector<T>> q_;
	size_t pos_;
};

#endif
