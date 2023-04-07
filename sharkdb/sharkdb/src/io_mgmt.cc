
#include "consts.h"
#include "db_impl.h"

#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>

void* io_mgmt_thr_body(void* arg) {
	// periodically spin to see if anything to do.
	// namespace lock should be held if any fsync in progress.
	return nullptr;
}
