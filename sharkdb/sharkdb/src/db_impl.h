
#ifndef _DB_IMPL_H_
#define _DB_IMPL_H_

#define TBB_PREVIEW_CONCURRENT_ORDERED_CONTAINERS 1

#include <string.h>
#include <tbb/concurrent_map.h>

class db_t {
public:
    db_t();

private:
    tbb::concurrent_map<std::string, std::string> ks;
};

#endif
