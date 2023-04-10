
#include <sys/resource.h>
#include <cstdio>
#include <cassert>

int main() {
    // get the available locked memory...
    struct rlimit mlock_lim;
    int rc = getrlimit(RLIMIT_MEMLOCK, &mlock_lim);
    assert(rc == 0);
    printf("%lu\n", mlock_lim.rlim_cur);
}
