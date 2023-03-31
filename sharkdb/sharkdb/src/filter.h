
#ifndef _BLOOM_FILTER_H_
#define _BLOOM_FILTER_H_

#include <cstdint>
#include <bitset>

template <uint32_t PRIME>
static uint64_t hash_key(const char* k) {
    uint64_t hash = 0;
    for (size_t i = 0; i<SHARKDB_KEY_BYTES/sizeof(uint64_t); ++i) {
        uint64_t view = *(i + ((uint64_t*) k));
        hash += view % PRIME;
        hash <<= 1;
    }
    return hash;
}

class bloom_filter_t {
public:
    bloom_filter_t(size_t n_bits) : n_bits_(n_bits) {
        buf_ = new uint64_t[n_bits];
    }
    ~bloom_filter_t() {
        delete[] buf_;
    }

    bool test(const char* k) {
        return test_single(hash_key<PRIMES[0]>(k)) && test_single(hash_key<PRIMES[1]>(k));
    }

    void set(const char* k) {
        set_single(hash_key<PRIMES[0]>(k));
        set_single(hash_key<PRIMES[1]>(k));
    }

private:
    uint64_t* buf_;
    size_t n_bits_;

    // https://t5k.org/lists/small/millions/
    static constexpr uint32_t PRIMES[2] = {858599509, 961748941};

    bool test_single(size_t p) {
        p %= n_bits_;
        return (buf_[p / 64] & (1ULL << (p % 64))) != 0;
    }

    void set_single(size_t p) {
        p %= n_bits_;
        buf_[p / 64] |= 1ULL << (p % 64);
    }
};

#endif
