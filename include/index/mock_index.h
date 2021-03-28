#if !defined(_MOCK_INDEX_)
#define _MOCK_INDEX_

#include "murmur_hash2.h"
#include "rw_lock.h"
#include "slice.h"
#include "timer.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace nap {

class MockIndex {

  struct alignas(kCachelineSize) MockLock {
    WRLock l;
  };

public:
  void put(const Slice &key, const Slice &value, bool is_update) {
    auto p = MurmurHash64A(key.data(), key.size()) % kPartition;
    (void)p;


    Timer::sleep(1000);
    // locks[p].l.wLock();

    // // if (*(uint64_t *)key.data() == 1) {
    // // 	printf("$%d [1] val = %ld\n", Topology::threadID(),
    // // 	       *(uint64_t *)value.data());
    // // }

    // kv[p][key.ToString()] = value.ToString();

    // locks[p].l.wUnlock();
  }

  bool get(const Slice &key, std::string &value) {
    auto p = MurmurHash64A(key.data(), key.size()) % kPartition;
    (void)p;

     Timer::sleep(500);
    // locks[p].l.rLock();

    // auto key_str = key.ToString();
    // auto it = kv[p].find(key_str);
    // if (it == kv[p].end()) {
    //   locks[p].l.rUnlock();
    //   return false;
    // }

    // value = it->second;
    // locks[p].l.rUnlock();
    return true;
  }

  void del(const Slice &key) {}

private:
  static const int kPartition = 1024;
  std::unordered_map<std::string, std::string> kv[kPartition];
  MockLock locks[kPartition];
};

} // namespace nap

#endif // _MOCK_INDEX_
