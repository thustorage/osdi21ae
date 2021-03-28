#if !defined(_NR_H_)
#define _NR_H_

#include "nap_common.h"
#include "rw_lock.h"
#include "topology.h"

thread_local char buf[4096];

template <class T> class NR {

  struct alignas(64) PerNode {
    nap::WRLock lock;
    std::atomic<uint64_t> local_tail;
    uint64_t padding[8];
    char *per_thread_slot[nap::Topology::kCorePerNuma][8];

    PerNode() {
      local_tail.store(0);
      for (int i = 0; i < nap::Topology::kCorePerNuma; ++i) {
        per_thread_slot[i][0] = nullptr;
      }
    }
  };

private:
  char *log;
  std::atomic<uint64_t> tail;
  uint64_t padding[8];
  PerNode meta[nap::Topology::kNumaCnt];

public:
  NR() {
    log = (char *)malloc(800 * 1024 * 1024);
    tail.store(0);
  }

  constexpr static int kKeySize = 15;
  bool try_write(T *raw_index, const char *key) {
    auto &local_meta = meta[nap::Topology::numaID()];
    int offset = nap::Topology::threadID() % nap::Topology::kCorePerNuma;

    local_meta.per_thread_slot[offset][0] = (char *)key;

  re_lock:
    if (!local_meta.lock.try_wLock()) {
      if (local_meta.per_thread_slot[offset][0] == nullptr) {
        return true;
      }
      goto re_lock;
    }

    int buf_size = 0;
    for (int i = 0; i < nap::Topology::kCorePerNuma; ++i) {
      auto &slot = local_meta.per_thread_slot[i][0];
      if (slot != nullptr) {
        memcpy(buf + buf_size, key, kKeySize);
        buf_size += kKeySize;
        slot = nullptr;
      }
    }

  retry:
    uint64_t now = tail.load(std::memory_order_relaxed);
    uint64_t target = now + buf_size;

    if (!tail.compare_exchange_strong(now, target)) {
      goto retry;
    }

    local_meta.local_tail.store(target, std::memory_order_release);

    local_meta.lock.wUnlock();

    // copy shared log
    memcpy(log + now, buf, buf_size);

    for (uint64_t i = now; i < target; i += kKeySize) {
      raw_index->put(nap::Slice(log + i, kKeySize), nap::Slice(buf, 8), true);
    }

    return 0;
  }

  void try_read(T *raw_index, const char *key) {

    auto &local_meta = meta[nap::Topology::numaID()];
    uint64_t read_tail = tail.load(std::memory_order_acquire);

    if (local_meta.local_tail.load(std::memory_order_acquire) < read_tail) {
      local_meta.lock.wLock();

      auto start = local_meta.local_tail.load(std::memory_order_acquire);
      if (start < read_tail) {

        for (uint64_t i = start; i < read_tail; i += kKeySize) {
          raw_index->put(nap::Slice(log + i, kKeySize), nap::Slice(buf, 8),
                         true);
        }

        local_meta.local_tail.store(read_tail, std::memory_order_release);
      }

      local_meta.lock.wUnlock();
    }

    std::string s;
    raw_index->get(nap::Slice(key, kKeySize), s);
  }
};

#endif // _NR_H_
