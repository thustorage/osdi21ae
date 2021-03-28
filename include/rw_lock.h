#ifndef __WRLOCK_H__
#define __WRLOCK_H__

#include <atomic>

#include "nap_common.h"
#include "topology.h"

#define CAS(ptr, oldval, newval)                                               \
  (__sync_bool_compare_and_swap(ptr, oldval, newval))

namespace nap {

struct ReadFirendlyLock {
  volatile uint64_t locks[kMaxThreadCnt][8];

  ReadFirendlyLock() {
    for (int i = 0; i < kMaxThreadCnt; ++i) {
      locks[i][0] = 0;
    }
  }

  bool write_lock() {
    for (int i = 0; i < kMaxThreadCnt; ++i) {
      while (!CAS(&locks[i][0], 0, 1)) {
      }
    }
    return true;
  }

  bool read_lock() {
    while (!CAS(&locks[Topology::threadID()][0], 0, 1)) {
    }
    return true;
  }

  void write_unlock() {
    compiler_barrier();
    for (int i = 0; i < kMaxThreadCnt; ++i) {
      locks[i][0] = 0;
    }
  }

  void read_unlock() {
    compiler_barrier();
    locks[Topology::threadID()][0] = 0;
  }
};

class WRLock {

private:
  std::atomic<uint16_t> l;
  const static uint16_t UNLOCKED = 0x0;
  const static uint16_t LOCKED = 0x1;
  const static uint16_t PUT_LOCKED = 0x3;

public:
  WRLock() { init(); }

  void operator=(const WRLock &o) { l.store(o.l.load()); }

  bool is_unlock() { return l.load() == UNLOCKED; }

  void init() { l.store(UNLOCKED); }

  void wLock() {
    while (true) {
      while (l.load(std::memory_order_relaxed) != UNLOCKED) {
        ;
      }

      uint16_t f = UNLOCKED;
      if (l.compare_exchange_strong(f, LOCKED)) {
        break;
      }
    }
  };

  bool try_wLock() {
    if (l.load(std::memory_order_relaxed) != UNLOCKED)
      return false;

    uint16_t f = UNLOCKED;
    return l.compare_exchange_strong(f, LOCKED);
  }

  void putLock() {
    while (true) {
      while (l.load(std::memory_order_relaxed) != UNLOCKED) {
        ;
      }

      uint16_t f = UNLOCKED;
      if (l.compare_exchange_strong(f, PUT_LOCKED)) {
        break;
      }
    }
  };

  bool try_putLock(bool &is_writer) {
    uint16_t v;
    if ((v = l.load(std::memory_order_relaxed)) != UNLOCKED) {
      is_writer = v == PUT_LOCKED;
      return false;
    }

    uint16_t f = UNLOCKED;
    auto res = l.compare_exchange_strong(f, PUT_LOCKED);
    is_writer = f == PUT_LOCKED;

    return res;
  }

  void rLock() {
    while (true) {
      uint16_t v;
      while (((v = l.load(std::memory_order_relaxed)) & 0x1) == LOCKED) {
        ;
      }

      uint16_t b = v + 4;

      if (l.compare_exchange_strong(v, b)) {
        break;
      }
    }
  }

  bool try_rLock() {
  retry:
    uint16_t v = l.load(std::memory_order_relaxed);
    if ((v & 0x1) == LOCKED)
      return false;

    uint16_t b = v + 4;
    if (!l.compare_exchange_strong(v, b)) {
      goto retry; // concurrent reader;
    }

    return true;
  }

  void rUnlock() {
    while (true) {
      uint16_t v = l.load();
      uint16_t b = v - 4;

      if (l.compare_exchange_strong(v, b)) {
        break;
      }
    }
  }

  void wUnlock() { l.store(UNLOCKED, std::memory_order_release); }

  void putUnlock() { l.store(UNLOCKED, std::memory_order_release); }
};

} // namespace nap

#endif /* __FAIRWRLOCK_H__ */