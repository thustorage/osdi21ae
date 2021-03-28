#if !defined(_TOPOLOGY_H_)
#define _TOPOLOGY_H_

#include <atomic>

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
#include <libpmemobj++/experimental/v.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

#include "nap_common.h"

inline void bindCore(uint16_t core) {

  // printf("bind to %d\n", core);
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    printf("can't bind core %d!", core);
    exit(-1);
  }
}


extern int numa_map[nap::kMaxThreadCnt];
namespace nap {

extern pmem::obj::pool_base nap_pop_numa[kMaxNumaCnt];

class Topology {

  static std::atomic<int> counter;

public:
  constexpr static int kNumaCnt = 4;
  constexpr static int kCorePerNuma = 18;
  static int threadID() {

    thread_local static int my_id = counter.fetch_add(1);
    return my_id;
  }

  static void reset() {
    counter.store(1);
    // id 0 is shift thread
  }

  static int numaID() { return threadID() / kCorePerNuma; }

  static pmem::obj::pool_base *pmdk_pool() { return nap_pop_numa + numaID(); }

  static pmem::obj::pool_base *pmdk_pool_at(int numa_id) {
    return nap_pop_numa + numa_id;
  }
};
} // namespace nap

#endif // _TOPOLOGY_H_
