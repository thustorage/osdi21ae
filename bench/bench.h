#if !defined(_BENCH_H_)
#define _BENCH_H_

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <cstdio>
#include <iterator>
#include <sstream>
#include <thread>
#include <time.h>
#include <vector>

#include <gperftools/profiler.h>

#define LOAD_CROSS_NUMA
// #define ENABLE_NAP
// #define GLOBAL_VERSION

// #define PCMM
// #define PCMNUMA

// #define SWITCH_TEST
// #define USE_GLOBAL_LOCK
// #define RANGE_BENCH

// #define RECOVERY_TEST

#define WARMUP_FILE "/home/wq/Nap/dataset/warmup"

#include "index/cceh_NUMA.hpp"
#include "index/clevel_hash_NUMA.hpp"
#include "index/fast_fair.h"
#include "index/level_hash_NUMA.hpp"
#include "index/masstree.h"
#include "latency_evaluation.h"

#include "index/mock_index.h"
#include "nap.h"
#include "slice.h"

#define LAYOUT "NAP_RAW_INDEX"
#define KEY_LEN 15
#define VALUE_LEN 8

#ifdef SWITCH_TEST
#define READ_WRITE_NUM (128 * 1000ull * 1000)
#else
#define READ_WRITE_NUM (64 * 1000ull * 1000)

#endif

thread_local uint64_t cur_value = 1;
thread_local uint8_t thread_local_buffer[4096];

inline void next_thread_id_for_load(int thread_num) {
  static int cur = 0;
  cur++;

#ifdef LOAD_CROSS_NUMA
#ifdef ENABLE_NAP
  int firstNUMA = thread_num + 1;
#else
  int firstNUMA = thread_num;
#endif
  int numa_cnt = (firstNUMA + nap::Topology::kCorePerNuma - 1) /
                 nap::Topology::kCorePerNuma;

  my_thread_id = (cur % numa_cnt) * nap::Topology::kCorePerNuma;

#endif
}

#endif // _BENCH_H_
