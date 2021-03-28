
#include <atomic>
#include <iostream>
#include <thread>

#include "nvm.h"
#include "timer.h"

// g++ aep_raw.cpp -pthread -std=c++11 -march=native -DNDEBUG -O3

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

void bindCore(uint16_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    printf("can't bind core %d!", core);
    exit(-1);
  }
}

#define _GB_ (1024ull * 1024 * 1024)

// #define DRAM
// #define RANDOM

const int io_size = 64;
const uint64_t kTraceSize = 1024000;
uint64_t kAEPSize = 2 * _GB_;
const std::string dev_name = "/dev/dax0.0";
int kThread = 1;
bool is_write = 1;
bool is_remote = 1;

char *trace[kTraceSize];
char *nvm_space;
// char *buffer;

void warm_up() {
  printf("warm up...\n");
  memset(nvm_space, 43, kAEPSize);
}

void gen_trace() {

#ifndef RANDOM
  printf("gen seq trace...\n");
#else
  printf("gen random trace...\n");
#endif

  // unsigned int seed = asm_rdtsc();

  // uint64_t uint_cnt = kAEPSize / io_size - 1;
  for (size_t i = 0; i < kTraceSize; ++i) {

#ifndef RANDOM

    trace[i] = nvm_space + i * io_size;
#else

    trace[i] = nvm_space + (rand_r(&seed) % uint_cnt) * io_size;
#endif
  }
}

#include <cstdint>
#include <immintrin.h>
/* ... */
void fastMemcpy(void *pvDest, void *pvSrc, size_t nBytes) {
  assert(nBytes % 32 == 0);
  assert((intptr_t(pvDest) & 31) == 0);
  assert((intptr_t(pvSrc) & 31) == 0);
  const __m256i *pSrc = reinterpret_cast<const __m256i *>(pvSrc);
  __m256i *pDest = reinterpret_cast<__m256i *>(pvDest);
  int64_t nVects = nBytes / sizeof(*pSrc);
  for (; nVects > 0; nVects--, pSrc++, pDest++) {
    const __m256i loaded = _mm256_stream_load_si256(pSrc);
    _mm256_stream_si256(pDest, loaded);
  }
  _mm_sfence();
}

void test_latency() {

  printf("test latency...\n");

  nap::Timer timer;
  char buf[io_size];

  uint64_t cycle = 0;

  uint64_t start, end;
  (void)start, (void)end;

  for (size_t i = 0; i < kTraceSize; ++i) {
    timer.begin();
    memcpy(buf, trace[i], io_size);

    cycle += timer.end();
    persistent::mfence();
  }
  // timer.end_print(kTraceSize);
  printf("%ld ns\n", cycle / kTraceSize);
}

void test_bandwidth_single() {
  nap::Timer timer;

  timer.begin();
  memset(nvm_space, 43, kAEPSize);
  auto cap = timer.end();

  printf("bandwidth: %f\n",
         kAEPSize / 1024.0 / 1024 / 1024 / (cap / 1000.0 / 1000 / 1000));
}

std::atomic<bool> is_test{false};
std::atomic<int> th_barrier{0};
double tp[48];
void test_write_bandwidth_thread(int id, char *start) {

  if (is_remote) {
    bindCore(id + 18);
  } else {
    bindCore(id);
  }

  char *buffer;
  const uint64_t buffer_size = 4 * 1024;
  auto ret = posix_memalign((void **)&buffer, 4096, buffer_size);
  (void)ret;

  assert(!ret);

  nap::Timer timer;
  th_barrier.fetch_add(1);
  while (th_barrier.load() != kThread) {
  }

  timer.begin();

  if (is_write) {
    for (size_t i = 0; i < kAEPSize / buffer_size; ++i) {
      fastMemcpy(start + buffer_size * i, buffer, buffer_size);
    }
  } else {
    for (size_t i = 0; i < kAEPSize / buffer_size; ++i) {
      memcpy(buffer, start + buffer_size * i, buffer_size);
    }
  }

  th_barrier.fetch_add(-1);

  auto cap = timer.end();

  auto bw = kAEPSize / 1024.0 / 1024 / 1024 / (cap / 1000.0 / 1000 / 1000);
  tp[id] = bw;
}

int main(int argc, char *argv[]) {

  if (argc != 4) {
    printf("Usage: ./aep_raw thread_nr is_write is_remote\n");

    exit(-1);
  }

  kThread = std::atoi(argv[1]);
  is_write = std::atoi(argv[2]);
  is_remote = std::atoi(argv[3]);

  if (is_remote && is_write && kThread >= 8) { // reduce test time
    kAEPSize = kAEPSize / 8;
  }

  // printf("thread %d\n", kThread);

#ifndef DRAM
  // printf("open aep..\n");
  nvm_space = persistent::alloc_nvm(kThread * kAEPSize, dev_name);
#else
  // printf("open dram..\n");
  nvm_space = persistent::alloc_dram(kThread * kAEPSize);
#endif

  // buffer = persistent::alloc_dram(kAEPSize);

  // warm_up();
  // gen_trace();

  std::thread th[64];

  for (int i = 0; i < kThread; ++i) {
    th[i] =
        std::thread(test_write_bandwidth_thread, i, nvm_space + i * kAEPSize);
  }

  for (int i = 0; i < kThread; ++i) {
    th[i].join();
  }

  is_test = true;

  for (int i = 0; i < kThread; ++i) {
    th[i] =
        std::thread(test_write_bandwidth_thread, i, nvm_space + i * kAEPSize);
  }

  for (int i = 0; i < kThread; ++i) {
    th[i].join();
  }

  double total_tp = 0;
  for (int i = 0; i < kThread; ++i) {
    total_tp += tp[i];
  }
  printf("%f\n", total_tp);

  // test_bandwidth_single();
}
