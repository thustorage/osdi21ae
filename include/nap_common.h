#if !defined(_NAP_COMMON_H_)
#define _NAP_COMMON_H_

#include "slice.h"
#include <string>

#define FIX_8_BYTE_VALUE
// #define SUPPORT_RANGE

namespace nap {

enum WhereIsData : char {
  IN_RAW_INDEX,
  IN_PREVIOUS_EPOCH,
  IN_CURRENT_EPOCH,
};

using NapPair = std::pair<std::string, WhereIsData>;

constexpr int kCachelineSize = 64;
constexpr int kMaxNumaCnt = 8;
constexpr int kMaxThreadCnt = 80;

constexpr int kHotKeys = 100000;

class CowAlloctor;
extern CowAlloctor *cow_alloc;


inline void mfence() { asm volatile("mfence\n" : : : "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }



inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void show_string(const std::string &s, bool end = true) {
  for (size_t i = 0; i < s.length(); ++i) {
    printf("%d ", (int)s[i]);
  }
  printf("@");
  if (end) {
    printf("\n");
  }
}

inline void show_string(const Slice &s, bool end = true) {
  show_string(s.ToString(), end);
}

} // namespace nap

#endif // _NAP_COMMON_H_
