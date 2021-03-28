#ifndef _NVM_H_
#define _NVM_H_

#include <emmintrin.h>
#include <stdint.h>
#include <string.h>
#include <xmmintrin.h>

#include <cassert>
#include <fcntl.h>
#include <string.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ndctl create-namespace -fe namespace0.0 -m devdax

namespace persistent {

const int kCachelineSize = 64;

// std::string default_dev("/dev/dax0.0");
inline char *
alloc_nvm(size_t size,
          const std::string &dev_name = std::string(("/dev/dax0.0"))) {
  int fd = open(dev_name.c_str(), O_RDWR);

  assert(fd > 0);
  void *pmem = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED , fd, 0);

  assert(pmem != nullptr);
  return (char *)pmem;
}

inline char *alloc_dram(size_t size) {
  void *pmem = mmap(NULL, size, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_ANONYMOUS, -1, 0);

  assert(pmem != nullptr);
  return (char *)pmem;
}

inline void clflush(void *addr) {
  asm volatile("clflush %0" : "+m"(*(volatile char *)(addr)));
}

inline void clwb(void *addr) {
  asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)(addr)));
}

inline void clflushopt(void *addr) {
  asm volatile(".byte 0x66; clflush %0" : "+m"(*(volatile char *)(addr)));
}

inline void persistent_barrier() { asm volatile("sfence\n" : :); }

inline void mfence() { asm volatile("mfence\n" : :); }


inline void clflush_range(void *des, size_t size) {
  char *addr = (char *)des;
  size = size + ((uint64_t)(addr) & (kCachelineSize - 1));
  for (size_t i = 0; i < size; i += kCachelineSize) {
    clflush(addr + i);
  }
}

inline void clwb_range(void *des, size_t size) {
  char *addr = (char *)des;
  size = size + ((uint64_t)(addr) & (kCachelineSize - 1));
  for (size_t i = 0; i < size; i += kCachelineSize) {
    clwb(addr + i);
  }
  persistent_barrier();
}

inline void clflushopt_range(void *des, size_t size) {
  char *addr = (char *)des;
  size = size + ((uint64_t)(addr) & (kCachelineSize - 1));
  for (size_t i = 0; i < size; i += kCachelineSize) {
    clflushopt(addr + i);
  }
  persistent_barrier();
}

inline void nt_copy(void *dst, void *src, int size) {
  __asm__ __volatile__("cmpl $8,%%edx;"
                       "jb L_4b_nocache_copy_entry;"
                       "movl %%edx,%%ecx;"
                       "andl $63,%%edx;"
                       "shrl $6,%%ecx;"
                       "L_4x8b_nocache_copy_loop:"
                       "movq (%%rsi),%%r8;"
                       "movq 1*8(%%rsi),%%r9;"
                       "movq 2*8(%%rsi),%%r10;"
                       "movq 3*8(%%rsi),%%r11;"
                       "movnti %%r8,(%%rdi);"
                       "movnti %%r9,1*8(%%rdi);"
                       "movnti %%r10,2*8(%%rdi);"
                       "movnti %%r11,3*8(%%rdi);"
                       "movq 4*8(%%rsi),%%r8;"
                       "movq 5*8(%%rsi),%%r9;"
                       "movq 6*8(%%rsi),%%r10;"
                       "movq 7*8(%%rsi),%%r11;"
                       "movnti %%r8,4*8(%%rdi);"
                       "movnti %%r9,5*8(%%rdi);"
                       "movnti %%r10,6*8(%%rdi);"
                       "movnti %%r11,7*8(%%rdi);"
                       "leaq 64(%%rsi),%%rsi;"
                       "leaq 64(%%rdi),%%rdi;"
                       "decl %%ecx;"
                       "jnz L_4x8b_nocache_copy_loop;"
                       "L_8b_nocache_copy_entry:"
                       "movl %%edx,%%ecx;"
                       "andl $7,%%edx;"
                       "shrl $3,%%ecx;"
                       "jz L_4b_nocache_copy_entry;"
                       "L_8b_nocache_copy_loop:"
                       "movq (%%rsi),%%r8;"
                       "movnti %%r8,(%%rdi);"
                       "leaq 8(%%rsi),%%rsi;"
                       "leaq 8(%%rdi),%%rdi;"
                       "decl %%ecx;"
                       "jnz L_8b_nocache_copy_loop;"
                       "L_4b_nocache_copy_entry:"
                       "andl %%edx,%%edx;"
                       "jz L_finish_copy;"
                       "movl %%edi,%%ecx;"
                       "andl $3,%%ecx;"
                       "jnz L_1b_cache_copy_entry;"
                       "movl %%edx,%%ecx;"
                       "andl $3,%%edx;"
                       "shrl $2,%%ecx;"
                       "jz L_1b_cache_copy_entry;"
                       "movl (%%rsi),%%r8d;"
                       "movnti %%r8d,(%%rdi);"
                       "leaq 4(%%rsi),%%rsi;"
                       "leaq 4(%%rdi),%%rdi;"
                       "andl %%edx,%%edx;"
                       "jz L_finish_copy;"
                       "L_1b_cache_copy_entry:"
                       "movl %%edx,%%ecx;"
                       "L_1b_cache_copy_loop:"
                       "movb (%%rsi),%%al;"
                       "movb %%al,(%%rdi);"
                       "incq %%rsi;"
                       "incq %%rdi;"
                       "decl %%ecx;"
                       "jnz L_1b_cache_copy_loop;"
                       "L_finish_copy:"
                       "xorl %%eax,%%eax;"
                       "sfence;"
                       :
                       : "d"(size), "S"(src), "D"(dst)
                       : "memory");
}

} // namespace persistent

#endif