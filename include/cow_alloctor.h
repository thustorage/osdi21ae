#if !defined(_COW_ALLOCTOR)
#define _COW_ALLOCTOR

#include "nap_common.h"
#include "nvm.h"
#include "topology.h"

#include <bitset>
#include <cassert>
#include <cstdio>
#include <list>

namespace nap {

namespace limit {
constexpr uint64_t k64All = 0xffffffffffffffffull;
constexpr uint64_t k32All = 0xffffffff;
constexpr uint64_t k16All = 0xffff;

} // namespace limit

constexpr size_t kSlabClass = 4;
constexpr size_t kSlabSizes[kSlabClass] = {64, 128, 256, 512};
constexpr uint64_t kFullArray[kSlabClass] = {limit::k64All, limit::k64All,
                                             limit::k32All, limit::k16All};
constexpr size_t kBlockSize = 1024 * 1024;
constexpr size_t kPageSize = 8 * 1024;

enum SlabClass {
  _64_B = 0,
  _128_B,
  _256_B,
  _512_B,
};

struct SlabPage {

  uint64_t cls;
  uint64_t bitmap_1;
  uint64_t bitmap_2;

  SlabPage(uint8_t cls) : cls(cls) {

    // the first slab is header.
    bitmap_1 = 0x1;
    bitmap_2 = 0x0;
  }

  char *alloc(bool &need_del) {

    int pos = -1;
    if (cls == _64_B && bitmap_1 == limit::k64All) {
      pos = __builtin_ffsll(~bitmap_2) - 1;
      bitmap_2 |= 1ull << pos;
      need_del = (bitmap_1 == limit::k64All) && (bitmap_2 == limit::k64All);

      pos += 64;
    } else {

      pos = __builtin_ffsll(~bitmap_1) - 1;
      bitmap_1 |= 1ull << pos;
      need_del = bitmap_1 == kFullArray[cls];
    }

    assert(pos != -1);

    return (char *)this + pos * kSlabSizes[cls];
  }

  static void free(char *addr) {
    SlabPage *page = (SlabPage *)((uint64_t)addr & (~(kPageSize - 1)));
    size_t offset = (uint64_t)addr & (kPageSize - 1);

    int pos = offset / kSlabSizes[page->cls];

    assert(pos != 0);
    if (pos >= 64) {
      assert(page->cls == _64_B);
      page->bitmap_2 &= ~(1ull << (pos - 64)); // clear
    } else {
      page->bitmap_1 &= ~(1ull << pos); // clear
    }
  }

} __attribute__((packed));

static_assert(sizeof(SlabPage) < 64, "XXX");

struct BlockMeta {
  char *block_addr;
  uint64_t bitmap[2];

  BlockMeta(char *addr) : block_addr(addr) { bitmap[0] = bitmap[1] = 0x0; }

  char *get_new_page(bool &is_del) {
    int pos = -1;
    if (bitmap[0] != limit::k64All) {
      pos = __builtin_ffsll(~bitmap[0]) - 1;
      bitmap[0] |= 1ull << pos;
    } else {
      pos = __builtin_ffsll(~bitmap[1]) - 1;
      bitmap[1] |= 1ull << pos;

      pos += 64;
    }
    assert(pos != -1);

    is_del = (bitmap[0] == limit::k64All) && (bitmap[1] == limit::k64All);

    return block_addr + pos * kPageSize;
  }
};

// stored in NVM
struct alignas(kCachelineSize) CowMeta {
  char *log;       // uint64_t block cnt | [block address arrays]
  PMEMoid buf_ptr; // to avoid memory leak

  void init() {
    *(uint64_t *)log = 0;
    buf_ptr = {0, 0};
  }

  PMEMoid *oid_at(int k) { return (PMEMoid *)(log + sizeof(uint64_t)) + k; }

  void add_oid(PMEMoid oid) {
    uint64_t new_cnt = *(uint64_t *)log;
    auto *p = oid_at(new_cnt);
    *p = oid;
    persistent::clwb_range(p, sizeof(PMEMoid));

    *(uint64_t *)log = new_cnt + 1;
    persistent::clflush(log);
  }
};

class BlockManager {
public:
  BlockManager(CowMeta *cow_meta) : meta(cow_meta) { meta->init(); }

  char *get_new_page(uint8_t cls) {
    if (free_blocks.empty()) {
      if (pmemobj_alloc(Topology::pmdk_pool()->handle(), &meta->buf_ptr,
                        kBlockSize + 2 * kPageSize, 0, nullptr, nullptr)) {
        fprintf(stderr, "fail to alloc nvm\n");
        exit(-1);
      }

      // printf("new block\n");

      meta->add_oid(meta->buf_ptr);

      auto *ptr = (char *)pmemobj_direct(meta->buf_ptr);

      ptr = (char *)(((uint64_t)ptr + kPageSize - 1) & (~(kPageSize - 1)));

      BlockMeta bm(ptr);
      free_blocks.push_back(bm);
    }

    bool is_del = false;
    auto res = free_blocks.back().get_new_page(is_del);
    if (is_del) {
      free_blocks.pop_back();
    }

    // printf("new page\n");

    return res;
  }

private:
  CowMeta *meta;
  std::list<BlockMeta> free_blocks;
};

class SlabManager {
public:
  SlabManager(BlockManager *blk_mgt) : blk_mgt(blk_mgt) {}

  char *alloc(size_t size) {
    if (size >= kSlabSizes[kSlabClass - 1]) {
      fprintf(stderr,
              "can not allocte PM for CoW, the max supported size is %lu\n",
              kSlabSizes[kSlabClass - 1]);
      exit(1);
    }

    uint8_t cls = 0;
    for (size_t i = 0; i < kSlabClass; ++i) {
      if (size < kSlabSizes[i]) {
        cls = i;
        break;
      }
    }

    if (free_list[cls].empty()) {
      char *page_base = blk_mgt->get_new_page(cls);

      assert((uint64_t)page_base % kPageSize == 0);

      SlabPage *page = new (page_base) SlabPage(cls);

      free_list[cls].push_back(page);
    }

    bool need_del = false;
    auto res = free_list[cls].back()->alloc(need_del);

    if (need_del) {
      free_list[cls].pop_back();
    }

    return res;
  }

private:
  BlockManager *blk_mgt;

  std::list<SlabPage *> free_list[kSlabClass];
};

class CowAlloctor {

private:
  BlockManager *blk_mgt[kMaxThreadCnt];
  SlabManager *slab_mgt[kMaxThreadCnt];

public:
  CowAlloctor(CowMeta *meta) {
    for (int k = 0; k < kMaxThreadCnt; ++k) {
      blk_mgt[k] = new BlockManager(meta + k);
      slab_mgt[k] = new SlabManager(blk_mgt[k]);
    }
  }

  void *malloc(size_t size) {
    return slab_mgt[Topology::threadID()]->alloc(size);
  }

  void free(void *addr) { SlabPage::free((char *)addr); }
};

} // namespace nap

#endif // _COW_ALLOCTOR
