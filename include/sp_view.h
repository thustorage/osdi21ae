#if !defined(_SP_VIEW_H_)
#define _SP_VIEW_H_

#include "murmur_hash2.h"
#include "nap_common.h"
#include "nvm.h"
#include "slice.h"
#include "topology.h"

#include "cow_alloctor.h"

#include <vector>

namespace nap {

struct alignas(kCachelineSize) AlignedCounter {
  std::atomic<uint64_t> ver;
};

inline uint64_t get_version(const Slice &s) {
  static AlignedCounter ver[128];

  int p = MurmurHash64A(s.data(), s.size()) % 128;

  return ver[p].ver.fetch_add(1, std::memory_order::memory_order_relaxed);
}

class SPView {
  friend class NapMeta;

public:
  SPView() : size(0) { memset(&array, 0, sizeof(array)); }

  SPView(const std::vector<std::pair<std::string, WhereIsData>> &list)
      : size(list.size()) {
    if (list.empty()) {
      return;
    }

    size_t key_total_length = 0;
    for (size_t i = 0; i < list.size(); ++i) {
      key_total_length += list[i].first.size();
    }

    pmem::obj::persistent_ptr<SPPair[]> array_p[Topology::kNumaCnt];
    pmem::obj::persistent_ptr<char[]> keys_p[Topology::kNumaCnt];

    for (int i = 0; i < Topology::kNumaCnt; ++i) {
      pmem::obj::transaction::manual tx(*Topology::pmdk_pool_at(i));
      array_p[i] = pmem::obj::make_persistent<SPPair[]>(list.size());
      keys_p[i] = pmem::obj::make_persistent<char[]>(key_total_length);
      pmem::obj::transaction::commit();
    }

    for (int k = 0; k < Topology::kNumaCnt; ++k) {
      array[k] = array_p[k].get();
      auto keys_start = keys_p[k].get();
      for (size_t i = 0; i < list.size(); ++i) {
        auto k_len = list[i].first.size();
        array_p[k][i].k_size = k_len;
        array_p[k][i].k = keys_start;
#ifdef FIX_8_BYTE_VALUE
        array_p[k][i].type = 2;
#else
        array_p[k][i].v.v_ptr = nullptr;
#endif
        memcpy(keys_start, list[i].first.c_str(), k_len);
        keys_start += k_len;
      }
      Topology::pmdk_pool()->persist(array_p[k]);
      Topology::pmdk_pool()->persist(keys_p[k]);
    }
  }

  ~SPView() {
    for (int i = 0; i < Topology::kNumaCnt; ++i) {
      if (array[i]) {
        PMEMoid oid = pmemobj_oid(array[i]);
        pmemobj_free(&oid);

#ifndef FIX_8_BYTE_VALUE
        for (size_t j = 0; j < size; ++j) {
          if (array[i][j].v.v_ptr) {
            cow_alloc.free(array[i][j].v.v_ptr);
          }
        }
#endif

        oid = pmemobj_oid(array[i][0].k);
        pmemobj_free(&oid);
      }
    }
  }

  struct AllocBuffer {
    uint32_t size;
    char *buf;

    AllocBuffer() : size(0), buf(nullptr) {}
  };

  constexpr static int kAllocBufferSize = 4;
  AllocBuffer *get_thread_local_alloc_buf() {
    static thread_local AllocBuffer free_array[kAllocBufferSize];
    return free_array;
  }

  char *alloc_before_update(const Slice &key, const Slice &value) {

#ifdef FIX_8_BYTE_VALUE
    return nullptr;
#else

    auto buf_size = value.size() + sizeof(uint64_t) + sizeof(uint32_t);
    char *raw_ptr = nullptr;

    auto *free_array = get_thread_local_alloc_buf();

    for (int i = 0; i < kAllocBufferSize; ++i) {
      if (free_array[i].size >= buf_size) {
        raw_ptr = free_array[i].buf;
        free_array[i].size = 0;
        free_array[i].buf = nullptr;
        break;
      }
    }

    if (!raw_ptr) {
      raw_ptr = (char *)cow_alloc.malloc(buf_size);
    }

    return raw_ptr;

#endif
  }

  void update(int index, char *ptr, const Slice &key, const Slice &value,
              uint64_t new_version, bool is_del = false) {

#ifdef GLOBAL_VERSION
    uint64_t v = get_version(key);
#else
    uint64_t v = new_version;
#endif

   if (is_del) {
     new_version = (1ull << 63) || new_version;
   }

#ifdef FIX_8_BYTE_VALUE

    // leverage in cache-line ordering, two-incarnation toggle mechanism
    auto &e = array[Topology::numaID()][index];
    uint8_t idx = e.type == 2 ? 0 : ((e.type + 1) % 2);

    e.ver[idx] = v;
    e.v64[idx] = *(uint64_t *)value.data();

    compiler_barrier();
    e.type = idx;

    persistent::clwb(&e.type);
    persistent::persistent_barrier();
#else

    auto buf_size = value.size() + sizeof(uint64_t) + sizeof(uint32_t);

    *(uint64_t *)ptr = v;
    *(uint32_t *)(ptr + sizeof(uint64_t)) = value.size();
    memcpy(ptr + sizeof(uint64_t) + sizeof(uint32_t), value.data(),
           value.size());

    persistent::clflushopt_range(ptr, buf_size);

    auto &e = array[Topology::numaID()][index];
    auto *free_array = get_thread_local_alloc_buf();
    if (e.v.v_ptr) {

      char *freed_ptr = e.v.v_ptr;
      uint32_t free_size = *(uint32_t *)(e.v.v_ptr + sizeof(uint64_t)) +
                           sizeof(uint64_t) + sizeof(uint32_t);
      for (int i = 0; i < kAllocBufferSize; ++i) {
        if (free_array[i].size < free_size) {
          freed_ptr = free_array[i].buf;
          free_array[i].size = free_size;
          free_array[i].buf = e.v.v_ptr;
          break;
        }
      }

      if (freed_ptr) {
        cow_alloc.free(freed_ptr);
      }
    }

    e.v.v_ptr = ptr;
    persistent::clwb_range(&e.v, sizeof(void *));

#endif
    // persistent::clflush(&e.v);
    // Topology::pmdk_pool()->persist(&e.v, sizeof(void *));

    // CHECK
    // assert(e.k_size = key.size());
    // assert(memcmp(e.k, key.data(), key.size()) == 0);
  }
  

  // merge per-NUMA PM-resident PC-view into the raw index
  template <class T> void flush_to_raw_index(T *raw_index) {
    auto keys = array[0];
    for (size_t i = 0; i < size; ++i) {

      uint64_t v_max = 0;

#ifdef FIX_8_BYTE_VALUE
      uint64_t v = (uint64_t)(-1);
#else
      SPValue v;
#endif
      for (int k = 0; k < Topology::kNumaCnt; ++k) {
#ifdef FIX_8_BYTE_VALUE
        auto idx = array[k][i].type;
        if (idx == 2) {
          continue;
        }
        auto cur_val = array[k][i].v64[idx];
        auto cur_ver = array[k][i].ver[idx];
#else
        auto &cur_val = array[k][i].v;
        if (cur_val.v_ptr == nullptr) {
          continue;
        }
        auto cur_ver = cur_val.get_version();
#endif

        if (cur_ver > v_max) {
          v_max = cur_ver;
          v = cur_val;
        }
      }

#ifdef FIX_8_BYTE_VALUE
      if (v != (uint64_t)(-1)) {
        raw_index->put(Slice(keys[i].k, keys[i].k_size),
                       Slice((char *)&v, sizeof(uint64_t)), true);
      }
#else
      if (v.v_ptr) {
        raw_index->put(Slice(keys[i].k, keys[i].k_size),
                       Slice(v.get_val(), v.get_size()), true);
      }
#endif
    }
  }

private:
  struct __attribute__((__packed__)) SPValue {
    char *v_ptr;

    // SPValue() : v_ptr(nullptr) {}

    uint64_t get_version() { return *(uint64_t *)v_ptr; }

    uint32_t get_size() { return *(uint32_t *)(v_ptr + sizeof(uint64_t)); }

    char *get_val() { return v_ptr + sizeof(uint64_t) + sizeof(uint32_t); }
  };
  static_assert(sizeof(SPValue) == 8, "XX");

  struct __attribute__((__packed__)) SPPair {
    char *k;
    uint32_t k_size;
    uint32_t padding[3];
    union {
      struct {
        SPValue v;
      };
      struct {
        uint64_t type; // 0,1: vaild; 2: nullptr;
        uint64_t ver[2];
        uint64_t v64[2];
      };
    };
  };

  static_assert(sizeof(SPPair) == 64, "XX");

  SPPair *array[Topology::kNumaCnt];
  size_t size;
};

} // namespace nap

#endif // _SP_VIEW
