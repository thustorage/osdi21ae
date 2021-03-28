#ifndef PMEMOBJ_CCEH_HPP
#define PMEMOBJ_CCEH_HPP

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
#include <libpmemobj++/experimental/v.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

#include "pmdk_helper.h"

#include "NUMA_Config.h"

#if LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
#include "tbb/spin_rw_mutex.h"
#else
#include <libpmemobj++/shared_mutex.hpp>
#endif

#include <libpmemobj++/detail/persistent_pool_ptr.hpp>

#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <thread>
#include <time.h>
#include <type_traits>
#include <unordered_map>

#include <atomic>

#define CAS(ptr, oldval, newval)                                               \
  (__sync_bool_compare_and_swap(ptr, oldval, newval))

struct DirLock {
  static const int kMaxLockThread = 73;
  volatile uint64_t locks[kMaxLockThread][8];

  DirLock() {
    for (int i = 0; i < kMaxLockThread; ++i) {
      locks[i][0] = 0;
    }
  }

  bool write_lock() {
    for (int i = 0; i < kMaxLockThread; ++i) {
      while (!CAS(&locks[i][0], 0, 1)) {
      }
    }
    return true;
  }

  bool read_lock() {
    while (!CAS(&locks[my_thread_id][0], 0, 1)) {
    }
    return true;
  }

  void write_unlock() {
    for (int i = 0; i < kMaxLockThread; ++i) {
      locks[i][0] = 0;
    }
  }

  void read_unlock() { locks[my_thread_id][0] = 0; }
};

DirLock dir_lock;

#if _MSC_VER
#include <intrin.h>
#include <windows.h>
#endif

namespace pmem {
namespace obj {
namespace experimental {

using namespace pmem::obj;

#if !LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
using internal::shared_mutex_scoped_lock;
#endif

class CCEH {
public:
  using key_type = uint8_t *;
  using mapped_type = uint8_t *;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = size_t;
  using difference_type = ptrdiff_t;
  using pointer = value_type *;
  using const_pointer = const value_type *;
  using reference = value_type &;
  using const_reference = const value_type &;
  using hv_type = size_t;

#if LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX
  using mutex_t = pmem::obj::experimental::v<tbb::spin_rw_mutex>;
  using scoped_t = tbb::spin_rw_mutex::scoped_lock;
#else
  using mutex_t = pmem::obj::shared_mutex;
  using scoped_t = internal::shared_mutex_scoped_lock;
#endif

  constexpr static size_type kSegmentBits = 8;
  constexpr static size_type kMask = (1 << kSegmentBits) - 1; //...1111 1111
  constexpr static size_type kShift = kSegmentBits;
  constexpr static size_type kSegmentSize = (1 << kSegmentBits) * 16 * 4;
  constexpr static size_type kNumPairPerCacheLine = 4;
  constexpr static size_type kNumCacheLine = 4;

  constexpr static size_type KNumSlot = 1024;

  /*
  hash calculator
  description:
          The hash function comes from the C++ Standard Template
  Library(STL).
  */
  class hasher {
  public:
    hv_type operator()(const key_type &key, size_type sz) {
      return std::hash<std::string>{}(
          std::string(reinterpret_cast<char *>(key), sz));
    }
  };

  // hash comparison
  class key_equal {
  public:
    bool operator()(const key_type &lhs, const key_type &rhs,
                    size_type sz) const {
      return strncmp(reinterpret_cast<const char *>(lhs),
                     reinterpret_cast<const char *>(rhs), sz) == 0;
    }
  };

  struct ret {
    bool found;
    uint8_t segment_idx;
    difference_type bucket_idx;

    ret(size_type _segment_idx, difference_type _bucket_idx)
        : found(true), segment_idx(_segment_idx), bucket_idx(_bucket_idx) {}

    ret() : found(false), segment_idx(0), bucket_idx(0) {}
  };

  /*
  data structure of KV pair
  description:
          To support variable-length items,
          our implementation stores pointers to the KV pairs in the hash
  table, instead of storing KV pairs directly, which is different from the
  original CCEH.
  */

  // int32_t all_len, int32_t key_len, key, value
  struct KV_entry {
    uint8_t *kv;

    uint8_t *get_key() { return (kv + sizeof(uint32_t) + sizeof(uint32_t)); }

    uint32_t get_key_len() { return *(uint32_t *)(kv + sizeof(uint32_t)); }
  };

  struct Slot {};

  /*
  data structure of segment
  description:
          The same type of reader/writer lock from cmap is used for
          the segment and slot locks.
  */
  struct segment {
    KV_entry slots[KNumSlot];
    // mutex_t bucket_lock[KNumSlot];
    mutex_t segment_lock;
    p<size_type> local_depth;
    p<size_type> pattern;

    segment(void) : local_depth{0} { memset(slots, 0, sizeof(slots)); }

    segment(size_type _depth) : local_depth(_depth) {
      memset(slots, 0, sizeof(slots));
    }

    ~segment(void) {}

    bool new_kv(uint8_t *old_ptr, KV_entry *kv, const key_type &key,
                size_type key_len, mapped_type value, size_type value_len,
                pool_base &pop) {
      size_t kv_buf_len = 2 * sizeof(uint32_t) + key_len + value_len;

      // persistent_ptr<char[]> tmp;
      // transaction::run(pop_numa[numa_map[my_thread_id]],
      //                  [&] { tmp = make_persistent<char[]>(kv_buf_len); });

      uint8_t *kv_ptr = (uint8_t *)index_pmem_alloc(kv_buf_len);

      auto cur = kv_ptr;
      *(uint32_t *)cur = value_len + key_len;
      cur += sizeof(uint32_t);

      *(uint32_t *)cur = key_len;
      cur += sizeof(uint32_t);

      memcpy(cur, key, key_len);
      cur += key_len;

      memcpy(cur, value, value_len);

      pop.persist(kv_ptr, kv_buf_len);

      if (!CAS(&kv->kv, old_ptr, kv_ptr)) {
        index_pmem_free(kv_ptr);
        // delete_persistent_atomic<char[]>(tmp, kv_buf_len);
        return false;
      }

      pop.persist(&kv->kv, sizeof(uint8_t *));

      return true;
    }

    size_type segment_insert(const key_type &key, size_type key_len,
                             mapped_type value, size_type value_len, size_t loc,
                             size_t key_hash, pool_base &pop) {
      if ((key_hash >> (8 * sizeof(key_hash) - local_depth)) != pattern) {
        // printf("XX\n");
        return 2;
      }

      size_type rc = 1;
      scoped_t lock_segment(segment_lock,
                            false); // true：write lock，false: read lock

      int empty_slot = -1;
      uint8_t *old_ptr = nullptr;
      for (size_type i = 0; i < kNumPairPerCacheLine * kNumCacheLine; i++) {
        auto slot_idx = (loc + i) % KNumSlot;
        // scoped_t lock_bucket(
        // 	bucket_lock[slot_idx],
        // 	true); // true：write lock，false: read
        // lock

        auto kv = slots[slot_idx];
        if (kv.kv != nullptr) {

          hv_type hv = hasher{}(kv.get_key(), key_len);
          if ((hv >> (8 * sizeof(key_hash) - local_depth)) !=
              pattern) { // invalid; lazy deletion
            // printf("invalid\n");
            if (-1 == empty_slot) {
              old_ptr = kv.kv;
              empty_slot = slot_idx;
              continue;
            }
          }

          // does not equal
          if (kv.get_key_len() != key_len ||
              !key_equal{}(key, kv.get_key(), key_len)) {
            continue;
          }

          old_ptr = kv.kv;
          empty_slot = slot_idx;

          break;

        } else if (-1 == empty_slot) {

          old_ptr = nullptr;
          empty_slot = slot_idx;
        }
      }

      if (empty_slot != -1) {
        // printf("GG\n");
        if (new_kv(old_ptr, &slots[empty_slot], key, key_len, value, value_len,
                   pop)) {
          rc = 0;
        } else {
          // printf("CXX\n");
          rc = 2;
        }
      }

      // printf("CC\n");
      lock_segment.release();
      return rc;
    }
    /*
    migrate inserted items to the new segment for segment
    split operation description: For efficient migration, we
    only copy the pointers of inserted items without
    allocating memory again.
    */
    void Insert4split(KV_entry &old_slot, size_t loc, pool_base &pop) {
      for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot_idx = (loc + i) % KNumSlot;
        if (slots[slot_idx].kv == nullptr) {
          // scoped_t lock_bucket(
          // 	bucket_lock[slot_idx],
          // 	true); // true：write
          // 	       // lock，false:
          // read lock
          // old_slot.kv = nullptr;
          slots[slot_idx] = old_slot;
          // lock_bucket.release();
          break;
        }
      }
      return;
    }

    segment **Split(pool_base &pop, hv_type key_hash) {
      using namespace std;
      scoped_t lock_segment;
      if (!lock_segment.try_acquire(segment_lock, true)) {
        // printf("write lock\n");
        return nullptr; // try to acquire
                        // segment write lock
      }

      static thread_local segment *split_segment[2];

      persistent_ptr<segment> tmp;
      transaction::run(pop_numa[numa_map[my_thread_id]], [&] {
        tmp = make_persistent<segment>(local_depth + 1);
      });
      split_segment[0] = this;
      split_segment[1] = (segment *)pmemobj_direct(tmp.raw());

      // transaction::run(pop, [&] {
      // 	split_segment = make_persistent<
      // 		persistent_ptr<segment>[]>(2);
      // 	split_segment[0] = this;
      // 	split_segment[1] =
      // make_persistent<segment>( 		local_depth +
      // 1);
      // });

      for (size_type i = 0; i < KNumSlot; i++) {
        if (slots[i].kv != nullptr) {
          hv_type hv = hasher{}(slots[i].get_key(), slots[i].get_key_len());
          if (hv & ((hv_type)1 << (sizeof(hv_type) * 8 - local_depth - 1))) {
            split_segment[1]->Insert4split(
                slots[i], (hv & kMask) * kNumPairPerCacheLine, pop);
          }
        }
      }

      pop.persist((char *)split_segment[1], sizeof(segment));
      local_depth = local_depth + 1;
      pop.persist((char *)&local_depth, sizeof(size_t));
      split_segment[0]->pattern =
          (key_hash >>
           (8 * sizeof(key_hash) - split_segment[0]->local_depth + 1))
          << 1;

      lock_segment.release();

      return split_segment;
    }
  };

  struct directory {
    static const size_type kDefaultDepth = 10;
    segment **segments;
    p<size_type> capacity;
    p<size_type> depth;
    // mutex_t m;

    directory(pool_base &pop) : directory(kDefaultDepth, pop) {}

    directory(size_type _depth, pool_base &pop) {
      depth = _depth;
      capacity = pow(2, depth);

      persistent_ptr<segment *[]> tmp;
      transaction::run(pop,
                       [&] { tmp = make_persistent<segment *[]>(capacity); });

      // printf("???\n");
      segments = (segment **)pmemobj_direct(tmp.raw());
      for (size_t i = 0; i < capacity; ++i) {
        segments[i] = nullptr;
      }
      // printf("####\n");
    }

    ~directory() {
      if (segments) {
        // FIXME
        // delete_persistent_atomic<
        // 	persistent_ptr<segment>[]>(segments,
        // 				   capacity);
      }
    }
  };

  CCEH(size_type _depth) {
    PMEMoid oid = pmemobj_oid(this);

    assert(!OID_IS_NULL(oid));

    my_pool_uuid = oid.pool_uuid_lo;

    pool_base pop = get_pool_base();

    transaction::run(pop,
                     [&] { dir = make_persistent<directory>(_depth, pop); });
    // printf("!!!\n");

    for (size_type i = 0; i < pow(2, _depth); i++) {

      persistent_ptr<segment> tmp;
      transaction::run(pop, [&] { tmp = make_persistent<segment>(_depth); });

      // if (i % 10000 == 0) {
      // 	printf("XX\n");
      // }

      dir->segments[i] = (segment *)pmemobj_direct(tmp.raw());
      dir->segments[i]->pattern = i;
    }
  }

  ret insert(const key_type &key, const mapped_type &value, size_type key_len,
             size_type value_len, size_type id) {
    pool_base pop = get_pool_base();
  STARTOVER:
    hv_type key_hash = hasher{}(key, key_len);
    size_type y = (key_hash & kMask) * kNumPairPerCacheLine;

  RETRY:
    // scoped_t lock_dir; // directory_lock
    size_type x = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
    persistent_ptr<segment> target = dir->segments[x];
    size_type rc = target->segment_insert(key, key_len, value, value_len, y,
                                          key_hash, pop);

    // sleep(1);

    if (rc == 1) // insertion failure: hash collision, need
                 // segment split
    {
      // printf("KK\n");
      // sleep(1);
      auto sp_segment = target->Split(pop, key_hash);

      if (sp_segment == nullptr) {
        // printf("QQ\n");
        goto RETRY;
      }

      sp_segment[1]->pattern =
          ((key_hash >> (8 * sizeof(key_hash) - sp_segment[0]->local_depth + 1))
           << 1) +
          1;

      // printf("%x %x\n", sp_segment[0]->pattern,
      //        sp_segment[1]->pattern);

      dir_lock.write_lock();

      // lock_dir.acquire(dir->m,
      // 		 true); // acquire write lock
      // for directory

      x = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
      if ((dir->segments[x]->local_depth - 1) <
          dir->depth) // segment split without
                      // directory doubling
      {
        // printf("split %d %d\n",
        //        dir->segments[x]->local_depth,
        //        dir->depth);
        size_type depth_diff = dir->depth - sp_segment[0]->local_depth;
        if (depth_diff == 0) {
          if (x % 2 == 0) {
            dir->segments[x + 1] = sp_segment[1];
            pop.persist(&dir->segments[x + 1], sizeof(segment *));
          } else {
            dir->segments[x] = sp_segment[1];
            pop.persist(&dir->segments[x], sizeof(segment *));
          }
        } else {
          size_type chunk_size =
              pow(2, dir->depth - (sp_segment[0]->local_depth - 1));
          x = x - (x % chunk_size);
          for (size_type i = 0; i < chunk_size / 2; i++) {
            dir->segments[x + chunk_size / 2 + i] = sp_segment[1];
          }
          pop.persist(&dir->segments[x + chunk_size / 2],
                      sizeof(void *) * chunk_size / 2);
        }
        dir_lock.write_unlock();
        // lock_dir.release();
      } else // directory doubling for segment split
      {
        // printf("double %d %d\n",
        //        dir->segments[x]->local_depth,
        //        dir->depth);
        persistent_ptr<directory> dir_old = dir;
        auto dir_old_segments = dir->segments;

        // printf("P1\n");

        persistent_ptr<directory> new_dir;
        transaction::run(pop, [&] {
          new_dir = make_persistent<directory>(dir->depth + 1, pop);
        });

        // printf("P2\n");

        for (size_type i = 0; i < dir->capacity; i++) {
          if (i == x) {
            new_dir->segments[2 * i] = sp_segment[0];
            new_dir->segments[2 * i + 1] = sp_segment[1];
          } else {
            new_dir->segments[2 * i] = dir_old_segments[i];
            new_dir->segments[2 * i + 1] = dir_old_segments[i];
          }
          pop.persist(&new_dir->segments[2 * i], sizeof(segment *));
          pop.persist(&new_dir->segments[2 * i + 1], sizeof(segment *));
        }
        printf("directory->depth: %ld\n", new_dir->depth.get_ro());

        pop.persist(&dir, sizeof(persistent_ptr<directory>));
        transaction::run(pop, [&] {
          dir = new_dir;

          delete_persistent<directory>(dir_old);
        });

        // printf("P3\n");
        dir_lock.write_unlock();
      }

      goto RETRY;
    } else if (rc == 2) // insertion failure: the pattern of
                        // the item does not match the
                        // pattern of current segment
    {
      // printf("CCC\n");
      // sleep(1);
      goto STARTOVER;
    } else // insertion success: do nothing
    {
    }

    return ret(x, y);
  }

  ret get(const key_type &key, size_type key_len) {
    hv_type key_hash = hasher{}(key, key_len);
    size_type y = (key_hash & kMask) * kNumPairPerCacheLine;
    /* The directory reader locks protect readers from
       accessing a reclaimed directory, which guarantees the
       thread safety for directory. */

    dir_lock.read_lock();

    // scoped_t lock_dir(dir->m,
    // 		  false); // directory reader lock
    size_type x = (key_hash >> (8 * sizeof(key_hash) - dir->depth));

    persistent_ptr<segment> target_segment = dir->segments[x];
    if (target_segment->local_depth < dir->depth) {
      x = (key_hash >> (8 * sizeof(key_hash) - target_segment->local_depth));
    }

    scoped_t lock_target_segment(target_segment->segment_lock,
                                 false); // true：write lock，false: read lock
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
      size_type location = (y + i) % KNumSlot;
      if (target_segment->slots[location].kv != nullptr &&
          key_equal{}(key, target_segment->slots[location].get_key(),
                      key_len)) {

        dir_lock.read_unlock();
        // lock_dir.release();
        lock_target_segment.release();
        return ret(x, y);
      }
    }

    dir_lock.read_unlock();
    // lock_dir.release();
    lock_target_segment.release();
    return ret();
  }

  size_t Capacity(void) {
    std::unordered_map<segment *, bool> set;
    for (size_t i = 0; i < dir->capacity; ++i) {
      set[dir->segments[i]] = true;
    }
    return set.size() * KNumSlot;
  }

  void clear() {}

  ~CCEH() { clear(); }

  void foo() { std::cout << "CCEH::foo()" << std::endl; }

  pool_base get_pool_base() {
    PMEMobjpool *pop = pmemobj_pool_by_oid(PMEMoid{my_pool_uuid, 0});

    return pool_base(pop);
  }

  persistent_ptr<directory> dir;
  p<uint64_t> my_pool_uuid;
};

} /* namespace experimental */
} /* namespace obj */
} /* namespace pmem */

#endif /* PMEMOBJ_CCEH_HPP */
