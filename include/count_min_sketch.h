#if !defined(_COUNT_MIN_SKETCH_H_)
#define _COUNT_MIN_SKETCH_H_

#include "hash32.h"
#include "murmur_hash2.h"
#include "nap_common.h"
#include "slice.h"
#include "timer.h"
#include "top_k.h"
#include "topology.h"

#include <functional>
#include <queue>
#include <set>
#include <string>
#include <unordered_set>

// insert <k, freq>;
// if (k in set) { // Hashtable O(1)
//     set[k].freq = freq; // Heap O(logn)
// } else if freq > min_freq(set) { // Heap O(1)
//     del min_freq from set; // Heap O(logn), Hashtable O(1)
//     add <k, freq> to set;  // Heap O(logn), Hashtable O(1)
// } else { //
// }

inline unsigned long __hash(const char *str, int size) {
  unsigned long hash = 5381;
  int c;
  for (int i = 0; i < size; ++i) {
    c = str[i];
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  }

  return hash;
}

namespace nap {

struct __attribute__((__packed__)) PerRecord {
  uint64_t timestamp;
  char *v;

  PerRecord() : timestamp(0), v(nullptr) {}
};

struct RecordCursor {
  uint64_t last_ts;
  uint32_t last_index;

  RecordCursor() : last_ts(0), last_index(0) {}
};

static_assert(sizeof(PerRecord) == 16, "XX");

class CountMin {
private:
  int hot_keys_cnt;

  const static int kHashCnt = 3;
  const static int kBloomLength = 876199;
  uint32_t *bloom_array[kHashCnt];
  uint64_t hash_seed[32] = {931901, 1974701, 7296907};

  TopK topK;

  const uint32_t kRecordBufferSize = 20000;
  PerRecord *record_buffer[kMaxThreadCnt];
  RecordCursor cursors[kMaxThreadCnt];

public:
  CountMin(int hot_keys_cnt) : hot_keys_cnt(hot_keys_cnt), topK(hot_keys_cnt) {
    for (int i = 0; i < kHashCnt; ++i) {
      bloom_array[i] = new uint32_t[kBloomLength];
      memset(bloom_array[i], 0, kBloomLength * sizeof(uint32_t));
    }

    for (int i = 0; i < kMaxThreadCnt; ++i) {
      record_buffer[i] = new PerRecord[kRecordBufferSize];
    }
  }

  ~CountMin() {
    for (int i = 0; i < kHashCnt; ++i) {
      if (bloom_array[i]) {
        delete[] bloom_array[i];
      }
    }

    for (int i = 0; i < kMaxThreadCnt; ++i) {
      if (record_buffer[i]) {
        delete[] record_buffer[i];
      }
    }
  }

  std::vector<Node> &get_list() { return topK.get_list(); }

  void reset() {
    topK.reset();
    for (int i = 0; i < kHashCnt; ++i) {
      memset(bloom_array[i], 0, kBloomLength * sizeof(uint32_t));
    }
  }

  void record(const Slice &key) {

    // for threads that access keys.
    static thread_local int index = 0;
    static thread_local char *free_buffer = nullptr;
    static thread_local PerRecord *thread_records =
        record_buffer[Topology::threadID()];

    char *buf;
    if (free_buffer && *(uint32_t *)(free_buffer) >= key.size()) {
      buf = free_buffer;
      free_buffer = nullptr;
    } else {
      buf = (char *)malloc(key.size() + sizeof(uint32_t));
    }

    *(uint32_t *)buf = key.size();
    memcpy(buf + sizeof(uint32_t), key.data(), key.size());

    char *old_ptr = thread_records[index].v;
    

    // record access pattern (key, timestamp), it is coordination-free
    thread_records[index].v = buf;
    thread_records[index].timestamp = asm_rdtsc();

    if (!old_ptr) {
      if (!free_buffer) {
        free_buffer = old_ptr;
      } else if (*(uint32_t *)(free_buffer) < *(uint32_t *)(old_ptr)) {
        free(free_buffer);
        free_buffer = old_ptr;
      }
    }

    index = (index + 1) % kRecordBufferSize;
  }

  void poll_workloads(double seconds) {

    uint64_t ns = seconds * (1000ull * 1000 * 1000);
    uint16_t kBatchPerThread = 8;
    Timer timer;
    timer.begin();

    while (true) {
      for (int i = 0; i < kMaxThreadCnt; ++i) {
        for (int k = 0; k < kBatchPerThread; ++k) {
          auto &c = cursors[i];
          auto &r = record_buffer[i][c.last_index];
          if (r.timestamp < c.last_ts ||
              r.v == nullptr) {
            break; // invalid record
          }
           

          // update count-min sketch and min heap
          this->access_a_key(Slice(r.v + sizeof(uint32_t), *(uint32_t *)r.v));

          c.last_ts = r.timestamp;
          c.last_index = (c.last_index + 1) % kRecordBufferSize;
        }

        if (timer.end() > ns) {
          return;
        }
      }
    }
  }

  void access_a_key(const Slice &key) {

    // static std::hash<std::string> hash_fn;
    uint64_t hash_val[kHashCnt];
    for (int i = 0; i < kHashCnt; ++i) {
      hash_val[i] =
          (MurmurHash64A(key.data(), key.size(), hash_seed[i])) % kBloomLength;
    }
    // hash_val[1] =__hash(key.c_str(), key.size()) % kBloomLength;
    // hash_val[2] = xxhash(key.c_str(), key.size(), 333) % kBloomLength;

    uint64_t min_freq = ++bloom_array[0][hash_val[0]];

    for (int i = 1; i < kHashCnt; ++i) {
      auto tmp = ++bloom_array[i][hash_val[i]];
      if (tmp < min_freq) {
        min_freq = tmp;
      }
    }
    topK.access_a_key(key.ToString(), min_freq);
  }
};

} // namespace nap

#endif // _COUNT_MIN_SKETCH_H_
