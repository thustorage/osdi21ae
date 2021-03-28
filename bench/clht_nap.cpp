#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include "bench.h"
#include "index/clht_NUMA.hpp"
#include "timer.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <iterator>
#include <sstream>
#include <thread>
#include <time.h>
#include <vector>

// #define TEST_LATENCY

// 65536 * 3 = 196608
#define N_BUCKETS (65536 * 2)

namespace nvobj = pmem::obj;

namespace {

class key_equal {
public:
  template <typename M, typename U>
  bool operator()(const M &lhs, const U &rhs) const {
    return lhs == rhs;
  }
};

class string_hasher {
  /* hash multiplier used by fibonacci hashing */
  static const size_t hash_multiplier = 11400714819323198485ULL;

public:
  using transparent_key_equal = key_equal;

  size_t operator()(const std::string &str) const {
    return hash(str.c_str(), str.size());
  }

private:
  size_t hash(const char *str, size_t size) const {
    size_t h = 0;
    for (size_t i = 0; i < size; ++i) {
      h = static_cast<size_t>(str[i]) ^ (h * hash_multiplier);
    }
    return h;
  }
};

using string_t = std::string;
typedef nvobj::experimental::clht<string_t, string_t, string_hasher,
                                  std::equal_to<string_t>>
    persistent_map_type;

struct root {
  nvobj::persistent_ptr<persistent_map_type> cons;
};

struct ClhtNapIndex {
  persistent_map_type *map;

  ClhtNapIndex(persistent_map_type *map) : map(map) {}

  void put(const nap::Slice &key, const nap::Slice &value, bool is_update) {

    map->put(persistent_map_type::value_type(key.ToString(), value.ToString()),
             my_thread_id + 1);
  }

  bool get(const nap::Slice &key, std::string &value) {
    auto ret = map->get(persistent_map_type::key_type(key.ToString()));
    return ret.found;
  }

  void del(const nap::Slice &key) {}
};

enum class clht_op {
  UNKNOWN,
  INSERT,
  READ,
  DELETE,
  UPDATE,

  MAX_OP
};

struct thread_queue {
  string_t key;
  clht_op operation;
};

struct alignas(64) sub_thread {
  uint32_t id;
  uint64_t inserted;
  uint64_t ins_failure;
  uint64_t found;
  uint64_t unfound;
  uint64_t deleted;
  uint64_t del_existing;
  uint64_t thread_num;
  thread_queue *run_queue;
  double *latency_queue;
};

} // namespace

int main(int argc, char *argv[]) {

  // parse inputs
  if (argc != 5 && argc != 6) {
    printf("usage: %s <pool_path> <load_file> <run_file> <thread_num>\n\n",
           argv[0]);
    printf("    pool_path: the pool file required for PMDK\n");
    printf("    load_file: a workload file for the load phase\n");
    printf("    run_file: a workload file for the run phase\n");
    printf("    thread_num: the number of threads\n");
    exit(1);
  }

  uint64_t hot_cnt = nap::kHotKeys;
  (void)hot_cnt;

  if (argc == 6) {
    hot_cnt = std::atoi(argv[5]);
  }

  printf("MACRO N_BUCKETS: %d\n", N_BUCKETS);

  const char *path = argv[1];
  size_t thread_num;

  std::stringstream s;
  s << argv[4];
  s >> thread_num;

  // initialize clht hash
  nvobj::pool<root> pop;
  remove(path); // delete the mapped file.

  pop = nvobj::pool<root>::create(path, LAYOUT, PMEMOBJ_MIN_POOL * 20480,
                                  S_IWUSR | S_IRUSR);
  auto proot = pop.root();

  {
    nvobj::transaction::manual tx(pop);

    proot->cons =
        nvobj::make_persistent<persistent_map_type>((uint64_t)N_BUCKETS);

    nvobj::transaction::commit();
  }

  auto map = pop.root()->cons;
  printf("initialization done.\n");
  printf("initial capacity %ld\n", map->capacity());

  init_numa_pool();

  // load benchmark files
  FILE *ycsb, *ycsb_read;
  char buf[1024];
  char *pbuf = buf;
  size_t len = 1024;
  size_t loaded = 0, inserted = 0, ins_failure = 0, found = 0, unfound = 0;
  size_t deleted = 0, del_existing = 0;

  if ((ycsb = fopen(argv[2], "r")) == nullptr) {
    printf("failed to read %s\n", argv[2]);
    exit(1);
  }

  printf("Load phase begins \n");

  while (getline(&pbuf, &len, ycsb) != -1) {
    if (strncmp(buf, "INSERT", 6) == 0) {
      string_t key(buf + 7, KEY_LEN);
      string_t val(buf + 7, VALUE_LEN);
      auto ret = map->put(persistent_map_type::value_type(key, val), loaded);
      (void)ret;
      loaded++;

      next_thread_id_for_load(thread_num);
    }
  }
  fclose(ycsb);
  printf("Load phase finishes: %ld items are inserted \n", loaded);

  // prepare data for the run phase
  if ((ycsb_read = fopen(argv[3], "r")) == NULL) {
    printf("fail to read %s\n", argv[3]);
    exit(1);
  }

  thread_queue *run_queue[thread_num];
  double *latency_queue[thread_num];
  int move[thread_num];
  for (size_t t = 0; t < thread_num; t++) {
#ifdef ENABLE_NAP
    bindCore(t + 1);
#else
    bindCore(t);
#endif
    run_queue[t] = new thread_queue[(READ_WRITE_NUM / thread_num + 1)];
    latency_queue[t] =
        (double *)calloc(READ_WRITE_NUM / thread_num + 1, sizeof(double));
    move[t] = 0;
  }

  size_t operation_num = 0;
  while (getline(&pbuf, &len, ycsb_read) != -1) {

    uint64_t cur = operation_num % thread_num;
    if ((size_t)move[cur] >= READ_WRITE_NUM / thread_num + 1) {
      break;
    }

    auto &op = run_queue[cur][move[cur]];
    if (strncmp(buf, "INSERT", 6) == 0 || strncmp(buf, "UPDATE", 6) == 0) {
      op.key = string_t(buf + 7, KEY_LEN);
      op.operation = clht_op::INSERT;
      move[cur]++;
    } else if (strncmp(buf, "READ", 4) == 0) {
      op.key = string_t(buf + 5, KEY_LEN);
      op.operation = clht_op::READ;
      move[cur]++;
    } else if (strncmp(buf, "DELETE", 6) == 0) {
      op.key = string_t(buf + 7, KEY_LEN);
      op.operation = clht_op::DELETE;
      move[cur]++;
    }
    operation_num++;
  }
  fclose(ycsb_read);

  sub_thread *THREADS = (sub_thread *)malloc(sizeof(sub_thread) * thread_num);
  inserted = 0;

  printf("Run phase begins: %s \n", argv[3]);
  for (size_t t = 0; t < thread_num; t++) {
    THREADS[t].id = t;
    THREADS[t].inserted = 0;
    THREADS[t].ins_failure = 0;
    THREADS[t].found = 0;
    THREADS[t].unfound = 0;
    THREADS[t].deleted = 0;
    THREADS[t].del_existing = 0;
    THREADS[t].thread_num = thread_num;
    THREADS[t].run_queue = run_queue[t];
    THREADS[t].latency_queue = latency_queue[t];
  }

#ifdef ENABLE_NAP
  ClhtNapIndex raw_index(proot->cons.get());
  nap::Nap<ClhtNapIndex> clht_nap(&raw_index, hot_cnt);
#endif

  // warm up
  {
    const std::string warm_up(WARMUP_FILE);
    if ((ycsb = fopen(warm_up.c_str(), "r")) == nullptr) {
      printf("failed to read %s\n", warm_up.c_str());
      exit(1);
    }
    printf("Warmup phase begins \n");
    char key[1024];
    while (getline(&pbuf, &len, ycsb) != -1) {
      if (strncmp(buf, "READ", 4) == 0) {
        memcpy(key, buf + 5, KEY_LEN);

#ifdef ENABLE_NAP
        std::string str;
        clht_nap.get(nap::Slice((char *)key, KEY_LEN), str);
#endif
      }
    }
#ifdef ENABLE_NAP
    nap::Topology::reset();

    clht_nap.set_sampling_interval(32);
#endif
    fclose(ycsb);
  }

#ifdef TEST_LATENCY
  latency_evaluation_t latency(thread_num);
#endif

  std::vector<std::thread> threads;
  threads.reserve(thread_num);

  constexpr int kTestThread = nap::kMaxThreadCnt;
  struct timespec start[kTestThread], end[kTestThread];
  bool is_test[kTestThread];
  memset(is_test, false, sizeof(is_test));

  std::atomic<uint64_t> th_counter{0};

  // sleep(1);
  //   int test_sleep[100];
  //   int now_sleep;
  // #ifdef ENABLE_NAP
  //   test_sleep[12] = 13;
  //   test_sleep[34] = 8;
  //   test_sleep[53] = 6;
  //   test_sleep[70] = 5;
  //   now_sleep = test_sleep[thread_num];
  // // #else
  //   test_sleep[12] = 17;
  //   test_sleep[34] = 10;
  //   test_sleep[53] = 8;
  //   test_sleep[70] = 7;
  //   now_sleep = test_sleep[thread_num];
  // #endif
  //   nap::Timer pcm_time;
  //   pcm_time.begin();
  // #ifdef PCMNUMA
  //   system(("/home/wq/pcm/pcm-numa.x " + std::to_string(now_sleep) +
  //           " >> /home/wq/Nap/build/pcm_out" + std::to_string(thread_num) +
  //           " 2>&1 &")
  //              .c_str());
  // #endif
  // #ifdef PCMM
  //   system(("/home/wq/pcm/pcm.x " + std::to_string(now_sleep) +
  //           " >> /home/wq/Nap/build/pcm_out_" + std::to_string(thread_num) +
  //           " 2>&1 &")
  //              .c_str());
  // #endif

  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [&](size_t thread_id) {
          my_thread_id = nap::Topology::threadID();
          bindCore(my_thread_id);
      // printf("Thread %d is opened\n", my_thread_id);

#ifdef TEST_LATENCY
          latency.init_thread(my_thread_id);
#endif

          size_t offset = loaded + READ_WRITE_NUM / thread_num * thread_id;
          (void)offset;

          thread_local string_t VAL((char *)thread_local_buffer, VALUE_LEN);

          constexpr int kBenchLoop = 1;
          for (int k = 0; k < kBenchLoop; ++k) {
            if (k == kBenchLoop - 1) { // enter into benchmark
              th_counter.fetch_add(1);
              while (th_counter.load() != thread_num) {
                ;
              }

#ifdef ENABLE_NAP
              clht_nap.clear();
#endif
              clock_gettime(CLOCK_MONOTONIC, start + my_thread_id);
              is_test[my_thread_id] = true;
            }
            for (size_t j = 0; j < READ_WRITE_NUM / thread_num; j++) {

#ifdef TEST_LATENCY
              latency.begin(my_thread_id);
// latency.count(my_thread_id);
#endif

              (*(uint64_t *)thread_local_buffer)++;
              auto &op = THREADS[thread_id].run_queue[j];
              if (op.operation == clht_op::INSERT) {
#ifdef ENABLE_NAP
                clht_nap.put(nap::Slice(op.key), nap::Slice(VAL));
#else
                auto ret = map->put(
                    persistent_map_type::value_type(op.key, VAL), offset + j);
                (void)ret;
#endif
              } else if (op.operation == clht_op::READ) {
#ifdef ENABLE_NAP
                std::string str;
                clht_nap.get(op.key, str);
#else
                auto ret = map->get(persistent_map_type::key_type(op.key));
                (void)ret;
#endif
              } else if (op.operation == clht_op::DELETE) {
                auto ret = map->erase(persistent_map_type::key_type(op.key));
                (void)ret;
              } else {
                printf("unknown clht_op\n");
                exit(1);
              }
#ifdef TEST_LATENCY
              latency.end(my_thread_id);
#endif
            }
          }
          clock_gettime(CLOCK_MONOTONIC, end + my_thread_id);

#if defined(RECOVERY_TEST) && defined(ENABLE_NAP)

          if (thread_id == 0) {
            nap::Timer s;
            s.sleep(1000ull * 1000 * 5);
            s.begin();
            clht_nap.recovery();
            uint64_t rt = s.end();
            printf("recovery time: %ld ms\n", rt / 1000 / 1000);
          }
#endif
        },
        i);
  }

  // bindCore(71);
  // latency.throughput_listen_ms(now_sleep * 3);

  for (auto &t : threads) {
    t.join();
  }

  // uint64_t real_time = pcm_time.end();
  // if (real_time / 1000000000.0 > now_sleep) {
  //   puts("!!!!!!!!!!!!!!!!!!!!!!!!!re - TEST !!!!!");
  // }
  // sleep(now_sleep + 10);
  // system("pkill pcm-numa.x");
  // system("pkill pcm.x");

#ifdef ENABLE_NAP
  clht_nap.show_statistics();
#endif

#ifdef TEST_LATENCY
  latency.merge_print();
#endif

  for (size_t t = 0; t < thread_num; ++t) {
    inserted += THREADS[t].inserted;
    ins_failure += THREADS[t].ins_failure;
    found += THREADS[t].found;
    unfound += THREADS[t].unfound;
    deleted += THREADS[t].deleted;
    del_existing += THREADS[t].del_existing;
  }

  uint64_t total_slots = map->capacity();
  printf("capacity (after insertion) %ld, load factor %f\n", total_slots,
         (loaded + inserted) * 1.0 / total_slots);

  printf("Insert operations: %ld loaded, %ld inserted, %ld failed\n", loaded,
         inserted, ins_failure);
  printf("Read operations: %ld found, %ld not found\n", found, unfound);
  printf("Delete operations: deleted existing %ld items via %ld delete "
         "operations in total\n",
         del_existing, deleted);

  size_t elapsed[kTestThread];

  for (int i = 0; i < kTestThread; ++i) {
    elapsed[i] =
        static_cast<size_t>((end[i].tv_sec - start[i].tv_sec) * 1000000000ull +
                            (end[i].tv_nsec - start[i].tv_nsec));
  }

  float sec = elapsed[0] / 1000000000.0;

  printf("%f seconds\n", sec);

  float elapsed_sec[kTestThread];
  float all = 0;
  for (int i = 0; i < kTestThread; ++i) {
    elapsed_sec[i] = elapsed[i] / 1000000000.0;

    if (is_test[i]) {
      auto per_thread = READ_WRITE_NUM / thread_num / elapsed_sec[i];
      all += per_thread;
      printf("%f  (%dth threads)\n", per_thread, i);
    }
  }
  printf("%f reqs per second (%ld threads)\n", all, thread_num);

  return 0;
}
