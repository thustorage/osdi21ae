#include "bench.h"

// (2^15 + 2^14) * 4 = 196608
#define HASH_POWER 25

namespace nvobj = pmem::obj;

#include "murmur_hash2.h"

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
typedef nvobj::experimental::level_hash<string_t, string_t, string_hasher,
                                        std::equal_to<string_t>>
    persistent_map_type;

struct root {
  nvobj::persistent_ptr<persistent_map_type> cons;
};

struct LevelNapIndex {
  persistent_map_type *map;

  LevelNapIndex(persistent_map_type *map) : map(map) {}

  void put(const nap::Slice &key, const nap::Slice &value, bool is_update) {
    if (is_update) {
      map->update(
          persistent_map_type::value_type(key.ToString(), value.ToString()),
          my_thread_id);
    } else { // insert
      map->insert(
          persistent_map_type::value_type(key.ToString(), value.ToString()),
          my_thread_id);
    }
  }

  bool get(const nap::Slice &key, std::string &value) {
    auto ret =
        map->query(persistent_map_type::key_type(key.ToString()), my_thread_id);
    return ret.found;
  }

  void del(const nap::Slice &key) {}
};

enum class level_hash_op {
  UNKNOWN,
  INSERT,
  READ,
  DELETE,
  UPDATE,

  MAX_OP
};

struct thread_queue {
  string_t key;
  level_hash_op operation;
};

struct alignas(64) sub_thread {
  uint32_t id;
  uint64_t inserted;
  uint64_t ins_failure;
  uint64_t found;
  uint64_t unfound;
  uint64_t deleted;
  uint64_t del_existing;
  uint64_t updated;
  uint64_t upd_existing;
  uint64_t thread_num;
  thread_queue *run_queue;
  double *latency_queue;
};

} // namespace

int main(int argc, char *argv[]) {

  // parse inputs
  if (argc != 5) {
    printf("usage: %s <pool_path> <load_file> <run_file> <thread_num>\n\n",
           argv[0]);
    printf("    pool_path: the pool file required for PMDK\n");
    printf("    load_file: a workload file for the load phase\n");
    printf("    run_file: a workload file for the run phase\n");
    printf("    thread_num: the number of threads\n");
    exit(1);
  }

  printf("MACRO HASH_POWER: %d\n", HASH_POWER);

  const char *path = argv[1];
  size_t thread_num;

  std::stringstream s;
  s << argv[4];
  s >> thread_num;

  // initialize level_hash
  nvobj::pool<root> pop;
  remove(path); // delete the mapped file.

  pop = nvobj::pool<root>::create(path, LAYOUT, PMEMOBJ_MIN_POOL * 1024 * 8,
                                  S_IWUSR | S_IRUSR);
  auto proot = pop.root();

  {
    nvobj::transaction::manual tx(pop);

    proot->cons =
        nvobj::make_persistent<persistent_map_type>((uint64_t)HASH_POWER, 1);

    nvobj::transaction::commit();
  }

  // proot->cons->init();

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
  size_t deleted = 0, del_existing = 0, updated = 0, upd_existing = 0;

  if ((ycsb = fopen(argv[2], "r")) == nullptr) {
    printf("failed to read %s\n", argv[2]);
    exit(1);
  }

  printf("Load phase begins \n");
  while (getline(&pbuf, &len, ycsb) != -1) {
    if (strncmp(buf, "INSERT", 6) == 0) {
      string_t key(buf + 7, KEY_LEN);
      auto ret =
          map->insert(persistent_map_type::value_type(key, key), (size_t)0);
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
    run_queue[t] = new thread_queue[(READ_WRITE_NUM / thread_num + 1)];
    latency_queue[t] =
        (double *)calloc(READ_WRITE_NUM / thread_num + 1, sizeof(double));
    move[t] = 0;
  }

  size_t insert_op = 0;
  size_t operation_num = 0;
  while (getline(&pbuf, &len, ycsb_read) != -1) {

    uint64_t cur = operation_num % thread_num;
    auto &e = run_queue[cur][move[cur]];
    if ((size_t)move[cur] >= READ_WRITE_NUM / thread_num + 1) {
      break;
    }
    if (strncmp(buf, "INSERT", 6) == 0) {
      insert_op++;
      e.key = string_t(buf + 7, KEY_LEN);
      e.operation = level_hash_op::INSERT;
      move[cur]++;
    } else if (strncmp(buf, "READ", 4) == 0) {
      e.key = string_t(buf + 5, KEY_LEN);
      e.operation = level_hash_op::READ;
      move[cur]++;
    } else if (strncmp(buf, "DELETE", 6) == 0) {
      e.key = string_t(buf + 7, KEY_LEN);
      e.operation = level_hash_op::DELETE;
      move[cur]++;
    } else if (strncmp(buf, "UPDATE", 6) == 0) {
      e.key = string_t(buf + 7, KEY_LEN);
      e.operation = level_hash_op::UPDATE;
      move[cur]++;
    }
    operation_num++;
  }
  fclose(ycsb_read);

  // printf("%ld insert XXX\n",insert_op );

  sub_thread *THREADS = (sub_thread *)malloc(sizeof(sub_thread) * thread_num);
  inserted = 0;

  printf("Run phase begins: %s \n", argv[3]);
  map->set_thread_num(thread_num);
  for (size_t t = 0; t < thread_num; t++) {
    THREADS[t].id = t;
    THREADS[t].inserted = 0;
    THREADS[t].ins_failure = 0;
    THREADS[t].found = 0;
    THREADS[t].unfound = 0;
    THREADS[t].deleted = 0;
    THREADS[t].del_existing = 0;
    THREADS[t].updated = 0;
    THREADS[t].upd_existing = 0;
    THREADS[t].thread_num = thread_num;
    THREADS[t].run_queue = run_queue[t];
    THREADS[t].latency_queue = latency_queue[t];
  }

  std::vector<std::thread> threads;
  threads.reserve(thread_num);

#ifdef ENABLE_NAP
  LevelNapIndex raw_index(proot->cons.get());
  nap::Nap<LevelNapIndex> level_nap(&raw_index);
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
        level_nap.get(nap::Slice((char *)key, KEY_LEN), str);
#endif
      }
    }
#ifdef ENABLE_NAP
    nap::Topology::reset();

    level_nap.set_sampling_interval(32);
#endif
    fclose(ycsb);
  }

  constexpr int kTestThread = nap::kMaxThreadCnt;
  struct timespec start[kTestThread], end[kTestThread];
  bool is_test[kTestThread];
  memset(is_test, false, sizeof(is_test));

#ifdef LATENCY_ENABLE
  struct timespec stop;
#endif
  // clock_gettime(CLOCK_MONOTONIC, start);

  std::atomic<uint64_t> th_counter{0};
  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [&](size_t thread_id) {
          my_thread_id = nap::Topology::threadID();
          bindCore(my_thread_id);
          printf("Thread %d is opened\n", my_thread_id);

          //   size_t offset = loaded + READ_WRITE_NUM / thread_num * thread_id;
          constexpr int kBenchLoop = 1;
          for (int k = 0; k < kBenchLoop; ++k) {
            if (k == kBenchLoop - 1) { // enter into benchmark
              th_counter.fetch_add(1);
              while (th_counter.load() != thread_num) {
                ;
              }

#ifdef ENABLE_NAP
              level_nap.clear();
#endif
              clock_gettime(CLOCK_MONOTONIC, start + my_thread_id);
              is_test[my_thread_id] = true;
            }
            for (size_t j = 0; j < READ_WRITE_NUM / thread_num; j++) {
              if (THREADS[thread_id].run_queue[j].operation ==
                  level_hash_op::INSERT) {

#ifdef ENABLE_NAP
                level_nap.put(THREADS[thread_id].run_queue[j].key,
                              THREADS[thread_id].run_queue[j].key, false);
#else
                auto ret = map->insert(persistent_map_type::value_type(
                                           THREADS[thread_id].run_queue[j].key,
                                           THREADS[thread_id].run_queue[j].key),
                                       thread_id);
                if (!ret.found) {
                  THREADS[thread_id].inserted++;
                } else {
                  THREADS[thread_id].ins_failure++;
                }
#endif
              } else if (THREADS[thread_id].run_queue[j].operation ==
                         level_hash_op::READ) {
#ifdef ENABLE_NAP
                std::string str;
                level_nap.get(THREADS[thread_id].run_queue[j].key, str);
#else
                auto ret = map->query(persistent_map_type::key_type(
                                          THREADS[thread_id].run_queue[j].key),
                                      thread_id);
                if (ret.found) {
                  THREADS[thread_id].found++;
                } else {
                  THREADS[thread_id].unfound++;
                }
#endif
              } else if (THREADS[thread_id].run_queue[j].operation ==
                         level_hash_op::DELETE) {
                auto ret = map->erase(persistent_map_type::key_type(
                                          THREADS[thread_id].run_queue[j].key),
                                      thread_id);
                THREADS[thread_id].deleted++;
                if (ret.found) {
                  THREADS[thread_id].del_existing++;
                }
              } else if (THREADS[thread_id].run_queue[j].operation ==
                         level_hash_op::UPDATE) {
                string_t new_val = THREADS[thread_id].run_queue[j].key;
                new_val[0] = ~new_val[0];

#ifdef ENABLE_NAP
                std::string str;
                level_nap.put(THREADS[thread_id].run_queue[j].key, new_val,
                              true);
#else
                auto ret = map->update(
                    persistent_map_type::value_type(
                        THREADS[thread_id].run_queue[j].key, new_val),
                    thread_id + 1);
                THREADS[thread_id].updated++;
                if (ret.found) {
                  THREADS[thread_id].upd_existing++;
                }
#endif
              } else {
                printf("unknown level_hash_op\n");
                exit(1);
              }
            }
          }
          clock_gettime(CLOCK_MONOTONIC, end + my_thread_id);
        },
        i);
  }

  for (auto &t : threads) {
    t.join();
  }

#ifdef ENABLE_NAP
  level_nap.show_statistics();
#endif

  // clock_gettime(CLOCK_MONOTONIC, &end);

  for (size_t t = 0; t < thread_num; ++t) {
    inserted += THREADS[t].inserted;
    ins_failure += THREADS[t].ins_failure;
    found += THREADS[t].found;
    unfound += THREADS[t].unfound;
    deleted += THREADS[t].deleted;
    del_existing += THREADS[t].del_existing;
    updated += THREADS[t].updated;
    upd_existing += THREADS[t].upd_existing;
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
  printf("Update operations: update existing %ld items via %ld update "
         "operations in total\n",
         upd_existing, updated);

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
