
#include "bench.h"

char max_str[15] = {(int8_t)255, (int8_t)255, (int8_t)255, (int8_t)255,
                    (int8_t)255,
                    (int8_t)255, (int8_t)255, (int8_t)255, (int8_t)255,
                    (int8_t)255,
                    (int8_t)255, (int8_t)255, (int8_t)255, (int8_t)255,
                    (int8_t)255};

namespace {

struct FastFairTreeIndex {
  fastfair::btree *map;

  FastFairTreeIndex(fastfair::btree *map) : map(map) {}

  void put(const nap::Slice &key, const nap::Slice &value, bool is_update) {
    map->btree_insert((char *)key.data(), (char *)(cur_value++));
  }

  bool get(const nap::Slice &key, std::string &value) {
    uint64_t *ret =
        reinterpret_cast<uint64_t *>(map->btree_search((char *)key.data()));
    return ret != nullptr;
  }

  void del(const nap::Slice &key) {}
};

enum class cceh_op { UNKNOWN, INSERT, READ, MAX_OP };

struct thread_queue {
  uint8_t key[KEY_LEN];
  cceh_op operation;

  thread_queue() { key[KEY_LEN - 1] = 0; }
};

struct alignas(64) sub_thread {
  uint32_t id;
  uint64_t inserted;
  uint64_t found;
  uint64_t unfound;
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

  const char *path = argv[1];
  (void)path;
  size_t thread_num;

  std::stringstream s;
  s << argv[4];
  s >> thread_num;

  assert(thread_num > 0);

  init_numa_pool();

  fastfair::btree *tree = new fastfair::btree();

  printf("initialization done.\n");

  // load benchmark files
  FILE *ycsb, *ycsb_read;
  char buf[1024];
  char *pbuf = buf;
  size_t len = 1024;
  uint8_t key[KEY_LEN];
  key[KEY_LEN - 1] = '\0';
  size_t loaded = 0, inserted = 0, found = 0, unfound = 0;

  if ((ycsb = fopen(argv[2], "r")) == nullptr) {
    printf("failed to read %s\n", argv[2]);
    exit(1);
  }

  printf("Load phase begins \n");
  while (getline(&pbuf, &len, ycsb) != -1) {
    if (strncmp(buf, "INSERT", 6) == 0) {
      memcpy(key, buf + 7, KEY_LEN - 1);

      //   printf("%d\n", strlen((char *)key));
      // assert(strlen((char *)key) == 14);

      tree->btree_insert((char *)key, (char *)(cur_value++));
      loaded++;

      next_thread_id_for_load(thread_num);
    }
  }

  fclose(ycsb);
  printf("Load phase finishes: %ld items are inserted \n", loaded);

  if ((ycsb_read = fopen(argv[3], "r")) == nullptr) {
    printf("fail to read %s\n", argv[3]);
    exit(1);
  }

  thread_queue *run_queue[thread_num];
  double *latency_queue[thread_num];
  int move[thread_num];
  for (size_t t = 0; t < thread_num; t++) {
    run_queue[t] = new thread_queue[READ_WRITE_NUM / thread_num + 1];
    latency_queue[t] =
        (double *)calloc(READ_WRITE_NUM / thread_num + 1, sizeof(double));
    move[t] = 0;
  }

  size_t operation_num = 0;
  while (getline(&pbuf, &len, ycsb_read) != -1) {
    auto cur = operation_num % thread_num;
    if ((size_t)move[cur] >= READ_WRITE_NUM / thread_num + 1) {
      break;
    }

    auto &e = run_queue[cur][move[cur]];

    if (strncmp(buf, "INSERT", 6) == 0 || strncmp(buf, "UPDATE", 6) == 0) {

      memcpy(e.key, buf + 7, KEY_LEN - 1);
      e.key[KEY_LEN - 1] = '\0';

      // printf("%d\n", strlen((char *)e.key));
      // assert(strlen((char *)e.key) == 14);
      e.operation = cceh_op::INSERT;
      move[cur]++;
    } else if (strncmp(buf, "READ", 4) == 0) {
      memcpy(e.key, buf + 5, KEY_LEN - 1);
      e.key[KEY_LEN - 1] = '\0';

      // printf("%d\n", strlen((char *)e.key));
      // assert(strlen((char *)e.key) == 14);
      e.operation = cceh_op::READ;
      move[cur]++;
    } else {
      assert(false);
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
    THREADS[t].found = 0;
    THREADS[t].unfound = 0;
    THREADS[t].thread_num = thread_num;
    THREADS[t].run_queue = run_queue[t];
    THREADS[t].latency_queue = latency_queue[t];
  }

#ifdef LATENCY_ENABLE
  struct timespec stop;
#endif

  std::vector<std::thread> threads;
  threads.reserve(thread_num);

#ifdef ENABLE_NAP
  FastFairTreeIndex raw_index(tree);
  nap::Nap<FastFairTreeIndex> fastfair_nap(&raw_index);
#endif

  // warm up
  {
    const std::string warm_up(WARMUP_FILE);
    if ((ycsb = fopen(warm_up.c_str(), "r")) == nullptr) {
      printf("failed to read %s\n", warm_up.c_str());
      exit(1);
    }
    printf("Warmup phase begins \n");
    while (getline(&pbuf, &len, ycsb) != -1) {
      if (strncmp(buf, "READ", 4) == 0) {
        memcpy(key, buf + 5, KEY_LEN - 1);

#ifdef ENABLE_NAP
        std::string str;
        fastfair_nap.get(nap::Slice((char *)key, KEY_LEN), str);
#endif
      }
    }
#ifdef ENABLE_NAP
    nap::Topology::reset();

    fastfair_nap.set_sampling_interval(32);
#endif
    fclose(ycsb);
  }

  constexpr int kTestThread = nap::kMaxThreadCnt;
  struct timespec start[kTestThread], end[kTestThread];
  bool is_test[kTestThread];
  memset(is_test, false, sizeof(is_test));

  std::atomic<uint64_t> th_counter{0};
  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [&](size_t thread_id) {
          my_thread_id = nap::Topology::threadID();
          printf("Thread %d is opened\n", my_thread_id);
          bindCore(my_thread_id);

          constexpr int kBenchLoop = 1;
          for (int k = 0; k < kBenchLoop; ++k) {
            if (k == kBenchLoop - 1) { // enter into benchmark
              th_counter.fetch_add(1);
              while (th_counter.load() != thread_num) {
                ;
              }

#ifdef ENABLE_NAP
              fastfair_nap.clear();
#endif
              clock_gettime(CLOCK_MONOTONIC, start + my_thread_id);
              is_test[my_thread_id] = true;
            }
            for (size_t j = 0; j < READ_WRITE_NUM / thread_num; j++) {

              auto &op = THREADS[thread_id].run_queue[j];
              if (op.operation == cceh_op::INSERT) {

#ifdef ENABLE_NAP
                fastfair_nap.put(nap::Slice((char *)op.key, KEY_LEN),
                                 nap::Slice((char *)op.key, 8), false);
#else
                tree->btree_insert((char *)op.key, (char *)(cur_value++));

#endif

              } else if (op.operation == cceh_op::READ) {
#ifdef RANGE_BENCH

#ifdef ENABLE_NAP

                thread_local static char buf_2[4096];
                fastfair_nap.internal_query(
                    (nap::Slice((char *)op.key, KEY_LEN)), 10, (char *)buf_2);

                int off = 0;
                tree->btree_search_range((char *)op.key, max_str,
                                         (unsigned long *)thread_local_buffer,
                                         10, off);

            // for (int i = 0; i < 10; ++i) {
            //   strncmp(buf_2 + i * KEY_LEN,
            //   (char *)thread_local_buffer + i * KEY_LEN, KEY_LEN);
            // }

#else
                int off = 0;
                tree->btree_search_range((char *)op.key, max_str,
                                         (unsigned long *)thread_local_buffer,
                                         10, off);


#endif
#else

#ifdef ENABLE_NAP
                std::string str;
                fastfair_nap.get(nap::Slice((char *)op.key, KEY_LEN), str);
#else
                // printf("XXX %d\n",
                //        strlen((char *)THREADS[thread_id].run_queue[j].key));
                tree->btree_search((char *)op.key);
#endif

#endif
              }

              else {
                printf("unknown op\n");
                assert(false);
                exit(1);
              }
            }
          }
          clock_gettime(CLOCK_MONOTONIC, end + my_thread_id);

#if defined(RECOVERY_TEST) && defined(ENABLE_NAP)

          if (thread_id == 0) {
            nap::Timer s;
            s.sleep(1000ull * 1000 * 5);
            s.begin();
            fastfair_nap.recovery();
            uint64_t rt = s.end();
            printf("recovery time: %ld ms\n", rt / 1000 /1000);
          }
#endif
        },
        i);
  }

  for (auto &t : threads) {
    t.join();
  }

  // printf("update time: %ld\n", update_counter.load());

#ifdef ENABLE_NAP
  fastfair_nap.show_statistics();
#endif

  for (size_t t = 0; t < thread_num; ++t) {
    inserted += THREADS[t].inserted;
    found += THREADS[t].found;
    unfound += THREADS[t].unfound;
  }

  printf("Read operations: %ld found, %ld not found\n", found, unfound);
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

  //   pop.close();

  return 0;
}
