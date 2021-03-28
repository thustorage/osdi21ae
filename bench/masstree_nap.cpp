#include "bench.h"

namespace {

struct MassTreeIndex {
  masstree::masstree *map;

  MassTreeIndex(masstree::masstree *map) : map(map) {}

  void put(const nap::Slice &key, const nap::Slice &value, bool is_update) {
    auto t = map->getThreadInfo();
    map->put(key.data(), cur_value++, t);
  }

  bool get(const nap::Slice &key, std::string &value) {
    auto t = map->getThreadInfo();
    auto ret = map->get(key.data(), t);
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

  auto tree = new masstree::masstree();

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

      auto t = tree->getThreadInfo();
      tree->put((char *)key, cur_value++, t);
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
#ifdef ENABLE_NAP
    bindCore(t + 1);
#else
    bindCore(t);
#endif
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

  std::vector<std::thread> threads;
  threads.reserve(thread_num);

#ifdef ENABLE_NAP
  MassTreeIndex raw_index(tree);
  nap::Nap<MassTreeIndex> masstree_nap(&raw_index);
#endif

#ifdef SWITCH_TEST
  masstree_nap.set_switch_interval(0.2);
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
        masstree_nap.get(nap::Slice((char *)key, KEY_LEN), str);
#endif
      }
    }
#ifdef ENABLE_NAP
    nap::Topology::reset();

    masstree_nap.set_sampling_interval(32);
#endif
    fclose(ycsb);
  }


#ifdef SWITCH_TEST
  latency_evaluation_t latency(thread_num);
#endif

  constexpr int kTestThread = nap::kMaxThreadCnt;
  struct timespec start[kTestThread], end[kTestThread];
  bool is_test[kTestThread];
  memset(is_test, false, sizeof(is_test));

  std::atomic<uint64_t> th_counter{0};
  // sleep(1);
  //   int test_sleep[100];
  //   int now_sleep;
  // #ifdef ENABLE_NAP
  //   test_sleep[12] = 15;
  //   test_sleep[30] = 9;
  //   test_sleep[48] = 6;
  //   test_sleep[70] = 6;
  //   now_sleep = test_sleep[thread_num];
  // #else
  // //   test_sleep[12] = 19;
  //   test_sleep[30] = 11;
  //   test_sleep[48] = 9;
  //   test_sleep[71] = 8;
  //   now_sleep = test_sleep[thread_num];
  // #endif
  // nap::Timer pcm_time;
  // pcm_time.begin();
  // system(("/home/wq/pcm/pcm-numa.x " + std::to_string(now_sleep) +
  //         " >> /home/wq/Nap/build/pcm_out" + std::to_string(thread_num) +
  //         " 2>&1 &")
  //            .c_str());
  // // system(("/home/wq/pcm/pcm.x " + std::to_string(now_sleep) +
  // //         " >> /home/wq/Nap/build/pcm_out_" + std::to_string(thread_num) +
  // //         " 2>&1 &")
  // //            .c_str());
  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [&](size_t thread_id) {
          my_thread_id = nap::Topology::threadID();
          // printf("Thread %d is opened\n", my_thread_id);
          bindCore(my_thread_id);

#ifdef SWITCH_TEST
          latency.init_thread(my_thread_id);
#endif
          constexpr int kBenchLoop = 1;
          for (int k = 0; k < kBenchLoop; ++k) {
            if (k == kBenchLoop - 1) { // enter into benchmark
              th_counter.fetch_add(1);
              while (th_counter.load() != thread_num) {
                ;
              }

#ifdef ENABLE_NAP
              masstree_nap.clear();
#endif
              clock_gettime(CLOCK_MONOTONIC, start + my_thread_id);
              is_test[my_thread_id] = true;
            }
            for (size_t j = 0; j < READ_WRITE_NUM / thread_num; j++) {

              auto &op = THREADS[thread_id].run_queue[j];
              if (op.operation == cceh_op::INSERT) {

#ifdef ENABLE_NAP
                masstree_nap.put(nap::Slice((char *)op.key, KEY_LEN),
                                 nap::Slice((char *)op.key, 8), false);
#else
                auto t = tree->getThreadInfo();
                tree->put((char *)op.key, cur_value++, t);

#endif

              } else if (op.operation == cceh_op::READ) {

#ifdef RANGE_BENCH
#ifdef ENABLE_NAP

                thread_local static char buf_2[4096];
                masstree_nap.internal_query( // scan NAL
                    (nap::Slice((char *)op.key, KEY_LEN)), 10, (char *)buf_2);

                auto t = tree->getThreadInfo(); // scan Raw Indexes
                tree->scan((char *)op.key, 10, (uint64_t *)thread_local_buffer,
                           t);

#else
                auto t = tree->getThreadInfo();
                tree->scan((char *)op.key, 10, (uint64_t *)thread_local_buffer,
                           t);

#endif

#else

#ifdef ENABLE_NAP
                std::string str;
                masstree_nap.get(nap::Slice((char *)op.key, KEY_LEN), str);
#else
                auto t = tree->getThreadInfo();

                tree->get((char *)op.key, t);
#endif

#endif
              }

              else {
                printf("unknown op\n");
                assert(false);
                exit(1);
              }
#ifdef SWITCH_TEST
              latency.count(my_thread_id);
#endif
            }
          }
          clock_gettime(CLOCK_MONOTONIC, end + my_thread_id);

          
#if defined(RECOVERY_TEST) && defined(ENABLE_NAP)

          if (thread_id == 0) {
            nap::Timer s;
            s.sleep(1000ull * 1000 * 5);
            s.begin();
            masstree_nap.recovery();
            uint64_t rt = s.end();
            printf("recovery time: %ld ms\n", rt / 1000 /1000);
          }
#endif
        },
        i);
  }

#ifdef SWITCH_TEST
  bindCore(71);
  latency.throughput_listen_ms(20);
#endif

  for (auto &t : threads) {
    t.join();
  }
  // uint64_t real_time = pcm_time.end();
  // if (real_time > 1000000000.0 * now_sleep) {
  //   puts("!!!!!!!!!!!!!!!!!!!!!!!!!re - TEST !!!!!");
  // }
  // sleep(now_sleep * 2 + 2);
  // system("pkill pcm-numa.x");
  // system("pkill pcm.x");

  // printf("update time: %ld\n", update_counter.load());

#ifdef ENABLE_NAP
  masstree_nap.show_statistics();
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
