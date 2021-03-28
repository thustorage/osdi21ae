#if !defined(_latncy_evaluation_)
#define _latncy_evaluation_
#include "timer.h"
#include <bits/stdc++.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
// using namespace nap;

inline uint32_t min(uint32_t a, uint32_t b) { return a < b ? a : b; }

class latency_evaluation_t {
private:
  /* data */
  // 100.00us
  const int range = 10000;
  uint64_t **latency_bucket;
  uint64_t *ans;
  nap::Timer **timer;
  uint64_t **req_count;
  int thread_num;
  uint64_t throughput;

public:
  latency_evaluation_t(int _thread_num);
  ~latency_evaluation_t();
  void init_thread(int thread_id) {
    if (latency_bucket[thread_id] != 0)
      return;
    if (thread_id >= thread_num) {
      puts("Error! init_thread for latency_evaluation");
    }

    latency_bucket[thread_id] = (uint64_t *)malloc(range * sizeof(uint64_t));
    timer[thread_id] = new nap::Timer();
    req_count[thread_id] = new uint64_t[8];

    memset(latency_bucket[thread_id], 0, range * sizeof(uint64_t));
    memset(req_count[thread_id], 0, 8 * sizeof(uint64_t));
    // printf("init_latency:%d\n", thread_id);
    return;
  }
  inline void count(int thread_id) {
    (req_count[thread_id][0])++;
    return;
  }
  inline uint64_t merge_count() {
    uint64_t last_throught = throughput;
    throughput = 0;
    for (int i = 0; i < thread_num; i++) {
      if (req_count[i] == 0)
        continue;
      throughput += req_count[i][0];
    }
    return throughput - last_throught;
  }

  void throughput_listen_ms(uint64_t listen_length) {
    int gap = 5;
    int old = dup(1);

    FILE *fp = freopen("throughput.txt", "w", stdout);
    puts("[Throughput listen]: begin");

    uint64_t print_max = listen_length * 1000 / gap;
    uint64_t *printf_array =
        (uint64_t *)malloc((print_max + 10) * sizeof(uint64_t));
    uint64_t print_num = 0;
    nap::Timer print_time;

    print_time.begin();
    while (print_num < print_max) {
      if (print_time.end() > 1000000ull * gap) {
        print_time.begin();
        printf_array[print_num++] = merge_count();
      }
    }
    for (size_t i = 0; i < print_max; i++) {
      printf("%lu %lu\n", i * gap, printf_array[i] / gap);
    }
    printf("Total: %lu\n", throughput);
    puts("[Throughput listen]: end");
    fflush(fp);
    dup2(old, 1);
  }

  inline void begin(int thread_id) { timer[thread_id]->begin(); }

  inline void end(int thread_id) {
    uint64_t latency_value = timer[thread_id]->end();
    latency_bucket[thread_id][min(latency_value / 10, range - 1)]++;
  }

  void merge_print(int thread_id = 0) {
    if (thread_id != 0)
      return;

    ans = (uint64_t *)malloc(sizeof(uint64_t) * (range + 1));
    ans[0] = 0;

    for (int i = 0; i < range; i++) {
      for (int j = 0; j < thread_num; j++) {
        if (latency_bucket[j] == 0) {
          continue;
        }
        ans[i] += latency_bucket[j][i];
      }
      ans[i + 1] = ans[i];
    }
    int old = dup(1);

    FILE *fp = freopen("latency.txt", "w", stdout);
    printf("total_num = %lu\n", ans[range]);
    uint64_t l_p50, l_p90, l_p99, l_p999;
    puts("[latency_cdf]: begin");
    for (int i = 0; i < range; i++) {
      if (ans[i] * 1.0 / ans[range] < 0.5) {
        l_p50 = i;
      }
      if (ans[i] * 1.0 / ans[range] < 0.9) {
        l_p90 = i;
      }
      if (ans[i] * 1.0 / ans[range] < 0.99) {
        l_p99 = i;
      }
      if (ans[i] * 1.0 / ans[range] < 0.999) {
        l_p999 = i;
      }
      printf("%f, %f\n", i * 0.01, ans[i] * 1.0 / ans[range]);
    }

    puts("[latency_cdf]: end");
    fflush(fp);
    dup2(old, 1);
    printf("[50,90,99,999]: %lu, %lu, %lu, %lu\n", l_p50, l_p90, l_p99, l_p999);
    free(ans);
    // fclose(stdout);
    // freopen("/dev/console","w",stdout);
  }
};

latency_evaluation_t::latency_evaluation_t(int _thread_num) {

  throughput = 0;

  thread_num = _thread_num + 1;
  timer = (nap::Timer **)malloc(sizeof(nap::Timer *) * thread_num);

  latency_bucket = (uint64_t **)malloc(sizeof(uint64_t *) * thread_num);
  memset(latency_bucket, 0, sizeof(uint64_t *) * thread_num);

  req_count = (uint64_t **)malloc(sizeof(uint64_t *) * thread_num);
  memset(req_count, 0, sizeof(uint64_t *) * thread_num);

  return;
}
latency_evaluation_t::~latency_evaluation_t() { return; }

#endif // _latncy_evaluation_
