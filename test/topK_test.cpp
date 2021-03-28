#include "count_min_sketch.h"

#include <algorithm>
#include <thread>

#include "zipf.h"

#define kKeySpace (1024ull * 1024 * 1024)

nap::CountMin *CM;

constexpr int kAccessThread = 16;
void access_thread(int i) {

  
  struct zipf_gen_state state;

  mehcached_zipf_init(&state, 1024ull * 1024 * 1024, 0.99, i * 12312312);

  uint64_t count = 0;
  nap::Timer timer;

  uint64_t center = 0;

  timer.begin();
  while (true) {
    uint64_t k = mehcached_zipf_next(&state) + center;
    nap::Timer::sleep(1000);
    if (count++ % 10 == 0) {
      CM->record(nap::Slice((char *)(&k), sizeof(uint64_t)));

      if (timer.end() > (1000ull * 1000 * 1000 * 10)) {
        timer.begin();
        center += 1024;

        printf("hot shift\n");
      }
    }
  }
};

void background_thread() {

  while (true) {
    CM->poll_workloads(1);

    auto &l = CM->get_list();

    std::sort(
        l.begin() + 1, l.end(),
        [](const nap::Node &a, const nap::Node &b) { return a.cnt > b.cnt; });

    // uint64_t all = 0;
    if (l.size() > 10)
      for (int i = 1; i < 10; ++i) {
        auto k = *(uint64_t *)(l[i].key.c_str());
        printf("%ld %d\n", k, l[i].cnt);
      }

    printf("------------------------\n");

    CM->reset();
  }
}

int main() {

  CM = new nap::CountMin(100000);

  for (int i = 0; i < kAccessThread; ++i) {
    new std::thread(access_thread, i);
  }

  (new std::thread(background_thread))->join();

  //   struct zipf_gen_state state;

  //   mehcached_zipf_init(&state, 1024ull * 1024 * 1024, 0.99, 0);

  //   nap::CountMin CM(100000);
  //   mehcached_zipf_next(&state);

  //   Timer t;

  //   uint64_t loop = 1024 * 1024 * 100;
  //   t.begin();
  //   for (size_t i = 1; i < loop; ++i) {
  //     uint64_t k = mehcached_zipf_next(&state);

  //     // uint64_t k = i;
  //     // count[k]++;

  //     // printf("%d %ld\n", i, k);
  //     ++count[k];

  //     if (i % 50 == 0)
  //       CM.access_a_key(nap::Slice((char *)(&k), sizeof(uint64_t)));
  //   }
  //   t.end_print(loop);

  //   printf("%d %d\n", count[2], count[3]);

  //   auto &l = CM.get_list();

  //   std::sort(l.begin() + 1, l.end(), [](const nap::Node &a, const nap::Node
  //   &b) {
  //     return *(uint64_t *)(a.key.c_str()) < *(uint64_t *)(b.key.c_str());
  //   });

  //   uint64_t all = 0;
  //   for (int i = 1; i < l.size(); ++i) {
  //     auto k = *(uint64_t *)(l[i].key.c_str());
  //     printf("%ld %d [error %d]\n", k, l[i].cnt, l[i].cnt - count[k]);
  //     all += count[k];
  //   }

  //   printf("all %ld\n", all);

  return 0;
}