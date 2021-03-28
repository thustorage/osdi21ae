#include "index/mock_index.h"
#include "nap.h"
#include "top_k.h"

#include <cassert>
#include <iostream>
#include <thread>

#include "zipf.h"

std::thread th[100];

nap::Nap<nap::MockIndex> *index_ptr;

void thread_read(int id) {
  printf("I am %d in numa %d\n", nap::Topology::threadID(),
         nap::Topology::numaID());
  auto &index = *index_ptr;
  (void)index;

  { // read only
    uint64_t count = 0;
    while (true) {
      std::string val;
      for (uint64_t k = 0; k < 102400; ++k) {
        auto key = std::string((char *)&k, sizeof(uint64_t));
        uint64_t t = k * 2;
        auto value = std::string((char *)&t, sizeof(uint64_t));
        assert(index.get(key, val));

        if (k < 51200) {

          if (value != val) {
            printf("[%ld]% ld % ld\n ", k, *(uint64_t *)value.c_str(),
                   *(uint64_t *)val.c_str());
            assert(value == val);
          }
        } else {
          if (key != val) {
            printf("[%ld] % ld % ld\n ", k, *(uint64_t *)value.c_str(),
                   *(uint64_t *)val.c_str());
            assert(key == val);
          }
        }
      }
      if (count++ % 10 == 0) {
        printf("%ld\n", count);
      }
    }
  }
}

int kWriteThread = 0;

void thread_write(int id) {
  printf("I am %d in numa %d\n", nap::Topology::threadID(),
         nap::Topology::numaID());

  bindCore(nap::Topology::threadID());

  uint64_t count = 0, pre_count = 0;
  auto &index = *index_ptr;

  struct zipf_gen_state state;

  mehcached_zipf_init(&state, 1024ull * 1024 * 1024, 0.99, id * 12312312);

  uint32_t seed = id * 123231;

  nap::Timer timer;
  uint64_t center = 0;

  timer.begin();

  std::string str;
  while (true) {
    uint64_t k = mehcached_zipf_next(&state) + center;

    auto key = std::string((char *)&k, sizeof(uint64_t));

    if (rand_r(&seed) % 100 < 5) {
      index.put(key, key);
    } else {
      index.get(key, str);
    }

    if (count++ % 10 == 0) {
      if (timer.end() > (1000ull * 1000 * 1000 * 5)) {
        timer.begin();
        // center += 1024;

        printf("[%d], %ld\n", nap::Topology::threadID(),
               (count - pre_count) * kWriteThread / 5);
        pre_count = count;
      }
    }
  }

  //   int cap = 102400 / kWriteThread;

  //   { //
  //     printf("[%ld %ld)", cap * id, cap * id + cap);
  //     while (true) {
  //       std::string val;

  //       for (uint64_t k = cap * id; k < cap * id + cap; ++k) {
  //         auto key = std::string((char *)&k, sizeof(uint64_t));
  //         uint64_t t = k * count;
  //         auto value = std::string((char *)&t, sizeof(uint64_t));

  //         index.put(key, value);
  //         assert(index.get(key, val));

  //         if (value != val) {
  //           printf("[%ld]% ld % ld\n ", k, *(uint64_t *)value.c_str(),
  //                  *(uint64_t *)val.c_str());
  //           assert(value == val);
  //         }
  //       }
  //       for (uint64_t k = cap * id; k < cap * id + cap; ++k) {
  //         auto key = std::string((char *)&k, sizeof(uint64_t));
  //         uint64_t t = k * count;
  //         auto value = std::string((char *)&t, sizeof(uint64_t));
  //         assert(index.get(key, val));
  //         // assert(val == value);

  //         if (value != val) {
  //           printf("[%ld]% ld % ld\n ", k, *(uint64_t *)value.c_str(),
  //                  *(uint64_t *)val.c_str());
  //           sleep(5);
  //           assert(value == val);
  //         }
  //       }

  //       if (count++ % 10 == 0) {
  //         printf("%ld\n", count);
  //       }
  //     }
  //   }
}

int main(int argc, char *argv[]) {

  if (argc != 2) {
    printf("usage: ./exe thread_num\n");
    exit(-1);
  }

  kWriteThread = std::atoi(argv[1]);

  nap::MockIndex raw_index;
  for (uint64_t k = 0; k < 102400; ++k) {
    auto key = std::string((char *)&k, sizeof(uint64_t));
    raw_index.put(key, key, false);
  }

  nap::Nap<nap::MockIndex> index(&raw_index);
  index_ptr = &index;

  // for (int k = 0; k < 70; ++k) {

  //    th[k] = std::thread([=] {
  //     for (int i = 0; i < 102330; ++i) {
  //       char *ds = (char *)nap::cow_alloc->malloc(rand() % 512);
  //       *ds = 23;
  //       nap::cow_alloc->free(ds);
  //       // printf("%p\n", ds);
  //     }
  //     printf("%d OK\n", k);
  //   });
  // }
  // for (int k = 0; k < 70; ++k) {
  //    th[k].join();
  // }

  // exit(-1);

  std::string val;

  //   for (uint64_t k = 0; k < 51200; ++k) {
  //     auto key = std::string((char *)&k, sizeof(uint64_t));
  //     // printf("%ld\n", k);
  //     assert(index.get(key, val));
  //     assert(key == val);
  //     assert(index.get(key, val));
  //     assert(key == val);

  //     uint64_t t = k * 2;
  //     auto value = std::string((char *)&t, sizeof(uint64_t));
  //     index.put(key, value);

  //     assert(index.get(key, val));
  //     assert(value == val);
  //   }

  for (int i = 0; i < kWriteThread; ++i) {
    th[i] = std::thread(thread_write, i);
  }
  while (true)
    ;

  return 0;
}