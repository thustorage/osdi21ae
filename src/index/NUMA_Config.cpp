#include "index/NUMA_Config.h"

pmem::obj::pool_base pop_numa[nap::kMaxNumaCnt];
int numa_map[nap::kMaxThreadCnt];

thread_local int my_thread_id = 0;

// void bindCore(uint16_t core) {
//   printf("bind %d\n", core);
//   cpu_set_t cpuset;
//   CPU_ZERO(&cpuset);
//   CPU_SET(core, &cpuset);
//   int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
//   if (rc != 0) {
//     printf("can't bind core %d!", core);
//     exit(-1);
//   }
// }


void init_numa_pool()
{

  // for (int i = 0; i < 14; ++i) {
  //   numa_map[i] = 0;
  // }
  //   for (int i = 14; i < 18; ++i) {
  //   numa_map[i] = 1;
  // }
  
  // for (int i = 0 + 18; i < 14 + 18; ++i) {
  //   numa_map[i] = 2;
  // }
  //   for (int i = 14 + 18; i < 18 + 18; ++i) {
  //   numa_map[i] = 3;
  // // }

  
  // for (int i = 36; i <40; ++i) {
  //   numa_map[i] = 4;
  // }
  //   for (int i = 40; i < 54; ++i) {
  //   numa_map[i] = 5;
  // }

  // core id => numa id
  for (int i = 0; i < 18; ++i)
  {
    numa_map[i] = 0;
  }
  for (int i = 18; i < 36; ++i)
  {
    numa_map[i] = 1;
  }
  for (int i = 36; i < 54; ++i)
  {
    numa_map[i] = 2;
  }
  for (int i = 54; i < 72; ++i)
  {
    numa_map[i] = 3;
  }

  for (int i = 0; i < nap::Topology::kNumaCnt; ++i)
  {
    std::string pool_name =
        std::string("/mnt/pm") + std::to_string(i) + "/numa";
    printf("numa %d pool: %s\n", i, pool_name.c_str());

    remove(pool_name.c_str());
    pop_numa[i] = pmem::obj::pool<int>::create(
        pool_name, "WQ", PMEMOBJ_MIN_POOL * 1024 * 8, S_IWUSR | S_IRUSR);
  }
}

void *index_pmem_alloc(size_t size)
{
  PMEMoid oid;
  if (pmemobj_alloc(pop_numa[numa_map[my_thread_id]].handle(), &oid, size, 0,
                    nullptr, nullptr))
  {
    fprintf(stderr, "fail to alloc nvm\n");
    exit(-1);
  }

  return (void *)pmemobj_direct(oid);
}

void index_pmem_free(void *ptr)
{
  auto f_oid = pmemobj_oid(ptr);
  pmemobj_free(&f_oid);
}