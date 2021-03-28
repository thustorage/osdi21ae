#include "index/NUMA_Config.h"
#include "index/fast_fair.h"

int main() {

  init_numa_pool();
  fastfair::btree *bt = new fastfair::btree();

  char a[] = "QWERTT";
  bt->btree_insert(a, (char *)21321);

  bt->btree_insert(a, (char *)1111);

  uint64_t *ret = reinterpret_cast<uint64_t *>(bt->btree_search(a));

  printf("%ld\n", (uint64_t)ret);
}