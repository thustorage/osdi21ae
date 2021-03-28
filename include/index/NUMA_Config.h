#if !defined(_NUMA_CONFIG)
#define _NUMA_CONFIG

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
#include <libpmemobj++/experimental/v.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "../topology.h"

extern pmem::obj::pool_base pop_numa[nap::kMaxNumaCnt];
extern int numa_map[nap::kMaxThreadCnt];
extern thread_local int my_thread_id;

void bindCore(uint16_t core);
void init_numa_pool();

void *index_pmem_alloc(size_t size);
void index_pmem_free(void *ptr);

#endif // _NUMA_CONFIG
