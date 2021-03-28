#include "nap.h"

namespace nap {

pmem::obj::pool_base nap_pop_numa[kMaxNumaCnt];
ThreadMeta thread_meta_array[kMaxThreadCnt];

CowAlloctor *cow_alloc;


} // namespace nap
