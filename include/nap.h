#if !defined(_NAP_H_)
#define _NAP_H_

#include "count_min_sketch.h"
#include "nap_common.h"
#include "nap_meta.h"
#include "slice.h"
#include "timer.h"
#include "topology.h"

#include <algorithm>
#include <string>
#include <thread>
#include <vector>

namespace nap {

struct alignas(kCachelineSize) ThreadMeta {
  uint64_t epoch;
  uint64_t op_seq;
  uint64_t hit_in_cap;
  bool is_in_nap;

  ThreadMeta() : epoch(0), op_seq(0), hit_in_cap(0), is_in_nap(false) {}
};

extern pmem::obj::pool_base pop_numa[kMaxNumaCnt];
extern ThreadMeta thread_meta_array[kMaxThreadCnt];

enum UndoLogType {
  Invalid,
  TYPE_1,
  TYPE_2,
};

struct alignas(64) UndoLog {
  UndoLogType type;
  void *cur;
  void *pre;
  void *gc;

  UndoLog() { type = UndoLogType::Invalid; }

  void logging_type1(void *cur_, void *pre_) {
    cur = cur_;
    pre = pre_;
    compiler_barrier();
    type = UndoLogType::TYPE_1;
    persistent::clflush(this);
  }

  void logging_type2(void *gc_, void *pre_) {
    gc = gc_;
    pre = pre_;
    compiler_barrier();
    type = UndoLogType::TYPE_2;
    persistent::clflush(this);
  }

  void truncate() {
    type = UndoLogType::Invalid;
    persistent::clflush(this);
  }
};

template <class T> class Nap {

private:
  T *raw_index;

  CountMin *CM;
  int hot_cnt;

  int kSampleInterval{1};
  double kSwitchInterval{5.0};

  // In PM
  NapMeta *g_cur_meta;
  NapMeta *g_pre_meta;
  NapMeta *g_gc_meta;

  UndoLog *undo_log;

#ifndef FIX_8_BYTE_VALUE
  CowMeta cow_meta[kMaxThreadCnt];
#endif

  std::atomic<uint64_t> g_cur_epoch;
  std::atomic<uint64_t> epoch_seq_lock;
  ReadFirendlyLock data_race_lock;
  ReadFirendlyLock shift_global_lock;

  void init_pmdk_pool();

  void nap_shift();

  bool find_in_views(CNView::Entry *e, NapMeta *pre_meta, const Slice &key,
                     std::string &value);

  void persist_meta_ptrs() { persistent::clflush(&g_cur_meta); }

  std::thread shift_thread;
  std::atomic_bool shift_thread_is_ready;

public:
  Nap(T *raw_index, int hot_cnt = kHotKeys);
  ~Nap();

  void put(const Slice &key, const Slice &value, bool is_update = false);

  bool get(const Slice &key, std::string &value);

  void del(const Slice &key);

  void range_query(const Slice &key, size_t count,
                   std::vector<std::string> &value_list);

  void internal_query(const Slice &key, size_t count, char *buf) {
#ifdef SUPPORT_RANGE
    auto &container = g_cur_meta->cn_view->view;
    auto it = container.lower_bound(key.ToString());

    size_t buf_size = 0;
    while (it != container.end() && count-- > 0) {

      memcpy(buf + buf_size, it->first.c_str(), it->first.size());
      buf_size += it->first.size();

      memcpy(buf + buf_size, it->second.v.c_str(), 8);

      buf_size += 8;
      it++;
    }

#endif
  }

  void recovery() { g_cur_meta->flush_sp_view(raw_index); }

  void set_sampling_interval(int v) {
    kSampleInterval = v;
    mfence();
  }

  void set_switch_interval(double v) {
    kSwitchInterval = v;
    mfence();
  }

  void clear() {
    for (int i = 0; i < kMaxThreadCnt; ++i) {
      thread_meta_array[i].op_seq = 0;
      thread_meta_array[i].hit_in_cap = 0;
    }
  }

  void show_statistics() {
    uint64_t all_op = 0;
    uint64_t all_hit = 0;
    for (int i = 0; i < kMaxThreadCnt; ++i) {
      all_op += thread_meta_array[i].op_seq;
      all_hit += thread_meta_array[i].hit_in_cap;
    }
    printf("nap hit ratio: %f\n", all_hit * 1.0 / all_op);
  }
};

template <class T>
Nap<T>::Nap(T *raw_index, int hot_cnt)
    : raw_index(raw_index), hot_cnt(hot_cnt), shift_thread_is_ready(false) {

  init_pmdk_pool();

  {

    // for switch thread
    PMEMoid oid;
    pmemobj_alloc(Topology::pmdk_pool()->handle(), &oid, sizeof(UndoLog), 0,
                  nullptr, nullptr);
    undo_log = (UndoLog *)pmemobj_direct(oid);

#ifndef FIX_8_BYTE_VALUE
    // for Cow alloctor

    for (int k = 0; k < kMaxThreadCnt; ++k) {
      pmemobj_alloc(Topology::pmdk_pool()->handle(), &oid, 1024 * 1024, 0,
                    nullptr, nullptr);
      cow_meta[k].log = (char *)pmemobj_direct(oid);
    }
    cow_alloc = new CowAlloctor(cow_meta);

#endif
  }

  shift_thread = std::thread(&Nap<T>::nap_shift, this);

  while (!shift_thread_is_ready)
    ;
}

template <class T> Nap<T>::~Nap() {
  shift_thread_is_ready.store(false);

  shift_thread.join();
}

template <class T> void Nap<T>::init_pmdk_pool() {

  // init per-NUMA PMDK pool
  for (int i = 0; i < Topology::kNumaCnt; ++i) {
    std::string pool_name = std::string("/mnt/pm") + std::to_string(i) + "/nap";
    printf("nap %d pool: %s\n", i, pool_name.c_str());

    remove(pool_name.c_str());
    nap_pop_numa[i] = pmem::obj::pool<int>::create(
        pool_name, "nap", PMEMOBJ_MIN_POOL * 1024 * 2, S_IWUSR | S_IRUSR);
  }
}

template <class T>
void Nap<T>::put(const Slice &key, const Slice &value, bool is_update) {

#ifdef USE_GLOBAL_LOCK
  shift_global_lock.read_lock();
#endif

  auto &thread_meta = thread_meta_array[Topology::threadID()];

  NapMeta *cur_meta, *pre_meta;
  uint64_t version, next_version, cur_epoch;
  thread_meta.is_in_nap = true;
  thread_meta.op_seq++;
   

  // sampling and publish access pattern
  if (thread_meta.op_seq % kSampleInterval == 0) {
    CM->record(key);
  }

retry:

  version = epoch_seq_lock.load(std::memory_order_acquire);
  if (version % 2 != 0) {
    goto retry;
  }

  mfence();

  /* save a metadata snapshot */
  cur_meta = g_cur_meta;
  pre_meta = g_pre_meta;
  cur_epoch = g_cur_epoch;

  compiler_barrier();
  next_version = epoch_seq_lock.load(std::memory_order_acquire);
  if (next_version != version) {
    goto retry;
  }

  thread_meta.epoch = cur_epoch;

  assert(cur_meta);
  CNView::Entry *e;
  if (cur_meta->cn_view->get_entry(key, e)) { // in the cur_meta

    thread_meta.hit_in_cap++;

    bool is_writer = false;

    auto alloc_ptr = cur_meta->sp_view->alloc_before_update(key, value);

  re_lock:
    if (!e->l.try_putLock(is_writer)) {
      if (is_writer) { // two requests of the same key, one of which can b
// returned directly after waiting for unlock.
#ifdef USE_GLOBAL_LOCK
        shift_global_lock.read_unlock();
#endif
        while (!e->l.is_unlock())
          ;
        return;
      }
      goto re_lock;
    }

    if (e->shifting) { // I am previous view, cannot update now
      e->l.putUnlock();
      goto retry;
    }

    if (e->location == WhereIsData::IN_PREVIOUS_EPOCH) { //
      CNView::Entry *pre_e;
      if (pre_meta->cn_view->get_entry(key, pre_e)) {
        pre_e->l.wLock();
        pre_e->shifting = true;
        pre_e->l.wUnlock();
      } else {
        assert(false);
      }
    }

#ifdef GLOBAL_VERSION
    cur_meta->sp_view->update(e->sp_view_index, alloc_ptr, key, value, 1);
#else
    cur_meta->sp_view->update(e->sp_view_index, alloc_ptr, key, value,
                              e->next_version());
#endif

    e->v = value.ToString();
    e->is_deleted = false;

    if (e->location != WhereIsData::IN_CURRENT_EPOCH) {
      e->location = WhereIsData::IN_CURRENT_EPOCH;
    }

    e->l.putUnlock();
  } else if (pre_meta) {
    if (pre_meta->cn_view->get_entry(key, e)) { // in the pre_meta
      thread_meta.is_in_nap = false;
      while (g_pre_meta != nullptr) {
        mfence();
      }
      thread_meta.is_in_nap = true;
      goto retry;
    } else {
      raw_index->put(key, value, is_update);
    }
  } else {

    data_race_lock.read_lock();
    if (pre_meta == nullptr) {
      raw_index->put(key, value, is_update);
    } else { // is_shifting
      assert(false);
      data_race_lock.read_unlock();
      goto retry;
    }
    data_race_lock.read_unlock();
  
  }

  compiler_barrier();
  thread_meta.is_in_nap = false;

#ifdef USE_GLOBAL_LOCK
  shift_global_lock.read_unlock();
#endif
}

template <class T> bool Nap<T>::get(const Slice &key, std::string &value) {
#ifdef USE_GLOBAL_LOCK
  shift_global_lock.read_lock();
#endif
  auto &thread_meta = thread_meta_array[Topology::threadID()];
  thread_meta.is_in_nap = true;
  thread_meta.op_seq++;
  
   // sampling and publish access pattern
  if (thread_meta.op_seq % kSampleInterval == 0) {
    CM->record(key);
  }

  NapMeta *cur_meta, *pre_meta;
  uint64_t version, next_version, cur_epoch;
  next_version = 0;

  int count = 0;
retry:
  if (++count > 10000) {
    printf("exit with retry! %ld %ld\n", version, next_version);
    exit(-1);
  }
  version = epoch_seq_lock.load(std::memory_order::memory_order_acquire);
  if (version % 2 != 0) {
    goto retry;
  }

  mfence();
  cur_meta = g_cur_meta;
  pre_meta = g_pre_meta;
  cur_epoch = g_cur_epoch;

  compiler_barrier();
  next_version = epoch_seq_lock.load(std::memory_order::memory_order_acquire);
  if (next_version != version) {
    goto retry;
  }

  thread_meta.epoch = cur_epoch;

  bool res = true;
  assert(cur_meta);
  CNView::Entry *e;
  if (cur_meta->cn_view->get_entry(key, e)) { // in the cur_meta
    thread_meta.hit_in_cap++;
    res = find_in_views(e, pre_meta, key, value);
  } else if (pre_meta) {
    assert(pre_meta->cn_view);
    if (pre_meta->cn_view->get_entry(key, e)) { // in the pre_meta

      res = find_in_views(e, nullptr, key, value);
    } else {
      res = raw_index->get(key, value); // in the raw index
    }
  } else {
    res = raw_index->get(key, value); // in the raw index
  }

  compiler_barrier();
  thread_meta.is_in_nap = false;
#ifdef USE_GLOBAL_LOCK
  shift_global_lock.read_unlock();
#endif
  return res;
}

template <class T>
bool Nap<T>::find_in_views(CNView::Entry *e, NapMeta *pre_meta,
                           const Slice &key, std::string &value) {

retry:
  bool res = false;
  e->l.rLock();
  switch (e->location) {
  case WhereIsData::IN_CURRENT_EPOCH: {
    if (!e->is_deleted) {
      value = e->v;
    }

    e->l.rUnlock();
    res = !e->is_deleted;
  }

  break;
  case WhereIsData::IN_PREVIOUS_EPOCH: {
    assert(pre_meta);
    CNView::Entry *pre_e;
    e->l.rUnlock();
    e->l.wLock();

    if (e->location != WhereIsData::IN_PREVIOUS_EPOCH) {
      e->l.wUnlock();
      goto retry;
    }

    if (pre_meta->cn_view->get_entry(key, pre_e)) {

      pre_e->l.wLock();
      pre_e->shifting = true;
      e->location = WhereIsData::IN_CURRENT_EPOCH;
      if (pre_e->location == WhereIsData::IN_CURRENT_EPOCH) {
        e->is_deleted = pre_e->is_deleted;
        e->v = pre_e->v;
        value = e->v;
        res = !e->is_deleted;
      } else if (pre_e->location == WhereIsData::IN_RAW_INDEX) { //
        res = raw_index->get(key, value);
        if (res) {
          e->v = value;
          pre_e->v = value;
        } else {
          e->is_deleted = true;
          pre_e->is_deleted = true;
        }
      } else {
        printf("%ld\n", *(uint64_t *)key.ToString().c_str());
        assert(false);
      }
      pre_e->l.wUnlock();
    } else {
      printf("%ld\n", *(uint64_t *)key.ToString().c_str());
      assert(false);
    }
    e->l.wUnlock();
  }

  break;
  case WhereIsData::IN_RAW_INDEX: {

    e->l.rUnlock();
    e->l.wLock();
    if (e->location != WhereIsData::IN_RAW_INDEX) {
      e->l.wUnlock();
      goto retry;
    }

    e->location = WhereIsData::IN_CURRENT_EPOCH;
    res = raw_index->get(key, value);

    if (res) {
      e->v = value;
    } else {
      e->is_deleted = true;
    }
    e->l.wUnlock();
  }

  break;

  default:
    break;
  }

  return res;
}

template <class T> void Nap<T>::del(const Slice &key) {
#ifdef USE_GLOBAL_LOCK
  shift_global_lock.read_lock();
#endif

  Slice value = Slice::null();

  auto &thread_meta = thread_meta_array[Topology::threadID()];

  NapMeta *cur_meta, *pre_meta;
  uint64_t version, next_version, cur_epoch;
  thread_meta.is_in_nap = true;
  thread_meta.op_seq++;

  if (thread_meta.op_seq % kSampleInterval == 0) {
    CM->record(key);
  }

retry:

  version = epoch_seq_lock.load(std::memory_order_acquire);
  if (version % 2 != 0) {
    goto retry;
  }

  mfence();
  cur_meta = g_cur_meta;
  pre_meta = g_pre_meta;
  cur_epoch = g_cur_epoch;

  compiler_barrier();
  next_version = epoch_seq_lock.load(std::memory_order_acquire);
  if (next_version != version) {
    goto retry;
  }

  thread_meta.epoch = cur_epoch;

  assert(cur_meta);
  CNView::Entry *e;
  if (cur_meta->cn_view->get_entry(key, e)) {

    thread_meta.hit_in_cap++;

    bool is_writer = false;

    e->l.wLock();

    if (e->shifting) { // I am previous view, cannot update now
      e->l.wUnlock();
      goto retry;
    }

    if (e->location == WhereIsData::IN_PREVIOUS_EPOCH) { //
      CNView::Entry *pre_e;
      if (pre_meta->cn_view->get_entry(key, pre_e)) {
        pre_e->l.wLock();
        pre_e->shifting = true;
        pre_e->l.wUnlock();
      } else {
        assert(false);
      }
    }

#ifdef GLOBAL_VERSION
    cur_meta->sp_view->update(e->sp_view_index, nullptr, key, value, 1, true);
#else
    cur_meta->sp_view->update(e->sp_view_index, nullptr, key, value,
                              e->next_version(), true);
#endif

    e->is_deleted = true;

    if (e->location != WhereIsData::IN_CURRENT_EPOCH) {
      e->location = WhereIsData::IN_CURRENT_EPOCH;
    }

    e->l.putUnlock();
  } else if (pre_meta) {
    if (pre_meta->cn_view->get_entry(key, e)) {
      thread_meta.is_in_nap = false;
      while (g_pre_meta != nullptr) {
        mfence();
      }
      thread_meta.is_in_nap = true;
      goto retry;
    } else {
      raw_index->del(key);
    }
  } else {

    // LOCK
    data_race_lock.read_lock();
    if (pre_meta == nullptr) {
      raw_index->del(key);
    } else { // is_shifting
      assert(false);
      data_race_lock.read_unlock();
      goto retry;
    }
    data_race_lock.read_unlock();

    // UNLOCK
  }

  compiler_barrier();
  thread_meta.is_in_nap = false;

#ifdef USE_GLOBAL_LOCK
  shift_global_lock.read_unlock();
#endif
}

template <class T>
void Nap<T>::range_query(const Slice &key, size_t count,
                         std::vector<std::string> &value_list) {
  // use ``internal_query`` for evaluation.
}

template <class T> void Nap<T>::nap_shift() {

  static auto sort_func = [](const NapPair &a, const NapPair &b) {
    return a.first < b.first;
  };

  bindCore(Topology::threadID());

  CM = new CountMin(hot_cnt);

  g_cur_epoch = 1;
  epoch_seq_lock = 0;
  g_cur_meta = g_pre_meta = g_gc_meta = nullptr;

  std::vector<NapPair> cur_list;
  g_cur_meta = new NapMeta(cur_list);

  shift_thread_is_ready.store(true);

  printf("shift thread finished init [%d].\n", Topology::threadID());

  const int kPreHotest = 8;
  std::string pre_hotest_keys[kPreHotest];
  while (shift_thread_is_ready) {

    CM->reset(); // clear min-count sketch and min heap
    CM->poll_workloads(kSwitchInterval /* seconds */);

    auto &l = CM->get_list();

    std::sort(
        l.begin() + 1, l.end(),
        [](const nap::Node &a, const nap::Node &b) { return a.cnt > b.cnt; });

    if (l.size() <= kPreHotest || l[1].cnt < 100) { // not need shift
      continue;
    }

    if (l[1].cnt / l.back().cnt < 3) { // it is a uniform workload
      continue;
    }

    bool need_shift = true;
    for (int i = 0; i < kPreHotest; ++i) {
      if (l[1].key == pre_hotest_keys[i]) {
        need_shift = false;
        break;
      }
    }

    if (!need_shift) {
      continue;
    }

    // for (size_t i = 1; i < 10; ++i) {
    //   auto k = *(uint64_t *)(l[i].key.c_str());
    //   printf("%ld %d\n", k, l[i].cnt);
    // }

#ifdef USE_GLOBAL_LOCK
    shift_global_lock.write_lock();
#endif

    for (size_t i = 0; i < kPreHotest; ++i) {
      pre_hotest_keys[i] = l[1 + i].key;
    }

    std::vector<NapPair> new_list;
    for (uint64_t k = 1; k < l.size(); ++k) {
      new_list.push_back({l[k].key, WhereIsData::IN_RAW_INDEX});
    }
    
    std::sort(new_list.begin(), new_list.end(), sort_func);

    uint64_t overlapped_cnt = 0;
    for (size_t i = 0, j = 0; i < cur_list.size() && j < new_list.size();) {
      int cmp = cur_list[i].first.compare(new_list[j].first);
      if (cmp == 0) { // overlapped kv in different epoch
        new_list[j].second = WhereIsData::IN_PREVIOUS_EPOCH;
        i++, j++;
        overlapped_cnt++;
      } else if (cmp < 0) {
        i++;
      } else {
        j++;
      }
    }

    if (overlapped_cnt > 0.75 * hot_cnt) { // not need shift
      continue;
    }

    auto new_meta = new NapMeta(new_list);
    auto old_meta = g_cur_meta;

    cur_list.swap(new_list);

    undo_log->logging_type1(g_cur_meta, g_pre_meta); // undo logging
    data_race_lock.write_lock();
    epoch_seq_lock.fetch_add(1); 
    g_cur_meta = new_meta;
    // sleep(1);
    g_pre_meta = old_meta;
    // sleep(1);
    g_cur_epoch++;
    epoch_seq_lock.fetch_add(1);
    data_race_lock.write_unlock();

    persist_meta_ptrs();
    undo_log->truncate();

    // Timer timer;
    // timer.begin();

    // printf("new epoch %ld {%p}\n", g_cur_epoch.load(), g_cur_meta->sp_view);

    // wait util all threads learn that a shifting is ongoing.
    for (int i = 0; i < kMaxThreadCnt; ++i) {
      auto &m = thread_meta_array[i];
      while (m.epoch != g_cur_epoch && m.is_in_nap) {
        mfence();
      }
    }

    // timer.end_print(1);
    // timer.begin();

    // printf("epoch %ld flush sp view\n", g_cur_epoch.load());
    old_meta->flush_sp_view<T>(raw_index); // flush the NAL into raw index

    // timer.end_print(1);
    // timer.begin();

    // printf("epoch %ld relocate_value\n", g_cur_epoch.load());
    new_meta->relocate_value(old_meta); // finish lazy initialization

    // timer.end_print(1);
    // timer.begin();

    compiler_barrier();

    auto del_meta = g_pre_meta;

    undo_log->logging_type2(g_gc_meta, g_pre_meta);
    epoch_seq_lock.fetch_add(1);
    g_gc_meta = g_pre_meta;
    g_pre_meta = nullptr;
    epoch_seq_lock.fetch_add(1);

    persist_meta_ptrs();
    undo_log->truncate();

    // timer.end_print(1);

    // printf("-----------%ld-----------\n", g_cur_epoch.load());

    // wait a grace period period for safe dealloction.
    for (int i = 0; i < kMaxThreadCnt; ++i) {
      auto &m = thread_meta_array[i];
      auto cur_seq = m.op_seq;

    retry:
      if (!m.is_in_nap) {
        continue;
      }
      if (m.op_seq != cur_seq) {
        continue;
      }

      mfence();
      goto retry;
    }

    delete del_meta;
    g_gc_meta = nullptr;
    persist_meta_ptrs();

#ifdef USE_GLOBAL_LOCK
    shift_global_lock.write_unlock();
#endif
    // printf("delete meta of epoch %ld safely\n", g_cur_epoch - 1);
  }

  printf("shift thread stopped.\n");
}

} // namespace nap

#endif // _NAP_H_
