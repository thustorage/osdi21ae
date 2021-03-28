#if !defined(_CN_VIEW_H_)
#define _CN_VIEW_H_

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "nap_common.h"
#include "rw_lock.h"
#include "slice.h"

namespace nap {

class CNView {

  friend class NapMeta;

public:
  struct Entry {

    WRLock l; // control concurrent accesses to the NAL
    bool is_deleted;
    bool shifting;
    int sp_view_index;


    // used for 3-phase switch for lazy initialization
    WhereIsData location;
    
    // serve lookup operation
    std::string v;
    
#ifndef GLOBAL_VERSION
    uint64_t version; // for recoverability
#endif

    Entry()
        : is_deleted(false), shifting(false), sp_view_index(0),
          location(WhereIsData::IN_RAW_INDEX) {
#ifndef GLOBAL_VERSION
      version = 0;
#endif
    }

#ifndef GLOBAL_VERSION
    uint64_t next_version() { return version++; }
#endif
  };

  CNView() {}

  CNView(const std::vector<std::pair<std::string, WhereIsData>> &list) {
    for (size_t i = 0; i < list.size(); ++i) {
      Entry e;
      e.sp_view_index = i;
      e.location = list[i].second;
      view[list[i].first] = std::move(e);
    }
  }

  bool get_entry(const Slice &key, Entry *&entry) {
    auto ret = view.find(key.ToString());
    if (ret == view.end()) {
      return false;
    };

    entry = &ret->second;
    return true;
  }
  

  // finish lazy initialization
  void relocate_value(CNView *old_view) {
    for (auto &e : view) {
      if (e.second.location == WhereIsData::IN_PREVIOUS_EPOCH) {
        e.second.l.wLock();
        if (e.second.location == WhereIsData::IN_PREVIOUS_EPOCH) {
          e.second.location = WhereIsData::IN_CURRENT_EPOCH;
          auto &old_e = old_view->view[e.first];
          e.second.v = old_e.v;
          e.second.is_deleted = old_e.is_deleted;
        }
        e.second.l.wUnlock();
      }
    }
  }

#ifdef SUPPORT_RANGE
  std::map<std::string, Entry> view;
#else
  std::unordered_map<std::string, Entry> view;
#endif

private:
};

} // namespace nap

#endif // _CN_VIEW_H_
