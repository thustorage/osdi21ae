#if !defined(_NAP_META_H_)
#define _NAP_META_H_

#include "cn_view.h"
#include "sp_view.h"

namespace nap
{

struct NapMeta {
	CNView *cn_view;
	SPView *sp_view;
	// TODO bloom filter

	NapMeta(): cn_view(nullptr), sp_view(nullptr)
	{
	}

	NapMeta(std::vector<NapPair> &list)
	{
		cn_view = new CNView(list);
		sp_view = new SPView(list);
	}

	~NapMeta() {
		if (cn_view) {
			delete cn_view;
		}
		if (sp_view) {
			delete sp_view;
		}
	}

	template <class T>
	void
	flush_sp_view(T *raw_index)
	{
		sp_view->flush_to_raw_index<T>(raw_index);
	}

	void
	relocate_value(NapMeta *old_meta)
	{
        cn_view->relocate_value(old_meta->cn_view);
	}
};
} // namespace nap

#endif // _NAP_META_H_
