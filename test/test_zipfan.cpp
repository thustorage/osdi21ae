#include "zipf.h"

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t kMaxKeySpace = 2000 * MB;
uint32_t counter[kMaxKeySpace];


int
main(int argc, char *argv[])
{
	if (argc != 2) {
		printf("Usage: ./test_zipfan KeySpace (M)\n");
		exit(-1);
	}

	uint64_t kKeySpace = MB * std::atoi(argv[1]);
	uint64_t kIterCnt = 100 * MB;

	struct zipf_gen_state state;

	mehcached_zipf_init(&state, kKeySpace, 0.99, 0);

	for (uint64_t i = 0; i < kIterCnt; ++i) {
		counter[mehcached_zipf_next(&state)]++;
	}

	uint64_t sum = 0;
	for (uint64_t k = 0; k < kKeySpace; ++k) {
		sum += counter[k];
		double cum = 1.0 * sum / kIterCnt;

		if (k > 1000000ull) {
			break;
		}

		if (k == 10000 || k == 100000 || k == 1000000) {
                printf("%lf\n", cum);

	
		}
	}

	return 0;
}
