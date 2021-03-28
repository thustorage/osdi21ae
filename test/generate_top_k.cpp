#include "zipf.h"
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>


const uint64_t MB = 1024ull * 1024;
const int key_len = 16;

std::string output_file;
uint64_t keySpace;
uint64_t topK;
uint32_t readRatio;
double zipfan_args;

inline uint64_t
getKey(uint64_t k)
{
	return k;
}

std::unordered_set<uint64_t> k_map;

__inline__ unsigned long long
rdtsc(void)
{
	unsigned hi, lo;
	__asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
	return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

void
parse_args(int argc, char *argv[])
{
	if (argc != 5) {
		printf("Usage: ./generate_load file_name keySpace(MB) topK zipfan_args\n"); 		exit(-1);
	}
	output_file = std::string(argv[1]);
	keySpace = std::atoi(argv[2]);
	topK = std::atoi(argv[3]);
	zipfan_args = std::atof(argv[4]);

	if (readRatio > 100) {
		printf("read ratio must <= 100\n");
		exit(-1);
	}

	printf("file: %s\tkey space: %luM\ttopK: %lu\tread ratio: %u\tzipfan%f\n", 	       output_file.c_str(), keySpace, topK, readRatio,
zipfan_args);
}

int
main(int argc, char *argv[])
{
	parse_args(argc, argv);

	std::ofstream file(output_file);
	if (!file) {
		printf("can not open %s\n", output_file.c_str());
		exit(-1);
	}

	uint32_t seed = rdtsc();
	struct zipf_gen_state state;

	uint64_t buf[1024];
	mehcached_zipf_init(&state, keySpace * MB, zipfan_args, seed);
	for (uint64_t i = 0; i < topK; ++i) {
		uint64_t key = getKey(i);

		for (size_t j = 0; j < key_len / sizeof(uint64_t) + 1; ++j) {
			buf[j] = key;
		}
		char *char_buf = (char *)buf;
		for (int j = 0; j < key_len; ++j) {
			if (char_buf[j] == '\n') {
				char_buf[j] = '4';
			}
		}
		file.write(char_buf, key_len);

		file << "\n";
	}

	return 0;
}