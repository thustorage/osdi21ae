#include "zipf.h"
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>

#include "murmur_hash2.h"

const uint64_t MB = 1024ull * 1024;
const int key_len = 16;

std::string output_file;
uint64_t keySpace;
uint64_t opCount;
uint32_t readRatio;
double zipfan_args;

inline uint64_t getKey(uint64_t k) {
  return MurmurHash64A(&k, sizeof(uint64_t));
}

std::unordered_set<uint64_t> k_map;

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

void parse_args(int argc, char *argv[]) {
  if (argc != 6) {
    printf("Usage: ./generate_load file_name keySpace(MB) opsCount(MB) "
           "readRatio zipfan_args\n");
    exit(-1);
  }
  output_file = std::string(argv[1]);
  keySpace = std::atoi(argv[2]);
  opCount = std::atoi(argv[3]);
  readRatio = std::atoi(argv[4]);
  zipfan_args = std::atof(argv[5]);

  if (readRatio > 100) {
    printf("read ratio must <= 100\n");
    exit(-1);
  }

  output_file += std::string("-read") + std::to_string(readRatio) + "-zipfan" +
                 std::to_string(int(zipfan_args * 100)) + "-space" +
                 std::to_string(keySpace);

  printf("file: %s\tkey space: %luM\tops count: %luM\tread ratio: %u \tzipfan "
         "%f\n",
         output_file.c_str(), keySpace, opCount, readRatio, zipfan_args);
}

int main(int argc, char *argv[]) {
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
  for (uint64_t i = 0; i < opCount * MB; ++i) {
    bool is_read = (uint32_t)(rand_r(&seed) % 100) < readRatio;
    uint64_t key = getKey(mehcached_zipf_next(&state));

    if (is_read) {
      file << "READ ";
    } else {
      if (k_map.find(key) == k_map.end()) {
        file << "INSERT ";
        k_map.insert(key);
      } else {
        file << "UPDATE ";
      }
    }

    for (size_t j = 0; j < key_len / sizeof(uint64_t) + 1; ++j) {
      buf[j] = key;
    }
    char *char_buf = (char *)buf;
    for (int j = 0; j < key_len; ++j) {
      if (char_buf[j] == '\n') {
        char_buf[j] = '4';
      } else if (char_buf[j] == '\0') {
        char_buf[j] = 'a';
      }
    }
    file.write(char_buf, key_len);

    file << "\n";
  }

  return 0;
}