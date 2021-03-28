#include "latency_evaluation.h"
#include <bits/stdc++.h>
#include <unistd.h>
int main() {
  latency_evaluation_t test(2);
  test.init_thread(0);
  test.init_thread(1);
  for (uint32_t i = 0; i < 10002222; i++) {
    test.begin(i%2);
    // usleep(1);
    test.end(i%2);
  }
  test.merge_print();
}