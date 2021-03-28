#include "topology.h"
#include <iostream>
#include <thread>

void thread_func() {
  bindCore(nap::Topology::threadID());

  while (true)
    ;
}

int main() {

  for (int i = 0; i < 64; ++i) {
    new std::thread(thread_func);
  }
  std::cout << "Hello World\n";

  sleep(1024);

  return 0;
}