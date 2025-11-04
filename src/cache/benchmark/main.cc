
#include <glog/logging.h>

#include <iostream>

#include "cache/benchmark/benchmarker.h"
#include "cache/utils/logging.h"

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);

  dingofs::cache::InitLogging(argv[0]);

  brpc::StartDummyServerAt(20000);

  char c;
  std::cout << "brpc start at :20000" << '\n';
  std::cout << "enter any to conitnue...." << '\n';
  std::cin >> c;
  std::cout << " benchmark is start." << '\n';

  // Init benchmarker
  dingofs::cache::Benchmarker benchmarker;
  auto status = benchmarker.Start();
  if (!status.ok()) {
    std::cerr << "Failed to initialize benchmarker: " << status.ToString()
              << '\n';
    return -1;
  }

  // Run until finish
  benchmarker.RunUntilFinish();

  return 0;
}
