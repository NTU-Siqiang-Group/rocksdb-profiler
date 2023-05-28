#pragma once

#include "stream.h"
#include <vector>
#include <thread>
#include <atomic>
#include <functional>

class WorkFlowManager {
 public:
  WorkFlowManager(): stop_(false) {}
  void Start() {
    for (auto src : srcs_) {
      workers.push_back(std::thread([this, src]() {
        startWorkflow(src);
      }));
    }
  }
  void Register(Computable* src) {
    srcs_.push_back(src);
  }
  void Stop() {
    stop_.store(true);
  }
  ~WorkFlowManager() {
    stop_.store(true);
    for (auto& w : workers) {
      w.join();
    }
  }
 private:
  void startWorkflow(Computable* src) {
    src->Start();
    while (true) {
      if (stop_.load()) {
        break;
      }
      src->Compute();
    }
  }
  std::atomic<bool> stop_;
  std::vector<Computable*> srcs_;
  std::vector<std::thread> workers;
};