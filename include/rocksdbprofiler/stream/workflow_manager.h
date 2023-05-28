#pragma once

#include "stream.h"
#include <vector>
#include <thread>
#include <atomic>

class WorkFlowManager {
 public:
  WorkFlowManager(): stop_(false) {}
  void Start() {
    workers.push_back(std::thread([this]() {
      for (auto& src : srcs_) {
        startWorkflow(src);
      }
    }));
  }
  void Register(Computable* src) {
    srcs_.push_back(src);
  }
  void Stop() {
    stop_.store(true);
  }
  ~WorkFlowManager() {
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