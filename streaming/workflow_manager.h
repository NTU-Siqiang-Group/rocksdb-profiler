#pragma once

#include "stream.h"
#include <atomic>

template<typename T>
class WorkFlowManager {
 public:
  WorkFlowManager(Source<T>* t): src_(t), stop_(0) {}
  void Start() {
    src_->Start();
    while (true) {
      src_->Compute();
      stop_++;
      if (stop_.load() > 20) {
        break;
      }
    }
  }
 private:
  Source<T>* src_;
  std::atomic<int> stop_;
};