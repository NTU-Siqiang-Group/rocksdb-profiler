#pragma once

#include "stream.h"
#include <atomic>

template<typename T>
class WorkFlowManager {
 public:
  WorkFlowManager(Source<T>* t): src_(t) {}
  void Start() {
    src_->Start();
    while (true) {
      src_->Compute();
    }
  }
 private:
  Source<T>* src_;
};