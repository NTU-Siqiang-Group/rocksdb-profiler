#pragma once

#include <string>

class Metric {
 public:
  virtual std::string GetLabel() = 0;
  virtual double GetValue() = 0;
};