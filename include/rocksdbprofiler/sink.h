#pragma once

#include "stream/stream.h"
#include "metrics.h"

#include <unordered_map>

class PrometheusDataSink : public Sink<std::shared_ptr<Metric> > {
 public:
  virtual void ComputeImpl() override = 0;
  virtual void RegisterMetric(const std::string& label, const std::string& kind="gauge") = 0;
};

PrometheusDataSink* GetDefaultPrometheusDataSink();
