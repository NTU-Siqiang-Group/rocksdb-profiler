#pragma once

#include "stream/stream.h"
#include "metrics.h"
#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "prometheus/family.h"
#include "prometheus/gauge.h"
#include <unordered_map>

class PrometheusDataSink : public Sink<std::shared_ptr<Metric>> {
 public:
  PrometheusDataSink(int port=8080): Sink() {
    exposer_.reset(new prometheus::Exposer("10.1.3.26:8080"));
    registry_ = std::make_shared<prometheus::Registry>();
    exposer_->RegisterCollectable(registry_);
  }
  virtual void ComputeImpl() override {
    auto data = Recv();
    for (auto& c : data) {
      auto label = c->GetLabel();
      auto value = c->GetValue();
      std::cout << label << " " << value << std::endl;
      if (gauges_.find(label) == gauges_.end()) {
        return;
      }
      auto gauge = gauges_[label];
      gauge->Set(value);
    }
  }
  void RegisterMetric(const std::string& label) {
    // auto& gauge = gauage_builder_->Add({{"label", label}});
    auto& builder = prometheus::BuildGauge()
      .Name(label)
      .Register(*registry_);
    auto& gauge = builder.Add({});
    gauges_[label] = &gauge;
  }
 private:
  std::shared_ptr<prometheus::Exposer> exposer_;
  std::shared_ptr<prometheus::Registry> registry_;
  std::unordered_map<std::string, prometheus::Gauge*> gauges_;
};