#include "../include/rocksdbprofiler/sink.h"
#include "prometheus/counter.h"
#include "prometheus/histogram.h"
#include "prometheus/exposer.h"
#include "prometheus/registry.h"
#include "prometheus/gauge.h"

class PrometheusDataSinkImpl : public PrometheusDataSink {
 public:
  PrometheusDataSinkImpl(int port=8080);
  virtual void ComputeImpl() override;
  void RegisterMetric(const std::string& label, const std::string& kind="gauge") override;
 private:
  std::mutex mtx_;
  std::shared_ptr<prometheus::Exposer> exposer_;
  std::shared_ptr<prometheus::Registry> registry_;
  std::unordered_map<std::string, prometheus::Gauge*> gauges_;
  std::unordered_map<std::string, prometheus::Gauge*> counters_;
};

PrometheusDataSinkImpl::PrometheusDataSinkImpl(int port) {
  exposer_.reset(new prometheus::Exposer("127.0.0.1:8080"));
  registry_ = std::make_shared<prometheus::Registry>();
  exposer_->RegisterCollectable(registry_);
}

void PrometheusDataSinkImpl::ComputeImpl() {
  std::lock_guard<std::mutex> lock(mtx_);
  auto data = Recv();
  for (auto& c : data) {
    auto label = c->GetLabel();
    auto value = c->GetValue();
    if (gauges_.find(label) != gauges_.end()) {
      gauges_[label]->Set(value);
    } else if (counters_.find(label) != counters_.end()) {
      counters_[label]->Increment(value);
    }
  }
}
void PrometheusDataSinkImpl::RegisterMetric(const std::string& label, const std::string& kind) {
  if (kind == "gauge") {
    auto& builder = prometheus::BuildGauge()
    .Name(label)
    .Register(*registry_);
    auto& gauge = builder.Add({});
    gauges_[label] = &gauge;
  } else if (kind == "counter") {
    auto& builder = prometheus::BuildGauge()
    .Name(label)
    .Register(*registry_);
    auto& counter = builder.Add({});
    counters_[label] = &counter;
  }
}

PrometheusDataSink* GetDefaultPrometheusDataSink() {
  return new PrometheusDataSinkImpl();
}