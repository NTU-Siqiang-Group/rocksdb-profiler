#pragma once

#include <unistd.h>
#include <sys/types.h>
#include <string>
#include <signal.h>
#include <cassert>
#include <functional>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>

#include "../streaming/stream.h"
#include "nlohmann/json.hpp"
#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "prometheus/family.h"
#include "prometheus/gauge.h"

class IOMonitor : public Source<std::string> {
 public:
  IOMonitor(): Source() {
    pid_ = ::getpid();
    std::cout << "pid: " << pid_ << std::endl;
  }
  virtual void Start() override;
  virtual void ComputeImpl() override;
  ~IOMonitor() {
    kill(child_, SIGKILL);
  }
 private:
  pid_t pid_;
  pid_t child_;
  int input_fd_;
};

struct IOStat {
  std::string timestamp;
  std::string read_speed;
  std::string write_speed;
  std::string stats_type = "io_stats";
  std::string ToString() {
    nlohmann::json j;
    j["timestamp"] = timestamp;
    j["read_speed"] = read_speed;
    j["write_speed"] = write_speed;
    j["stats_type"] = stats_type;
    return j.dump();
  }
};

class IOLogProcessor : public Node<std::string, std::string> {
 public:
  IOLogProcessor(): Node() {}
  virtual void ComputeImpl() override;
 private:
  std::string buffer_;
};

class PrometheusDataSink : public Sink<std::string> {
 public:
  PrometheusDataSink(int port=8080): Sink() {
    exposer_.reset(new prometheus::Exposer("10.1.3.26:8080"));
    registry_ = std::make_shared<prometheus::Registry>();
    // prometheus::Registry reg;
    auto& builder = prometheus::BuildGauge().Name("sink_data").Help("")
      .Register(*registry_);
    auto& temp = builder.Add({});
    gauge_.reset(&temp);
    exposer_->RegisterCollectable(registry_);
  }
  virtual void ComputeImpl() override {
    auto data = Recv();
    for (auto& c : data) {
      std::cout << c << std::endl;
      // gauge_->Set(c.size());
      const auto random_value = std::rand() % 100;
      gauge_->Set(c.size() * random_value);
    }
  }
  ~PrometheusDataSink() {
    // kill(child_, SIGKILL);
  }
 private:
  std::shared_ptr<prometheus::Exposer> exposer_;
  std::shared_ptr<prometheus::Registry> registry_;
  std::shared_ptr<prometheus::Gauge> gauge_;
};
