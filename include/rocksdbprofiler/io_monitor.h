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

#include "stream/stream.h"
#include "metrics.h"

class IOMonitor : public Source<std::string> {
 public:
  IOMonitor(): Source() {
    pid_ = ::getpid();
  }
  virtual void Start() override;
  virtual void ComputeImpl() override;
  ~IOMonitor() {
    // kill(child_, SIGKILL);
  }
 private:
  pid_t pid_;
  pid_t child_;
  int input_fd_;
};

class ReadIOMetric : public Metric {
 public:
  ReadIOMetric(const double v=0.0): label_("read_io"), value_(v) {}
  virtual std::string GetLabel() override {
    return label_;
  }
  virtual double GetValue() override {
    return value_;
  }
 private:
  std::string label_;
  double value_;
};

class WriteIOMetric : public Metric {
 public:
  WriteIOMetric(const double v=0.0): label_("write_io"), value_(v) {}
  virtual std::string GetLabel() override {
    return label_;
  }
  virtual double GetValue() override {
    return value_;
  }
 private:
  std::string label_;
  double value_;
};

class IOLogProcessor : public Node<std::string, std::shared_ptr<Metric> > {
 public:
  IOLogProcessor(): Node() {}
  virtual void ComputeImpl() override;
 private:
  std::string buffer_;
};

static std::shared_ptr<IOMonitor> GetDefaultIOMonitor() {
  auto ret = std::make_shared<IOMonitor>();
  auto io_log_processor = std::make_shared<IOLogProcessor>();
  ret->Next(io_log_processor.get());
  return ret;
}
