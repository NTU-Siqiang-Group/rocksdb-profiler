#pragma once

#include "rocksdb/listener.h"
#include "rocksdb/statistics.h"
#include "metrics.h"
#include "stream/stream.h"

#include <mutex>

class RocksDBEventMetric : public Metric {
 public:
  RocksDBEventMetric(const std::string& label): label_(label) {}
  virtual std::string GetLabel() override {
    return label_;
  }
 private:
  std::string label_;
};

class RocksDBCompactionMetric : public RocksDBEventMetric {
 public:
  RocksDBCompactionMetric(double v): RocksDBEventMetric("comp_end"), value_(v) {}
  virtual double GetValue() override {
    return value_;
  }
 private:
  double value_;
};

class RocksDBFlushMetric : public RocksDBEventMetric {
 public:
  RocksDBFlushMetric(double v): RocksDBEventMetric("flush_end"), value_(v) {}
  virtual double GetValue() override {
    return value_;
  }
 private:
  double value_;
};

class RocksDBOpStat : public Metric {
 public:
  RocksDBOpStat(const std::string& label): label_(label) {}
  virtual std::string GetLabel() override {
    return label_;
  }
private:
  std::string label_;
};

class RocksDBReadStat : public RocksDBOpStat {
 public:
  RocksDBReadStat(double v): RocksDBOpStat("read_stat"), value_(v) {}
  virtual double GetValue() override {
    return value_;
  }
 private:
  double value_;
};

class RocksDBWriteStat : public RocksDBOpStat {
 public:
  RocksDBWriteStat(double v): RocksDBOpStat("write_stat"), value_(v) {}
  virtual double GetValue() override {
    return value_;
  }
 private:
  double value_;
};

class StatsListener : public rocksdb::EventListener, public Source<std::shared_ptr<Metric>> {
 public:
  void OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mtx_);
    metrics_.push(std::make_shared<RocksDBCompactionMetric>(-1));
  }
  void OnCompactionBegin(rocksdb::DB* db, const rocksdb::CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mtx_);
    metrics_.push(std::make_shared<RocksDBCompactionMetric>(1));
  }
  void OnFlushBegin(rocksdb::DB* db, const rocksdb::FlushJobInfo& fi) override {
    std::lock_guard<std::mutex> lock(mtx_);
    metrics_.push(std::make_shared<RocksDBFlushMetric>(1));
  }
  void OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& fi) override {
    std::lock_guard<std::mutex> lock(mtx_);
    metrics_.push(std::make_shared<RocksDBFlushMetric>(-1));
  }
  void ComputeImpl() override {
    std::lock_guard<std::mutex> lock(mtx_);
    while (!metrics_.empty()) {
      Emit(metrics_.front());
      metrics_.pop();
    }
  }
  void Start() override {}
 private:
  std::queue<std::shared_ptr<Metric>> metrics_;
  std::mutex mtx_;
};

class RocksDBStats : public rocksdb::Statistics, public Source<std::shared_ptr<Metric>> {
 public:
  RocksDBStats() {
    stats_ = rocksdb::CreateDBStatistics();
  }
  const char* Name() const override { return stats_->Name(); }
  virtual uint64_t getTickerCount(uint32_t tickerType) const override {
    return stats_->getTickerCount(tickerType);
  }
  virtual void histogramData(uint32_t type,
                             rocksdb::HistogramData* const data) const override {
    return stats_->histogramData(type, data);
  }
  virtual std::string getHistogramString(uint32_t t) const { return stats_->getHistogramString(t); }
  virtual void recordTick(uint32_t tickerType, uint64_t count = 0) {
    stats_->recordTick(tickerType, count);
  }
  virtual void setTickerCount(uint32_t tickerType, uint64_t count) {
    stats_->setTickerCount(tickerType, count);
  }
  virtual uint64_t getAndResetTickerCount(uint32_t tickerType) {
    return stats_->getAndResetTickerCount(tickerType);
  }
  virtual void reportTimeToHistogram(uint32_t histogramType, uint64_t time) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (histogramType == rocksdb::DB_GET) {
      get_buffer_ += time;
      get_time_++;
    } else if (histogramType == rocksdb::DB_WRITE) {
      write_buffer_ += time;
      write_time_++;
    }
    return stats_->reportTimeToHistogram(histogramType, time);
  }
  virtual void measureTime(uint32_t h, uint64_t t) {
    stats_->measureTime(h, t);
  }
  virtual void recordInHistogram(uint32_t histogramType, uint64_t time) {
    stats_->recordInHistogram(histogramType, time);
  }

  // Resets all ticker and histogram stats
  virtual rocksdb::Status Reset() {
    return stats_->Reset();
  }
  using Customizable::ToString;
  // String representation of the statistic object. Must be thread-safe.
  virtual std::string ToString() const {
    return stats_->ToString();
  }

  virtual bool getTickerMap(std::map<std::string, uint64_t>* t) const {
    return stats_->getTickerMap(t);
  }
  // Override this function to disable particular histogram collection
  virtual bool HistEnabledForType(uint32_t type) const {
    return type < rocksdb::HISTOGRAM_ENUM_MAX;
  }
  virtual void Start() {
    cur_ = std::chrono::steady_clock::now();
  }
  virtual void ComputeImpl() override {
    std::lock_guard<std::mutex> lock(mtx_);
    auto now = std::chrono::steady_clock::now();
    if (now - cur_ < std::chrono::seconds(1)) {
      return;
    }
    cur_ = now;
    if (get_time_ > 0) {
      Emit(std::make_shared<RocksDBReadStat>(get_buffer_ / get_time_));
    } else {
      Emit(std::make_shared<RocksDBReadStat>(0));
    }
    if (write_time_ > 0) {
      Emit(std::make_shared<RocksDBWriteStat>(write_buffer_ / write_time_));
    } else {
      Emit(std::make_shared<RocksDBWriteStat>(0));
    }
    get_buffer_ = 0;
    write_buffer_ = 0;
    get_time_ = 0;
    write_time_ = 0;
  }
 private:
  std::mutex mtx_;
  std::chrono::steady_clock::time_point cur_;
  std::shared_ptr<rocksdb::Statistics> stats_;
  uint64_t get_buffer_;
  uint64_t write_buffer_;
  uint64_t get_time_;
  uint64_t write_time_;
};