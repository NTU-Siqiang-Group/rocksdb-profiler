#pragma once

#include "rocksdb_monitor.h"
#include "sink.h"
#include "io_monitor.h"
#include "stream/workflow_manager.h"

namespace rocksdbprofiler {

std::shared_ptr<WorkFlowManager> manager;
std::shared_ptr<PrometheusDataSink> sinker;
std::shared_ptr<IOMonitor> io_monitor;
std::shared_ptr<IOLogProcessor> io_log_processor;
std::shared_ptr<StatsListener> stats_listener;
std::shared_ptr<RocksDBStats> rocks_stats;

void CreateRocksDBProfiler(void** compaction_listener, void** statistics, bool enable_io_monitor=true) {
  sinker.reset(GetDefaultPrometheusDataSink());
  sinker->RegisterMetric(RocksDBCompactionMetric(0.0).GetLabel(), "counter");
  sinker->RegisterMetric(RocksDBFlushMetric(0.0).GetLabel(), "counter");
  sinker->RegisterMetric(RocksDBWriteStat(0.0).GetLabel(), "gauge");
  sinker->RegisterMetric(RocksDBReadStat(0.0).GetLabel(), "gauge");
  manager.reset(new WorkFlowManager());
  stats_listener.reset(new StatsListener());
  rocks_stats.reset(new RocksDBStats());
  stats_listener->Next(sinker.get());
  rocks_stats->Next(sinker.get());
  manager->Register(stats_listener.get());
  manager->Register(rocks_stats.get());
  if (enable_io_monitor) {
    io_monitor = std::make_shared<IOMonitor>();
    io_log_processor = std::make_shared<IOLogProcessor>();
    io_monitor->Next(io_log_processor.get());
    io_log_processor->Next(sinker.get());
    sinker->RegisterMetric(ReadIOMetric(0.0).GetLabel(), "gauge");
    sinker->RegisterMetric(WriteIOMetric(0.0).GetLabel(), "gauge");
    manager->Register(io_monitor.get());
  }
  if (compaction_listener) {
    *compaction_listener = stats_listener.get();
  }
  if (statistics) {
    *statistics = rocks_stats.get();
  }
}

void StartRocksDBProfiler() {
  manager->Start();
}

void StopRocksDBProfiler() {
  manager->Stop();
}

};