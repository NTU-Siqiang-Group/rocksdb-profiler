#pragma once

#include "rocksdb_monitor.h"
#include "sink.h"
#include "io_monitor.h"
#include "stream/workflow_manager.h"

namespace rocksdbprofiler {

std::shared_ptr<WorkFlowManager> manager;
PrometheusDataSink* sinker;
IOMonitor* io_monitor;
IOLogProcessor* io_log_processor;
StatsListener* stats_listener;
RocksDBStats* rocks_stats;

void CreateRocksDBProfiler(void** compaction_listener, void** statistics, bool enable_io_monitor=true) {
  sinker = GetDefaultPrometheusDataSink();
  sinker->RegisterMetric(RocksDBCompactionMetric(0.0).GetLabel(), "counter");
  sinker->RegisterMetric(RocksDBFlushMetric(0.0).GetLabel(), "counter");
  sinker->RegisterMetric(RocksDBWriteStat(0.0).GetLabel(), "gauge");
  sinker->RegisterMetric(RocksDBReadStat(0.0).GetLabel(), "gauge");
  manager.reset(new WorkFlowManager());
  stats_listener = new StatsListener();
  rocks_stats = new RocksDBStats();
  stats_listener->Next(sinker);
  rocks_stats->Next(sinker);
  manager->Register(stats_listener);
  manager->Register(rocks_stats);
  if (enable_io_monitor) {
    io_monitor = new IOMonitor();
    io_log_processor = new IOLogProcessor();
    io_monitor->Next(io_log_processor);
    io_log_processor->Next(sinker);
    sinker->RegisterMetric(ReadIOMetric(0.0).GetLabel(), "gauge");
    sinker->RegisterMetric(WriteIOMetric(0.0).GetLabel(), "gauge");
    manager->Register(io_monitor);
  } else {
    io_monitor = nullptr;
    io_log_processor = nullptr;
  }
  if (compaction_listener) {
    *compaction_listener = stats_listener;
  }
  if (statistics) {
    *statistics = rocks_stats;
  }
}

void StartRocksDBProfiler() {
  manager->Start();
}

void StopRocksDBProfiler() {
  manager->Stop();
  delete sinker;
  if (io_monitor) {
    delete io_monitor;
    delete io_log_processor;
  }
}

};