#include "rocksdbprofiler/io_monitor.h"
#include "rocksdbprofiler/sink.h"
#include "rocksdbprofiler/stream/workflow_manager.h"
#include "rocksdbprofiler/sink.h"
#include "rocksdbprofiler/rocksdb_monitor.h"

#include "rocksdb/db.h"

int main() {
  rocksdb::Options options;
  options.create_if_missing = true;
  auto sinker = std::shared_ptr<PrometheusDataSink>(GetDefaultPrometheusDataSink());
  auto workflow_manager = std::shared_ptr<WorkFlowManager>(new WorkFlowManager());
  auto db_stats = std::shared_ptr<RocksDBStats>(new RocksDBStats());
  auto comp_listener = std::shared_ptr<StatsListener>(new StatsListener());
  sinker->RegisterMetric(RocksDBCompactionMetric(0).GetLabel(), "counter");
  sinker->RegisterMetric(RocksDBReadStat(0).GetLabel(), "gauge");
  sinker->RegisterMetric(RocksDBWriteStat(0).GetLabel(), "gauge");
  db_stats->Next(sinker.get());
  comp_listener->Next(sinker.get());
  workflow_manager->Register(db_stats.get());
  workflow_manager->Register(comp_listener.get());
  options.statistics = db_stats;
  options.listeners.push_back(comp_listener);
  rocksdb::DB* db;
  rocksdb::DB::Open(options, "/tmp/testdb", &db);
  workflow_manager->Start();
  for (int i = 0; i < 10000000; i++) {
    db->Put(rocksdb::WriteOptions(), std::to_string(i), std::to_string(i));
  }
  for (int i = 0; i < 10000000; i++) {
    std::string value;
    db->Get(rocksdb::ReadOptions(), std::to_string(i), &value);
  }
  workflow_manager->Stop();
  std::cout << options.statistics->ToString() << std::endl;
  delete db;
  return 0;
}