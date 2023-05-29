#include "include/rocksdbprofiler/io_monitor.h"
#include "include/rocksdbprofiler/sink.h"
#include "include/rocksdbprofiler/stream/workflow_manager.h"

#include <memory>
#include <thread>
#include <fcntl.h>

int main() {
  std::string bulk;
  for (int i = 0; i < 1000000; ++i) {
    bulk += "a";
  }
  auto t = std::thread([&] {
    // mock write
    sleep(5);
    std::cout << "start writing" << std::endl;
    int file = open("test.txt", O_WRONLY | O_CREAT, 0644);
    for (int j = 0; j < 10000; j++) {
      write(file, bulk.c_str(), bulk.size());
      sleep(1);
    }
  });
  t.detach();
  auto io_monitor = std::shared_ptr<IOMonitor>(new IOMonitor());
  auto io_log_processor = std::shared_ptr<IOLogProcessor>(new IOLogProcessor());
  auto sinker = std::shared_ptr<PrometheusDataSink>(GetDefaultPrometheusDataSink());
  io_monitor->Next(
    io_log_processor->Next(
      sinker.get()
    )
  );
  sinker->RegisterMetric(ReadIOMetric().GetLabel());
  sinker->RegisterMetric(WriteIOMetric().GetLabel());
  // sinker->RegisterMetric(RocksDBCompactionCompleteMetric(0).GetLabel());
  WorkFlowManager manager;
  manager.Register(io_monitor.get());
  manager.Start();
  sleep(120);
  manager.Stop();
  return 0;
}