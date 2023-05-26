#include "monitor/io_monitor.h"
#include "streaming/workflow_manager.h"
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
  auto data_printer = std::shared_ptr<DataPrinter>(new DataPrinter());
  io_monitor->Next(
    io_log_processor->Next(
      data_printer.get()
    )
  );
  WorkFlowManager<std::string> manager(io_monitor.get());
  manager.Start();
  return 0;
}