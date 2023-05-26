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

class DataPrinter : public Sink<std::string> {
 public:
  DataPrinter(int port=8080): Sink(), port_(port) {
    // check server is up
    sleep(3); // wait for the server to start
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    serv_addr.sin_port = htons(uint16_t(port_));
    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1) {
      perror("connect to server error\n");
      exit(1);
    }
    output_fd_ = sock;
    std::cout << "connect to server successfully\n"
      << "fd: " << output_fd_ << std::endl;
  }
  virtual void ComputeImpl() override {
    auto data = Recv();
    for (auto& c : data) {
      write(output_fd_, c.c_str(), data.size());
    }
  }
  ~DataPrinter() {
    // kill(child_, SIGKILL);
  }
 private:
  pid_t child_;
  int port_;
  int output_fd_;
};
