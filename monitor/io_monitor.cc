#include "io_monitor.h"

#include <sstream>
#include <iostream>

void IOMonitor::Start() {
  // continuously reading output from iotop
  int fd[2]; // pipe
  if (pipe(fd) == -1) {
    perror("pipe");
    exit(1);
  }
  child_ = fork();
  if (child_ == 0) {
    // child process
    // close the stdout and redirect it to the pipe
    std::string cmd("iotop -P -t -o | grep ");
    cmd += std::to_string(pid_);
    std::cout << "child: " << cmd << std::endl;
    close(STDOUT_FILENO);
    dup2(fd[1], STDOUT_FILENO);
    system(cmd.c_str()); // loop
    exit(0); // should not reach here
  }
  input_fd_ = fd[0];
}

void IOMonitor::ComputeImpl() {
  char buf[300];
  int n = read(input_fd_, buf, sizeof(buf));
  if (n == -1) {
    perror("read from pipe\n");
    kill(child_, SIGKILL);
    exit(1);
  }
  buf[n] = '\0';
  Emit(std::string(buf));
}

static std::vector<std::string> split_by_space(const std::string& src) {
  std::vector<std::string> ret;
  std::stringstream ss(src);
  while (!ss.eof()) {
    std::string target;
    ss >> target;
    ret.push_back(target);
  }
  return ret;
}

static std::vector<std::string> split_by_line(const std::string& src) {
  std::vector<std::string> ret;
  std::stringstream ss(src);
  while (!ss.eof()) {
    std::string target;
    getline(ss, target);
    ret.push_back(target);
  }
  return ret;
}

void IOLogProcessor::ComputeImpl() {
  auto logs = Recv();
  for (auto& log : logs) {
    std::cout << "raw log: " << log << std::endl;
    log = buffer_ + log;
    buffer_.clear();
    auto lines = split_by_line(log);
    for (size_t i = 0; i < lines.size() - 1; ++i) {
      if (lines[i].empty()) {
        continue;
      }
      auto ret = split_by_space(lines[i]);
      IOStat stat{.timestamp = ret[0], .read_speed = ret[4], .write_speed = ret[6]};
      std::cout << "parsed log: " << stat.ToString() << std::endl;
      Emit(stat.ToString());
    }
    buffer_ += lines.back();
  }
}
