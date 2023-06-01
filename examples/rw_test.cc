#include "rw_test.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rw_test.h"

#include "rocksdbprofiler/profiler.h"

DEFINE_string(mode, "write", "");
DEFINE_int64(num, 2000000, "");
DEFINE_string(db_path, "/tmp/SOMENAME", "");
DEFINE_string(compaction_style, "FAN", "");
DEFINE_int64(write_num, 1000000, "");
DEFINE_int64(read_num, 1000000, "");
DEFINE_int64(seek_num, 1000000, "");
DEFINE_int64(seek_keys, 2000, "");
DEFINE_int64(zr_num, 10000000, "");
DEFINE_bool(monkey_filter, false, "");
DEFINE_int64(filter_bits, 30 * 1024 * 1024, "");

void WaitForCompaction(rocksdb::DB* db) {
  // This is an imperfect way of waiting for compaction. The loop and sleep
  // is done because a thread that finishes a compaction job should get a
  // chance to pickup a new compaction job.
  using rocksdb::DB;
  // 5 second
  db->GetEnv()->SleepForMicroseconds(10 * 1000000);
  std::vector<std::string> keys = {DB::Properties::kMemTableFlushPending,
                                   DB::Properties::kNumRunningFlushes,
                                   DB::Properties::kNumRunningCompactions};

  fprintf(stdout, "waitforcompaction(%s): started\n", db->GetName().c_str());

  while (true) {
    bool retry = false;

    for (const auto& k : keys) {
      uint64_t v;
      if (!db->GetIntProperty(k, &v)) {
        fprintf(stderr, "waitforcompaction(%s): GetIntProperty(%s) failed\n",
                db->GetName().c_str(), k.c_str());
        exit(1);
      } else if (v > 0) {
        fprintf(stdout, "waitforcompaction(%s): active(%s). Sleep 10 seconds\n",
                db->GetName().c_str(), k.c_str());
        retry = true;
        break;
      }
    }

    if (!retry) {
      fprintf(stdout, "waitforcompaction(%s): finished\n",
              db->GetName().c_str());
      return;
    }
    db->GetEnv()->SleepForMicroseconds(10 * 1000000);
  }
}

rocksdb::Options get_fan_options() {
  rocksdb::Options options;
  // level_runs[0] = T
  options.level_runs = {4, 2, 2, 2, 2};
  options.num_levels = 10;
  options.create_if_missing = true;
  options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleFAN;
  options.max_bytes_for_level_multiplier = 4;
  options.max_level_multiplier = 8;
  // 100,000 key for L0
  // 2,000,000 8,000,000 32,000,000 64,000,000
  // capacity of L0 = buffer size * T
  options.max_bytes_for_level_base = 200000;
  // flush per 2,000 key
  options.target_file_size_base = 50000;
  options.write_buffer_size = 50000;

  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->no_block_cache = true;
  // table_options->filter_policy.reset(
  //     rocksdb::NewMonkeyFilterPolicy({5, 10, 20, 30}));
  // initialize the bloom filter with bits/key = M / T*C*F

  if (FLAGS_monkey_filter) {
    double init_bpk = (double)FLAGS_filter_bits * 8 /
                      options.max_bytes_for_level_multiplier /
                      options.max_level_multiplier / options.write_buffer_size;

    auto adaptor = std::make_shared<BloomFilterAdaptor>(
        "FAN", options.max_bytes_for_level_multiplier,
        options.max_level_multiplier, options.level_runs, FLAGS_filter_bits,
        options.write_buffer_size);
    auto bits_per_key = std::vector<double>{init_bpk};
    if (FLAGS_mode == "rw") {
      bits_per_key = adaptor->InitializeFAN(options.num_levels, FLAGS_num);
      std::cout << "\n Initialize Fan BPK: ";
      for (int i = 0; i < bits_per_key.size(); i++) {
        std::cout << bits_per_key[i] << ", ";
      }
      std::cout << std::endl;
    }
    table_options->filter_policy.reset(
        rocksdb::NewMonkeyFilterPolicy(bits_per_key));
    options.listeners.push_back(adaptor);
  }
  options.statistics = rocksdb::CreateDBStatistics();
  return options;
}

rocksdb::Options get_bush_options() {
  rocksdb::Options options;
  options.num_levels = 100;
  options.create_if_missing = true;
  options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleBush;
  // T
  options.max_bytes_for_level_multiplier = 2;
  // C
  options.max_level_multiplier = 1;
  options.target_file_size_base = 50000;
  options.write_buffer_size = 50000;

  // auto table_options =
  //     options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  // table_options->filter_policy.reset(
  //     rocksdb::NewMonkeyFilterPolicy({5, 10, 20, 30}));
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->no_block_cache = true;
  if (FLAGS_monkey_filter) {
    auto adaptor = std::make_shared<BloomFilterAdaptor>(
        "Bush", 2, 2, 1, FLAGS_filter_bits, 0.1, 500000);
    auto bits_per_key = std::vector<double>{4.79};
    if (FLAGS_mode == "rw") {
      bits_per_key = adaptor->Initialize(FLAGS_num);
    }
    table_options->filter_policy.reset(
        rocksdb::NewMonkeyFilterPolicy(bits_per_key));
    options.listeners.push_back(adaptor);
  }
  options.statistics = rocksdb::CreateDBStatistics();
  return options;
}

rocksdb::Options get_default_options() {
  auto options = rocksdb::Options();
  options.create_if_missing = true;
  options.write_buffer_size = 5 * 1024 * 1024;
  options.target_file_size_base = 1024 * 1024 * 1024;
  options.target_file_size_multiplier = 10;
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->no_block_cache = true;
  if (FLAGS_monkey_filter) {
    table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(8.3));
  }
  // options.statistics = rocksdb::CreateDBStatistics();
  rocksdb::EventListener* listener;
  rocksdb::Statistics* stats;
  rocksdbprofiler::CreateRocksDBProfiler((void**)&listener, (void**)&stats);
  options.listeners.emplace_back(listener);
  options.statistics.reset(stats);
  return options;
}

std::string val_suffix(100, 'v');

void WriteKeys(int num, rocksdb::DB* db) {
  std::vector<int> keys;
  for (int i = 0; i < num; i++) {
    keys.push_back(i);
  }
  auto rng = std::default_random_engine{};
  std::vector<std::thread> thrds;
  std::atomic<int> fin(0);
  std::shuffle(std::begin(keys), std::end(keys), rng);
  for (int i = 0; i < 10; i++) {
    int start = i * num / 10;
    int end = (i + 1) * num / 10;
    if (i == 9) {
      end = num;
    }
    auto t = std::thread([=, &keys, &fin]() {
      for (int j = start; j < end; j++) {
        std::string key = std::to_string(keys[j]);
        std::string val = key + val_suffix;
        rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, val);
        if (!s.ok()) {
          std::cout << "fail to put " << key << " because of " << s.ToString()
                    << std::endl;
          exit(1);
        }
        fin++;
      }
    });
    thrds.push_back(std::move(t));
  }
  for (int i = 0; i < 10; i++) {
    thrds[i].join();
  }
  db->Flush(rocksdb::FlushOptions());
  WaitForCompaction(db);
  std::cout << "finish writing" << std::endl;
}

void ReadKeys(int num, rocksdb::DB* db) {
  for (int i = 0; i < num; i++) {
    std::string val;
    std::string key = std::to_string(i);
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
    if (!s.ok()) {
      std::cout << "fail to read " << key << " because of " << s.ToString()
                << std::endl;
      exit(1);
    }
    if (val != std::to_string(i) + val_suffix) {
      std::cout << "invalid val " << val << std::endl;
      exit(1);
    }
    if (i % (num / 100) == 0) {
      std::cout << "reading progress: " << i << "/" << num << std::endl;
    }
  }
  std::cout << "finish reading" << std::endl;
}

void IterKeys(int num, rocksdb::DB* db) {
  std::vector<std::string> keys;
  for (int i = 0; i < num; i++) {
    keys.push_back(std::to_string(i));
  }
  std::sort(keys.begin(), keys.end());
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  it->SeekToFirst();
  std::cout << "start iterating" << std::endl;
  for (size_t i = 0; i < keys.size(); i++) {
    rocksdb::Slice key = it->key();
    rocksdb::Slice val = it->value();
    std::string expected_key = keys[i];
    std::string expected_val = keys[i] + val_suffix;
    if (key.ToString() != expected_key || val.ToString() != expected_val) {
      std::cout << "invalid key/val: " << key.ToString() << "/"
                << val.ToString() << std::endl;
      std::cout << "valid key/val: " << expected_key << "/" << expected_val
                << std::endl;
      exit(1);
    }
    if (i % (num / 100) == 0) {
      std::cout << "iter progress: " << i << "/" << num << std::endl;
    }
    it->Next();
  }
  delete it;
  std::cout << "finish iterating" << std::endl;
}

void ReadWriteWorkload(int num, rocksdb::DB* db) {
  srand(100);
  std::vector<int> keys;
  std::vector<std::string> skeys;
  std::vector<char> ops;
  for (int i = 0; i < FLAGS_read_num; i++) {
    ops.push_back('r');
  }
  for (int i = 0; i < FLAGS_zr_num; i++) {
    ops.push_back('z');
  }
  for (int i = 0; i < FLAGS_write_num; i++) {
    ops.push_back('w');
    keys.push_back(i);
  }
  for (int i = 0; i < FLAGS_seek_num; i++) {
    ops.push_back('s');
  }
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(keys), std::end(keys), rng);
  std::shuffle(std::begin(ops), std::end(ops), rng);
  int idx = 0;
  for (size_t i = 0; i < ops.size(); i++) {
    auto op = ops[i];
    if (op == 'w') {
      // write
      std::string key = std::to_string(keys[idx]) + "-new";
      std::string val = key + val_suffix;
      idx++;
      rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, val);
      if (!s.ok()) {
        std::cout << "fail to put " << key << " because of " << s.ToString()
                  << std::endl;
        exit(1);
      }
    } else if (op == 'r') {
      // read
      int rand_num = rand() % num;
      std::string val;
      std::string key = std::to_string(rand_num);
      rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
      if (!s.ok()) {
        std::cout << "fail to read " << key << " because of " << s.ToString()
                  << std::endl;
        exit(1);
      }
      if (val != std::to_string(rand_num) + val_suffix) {
        std::cout << "invalid val " << val << std::endl;
        exit(1);
      }
    } else if (op == 'z') {
      // zero read
      std::string val;
      std::string key = std::to_string(rand() % num) + ".5";
      rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &val);
      if (!s.IsNotFound()) {
        std::cout << "fail to zero read " << key << " because of "
                  << s.ToString() << std::endl;
        std::cout << "key: " << key << ", val: " << val << std::endl;
        exit(1);
      }
    } else if (op == 's') {
      // seek
      int rand_start = rand() % num;
      auto it = db->NewIterator(rocksdb::ReadOptions());
      it->Seek(rocksdb::Slice(std::to_string(rand_start)));
      for (int j = 0; j < FLAGS_seek_keys; j++) {
        if (!it->Valid()) {
          break;
        }
        rocksdb::Slice val = it->value();
        rocksdb::Slice key = it->key();
        if (val.ToString() != key.ToString() + val_suffix) {
          std::cout << "invalid val for seek: " << val.ToString() << std::endl;
          std::cout << "expected: " << key.ToString() + val_suffix << std::endl;
          exit(1);
        }
        it->Next();
      }
      delete it;
    } else {
      std::cout << "should not reach here" << std::endl;
      exit(1);
    }
    if (i % (ops.size() / 100) == 0) {
      std::cout << "rw progress: " << i << "/" << ops.size() << std::endl;
    }
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  int total_num = FLAGS_num;
  rocksdb::DB* db;
  rocksdb::Options options;
  if (FLAGS_compaction_style == "FAN") {
    std::cout << "FAN compaction" << std::endl;
    options = get_fan_options();
  } else if (FLAGS_compaction_style == "Bush") {
    std::cout << "Bush compaction" << std::endl;
    options = get_bush_options();
  } else if (FLAGS_compaction_style == "Level") {
    std::cout << "default options" << std::endl;
    options = get_default_options();
  } else {
    std::cout << "invalid compaction style" << std::endl;
    return 0;
  }
  rocksdb::Status status = rocksdb::DB::Open(options, FLAGS_db_path, &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    exit(1);
  }
  rocksdbprofiler::StartRocksDBProfiler();
  std::chrono::steady_clock::time_point begin =
      std::chrono::steady_clock::now();
  if (FLAGS_mode == std::string("write")) {
    WriteKeys(total_num, db);
  } else if (FLAGS_mode == std::string("read")) {
    ReadKeys(total_num, db);
  } else if (FLAGS_mode == std::string("iter")) {
    IterKeys(total_num, db);
  } else if (FLAGS_mode == std::string("rw")) {
    ReadWriteWorkload(total_num, db);
  } else {
    std::cout << "invalid mode" << std::endl;
    exit(1);
  }
  rocksdbprofiler::StopRocksDBProfiler();
  std::cout << options.statistics->ToString() << std::endl;
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  std::cout
      << "Time Elapsed: "
      << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count()
      << std::endl;
  std::cout << "validation complete" << std::endl;
  delete db;
  return 0;
}