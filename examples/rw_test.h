#pragma once

#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/table.h"

class MonkeyFilterForLeveling {
 public:
  MonkeyFilterForLeveling(const int l, const int t, const uint64_t n,
                          const uint64_t mf)
      : L(l), T(t), N(n), Mf(mf) {}
  std::vector<double> GetBitsPerKey() const {
    std::vector<double> ret;
    const double ln22 = std::log(2) * std::log(2);
    double t1 = 0.0, t2 = 0.0;
    for (int i = 1; i <= L; i++) {
      t1 += std::log(ln22 * std::pow(T, L - i + 1)) / std::pow(T, L - i);
      t2 += 1 / std::pow(T, L - i);
    }
    double M = (t1 - Mf * 8 * ln22 * T / N / (T - 1)) / t2;
    double lmda = -exp(M) / N / (T - 1);
    for (int i = 1; i <= L; i++) {
      auto fpr = -lmda * N * (T - 1) / ln22 / std::pow(T, L - i + 1);
      ret.push_back(log(1 / fpr) / ln22);
    }
    return ret;
  }

 private:
  int L;
  int T;
  int N;
  uint64_t Mf;
};

class BloomFilterAdaptor : public rocksdb::EventListener {
 public:
  // construct bloom filter for Bush
  BloomFilterAdaptor(const std::string& compaction_style, int t, int x, int c,
                     uint64_t mem_size, double pl, uint64_t write_buffer)
      : compaction_style_(compaction_style),
        T(t),
        X(x),
        C(c),
        level_runs_({0}),
        F(write_buffer),
        mem_size_(mem_size),
        pl_(pl) {}
  // construct bloom filter for FAN
  BloomFilterAdaptor(const std::string& compaction_style, int t, int c,
                     std::vector<int> level_runs, uint64_t mem_size,
                     uint64_t write_buffer)
      : compaction_style_(compaction_style),
        T(t),
        X(0),
        C(c),
        level_runs_(level_runs),
        F(write_buffer),
        mem_size_(mem_size),
        pl_(1) {
    std::cout << "fan filter construct";
  }

  std::vector<double> Initialize(uint64_t NL) {
    auto new_filter = GetBitsPerKeyBush(NL);
    return new_filter;
  }

  std::vector<double> InitializeFAN(int num_levels, uint64_t total_num) {
    int max_level, NL, runsl;
    std::tie(max_level, NL, runsl) = CalInitStatusFan(num_levels, total_num);
    return GetBitsPerKeyFAN(max_level, NL, runsl);
  }

  void OnCompactionCompleted(rocksdb::DB* db,
                             const rocksdb::CompactionJobInfo& c) override {
    if (c.compaction_reason ==
            rocksdb::CompactionReason::kBushCompactionMajor ||
        c.compaction_reason ==
            rocksdb::CompactionReason::kFanCompactionOnMaxLevel) {
      uint64_t NL = GetMaxLevelSize(c);
      auto new_filter = GetBitsPerKey(c, NL);
      auto options = db->GetOptions(db->DefaultColumnFamily());
      auto table_options =
          options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
      table_options->filter_policy.reset(
          rocksdb::NewMonkeyFilterPolicy(new_filter));
      std::cout << compaction_style_ << ": ";
      for (int i = 0; i < new_filter.size(); i++) {
        std::cout << new_filter[i] << ", ";
      }
      std::cout << std::endl;
    } else {
      return;
    }
  }

 private:
  uint64_t GetMaxLevelSize(const rocksdb::CompactionJobInfo& c) const {
    uint64_t NL = 0;
    if (compaction_style_ == "Bush") {
      for (auto f : c.output_files) {
        auto t = c.table_properties.at(f).get();
        if (t == nullptr) {
          std::cout << "invalid table properties for output: " << f
                    << std::endl;
          exit(1);
        }
        NL += t->num_entries;
      }
    } else if (compaction_style_ == "FAN") {
      NL = c.output_level_infos.first;
    }
    return NL;
  }
  std::vector<double> GetBitsPerKey(const rocksdb::CompactionJobInfo& c,
                                    uint64_t NL) {
    if (compaction_style_ == "Bush") {
      return GetBitsPerKeyBush(NL);
    } else if (compaction_style_ == "FAN") {
      return GetBitsPerKeyFAN(c.output_level, NL, c.output_level_infos.second);
    } else {
      return {0};
    }
  }

  int GetMaxLevelBush(uint64_t NL) {
    if (NL < T * C * F) {
      return 1;
    }
    for (int next_max_level = 2; next_max_level < 100; next_max_level++) {
      double tmp = (double)NL / C;
      double prev = F;
      for (int i = 1; i < next_max_level; i++) {
        prev = prev * std::pow(T, std::pow(X, next_max_level - i - 1));
        tmp -= prev;
      }
      if (tmp == 0) {
        // current level
        return next_max_level;
      } else if (tmp < 0) {
        // prev level
        return next_max_level - 1;
      }
    }
    return -1;
  }
  std::vector<double> GetBitsPerKeyBush(uint64_t NL) {
    int L = std::max(1, GetMaxLevelBush(NL));
    // get r_i
    std::vector<double> caps, rs;
    std::vector<double> chunks;
    for (int i = 1; i < L; i++) {
      rs.push_back(std::pow(T, std::pow(X, L - i - 1)));
      if (i == 1) {
        caps.push_back(std::pow(T, std::pow(X, L - i - 1)) * F);
      } else {
        caps.push_back(caps[i - 2] * std::pow(T, std::pow(X, L - i - 1)));
      }
    }
    double tmp = 0.0, tmp1 = 0.0;
    for (int i = 1; i < L; i++) {
      double a = rs[i - 1];
      tmp += log(caps[i - 1] / ln22 / a) * caps[i - 1];
      tmp1 += caps[i - 1];
    }
    double A = tmp + ln22 * mem_size_ * 8 + NL * log(pl_);
    double lmda = std::exp(-A / tmp1);
    std::vector<double> ret;
    for (int i = 1; i < L; i++) {
      double a = rs[i - 1];
      double fpr = lmda * caps[i - 1] / ln22 / a;
      if (fpr > 1 || fpr < 0) {
        fpr = 1;
      }
      ret.push_back(log(1 / fpr) / ln22);
    }
    ret.push_back(log(1 / pl_) / ln22);
    return ret;
  }
  // calculate the <max_level, max_level_cap> for FAN
  std::pair<int, uint64_t> CalMaxLevelInfoFAN(int num_levels,
                                              uint64_t total_num) {
    std::pair<int, uint64_t> result;
    int max_level = 0;
    uint64_t max_level_cap = 0;
    if (total_num < (T * C * F)) {
      max_level = 0;
      max_level_cap = T * C * F;
    } else {
      for (int i = 1; i < num_levels; i++) {
        int tmp_num = T * F;
        for (int j = 1; j < i; j++) {
          tmp_num += std::pow(T, (j + 1)) * F;
        }
        if (i == num_levels - 1) {
          max_level_cap = std::pow(T, i) * C * F;
        } else {
          max_level_cap = std::pow(T, (i + 1)) * C * F;
        }
        tmp_num += max_level_cap;
        if (tmp_num > total_num) {
          max_level = i;
          break;
        }
      }
    }
    result = std::make_pair(max_level, max_level_cap);
    return result;
  }
  // calculate the initial data distribution of FAN
  std::tuple<int, int, int> CalInitStatusFan(int num_levels,
                                             uint64_t total_num) {
    std::tuple<int, int, int> result;
    int NL = 0;
    int runsl = 0;
    int max_level, max_level_cap;
    // find the max_level
    std::tie(max_level, max_level_cap) =
        CalMaxLevelInfoFAN(num_levels, total_num);
    // if the max_level is L0
    if (max_level == 0) {
      NL = total_num;
      runsl = std::ceil((double)total_num / F);
    } else {
      uint64_t tmp_num = 0;
      for (int i = 0; i < max_level; i++) {
        tmp_num += std::pow(T, (i + 1)) * F;
      }
      NL = total_num - tmp_num;
      int max_level_runs = level_runs_[level_runs_.size() - 1];
      if (max_level_runs == 1) {
        runsl = 1;
      } else {
        runsl = std::ceil((double)NL / (max_level_cap / max_level_runs));
      }
    }
    // determine NL and runsl assuming non-max-levels are full
    result = std::make_tuple(max_level, NL, runsl);
    return result;
  }

  std::vector<double> GetBitsPerKeyFAN(int output_level, uint64_t NL,
                                       int output_level_runs) {
    std::vector<double> ret;
    // this line is for initialization only because
    // there is no update before the tree reaches L1
    // int nl = level_runs_[level_runs_.size()-1];
    int nl = output_level_runs;
    if (output_level == 0) {
      // actually this line should not be reached
      // because the output level of the first major compaction is L1
      ret.push_back((double)mem_size_ * 8 / T / C / F);
    } else if (output_level == 1) {
      // L0 has T files & L1 has n_l files
      double cap0 = T * F;
      double A = ln22 * mem_size_ * 8 + cap0 * log(F / ln22) +
                 NL * log(NL / nl / ln22);
      double lambda = std::exp(-A / (cap0 + NL));
      std::vector<double> fprs;
      fprs.push_back(lambda * F / ln22);
      fprs.push_back(lambda * NL / nl / ln22);
      for (int i = 0; i < 2; i++) {
        double fpr = fprs[i];
        if (fpr > 1 || fpr < 0) {
          fpr = 1;
        }
        ret.push_back(log(1 / fpr) / ln22);
      }
    } else {
      // levels between L0 and L1 are normal files
      std::vector<double> caps, run_nums;
      caps.push_back(T * F);
      run_nums.push_back(T);
      for (int i = 1; i < output_level; i++) {
        caps.push_back(caps[i - 1] * T);
        run_nums.push_back(level_runs_[i]);
      }
      caps.push_back(NL);
      run_nums.push_back(nl);

      // calculate lambda
      double sigm_level_size = 0.0;
      double A = 0.0;
      for (int i = 0; i <= output_level; i++) {
        sigm_level_size += caps[i];
        A += (log(caps[i] / run_nums[i] / ln22) * caps[i]);
      }
      A += ln22 * mem_size_ * 8;
      double lambda = std::exp(-A / sigm_level_size);

      // calculate bits/key for each level
      for (int i = 0; i <= output_level; i++) {
        double fpr = lambda * caps[i] / run_nums[i] / ln22;
        if (fpr > 1 || fpr < 0) {
          fpr = 1;
        }
        ret.push_back(log(1 / fpr) / ln22);
      }
    }
    return ret;
  }
  std::string path_;
  std::string compaction_style_;
  int T;
  int X;
  int C;
  std::vector<int> level_runs_;
  uint64_t F;
  uint64_t mem_size_;
  double pl_;
  const double ln2 = std::log(2);
  const double ln22 = std::log(2) * std::log(2);
};