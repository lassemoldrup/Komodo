#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

// Only the sampling worker writes counters between start() and stop(). Read
// them after stop() has joined it. No references to checker state are retained.
class WorkloadRssSampler final {
public:
  static constexpr std::uint64_t interval_ms = 100;

  ~WorkloadRssSampler() { stop(); }

  static std::uint64_t parseRss(std::istream &input) {
    std::string line;
    while (std::getline(input, line)) {
      if (!line.starts_with("Rss:")) continue;
      std::istringstream fields(line);
      std::string name, value, unit, extra;
      if (!(fields >> name >> value >> unit) || unit != "kB" ||
          (fields >> extra) || value.empty() ||
          value.find_first_not_of("0123456789") != std::string::npos) break;
      return std::stoull(value);
    }
    throw std::runtime_error("cannot read Rss from /proc/self/smaps_rollup");
  }

  void start() {
    if (started_) return;
    started_ = true;
    sample(); // Before publishing the workload-admission acknowledgement.
    try {
      worker_ = std::thread([this] {
        std::unique_lock lock(mutex_);
        while (!wake_.wait_for(lock, std::chrono::milliseconds(interval_ms),
                               [this] { return stopping_; })) {
          lock.unlock();
          sample();
          lock.lock();
        }
      });
    } catch (...) {
      ++errors_;
    }
  }

  void stop() {
    if (!started_ || stopped_) return;
    {
      std::lock_guard lock(mutex_);
      stopping_ = true;
    }
    wake_.notify_one();
    if (worker_.joinable()) worker_.join();
    sample(); // Before checker/controller destruction.
    stopped_ = true;
  }

  bool started() const { return started_; }
  bool valid() const { return stopped_ && samples_ > 0 && errors_ == 0; }
  std::uint64_t peak() const { return peak_; }
  std::uint64_t samples() const { return samples_; }
  std::uint64_t errors() const { return errors_; }

private:
  void sample() noexcept {
    try {
      std::ifstream input("/proc/self/smaps_rollup");
      const auto rss = parseRss(input);
      peak_ = std::max(peak_, rss);
      ++samples_;
    } catch (...) {
      ++errors_;
    }
  }

  std::mutex mutex_;
  std::condition_variable wake_;
  std::thread worker_;
  bool started_ = false;
  bool stopped_ = false;
  bool stopping_ = false;
  std::uint64_t peak_ = 0, samples_ = 0, errors_ = 0;
};
