#pragma once

#include "LogConfig.h"
#include "LogMessage.h"
#include "third/concurrentqueue.h" // ConcurrentQueue

#include <chrono>
#include <cstddef>
#include <thread>

namespace lyf {

class LogQueue {
public:
  using ConcurrentQueueType = moodycamel::ConcurrentQueue<LogMessage>;

  LogQueue(const QueConfig &cfg)
      : cfg_(cfg), que_(cfg.capacity == 0 ? LogConfig::kDefaultQueueCapacity
                                          : cfg.capacity),
        capacity_(cfg.capacity) {}

  bool Push(LogMessage &&msg, bool force = false) {
    if (!force && cfg_.capacity > 0 && que_.size_approx() >= capacity_) {
      // 需要背压处理
      return HandleBackPressure(std::move(msg));
    }
    return que_.enqueue(std::move(msg));
  }

  size_t PopBatch(std::vector<LogMessage> &output,
                  size_t batch_size = LogConfig::kDefaultWorkerBatchSize) {
    return que_.try_dequeue_bulk(std::back_inserter(output), batch_size);
  }

  size_t size_approx() const { return que_.size_approx(); }

private:
  bool HandleBackPressure(LogMessage &&msg) {
    if (cfg_.full_policy == QueueFullPolicy::DROP) {
      // 返回 false，msg 析构，buffer 自动归还 Pool
      return false;
    }

    // BLOCK 策略优化：混合自旋 + 线程休眠
    int spin_count = 0;
    auto start = std::chrono::steady_clock::now();

    while (true) {
      if (que_.size_approx() < capacity_) {
        return que_.enqueue(std::move(msg));
      }

      // 超时检查
      if (std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - start)
              .count() > cfg_.block_timeout_us) {
        return false; // 超时丢弃
      }

      if (spin_count < LogConfig::kDefaultBackpressureSpinCount) {
        std::this_thread::yield();
        spin_count++;
      } else {
        std::this_thread::sleep_for(
            std::chrono::microseconds(LogConfig::kDefaultBackpressureSleepUs));
      }
    }
  }

private:
  QueConfig cfg_;
  ConcurrentQueueType que_;
  size_t capacity_;
};

} // namespace lyf
