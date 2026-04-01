#pragma once
#include "LogConfig.h"
#include "LogQue.h"
#include "sinks/ISink.h"

#include <mutex>
#include <shared_mutex>
#include <typeindex>
#include <unordered_map>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace lyf {
class AsyncLogger {
  using system_clock = std::chrono::system_clock;

public:
  AsyncLogger(const LogConfig &config)
      : config_(config), queue_(config.GetQueueConfig()), running_(true) {
    UpdateNow();
    worker_thread_ = std::thread(&AsyncLogger::WorkerLoop, this);
    timer_thread_ = std::thread(&AsyncLogger::TimerLoop, this);
  }

  ~AsyncLogger() {
    running_ = false;
    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
    if (timer_thread_.joinable()) {
      timer_thread_.join();
    }
    // Sinks 会在析构时自动 Flush
  }

  // ==================== Sink 管理接口 ====================
  template <typename T> bool AddSink(std::shared_ptr<T> sink) {
    static_assert(std::is_base_of_v<ILogSink, T>,
                  "T must derive from ILogSink");
    if (!sink) {
      return false; // 无效的 Sink
    }
    std::unique_lock lock(sinks_mutex_);

    auto type_idx = std::type_index(typeid(T));
    if (sinks_map_.contains(type_idx)) {
      return false; // 该类型已存在
    }
    sinks_map_[type_idx] = sink;
    UpdateSinkCacheLocked();
    return true;
  }

  template <typename T> bool HasSink() const {
    static_assert(std::is_base_of_v<ILogSink, T>,
                  "T must derive from ILogSink");
    std::shared_lock lock(sinks_mutex_);
    return sinks_map_.contains(std::type_index(typeid(T)));
  }

  template <typename T> bool RemoveSink() {
    static_assert(std::is_base_of_v<ILogSink, T>,
                  "T must derive from ILogSink");
    std::unique_lock lock(sinks_mutex_);

    auto type_idx = std::type_index(typeid(T));
    auto it = sinks_map_.find(type_idx);
    if (it == sinks_map_.end()) {
      return false; // 该类型 Sink 不存在
    }

    if (it->second) {
      it->second->Flush();
    }

    sinks_map_.erase(it);
    UpdateSinkCacheLocked();
    return true;
  }

  template <typename T> std::shared_ptr<T> GetSink() const {
    static_assert(std::is_base_of_v<ILogSink, T>,
                  "T must derive from ILogSink");
    std::shared_lock lock(sinks_mutex_);

    auto it = sinks_map_.find(std::type_index(typeid(T)));
    if (it == sinks_map_.end()) {
      return nullptr;
    }
    return std::static_pointer_cast<T>(it->second);
  }

  size_t GetSinkCount() const {
    std::shared_lock lock(sinks_mutex_);
    return sinks_map_.size();
  }

  void ClearSinks() {
    std::unique_lock lock(sinks_mutex_);
    for (auto &[type_idx, sink] : sinks_map_) {
      if (sink) {
        sink->Flush();
      }
    }
    sinks_map_.clear();
    UpdateSinkCacheLocked();
  }

  // ==================== 日志提交接口 ====================
  bool Commit(LogMessage &&msg) { return queue_.Push(std::move(msg)); }

  void Flush() {
    std::shared_lock lock(sinks_mutex_);
    for (auto &sink : sinks_cache_) {
      sink->Flush();
    }
  }

  void Sync() {
    std::promise<void> prom;
    std::future<void> fut = prom.get_future();
    // 构造 FlushMessage 并提交到队列
    if (queue_.Push(LogMessage(&prom), true)) {
      // 等待 FlushMessage 处理完成
      fut.wait();
    }

    std::shared_lock lock(sinks_mutex_);
    for (auto &sink : sinks_cache_) {
      sink->Sync();
    }
  }

  size_t GetDropCount() const { return drop_cnt_; }
  void AddDropCount(size_t cnt) { drop_cnt_.fetch_add(cnt); }

  int64_t GetCoarseTime() {
    return coarse_time_ns_.load(std::memory_order_relaxed);
  }

private:
  void UpdateNow() {
    coarse_time_ns_.store(system_clock::now().time_since_epoch().count(),
                          std::memory_order_relaxed);
  }

  // 在持有写锁的情况下更新sink缓存
  void UpdateSinkCacheLocked() {
    sinks_cache_.clear();
    sinks_cache_.reserve(sinks_map_.size());
    for (auto &[type_idx, sink] : sinks_map_) {
      sinks_cache_.push_back(sink);
    }
  }

  void TimerLoop() {
    while (running_) {
      UpdateNow();
      std::this_thread::sleep_for(
          std::chrono::milliseconds(LogConfig::kCoarseTimeIntervalMs));
    }
  }

  void WorkerLoop() {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#elif defined(__APPLE__)
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INITIATED, 0);
#endif

    std::vector<LogMessage> buffer_batch;
    size_t batch_size = config_.GetWorkerBatchSize();
    if (batch_size == 0) {
      batch_size = LogConfig::kDefaultWorkerBatchSize;
    }
    buffer_batch.reserve(batch_size);

    while (running_ || queue_.size_approx() > 0) {
      size_t count = queue_.PopBatch(buffer_batch, batch_size);
      if (count > 0) {
        // 使用缓存的 sink 列表（读锁保护）
        std::shared_lock lock(sinks_mutex_);
        auto sinks = sinks_cache_; // 复制一份避免长时间持有锁
        lock.unlock();             // 尽快释放锁

        for (const auto &msg : buffer_batch) {
          if (msg.level == LogLevel::FLUSH) {
            // 遇到 Flush 指令，先强制刷新所有 Sink
            for (auto &sink : sinks) {
              sink->Flush();
            }
            // 通知主线程 Flush 指令之前的数据都处理完了
            if (msg.sync_promise) {
              msg.sync_promise->set_value();
            }
            continue;
          }
          for (auto &sink : sinks) {
            sink->Log(msg);
          }
        }
        // LogMessage 在 vector clear 时析构，Buffer 自动归还 Pool
        buffer_batch.clear();
      } else {
        if (running_) {
          std::this_thread::sleep_for(
              std::chrono::microseconds(LogConfig::kDefaultWorkerIdleSleepUs));
        } else {
          break;
        }
      }
    }
  }

private:
  const LogConfig &config_;
  LogQueue queue_;
  std::atomic<bool> running_;
  std::thread worker_thread_;
  std::atomic<size_t> drop_cnt_{0};

  // Sink 管理（类型 -> Sink 的映射）
  mutable std::shared_mutex sinks_mutex_;
  std::unordered_map<std::type_index, std::shared_ptr<ILogSink>> sinks_map_;
  // 缓存的 Sink 列表（用于快速遍历，避免频繁访问 map）
  std::vector<std::shared_ptr<ILogSink>> sinks_cache_;

  std::thread timer_thread_;
  std::atomic<int64_t> coarse_time_ns_{0};
};

} // namespace lyf
