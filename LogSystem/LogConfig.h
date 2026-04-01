#pragma once

#include "tool/Utility.h"

#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>

namespace lyf {

enum class LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  FATAL = 4,
  FLUSH = 99,
};

inline constexpr std::string_view LevelToString(LogLevel level) {
  switch (level) {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::INFO:
    return "INFO";
  case LogLevel::WARN:
    return "WARN";
  case LogLevel::ERROR:
    return "ERROR";
  case LogLevel::FATAL:
    return "FATAL";
  default:
    return "UNKNOWN";
  }
}

inline LogLevel ParseLevel(std::string_view value) {
  if (value == "DEBUG") {
    return LogLevel::DEBUG;
  }
  if (value == "INFO") {
    return LogLevel::INFO;
  }
  if (value == "WARN") {
    return LogLevel::WARN;
  }
  if (value == "ERROR") {
    return LogLevel::ERROR;
  }
  if (value == "FATAL") {
    return LogLevel::FATAL;
  }
  return LogLevel::INFO; // 默认 INFO 级别
}

enum class QueueFullPolicy { BLOCK = 0, DROP = 1 };

inline constexpr std::string_view
QueueFullPolicyToString(QueueFullPolicy policy) {
  switch (policy) {
  case QueueFullPolicy::BLOCK:
    return "BLOCK";
  case QueueFullPolicy::DROP:
    return "DROP";
  default:
    return "BLOCK"; // 默认 BLOCK 策略
  }
}

inline QueueFullPolicy ParsePolicy(std::string_view value) {
  if (value == "DROP") {
    return QueueFullPolicy::DROP;
  }
  return QueueFullPolicy::BLOCK;
}

// 文件轮转策略枚举
enum class RotatePolicy {
  NONE = 0,  // 不轮转
  DAILY = 1, // 按日轮转
  SIZE = 2,  // 按大小轮转
};

inline constexpr std::string_view RotatePolicyToString(RotatePolicy policy) {
  switch (policy) {
  case RotatePolicy::DAILY:
    return "DAILY";
  case RotatePolicy::SIZE:
    return "SIZE";
  default:
    return "NONE";
  }
}

inline RotatePolicy ParseRotatePolicy(std::string_view value) {
  if (value == "DAILY") {
    return RotatePolicy::DAILY;
  }
  if (value == "SIZE") {
    return RotatePolicy::SIZE;
  }
  return RotatePolicy::NONE;
}

struct QueConfig {
  constexpr static size_t kMaxBlockTimeout_us =
      std::chrono::microseconds::max().count();

  size_t capacity;
  QueueFullPolicy full_policy;
  size_t block_timeout_us;

  QueConfig(size_t capacity = 65536,
            QueueFullPolicy policy = QueueFullPolicy::BLOCK,
            size_t timeout_us = kMaxBlockTimeout_us)
      : capacity(capacity), full_policy(policy), block_timeout_us(timeout_us) {}
};

#define LOAD_RELAXED(v) v.load(std::memory_order_relaxed)
#define STORE_RELAXED(v, val) v.store(val, std::memory_order_relaxed)

class LogConfig {
public:
  // 默认配置文件路径
  static constexpr std::string_view kDefaultConfigPath = "config.toml";

  // 默认日志级别
  static constexpr LogLevel kDefaultLogLevel = LogLevel::INFO;
  // 默认背压策略
  static constexpr QueueFullPolicy kDefaultBackpressurePolicy =
      QueueFullPolicy::BLOCK;
  // 默认时间格式
  static constexpr std::string_view kDefaultTimeFormat = "%Y-%m-%d %H:%M:%S";

  // 默认工作线程批处理大小
  static constexpr size_t kDefaultWorkerBatchSize = 2048;
  // 默认队列容量
  static constexpr size_t kDefaultQueueCapacity = 65536;

  // 单条日志最大长度
  static constexpr size_t kPerLogMaxSize = 4096;
  // 默认全局Buffer池大小
  static constexpr size_t kDefaultBufferPoolSize = 65536;
  // 默认线程本地缓存的Buffer数量
  static constexpr size_t kDefaultTLSBufferCount = 64;

  // 默认文件缓冲区大小
  static constexpr size_t kDefaultFileBufferSize = 128 * 1024;
  // 默认文件日志路径
  static constexpr std::string_view kDefaultLogPath = "logfile.log";
  // 默认控制台缓冲区大小
  static constexpr size_t kDefaultConsoleBufferSize = 1024;
  // 默认轮转策略
  static constexpr RotatePolicy kDefaultRotatePolicy = RotatePolicy::NONE;
  // 默认轮转大小（MB）
  static constexpr size_t kDefaultRotateSizeMB = 1024;
  // 默认最大轮转文件数
  static constexpr size_t kDefaultMaxRotateFiles = 7;
  // 默认背压自旋次数
  static constexpr size_t kDefaultBackpressureSpinCount = 100;
  // 默认背压线程休眠时间
  static constexpr size_t kDefaultBackpressureSleepUs = 100;
  // 默认工作线程空闲休眠时间
  static constexpr size_t kDefaultWorkerIdleSleepUs = 100;
  // 默认粗时间间隔
  static constexpr size_t kCoarseTimeIntervalMs = 1;

  // 默认配置文件热加载间隔
  static constexpr size_t kDefaultReloadIntervalMs = 1000;

  LogConfig() = default;
  LogConfig(std::string_view config_path) { LoadFromFile(config_path); }
  ~LogConfig() { StopHotReload(); }

  LogConfig(const LogConfig &other) { CopyFrom(other); }
  LogConfig &operator=(const LogConfig &other) {
    if (this != &other) {
      StopHotReload();
      CopyFrom(other);
    }
    return *this;
  }

  bool LoadFromFile(std::string_view path = kDefaultConfigPath,
                    bool apply_all = true) {
    config_path_ = std::string(path);
    TomlHelper helper;
    if (!helper.LoadFromFile(std::string(path))) {
      return false;
    }

    if (auto level_value = helper["logger"]["level"].value<std::string>()) {
      STORE_RELAXED(level_, ParseLevel(*level_value));
    }
    // 仅 Level 支持热更新，避免语义不一致
    if (!apply_all) {
      return true;
    }

    if (auto policy_value =
            helper["logger"]["full_policy"].value<std::string>()) {
      STORE_RELAXED(queue_full_policy_, ParsePolicy(*policy_value));
    }

    if (auto time_format =
            helper["logger"]["time_format"].value<std::string>()) {
      if (IsValidTimeFormat(*time_format)) {
        time_format_ = *time_format;
      }
    }
    if (auto value = helper["logger"]["performance"]["worker_batch_size"]
                         .value<int64_t>()) {
      if (*value > 0) {
        STORE_RELAXED(worker_batch_size_, static_cast<size_t>(*value));
      }
    }
    if (auto value = helper["logger"]["performance"]["queue_capacity"]
                         .value<int64_t>()) {
      if (*value >= 0) {
        STORE_RELAXED(queue_capacity_, static_cast<size_t>(*value));
      }
    }
    if (auto value = helper["logger"]["performance"]["queue_block_timeout_us"]
                         .value<int64_t>()) {
      if (*value < 0) {
        STORE_RELAXED(queue_block_timeout_us_, QueConfig::kMaxBlockTimeout_us);
      } else {
        STORE_RELAXED(queue_block_timeout_us_, static_cast<size_t>(*value));
      }
    }
    if (auto value = helper["logger"]["performance"]["buffer_pool_size"]
                         .value<int64_t>()) {
      if (*value > 0) {
        STORE_RELAXED(buffer_pool_size_, static_cast<size_t>(*value));
      }
    }
    if (auto value = helper["logger"]["performance"]["tls_buffer_count"]
                         .value<int64_t>()) {
      if (*value > 0) {
        STORE_RELAXED(tls_buffer_count_, static_cast<size_t>(*value));
      }
    }
    if (auto value =
            helper["sink"]["file"]["file_buffer_size_kb"].value<int64_t>()) {
      if (*value > 0) {
        STORE_RELAXED(file_buffer_size_, static_cast<size_t>(*value) * 1024);
      }
    }
    if (auto value = helper["sink"]["file"]["log_path"].value<std::string>()) {
      if (!value->empty()) {
        log_path_ = *value;
      }
    }
    if (auto value =
            helper["sink"]["file"]["rotate_policy"].value<std::string>()) {
      STORE_RELAXED(rotate_policy_, ParseRotatePolicy(*value));
    }
    if (auto value =
            helper["sink"]["file"]["rotate_size_mb"].value<int64_t>()) {
      if (*value > 0) {
        STORE_RELAXED(rotate_size_mb_, static_cast<size_t>(*value));
      }
    }
    if (auto value =
            helper["sink"]["file"]["max_rotate_files"].value<int64_t>()) {
      if (*value >= 0) {
        STORE_RELAXED(max_rotate_files_, static_cast<size_t>(*value));
      }
    }
    if (auto value = helper["sink"]["console"]["console_buffer_size_kb"]
                         .value<int64_t>()) {
      if (*value > 0) {
        STORE_RELAXED(console_buffer_size_, static_cast<size_t>(*value) * 1024);
      }
    }
    if (auto value = helper["other"]["reload_interval_ms"].value<int64_t>()) {
      if (*value >= 0) {
        STORE_RELAXED(reload_interval_ms_, static_cast<size_t>(*value));
      }
    }

    return true;
  }

  void StartHotReload(std::string_view path = kDefaultConfigPath) {
    if (watching_.exchange(true)) {
      return;
    }
    config_path_ = std::string(path);
    last_write_time_ = GetFileLastWriteTime(config_path_);
    watch_thread_ = std::thread([this] { WatchLoop(); });
  }

  void StopHotReload() {
    if (!watching_.exchange(false)) {
      return;
    }
    if (watch_thread_.joinable()) {
      watch_thread_.join();
    }
  }

  QueConfig GetQueueConfig() const {
    return QueConfig(LOAD_RELAXED(queue_capacity_),
                     LOAD_RELAXED(queue_full_policy_),
                     LOAD_RELAXED(queue_block_timeout_us_));
  }

  size_t GetBufferPoolSize() const { return LOAD_RELAXED(buffer_pool_size_); }
  size_t GetTLSBufferCount() const { return LOAD_RELAXED(tls_buffer_count_); }

  size_t GetWorkerBatchSize() const { return LOAD_RELAXED(worker_batch_size_); }

  size_t GetFileBufferSize() const { return LOAD_RELAXED(file_buffer_size_); }
  size_t GetConsoleBufferSize() const {
    return LOAD_RELAXED(console_buffer_size_);
  }
  std::string_view GetLogPath() const { return log_path_; }
  RotatePolicy GetRotatePolicy() const { return LOAD_RELAXED(rotate_policy_); }
  size_t GetRotateSizeMB() const { return LOAD_RELAXED(rotate_size_mb_); }
  size_t GetMaxRotateFiles() const { return LOAD_RELAXED(max_rotate_files_); }

  LogLevel GetLevel() const { return LOAD_RELAXED(level_); }
  std::string_view GetTimeFormat() const { return time_format_; }

  LogConfig &SetLevel(LogLevel level) {
    STORE_RELAXED(level_, level);
    return *this;
  }

  LogConfig &SetQueueCapacity(size_t capacity) {
    STORE_RELAXED(queue_capacity_, capacity);
    return *this;
  }

  LogConfig &SetQueueFullPolicy(QueueFullPolicy policy) {
    STORE_RELAXED(queue_full_policy_, policy);
    return *this;
  }

  LogConfig &SetQueueBlockTimeoutUs(size_t timeout_us) {
    STORE_RELAXED(queue_block_timeout_us_, timeout_us);
    return *this;
  }

  LogConfig &SetWorkerBatchSize(size_t size) {
    STORE_RELAXED(worker_batch_size_, size);
    return *this;
  }

  LogConfig &SetBufferPoolSize(size_t size) {
    STORE_RELAXED(buffer_pool_size_, size);
    return *this;
  }

  LogConfig &SetTLSBufferCount(size_t count) {
    STORE_RELAXED(tls_buffer_count_, count);
    return *this;
  }

  LogConfig &SetFileBufferSize(size_t size) {
    STORE_RELAXED(file_buffer_size_, size);
    return *this;
  }

  LogConfig &SetLogPath(std::string_view path) {
    if (!path.empty()) {
      log_path_ = std::string(path);
    }
    return *this;
  }

  LogConfig &SetConsoleBufferSize(size_t size) {
    STORE_RELAXED(console_buffer_size_, size);
    return *this;
  }

  LogConfig &SetRotatePolicy(RotatePolicy policy) {
    STORE_RELAXED(rotate_policy_, policy);
    return *this;
  }

  LogConfig &SetRotateSizeMB(size_t size_mb) {
    STORE_RELAXED(rotate_size_mb_, size_mb);
    return *this;
  }

  LogConfig &SetMaxRotateFiles(size_t max_files) {
    STORE_RELAXED(max_rotate_files_, max_files);
    return *this;
  }

  LogConfig &SetReloadIntervalMs(size_t interval_ms) {
    STORE_RELAXED(reload_interval_ms_, interval_ms);
    return *this;
  }

  LogConfig &SetTimeFormat(std::string_view format) {
    if (IsValidTimeFormat(format)) {
      time_format_ = std::string(format);
    }
    return *this;
  }

private:
  void CopyFrom(const LogConfig &other) {
    STORE_RELAXED(queue_capacity_, other.queue_capacity_);
    STORE_RELAXED(queue_block_timeout_us_, other.queue_block_timeout_us_);
    STORE_RELAXED(buffer_pool_size_, other.buffer_pool_size_);
    STORE_RELAXED(tls_buffer_count_, other.tls_buffer_count_);
    STORE_RELAXED(file_buffer_size_, other.file_buffer_size_);
    STORE_RELAXED(console_buffer_size_, other.console_buffer_size_);
    STORE_RELAXED(rotate_policy_, other.rotate_policy_);
    STORE_RELAXED(rotate_size_mb_, other.rotate_size_mb_);
    STORE_RELAXED(max_rotate_files_, other.max_rotate_files_);
    STORE_RELAXED(reload_interval_ms_, other.reload_interval_ms_);
    STORE_RELAXED(worker_batch_size_, other.worker_batch_size_);
    STORE_RELAXED(queue_full_policy_, other.queue_full_policy_);
    STORE_RELAXED(level_, other.level_);
    time_format_ = other.time_format_;
    log_path_ = other.log_path_;
    STORE_RELAXED(watching_, false);

    config_path_ = other.config_path_;
    last_write_time_ = std::filesystem::file_time_type::min();
  }

  void WatchLoop() {
    while (LOAD_RELAXED(watching_)) {
      auto current = GetFileLastWriteTime(config_path_);
      if (current != last_write_time_) {
        last_write_time_ = current;
        LoadFromFile(config_path_, false);
      }
      auto interval_ms = LOAD_RELAXED(reload_interval_ms_);
      if (interval_ms == 0) {
        interval_ms = kDefaultReloadIntervalMs;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
  }

  static bool IsValidTimeFormat(std::string_view format) {
    if (format.empty()) {
      return false;
    }
    std::time_t t = 0;
    std::tm tm_buf{};
#ifdef _WIN32
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&t, &tm_buf);
#endif
    char buffer[128];
    auto len = std::strftime(buffer, sizeof(buffer), format.data(), &tm_buf);
    return len > 0;
  }

private:
  // -------- 基本参数 -----------
  // 日志级别
  std::atomic<LogLevel> level_{kDefaultLogLevel};
  // 背压策略
  std::atomic<QueueFullPolicy> queue_full_policy_{kDefaultBackpressurePolicy};
  // 时间格式
  std::string time_format_{kDefaultTimeFormat};

  // --------- 性能参数 ------------
  // 工作线程批处理大小
  std::atomic<size_t> worker_batch_size_{kDefaultWorkerBatchSize};
  // 队列容量
  std::atomic<size_t> queue_capacity_{kDefaultQueueCapacity};
  // BLOCK 策略超时时间(us)
  std::atomic<size_t> queue_block_timeout_us_{QueConfig::kMaxBlockTimeout_us};
  // 全局日志缓冲区池大小
  std::atomic<size_t> buffer_pool_size_{kDefaultBufferPoolSize};
  // 线程本地缓存的Buffer数量
  std::atomic<size_t> tls_buffer_count_{kDefaultTLSBufferCount};

  // ---------- sinks 参数 ----------
  // 文件缓冲区大小
  std::atomic<size_t> file_buffer_size_{kDefaultFileBufferSize};
  std::string log_path_{kDefaultLogPath};
  // 控制台缓冲区大小
  std::atomic<size_t> console_buffer_size_{kDefaultConsoleBufferSize};
  // 文件轮转策略
  std::atomic<RotatePolicy> rotate_policy_{kDefaultRotatePolicy};
  // 文件轮转大小（MB）
  std::atomic<size_t> rotate_size_mb_{kDefaultRotateSizeMB};
  // 最大轮转文件数
  std::atomic<size_t> max_rotate_files_{kDefaultMaxRotateFiles};

  // --------- 其他参数 -----------
  // 配置文件热加载间隔(ms)
  std::atomic<size_t> reload_interval_ms_{kDefaultReloadIntervalMs};

  // 配置文件热加载
  std::atomic<bool> watching_{false};
  std::thread watch_thread_;
  std::string config_path_;
  std::filesystem::file_time_type last_write_time_{
      std::filesystem::file_time_type::min()};
};

} // namespace lyf
