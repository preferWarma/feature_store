#pragma once
#include "ISink.h"
#include "third/httplib.h"
#include "third/nlohmann_json.h"
#include "tool/Utility.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace lyf {

// HTTP Sink 配置结构体
struct HttpSinkConfig {
  std::string host = "127.0.0.1";
  int port = 8080;
  std::string endpoint = "/api/logs";
  std::string content_type = "application/json";
  // 超时时间（毫秒）
  size_t timeout_ms = 5000;
  // 连接超时时间（毫秒）
  size_t connection_timeout_ms = 3000;
  // 最大重试次数
  size_t max_retries = 3;
  // 重试间隔（毫秒）
  size_t retry_interval_ms = 1000;
  // 批量发送阈值（条数）
  size_t batch_size = 100;
  // 批量发送间隔（毫秒）
  size_t flush_interval_ms = 5000;
  // 是否使用 HTTPS
  bool use_https = false;
  // 自定义请求头
  std::vector<std::pair<std::string, std::string>> headers;
};

// --- HTTP Sink 实现 ---
class HttpSink : public ILogSink {
public:
  explicit HttpSink(const HttpSinkConfig &config)
      : config_(config), stop_worker_(false) {
    buffer_.reserve(LogConfig::kDefaultConsoleBufferSize);
    pending_logs_.reserve(config_.batch_size);
    InitClient();
    StartWorkerThread();
  }

  HttpSink(std::string_view host, int port, std::string_view path)
      : stop_worker_(false) {
    config_.host = std::string(host);
    config_.port = port;
    config_.endpoint = std::string(path);
    buffer_.reserve(LogConfig::kDefaultConsoleBufferSize);
    pending_logs_.reserve(config_.batch_size);
    InitClient();
    StartWorkerThread();
  }

  ~HttpSink() {
    StopWorkerThread();
    // 发送剩余日志
    FlushBatch(true);
  }

  void Log(const LogMessage &msg) override {
    std::unique_lock<std::mutex> lock(mutex_);
    buffer_.clear();
    formatter_.Format(msg, buffer_);

    // 将日志转换为 JSON 格式
    nlohmann::json log_entry;
    log_entry["timestamp"] = msg.time;
    log_entry["level"] = std::string(LevelToString(msg.level));
    log_entry["file"] = msg.file_name;
    log_entry["line"] = msg.file_line;
    log_entry["thread_id"] = msg.hash_tid;
    log_entry["message"] = std::string(msg.GetContent());

    pending_logs_.push_back(std::move(log_entry));

    // 达到批量阈值时触发发送
    if (pending_logs_.size() >= config_.batch_size) {
      lock.unlock();
      FlushBatch(false);
    }
  }

  void Flush() override { FlushBatch(true); }
  void Sync() override { FlushBatch(true); }

  void ApplyConfig(const LogConfig &config) override {
    formatter_.SetConfig(&config);
    std::lock_guard<std::mutex> lock(mutex_);
    buffer_.reserve(config.GetConsoleBufferSize());
  }

  // 更新 HTTP 配置
  void SetHttpConfig(const HttpSinkConfig &config) {
    std::lock_guard<std::mutex> lock(mutex_);
    bool need_reconnect =
        (config_.host != config.host || config_.port != config.port ||
         config_.use_https != config.use_https);
    config_ = config;
    if (need_reconnect) {
      InitClient();
    }
  }

  // 获取当前配置
  HttpSinkConfig GetHttpConfig() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_;
  }

  // 获取待发送日志数量
  size_t GetPendingCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_logs_.size();
  }

  // 检查客户端是否有效
  bool IsClientValid() const {
    return client_ != nullptr && client_->is_valid();
  }

  // 获取最后一次错误信息
  std::string GetLastError() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_error_;
  }

private:
  void InitClient() {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
      client_ = std::make_unique<httplib::Client>(config_.host, config_.port);
      client_->set_connection_timeout(config_.connection_timeout_ms / 1000.0);
      client_->set_read_timeout(config_.timeout_ms / 1000.0);
      client_->set_write_timeout(config_.timeout_ms / 1000.0);
      last_error_.clear();
    } catch (const std::exception &e) {
      last_error_ = std::string("Failed to create HTTP client: ") + e.what();
      lyf_inner_log("[ERROR] HttpSink init failed: {}", last_error_);
      client_.reset();
    }
  }

  void StartWorkerThread() {
    worker_thread_ = std::thread([this]() {
      while (!stop_worker_.load()) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(config_.flush_interval_ms));
        FlushBatch(false);
      }
    });
  }

  void StopWorkerThread() {
    stop_worker_.store(true);
    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
  }

  void FlushBatch(bool force) {
    std::vector<nlohmann::json> logs_to_send;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (pending_logs_.empty() ||
          (!force && pending_logs_.size() < config_.batch_size)) {
        return;
      }
      logs_to_send.swap(pending_logs_);
    }

    if (logs_to_send.empty()) {
      return;
    }

    SendLogsWithRetry(logs_to_send);
  }

  void SendLogsWithRetry(const std::vector<nlohmann::json> &logs) {
    if (!client_) {
      std::lock_guard<std::mutex> lock(mutex_);
      last_error_ = "HTTP client not initialized";
      return;
    }

    nlohmann::json payload;
    payload["logs"] = logs;
    payload["count"] = logs.size();
    payload["timestamp"] =
        std::chrono::system_clock::now().time_since_epoch().count();

    std::string body = payload.dump();
    httplib::Headers headers;
    headers.emplace("Content-Type", config_.content_type);

    // 添加自定义请求头
    for (const auto &header : config_.headers) {
      headers.emplace(header.first, header.second);
    }

    size_t attempts = 0;
    while (attempts <= config_.max_retries) {
      auto result =
          client_->Post(config_.endpoint, headers, body, config_.content_type);

      if (result && result->status == 200) {
        std::lock_guard<std::mutex> lock(mutex_);
        last_error_.clear();
        return;
      }

      // 记录错误信息
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (result) {
          last_error_ = std::format("HTTP error: status={}, body={}",
                                    result->status, result->body);
        } else {
          last_error_ = std::format("HTTP request failed: {}",
                                    httplib::to_string(result.error()));
        }
      }

      ++attempts;
      if (attempts <= config_.max_retries) {
        lyf_inner_log("[WARN] HttpSink send failed, retrying {}/{}: {}",
                      attempts, config_.max_retries, last_error_);
        std::this_thread::sleep_for(
            std::chrono::milliseconds(config_.retry_interval_ms));
      }
    }

    lyf_inner_log("[ERROR] HttpSink failed after {} retries: {}",
                  config_.max_retries, last_error_);
  }

private:
  mutable std::mutex mutex_;
  HttpSinkConfig config_;
  std::unique_ptr<httplib::Client> client_;
  std::vector<nlohmann::json> pending_logs_;
  std::atomic<bool> stop_worker_;
  std::thread worker_thread_;
  std::string last_error_;
};

} // namespace lyf
