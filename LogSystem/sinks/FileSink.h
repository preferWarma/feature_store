#pragma once
#include "ISink.h"
#include "tool/Utility.h"
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <string>
#include <string_view>

namespace lyf {
class FileSink : public ILogSink {
public:
  FileSink(std::string_view filepath) {
    Init(filepath);
    // 默认配置初始化
    auto file_buffer_size = LogConfig::kDefaultFileBufferSize;
    if (file_) {
      setvbuf(file_, nullptr, _IOFBF, file_buffer_size);
    }
    buffer_.reserve(file_buffer_size);
  }

  FileSink(std::string_view filepath, const LogConfig &config) {
    Init(filepath);
    ApplyConfig(config);
  }

  ~FileSink() {
    if (file_) {
      fflush(file_);
      fclose(file_);
    }
  }

  void Log(const LogMessage &msg) override {
    if (!file_) {
      return;
    }

    // 检查是否需要轮转
    CheckRotate();

    buffer_.clear();
    formatter_.Format(msg, buffer_);
    fwrite(buffer_.data(), 1, buffer_.size(), file_);
    current_file_size_ += buffer_.size();
  }

  void Flush() override {
    if (file_) {
      fflush(file_);
    }
  }

  void Sync() override {
    if (file_) {
#ifdef _WIN32
      _commit(_fileno(file_));
#else
      fsync(fileno(file_));
#endif
    }
  }

  void ApplyConfig(const LogConfig &config) override {
    formatter_.SetConfig(&config);
    rotate_policy_ = config.GetRotatePolicy();
    rotate_size_bytes_ = config.GetRotateSizeMB() * 1024 * 1024;
    max_rotate_files_ = config.GetMaxRotateFiles();

    // 如果策略切换为 DAILY，立即计算下一次轮转时间
    if (rotate_policy_ == RotatePolicy::DAILY) {
      UpdateNextRotationTime();
    }

    if (file_) {
      auto file_buffer_size = config.GetFileBufferSize();
      setvbuf(file_, nullptr, _IOFBF, file_buffer_size);
      buffer_.reserve(file_buffer_size);
    }
  }

private:
  void Init(std::string_view filepath) {
    base_filepath_ = std::string(filepath);
    file_ = fopen(filepath.data(), "a");
    if (!file_) {
      lyf_inner_log("[ERROR] open file {} failed, errno: {}", filepath,
                    strerror(errno));
      return;
    }
    current_file_size_ = std::filesystem::file_size(filepath);

    // 初始化时计算一次明天的零点时间
    UpdateNextRotationTime();
  }

  // 检查并执行轮转
  void CheckRotate() {
    switch (rotate_policy_) {
    case RotatePolicy::DAILY:
      CheckDailyRotate();
      break;
    case RotatePolicy::SIZE:
      CheckSizeRotate();
      break;
    default:
      break;
    }
  }

  // 计算明天 00:00:00 的时间戳
  void UpdateNextRotationTime() {
    auto now = std::time(nullptr);
    std::tm tm_val{};
#ifdef _WIN32
    localtime_s(&tm_val, &now);
#else
    localtime_r(&now, &tm_val);
#endif
    // 设置为明天 00:00:00
    tm_val.tm_hour = 0;
    tm_val.tm_min = 0;
    tm_val.tm_sec = 0;
    tm_val.tm_mday += 1;
    // 转换为时间戳并缓存
    next_rotation_tp_ = std::mktime(&tm_val);
  }

  // 检查日轮转
  void CheckDailyRotate() {
    if (std::time(nullptr) < next_rotation_tp_) {
      return;
    }
    RotateFile();
    // 轮转完成后，更新下一次轮转的时间
    UpdateNextRotationTime();
  }

  // 检查大小轮转
  void CheckSizeRotate() {
    if (current_file_size_ < rotate_size_bytes_) {
      return;
    }

    RotateFile();
    // 轮转完成后，更新当前文件大小为 0
    current_file_size_ = 0;
  }

  // 执行文件轮转
  void RotateFile() {
    if (!file_) {
      return;
    }

    // 关闭当前文件
    Sync();
    fclose(file_);
    file_ = nullptr;

    auto rotated_name = GenerateRotatedName();
    std::filesystem::rename(base_filepath_, rotated_name);
    CollectRotateFilesAndCleanOld(rotated_name);

    // 重新打开新文件
    file_ = fopen(base_filepath_.c_str(), "a");
    if (file_) {
      auto file_buffer_size = buffer_.capacity();
      if (file_buffer_size == 0) {
        file_buffer_size = LogConfig::kDefaultFileBufferSize;
      }
      setvbuf(file_, nullptr, _IOFBF, file_buffer_size);
    } else {
      lyf_inner_log("[ERROR] open file {} failed, errno: {}", rotated_name,
                    strerror(errno));
    }
  }

  // 生成轮转后的文件名
  // Daily: <base_file><datetime>
  // Size: <base_file>_<index>
  std::string GenerateRotatedName() {
    if (rotate_policy_ == RotatePolicy::DAILY) {
      return base_filepath_ + CurrentTimeToString("%Y-%m-%d");
    } else if (rotate_policy_ == RotatePolicy::SIZE) {
      static int i = 1;
      auto rotated_name = base_filepath_ + "_" + std::to_string(i++);
      while (std::filesystem::exists(rotated_name)) {
        rotated_name = base_filepath_ + "_" + std::to_string(i++);
      }
      return rotated_name;
    }
    return "";
  }

  // 收集新的轮转文件并清理旧的轮转文件，只保留最近的 max_rotate_files_ 个文件
  void CollectRotateFilesAndCleanOld(const std::string &rotated_name) {
    rotate_files_.push_back(rotated_name);
    lyf_inner_log("[INFO] rotate file {}", rotated_name);
    // 删除超过保留数量的旧文件
    while (rotate_files_.size() > max_rotate_files_) {
      std::filesystem::remove(rotate_files_.front());
      rotate_files_.pop_front();
    }
  }

private:
  FILE *file_ = nullptr;
  std::string base_filepath_;    // 基础文件路径
  size_t current_file_size_ = 0; // 当前文件大小

  // 轮转相关
  RotatePolicy rotate_policy_{LogConfig::kDefaultRotatePolicy}; // 轮转策略
  std::deque<std::string> rotate_files_; // 轮转文件收集队列(队尾入，队头出)
  size_t max_rotate_files_{LogConfig::kDefaultMaxRotateFiles}; // 最大轮转文件数
  size_t rotate_size_bytes_{LogConfig::kDefaultRotateSizeMB * 1024 *
                            1024};   // 轮转大小（Size 策略）
  std::time_t next_rotation_tp_ = 0; // 下一次轮转时间点（Daily策略）
};
} // namespace lyf