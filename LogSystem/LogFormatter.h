#pragma once

#include "LogMessage.h"
#include <chrono>
#include <ctime>
#include <format>
#include <vector>

namespace lyf {
template <size_t N> class FixedBuffer {
public:
  std::string_view FormatTime(int64_t timestamp_ns, std::string_view format) {
    auto time_point = std::chrono::system_clock::time_point(
        std::chrono::system_clock::duration(timestamp_ns));
    std::time_t current_time = std::chrono::system_clock::to_time_t(time_point);

    // 缓存最近格式化结果，避免重复格式化
    if (current_time == last_time_ && length_ > 0) {
      return std::string_view(buf_, length_);
    }

    last_time_ = current_time;
    std::tm tm_buf;
#ifdef _WIN32
    localtime_s(&tm_buf, &current_time);
#else
    localtime_r(&current_time, &tm_buf);
#endif
    length_ = std::strftime(buf_, sizeof(buf_), format.data(), &tm_buf);
    return std::string_view(buf_, length_);
  }

private:
  char buf_[N];
  std::time_t last_time_ = 0;
  size_t length_ = 0;
};

class LogFormatter {
public:
  void SetConfig(const LogConfig *config) { config_ = config; }
  const LogConfig *GetConfig() const { return config_; }

  void Format(const LogMessage &msg, std::vector<char> &dest) {
    // 1. static: 保证跨函数调用时，对象不销毁，缓存（last_time_）有效。
    // 2. thread_local: 保证每个线程有一份独立的副本，彻底消除竞争。
    static thread_local FixedBuffer<64> tls_time_buffer;

    auto format =
        config_ ? config_->GetTimeFormat() : LogConfig::kDefaultTimeFormat;
    std::string_view time_sv = tls_time_buffer.FormatTime(msg.time, format);
    std::format_to(std::back_inserter(dest), "{} {} {} {}:{} ", time_sv,
                   LevelToString(msg.level), msg.hash_tid, msg.file_name,
                   msg.file_line);

    auto content = msg.GetContent();
    dest.insert(dest.end(), content.begin(), content.end());
    dest.emplace_back('\n');
  }

private:
  const LogConfig *config_{nullptr};
};

} // namespace lyf
