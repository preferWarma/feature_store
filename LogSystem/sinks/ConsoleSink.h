#pragma once
#include "ISink.h"
namespace lyf {

// --- 控制台 Sink 实现 ---
class ConsoleSink : public ILogSink {
public:
  ConsoleSink() { buffer_.reserve(LogConfig::kDefaultConsoleBufferSize); }
  explicit ConsoleSink(const LogConfig &config) { ApplyConfig(config); }

  void Log(const LogMessage &msg) override {
    buffer_.clear();
    formatter_.Format(msg, buffer_);
    fwrite(buffer_.data(), 1, buffer_.size(), stdout);
  }

  void Flush() override { fflush(stdout); }
  void Sync() override {
#ifdef _WIN32
    _commit(_fileno(stdout));
#else
    fsync(fileno(stdout));
#endif
  }

  void ApplyConfig(const LogConfig &config) override {
    formatter_.SetConfig(&config);
    buffer_.reserve(config.GetConsoleBufferSize());
  }
};
} // namespace lyf