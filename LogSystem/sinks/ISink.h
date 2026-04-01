#pragma once

#include "LogConfig.h"
#include "LogFormatter.h"

#include <unistd.h>
#include <vector>

namespace lyf {

class ILogSink {
public:
  virtual ~ILogSink() = default;
  virtual void Log(const LogMessage &msg) = 0;
  virtual void Flush() = 0;
  virtual void Sync() = 0;

  virtual void ApplyConfig(const LogConfig &config) {
    formatter_.SetConfig(&config);
  }

protected:
  // 每个 Sink 拥有自己的 Formatter 和 暂存 Buffer
  LogFormatter formatter_;
  std::vector<char> buffer_;
};



} // namespace lyf
