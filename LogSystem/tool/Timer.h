#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

namespace lyf {
namespace chrono = std::chrono;

using std::queue, std::vector, std::unique_lock, std::lock_guard;
using std::thread, std::mutex, std::condition_variable, std::atomic;
using std::unique_ptr;

/// @brief 简单的线程池实现，用于执行定时任务
class ThreadPool {
private:
  vector<thread> workers_;
  queue<std::function<void()>> tasks_;
  mutex que_mtx_;
  condition_variable cv_;
  atomic<bool> stop_;

public:
  explicit ThreadPool(size_t threads = thread::hardware_concurrency())
      : stop_(false) {
    for (size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this] {
        while (true) {
          std::function<void()> task;
          {
            unique_lock<mutex> lock(this->que_mtx_);
            this->cv_.wait(
                lock, [this] { return this->stop_ || !this->tasks_.empty(); });
            if (this->stop_ && this->tasks_.empty())
              return;
            task = std::move(this->tasks_.front());
            this->tasks_.pop();
          }
          task();
        }
      });
    }
  }

  template <class F> void enqueue(F &&f) {
    {
      unique_lock<mutex> lock(que_mtx_);
      if (stop_)
        throw std::runtime_error("enqueue on stopped ThreadPool");
      tasks_.emplace(std::forward<F>(f));
    }
    cv_.notify_one();
  }

  ~ThreadPool() {
    {
      unique_lock<mutex> lock(que_mtx_);
      stop_ = true;
    }
    cv_.notify_all();
    for (thread &worker : workers_)
      worker.join();
  }
};

// =============================
// Stopwatch 计时器类,用于测量时间间隔
// =============================
class stopwatch {
  using system_clock = chrono::system_clock;
  using time_point = system_clock::time_point;

protected:
  bool started{false};   // 是否已经开始计时
  bool stopped{false};   // 是否已经停止计时
  double rate{1.f};      // 时间比例(默认为1us)
  time_point begin_time; // 开始时间
  time_point end_time;   // 停止时间
  size_t tick{0ull};     // duration的tick数(纳秒)

public:
  enum class TimeType { ns = 1, us = 1000, ms = 1000000, s = 1000000000 };

  // 指定rate倍数的ns作为单位
  stopwatch(double rate = 1.0)
      : started(false), stopped(false), rate(rate), tick(0),
        begin_time(system_clock::now()), end_time(system_clock::now()) {}

  // 指定时间类型作为单位
  stopwatch(TimeType type) : stopwatch(static_cast<double>(type)) {}

  virtual ~stopwatch() = default;

  inline bool is_started() const { return started; }

  inline void start() {
    reset();
    started = true;
    stopped = false;
    begin_time = system_clock::now();
  }

  inline void stop() {
    if (!started) {
      return;
    }
    stopped = true;
    end_time = system_clock::now();
    tick = static_cast<size_t>(
        chrono::duration_cast<chrono::nanoseconds>(end_time - begin_time)
            .count());
  }

  inline void reset() {
    started = false;
    stopped = false;
    tick = 0;
  }

  inline double duration() {
    // 未启动计时器, 抛出异常
    if (!started) {
      throw std::runtime_error("StopWatch::duration(): Not started yet.");
      return -1;
    }
    // 未停止计时器, 停止计时器计算duration后再启动
    if (!stopped) {
      stop();
      stopped = true;
    }
    return static_cast<double>(tick) / rate; // 返回单位为rate倍的ns
  }
};

// 对TimeType的输出运算符重载
inline std::ostream &operator<<(std::ostream &os, stopwatch::TimeType type) {
  switch (type) {
  case stopwatch::TimeType::ns:
    os << "ns";
    break;
  case stopwatch::TimeType::us:
    os << "us";
    break;
  case stopwatch::TimeType::ms:
    os << "ms";
    break;
  case stopwatch::TimeType::s:
    os << "s";
    break;
  default:
    break;
  }
  return os;
}

// =============================
// auto_stopwatch 自动计时器类 - 构造时开始计时，析构时输出时间
// =============================
class auto_stopwatch : public stopwatch {
public:
  inline auto_stopwatch(double rate = 1.0) : stopwatch(rate) { this->start(); }
  inline auto_stopwatch(TimeType type) : stopwatch(type) { this->start(); }
  inline ~auto_stopwatch() {
    std::cout << "duration time: " << this->duration()
              << static_cast<TimeType>(rate) << std::endl;
  }
};

// =============================
// Timer 定时器类, 用于定时触发任务
// =============================
class Timer {
public:
  using system_clock = chrono::system_clock;
  using steady_clock = chrono::steady_clock;
  using Duration_ms = chrono::milliseconds;
  using Callback_t = std::function<void()>;

  enum class TimerType {
    ONCE,     // 延迟一次性触发
    PERIODIC, // 周期性触发
    AT_TIME   // 在指定时刻触发
  };

  enum class ExecutionMode {
    ASYNC, // 异步执行（使用线程池）
    SYNC   // 同步执行（在工作线程中直接执行）
  };

  enum class ThreadPoolMode {
    DISABLED, // 不使用线程池，所有任务同步执行
    ENABLED,  // 启用线程池
    LAZY      // 延迟初始化（首次使用异步任务时创建）
  };

private:
  struct TimerTask {
    TimerType type_;                       // 定时器类型
    Callback_t callback_;                  // 回调函数
    Duration_ms interval_ms_;              // 时间间隔(ms)
    steady_clock::time_point nextRunTime_; // 下一次触发时间
    system_clock::time_point targetTime_;  // 用于AT_TIME类型
    bool active;                           // 任务是否激活
    ExecutionMode exec_mode;               // 执行模式

    TimerTask(TimerType t, Callback_t cb, Duration_ms d = Duration_ms(0),
              ExecutionMode mode = ExecutionMode::SYNC)
        : type_(t), callback_(cb), interval_ms_(d), active(true),
          exec_mode(mode) {
      if (type_ == TimerType::ONCE || type_ == TimerType::PERIODIC) {
        nextRunTime_ = steady_clock::now() + interval_ms_;
      }
    }
  };

  atomic<bool> running_;      // 定时器是否运行
  thread workerThread_;       // 工作线程(异步模式下只负责调度)
  mutex taskMtx_;             // 保护任务列表的互斥锁
  condition_variable taskCv_; // 任务相关条件变量
  std::map<size_t, unique_ptr<TimerTask>> tasks_; // task_id ->task
  atomic<size_t> nextId_;             // 用于生成递增的任务 ID
  unique_ptr<ThreadPool> threadPool_; // 线程池(用于实际执行异步任务)
  ThreadPoolMode poolMode_;           // 线程池模式
  size_t poolSizeConfig_;             // 线程池配置大小
  ExecutionMode defaultExecMode_;     // 默认执行模式
  mutex poolMtx_;                     // 保护线程池的延迟初始化

public:
  /// 默认不创建线程池，纯同步模式
  Timer()
      : running_(true), nextId_(1), poolMode_(ThreadPoolMode::DISABLED),
        poolSizeConfig_(0), defaultExecMode_(ExecutionMode::SYNC) {
    workerThread_ = thread(&Timer::worker, this);
  }

  /// @param mode 不启用 DISABLED/启用 ENABLED/懒加载 LAZY
  /// @param pool_size 仅在ENABLED或LAZY模式下有效，0表示使用硬件并发数
  explicit Timer(ThreadPoolMode mode, size_t pool_size = 0)
      : running_(true), nextId_(1), poolMode_(mode), poolSizeConfig_(pool_size),
        defaultExecMode_(mode == ThreadPoolMode::DISABLED
                             ? ExecutionMode::SYNC
                             : ExecutionMode::ASYNC) {
    // ENABLED模式下立即创建线程池
    if (mode == ThreadPoolMode::ENABLED) {
      initThreadPool();
    }
    // LAZY模式下不立即创建线程池
    workerThread_ = thread(&Timer::worker, this);
  }

  // 直接指定线程池大小，自动启用
  explicit Timer(size_t pool_size)
      : Timer(ThreadPoolMode::ENABLED, pool_size) {}

  ~Timer() { stop(); }

  /// @brief 经过固定时间后触发（一次性）
  /// @param delay_ms 延迟时间（毫秒）
  /// @param callback 回调函数
  /// @param exec_mode 执行模式（可选，默认根据线程池状态自动选择）
  /// @return 定时器ID
  size_t setTimeout(size_t delay_ms, Callback_t callback,
                    std::optional<ExecutionMode> exec_mode = std::nullopt) {
    lock_guard<mutex> lock(taskMtx_);
    size_t id = nextId_++;

    // 自动确定执行模式
    ExecutionMode mode = exec_mode.value_or(defaultExecMode_);
    if (mode == ExecutionMode::ASYNC && poolMode_ == ThreadPoolMode::LAZY) {
      // 延迟初始化线程池
      ensureThreadPool();
    }

    auto task = make_unique<TimerTask>(TimerType::ONCE, callback,
                                       Duration_ms(delay_ms), mode);

    tasks_[id] = std::move(task);
    taskCv_.notify_one();
    return id;
  }

  /// @brief 按照固定频率周期性触发
  /// @param interval_ms 间隔时间（毫秒）
  /// @param callback 回调函数
  /// @param exec_mode 执行模式（可选）
  /// @return 定时器ID
  size_t setInterval(size_t interval_ms, Callback_t callback,
                     std::optional<ExecutionMode> exec_mode = std::nullopt) {
    lock_guard<mutex> lock(mutex);
    size_t id = nextId_++;

    ExecutionMode mode = exec_mode.value_or(defaultExecMode_);
    if (mode == ExecutionMode::ASYNC && poolMode_ == ThreadPoolMode::LAZY) {
      ensureThreadPool();
    }

    auto task = make_unique<TimerTask>(TimerType::PERIODIC, callback,
                                       Duration_ms(interval_ms), mode);

    tasks_[id] = std::move(task);
    taskCv_.notify_one();
    return id;
  }

  /// @brief 在某个时刻触发
  /// @param callback 回调函数
  /// @param year 年
  /// @param month 月
  /// @param day 日
  /// @param hour 时
  /// @param minute 分
  /// @param second 秒
  /// @param exec_mode 执行模式（可选）
  /// @return 定时器ID
  size_t setAlarm(Callback_t callback, int year, int month, int day, int hour,
                  int minute, int second,
                  std::optional<ExecutionMode> exec_mode = std::nullopt) {
    lock_guard<mutex> lock(mutex);
    size_t id = nextId_++;

    ExecutionMode mode = exec_mode.value_or(defaultExecMode_);
    if (mode == ExecutionMode::ASYNC && poolMode_ == ThreadPoolMode::LAZY) {
      ensureThreadPool();
    }

    // 构造目标时间
    tm tm = {};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;

    auto target_time = chrono::system_clock::from_time_t(mktime(&tm));

    auto task = make_unique<TimerTask>(TimerType::AT_TIME, callback,
                                       Duration_ms(0), mode);
    task->targetTime_ = target_time;

    // 计算从现在到目标时间的duration
    auto now = chrono::system_clock::now();
    if (target_time > now) {
      auto delay = chrono::duration_cast<Duration_ms>(target_time - now);
      task->nextRunTime_ = chrono::steady_clock::now() + delay;
    } else {
      // 如果目标时间已过，立即执行
      task->nextRunTime_ = chrono::steady_clock::now();
    }

    tasks_[id] = std::move(task);
    taskCv_.notify_one();
    return id;
  }

  /// @brief 在指定时间戳触发
  /// @param callback 回调函数
  /// @param timestamp Unix时间戳
  /// @param exec_mode 执行模式（可选）
  /// @return 定时器ID
  size_t setAlarmAt(Callback_t callback, time_t timestamp,
                    std::optional<ExecutionMode> exec_mode = std::nullopt) {
    lock_guard<mutex> lock(mutex);
    size_t id = nextId_++;

    ExecutionMode mode = exec_mode.value_or(defaultExecMode_);
    if (mode == ExecutionMode::ASYNC && poolMode_ == ThreadPoolMode::LAZY) {
      ensureThreadPool();
    }

    auto target_time = chrono::system_clock::from_time_t(timestamp);

    auto task = make_unique<TimerTask>(TimerType::AT_TIME, callback,
                                       Duration_ms(0), mode);
    task->targetTime_ = target_time;

    auto now = chrono::system_clock::now();
    if (target_time > now) {
      auto delay = chrono::duration_cast<Duration_ms>(target_time - now);
      task->nextRunTime_ = chrono::steady_clock::now() + delay;
    } else {
      task->nextRunTime_ = chrono::steady_clock::now();
    }

    tasks_[id] = std::move(task);
    taskCv_.notify_one();
    return id;
  }

  /// @brief 在指定时间点触发（std::chrono::system_clock版本）
  /// @param callback 回调函数
  /// @param time_point chrono::system_clock时间点
  /// @param exec_mode 执行模式（可选）
  /// @return 定时器ID
  size_t setAlarmAt(Callback_t callback,
                    chrono::system_clock::time_point time_point,
                    std::optional<ExecutionMode> exec_mode = std::nullopt) {
    lock_guard<mutex> lock(mutex);
    size_t id = nextId_++;

    ExecutionMode mode = exec_mode.value_or(defaultExecMode_);
    if (mode == ExecutionMode::ASYNC && poolMode_ == ThreadPoolMode::LAZY) {
      ensureThreadPool();
    }

    auto task = make_unique<TimerTask>(TimerType::AT_TIME, callback,
                                       Duration_ms(0), mode);
    task->targetTime_ = time_point;

    auto now = chrono::system_clock::now();
    if (time_point > now) {
      auto delay = chrono::duration_cast<Duration_ms>(time_point - now);
      task->nextRunTime_ = chrono::steady_clock::now() + delay;
    } else {
      task->nextRunTime_ = chrono::steady_clock::now();
    }

    tasks_[id] = std::move(task);
    taskCv_.notify_one();
    return id;
  }

  /// @brief 取消定时器
  /// @param timer_id 定时器ID
  /// @return 是否成功取消
  bool cancel(size_t timer_id) {
    lock_guard<mutex> lock(mutex);
    auto it = tasks_.find(timer_id);
    if (it != tasks_.end()) {
      tasks_.erase(it);
      return true;
    }
    return false;
  }

  /// @brief 暂停定时器
  /// @param timer_id 定时器ID
  /// @return 是否成功暂停
  bool pause(size_t timer_id) {
    lock_guard<mutex> lock(mutex);
    auto it = tasks_.find(timer_id);
    if (it != tasks_.end()) {
      it->second->active = false;
      return true;
    }
    return false;
  }

  /// @brief 恢复定时器
  /// @param timer_id 定时器ID
  /// @return 是否成功恢复
  bool resume(size_t timer_id) {
    lock_guard<mutex> lock(mutex);
    auto it = tasks_.find(timer_id);
    if (it != tasks_.end()) {
      it->second->active = true;
      // 重新计算下次运行时间
      if (it->second->type_ == TimerType::PERIODIC) {
        it->second->nextRunTime_ =
            chrono::steady_clock::now() + it->second->interval_ms_;
      }
      taskCv_.notify_one();
      return true;
    }
    return false;
  }

  /// @brief 停止所有定时器
  void stop() {
    running_ = false;
    taskCv_.notify_all();
    if (workerThread_.joinable()) {
      workerThread_.join();
    }
    tasks_.clear();
    threadPool_.reset();
  }

  /// @brief 获取活动定时器数量
  /// @return 活动定时器数量
  size_t getActiveCount() const {
    lock_guard<mutex> lock(const_cast<mutex &>(taskMtx_));
    return tasks_.size();
  }

  /// @brief 设置默认执行模式
  /// @param mode 执行模式
  void setDefaultExecutionMode(ExecutionMode mode) { defaultExecMode_ = mode; }

  /// @brief 手动启用线程池
  /// @param size 线程池大小（0表示使用硬件并发数）
  /// @return 是否成功启用
  bool enableThreadPool(size_t size = 0) {
    lock_guard<mutex> lock(poolMtx_);
    if (threadPool_) {
      return false; // 已经存在
    }
    poolMode_ = ThreadPoolMode::ENABLED;
    poolSizeConfig_ = size;
    initThreadPool();
    return true;
  }

  /// @brief 获取线程池状态
  /// @return 线程池是否可用
  bool isThreadPoolAvailable() const {
    lock_guard<mutex> lock(const_cast<mutex &>(poolMtx_));
    return threadPool_ != nullptr;
  }

  /// @brief 获取线程池模式
  /// @return 当前的线程池模式
  ThreadPoolMode getThreadPoolMode() const { return poolMode_; }

private:
  /// @brief 初始化线程池
  void initThreadPool() {
    if (!threadPool_) {
      size_t size = poolSizeConfig_;
      if (size == 0) {
        size = thread::hardware_concurrency();
        if (size == 0)
          size = 4; // 保底值
      }
      threadPool_ = std::make_unique<ThreadPool>(size);
    }
  }

  /// @brief 确保线程池存在（用于延迟初始化）
  void ensureThreadPool() {
    lock_guard<mutex> lock(poolMtx_);
    if (!threadPool_ && poolMode_ == ThreadPoolMode::LAZY) {
      initThreadPool();
    }
  }

  void worker() {
    while (running_) {
      unique_lock<mutex> lock(taskMtx_);

      if (tasks_.empty()) {
        taskCv_.wait(lock);
        continue;
      }

      // 找到下一个要执行的任务
      auto now = steady_clock::now();
      TimerTask *next_task = nullptr;
      size_t next_task_id = 0;
      auto earliest = steady_clock::time_point::max();

      for (auto &[id, task] : tasks_) {
        if (task->active && task->nextRunTime_ < earliest) {
          earliest = task->nextRunTime_;
          next_task = task.get();
          next_task_id = id;
        }
      }

      if (!next_task) {
        taskCv_.wait(lock);
        continue;
      }

      if (earliest <= now) {
        // 准备执行任务
        Callback_t cb = next_task->callback_;
        TimerType type = next_task->type_;
        ExecutionMode exec_mode = next_task->exec_mode;

        // 检查是否需要异步执行
        bool should_async = false;
        if (exec_mode == ExecutionMode::ASYNC) {
          lock_guard<mutex> pool_lock(poolMtx_);
          should_async = (threadPool_ != nullptr);
        }

        if (type == TimerType::ONCE || type == TimerType::AT_TIME) {
          // 一次性任务，执行后删除
          tasks_.erase(next_task_id);
          lock.unlock();

          if (should_async) {
            threadPool_->enqueue(cb);
          } else {
            cb(); // 同步执行或线程池不可用时直接执行
          }
        } else if (type == TimerType::PERIODIC) {
          // 周期性任务，更新下次运行时间
          next_task->nextRunTime_ = now + next_task->interval_ms_;
          lock.unlock();

          if (should_async) {
            threadPool_->enqueue(cb);
          } else {
            cb(); // 同步执行或线程池不可用时直接执行
          }
        }
      } else {
        // 等待到下一个任务的执行时间
        taskCv_.wait_until(lock, earliest);
      }
    }
  }
};

} // namespace lyf