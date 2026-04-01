#pragma once
#include <type_traits>

namespace lyf {

template <typename T> class Singleton {
public:
  // 强制内联，减少函数调用开销
  [[nodiscard]] static T &
  Instance() noexcept(std::is_nothrow_constructible_v<T>) {
    static T instance;
    return instance;
  }

  // 严禁拷贝和移动
  Singleton(const Singleton &) = delete;
  Singleton &operator=(const Singleton &) = delete;
  Singleton(Singleton &&) = delete;
  Singleton &operator=(Singleton &&) = delete;

protected:
  Singleton() = default;
  virtual ~Singleton() = default; // 虚析构保证子类安全
};

} // namespace lyf