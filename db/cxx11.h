#pragma once

#include <memory>

namespace std {

#if __cplusplus <= 201103L
template <class T, class... Args>
std::unique_ptr<T> make_unique(Args&&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}
#endif
}  // namespace std
