#pragma once

#include <stdint.h>
#include <map>
#include <vector>
#include <mutex>
#include "key.h"
#include "node.h"

namespace keyvadb {

// Buffer is thread-safe
template <uint32_t BITS>
class Buffer {
  typedef Key<BITS> key_t;

 private:
  std::map<key_t, uint64_t> map_;
  std::mutex lock_;

 public:
  size_t Add(const key_t& key, const uint64_t value) {
    std::lock_guard<std::mutex> lock(lock_);
    map_.emplace(key, value);
    return map_.size();
  }

  bool Remove(KeyValue<BITS> const& key) {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.erase(key.key) > 0;
  }

  // Buffer must contain a and must not contain b.
  // a must not equal b
  void Swap(KeyValue<BITS> const& a, KeyValue<BITS> const& b) {
    if (a.key == b.key) throw std::domain_error("a==b");
    std::lock_guard<std::mutex> lock(lock_);
    if (map_.erase(a.key) == 0) throw std::domain_error("a does not exist");
    if (!map_.emplace(b.key, b.value).second)
      throw std::domain_error("b exists");
  }

  size_t Size() {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.size();
  }
};

}  // namespace keyvadb