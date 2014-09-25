#pragma once

#include <stdint.h>
#include <map>
#include <vector>
#include <mutex>
#include "key.h"
#include "node.h"

namespace keyvadb {

// Buffer is thread-safe
template <uint32_t BITS, uint32_t DEGREE>
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

  bool Swap(KeyValue<BITS> const& a, KeyValue<BITS> const& b) {
    std::lock_guard<std::mutex> lock(lock_);
    map_.emplace(b.key, b.value);
    return map_.erase(a.key) > 0;
  }

  size_t Size() {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.size();
  }
};

}  // namespace keyvadb