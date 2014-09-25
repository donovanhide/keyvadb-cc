#pragma once

#include <stdint.h>
#include <map>
#include <vector>
#include <mutex>
#include "key.h"
#include "node.h"

namespace keyvadb {

template <uint32_t BITS, uint32_t DEGREE>
class Buffer {
  typedef Key<BITS> key_t;

 private:
  std::map<key_t, uint64_t> map_;
  std::mutex lock_;

 public:
  void Add(const key_t& key, const uint64_t value) {
    std::lock_guard<std::mutex> lock(lock_);
    map_.emplace(std::make_pair(key, value));
  }

  size_t Size() {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.size();
  }

  void Swap(Node<BITS, DEGREE>& node) {}
};

}  // namespace keyvadb