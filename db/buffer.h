#pragma once

#include <cstdint>
#include <cstddef>
#include <map>
#include <vector>
#include <mutex>
#include "key.h"
#include "node.h"

namespace keyvadb {

// Buffer is thread-safe
template <uint32_t BITS>
class Buffer {
  typedef Key<BITS> key_type;

 private:
  std::map<key_type, uint64_t> map_;
  mutable std::mutex lock_;

 public:
  std::size_t Add(const key_type& key, const uint64_t value) {
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

  std::vector<KeyValue<BITS>> Snapshot() const {
    std::vector<KeyValue<BITS>> snapshot;
    std::lock_guard<std::mutex> lock(lock_);
    for (auto const& kv : map_)
      snapshot.emplace_back(KeyValue<BITS>{kv.first, kv.second});
    return snapshot;
  }

  std::size_t Size() const {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.size();
  }

  void FillRandom(std::size_t n) {
    auto keys = RandomKeys<BITS>(n);
    for (std::size_t i = 0; i < n; i++) Add(keys[i], i);
  }
};

template <uint32_t BITS>
std::shared_ptr<Buffer<BITS>> MakeBuffer() {
  return std::make_shared<Buffer<BITS>>();
}

}  // namespace keyvadb