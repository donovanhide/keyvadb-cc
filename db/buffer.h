#pragma once

#include <cstdint>
#include <cstddef>
#include <map>
#include <vector>
#include <mutex>
#include "key.h"
#include "node.h"
#include "snapshot.h"

namespace keyvadb {

// Buffer is thread-safe
template <uint32_t BITS>
class Buffer {
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;

 private:
  std::map<key_type, uint64_t> map_;
  mutable std::mutex lock_;

 public:
  std::size_t Add(const key_type& key, const uint64_t value) {
    std::lock_guard<std::mutex> lock(lock_);
    map_.emplace(key, value);
    return map_.size();
  }

  bool Remove(key_value_type const& key) {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.erase(key.key) > 0;
  }

  std::unique_ptr<Snapshot<BITS>> GetSnapshot() const {
    auto snapshot = std::make_unique<Snapshot<BITS>>();
    std::lock_guard<std::mutex> lock(lock_);
    for (auto const& kv : map_) snapshot->Add(kv.first, kv.second);
    return snapshot;
  }

  std::size_t Size() const {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.size();
  }

  void FillRandom(std::size_t n, std::uint32_t seed) {
    auto keys = RandomKeys<BITS>(n, seed);
    for (std::size_t i = 0; i < n; i++) Add(keys[i], i);
  }
};

template <uint32_t BITS>
std::shared_ptr<Buffer<BITS>> MakeBuffer() {
  return std::make_shared<Buffer<BITS>>();
}

}  // namespace keyvadb