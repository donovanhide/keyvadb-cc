#pragma once

#include <cstdint>
#include <cstddef>
#include <map>
#include <vector>
#include <mutex>
#include "db/key.h"
#include "db/node.h"
#include "db/snapshot.h"

namespace keyvadb {

// Buffer is thread-safe
template <uint32_t BITS>
class Buffer {
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;
  using snapshot_type = std::unique_ptr<Snapshot<BITS>>;

 private:
  std::map<key_type, uint64_t> map_;
  mutable std::mutex lock_;

 public:
  std::size_t Add(key_type const& key, uint64_t const value) {
    std::lock_guard<std::mutex> lock(lock_);
    map_.emplace(key, value);
    return map_.size();
  }

  std::uint64_t Get(key_type const& key) const {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.at(key);
  }

  bool Remove(key_value_type const& key) {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.erase(key.key) > 0;
  }

  snapshot_type GetSnapshot() const {
    auto snapshot = std::make_unique<Snapshot<BITS>>();
    std::lock_guard<std::mutex> lock(lock_);
    for (auto const& kv : map_) snapshot->Add(kv.first, kv.second);
    return snapshot;
  }

  void ClearSnapshot(snapshot_type const& snapshot) {
    std::lock_guard<std::mutex> lock(lock_);
    for (auto const& kv : *snapshot) map_.erase(kv.key);
  }

  std::size_t Size() const {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.size();
  }

  void FillRandom(std::size_t n, std::uint32_t seed) {
    auto keys = RandomKeys<BITS>(n, seed);
    for (std::size_t i = 0; i < n; i++) Add(keys[i], i + 1);
  }
};

template <uint32_t BITS>
std::shared_ptr<Buffer<BITS>> MakeBuffer() {
  return std::make_shared<Buffer<BITS>>();
}

}  // namespace keyvadb
