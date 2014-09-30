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
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;

 private:
  std::map<key_type, uint64_t> map_;
  mutable std::mutex lock_;

 public:
  class Snapshot2 {
    using snapshot_func = std::function<void(key_value_type const)>;

   private:
    std::set<key_value_type> set_;

   public:
    void Add(const key_type& key, const uint64_t value) {
      set_.emplace(key_value_type{key, value});
    }

    // Returns true if there are values greater than first and less than
    // last
    bool ContainsRange(const key_type& first, const key_type& last) {
      auto lower = set_.upper_bound(key_value_type{first, 0});
      auto upper = set_.lower_bound(key_value_type{last, 0});
      return std::distance(lower, upper) > 0;
    }

    void EachRange(const key_type& first, const key_type& last,
                   snapshot_func f) const {
      auto lower = set_.upper_bound(key_value_type{first, 0});
      auto upper = set_.lower_bound(key_value_type{last, 0});
      for (; lower != upper; ++lower) f(*lower);
    }

    friend std::ostream& operator<<(std::ostream& stream,
                                    const Snapshot2& snapshot) {
      stream << "Snaphot" << std::endl;
      for (auto const& kv : snapshot.set_) stream << kv << std::endl;
      stream << "--------" << std::endl;
      return stream;
    }
  };

  std::size_t Add(const key_type& key, const uint64_t value) {
    std::lock_guard<std::mutex> lock(lock_);
    map_.emplace(key, value);
    return map_.size();
  }

  bool Remove(key_value_type const& key) {
    std::lock_guard<std::mutex> lock(lock_);
    return map_.erase(key.key) > 0;
  }

  Buffer::Snapshot2 GetSnapshot() const {
    Buffer<BITS>::Snapshot2 snapshot;
    std::lock_guard<std::mutex> lock(lock_);
    for (auto const& kv : map_) snapshot.Add(kv.first, kv.second);
    return snapshot;
  }

  std::vector<key_value_type> Snapshot() const {
    std::vector<key_value_type> snapshot;
    std::lock_guard<std::mutex> lock(lock_);
    for (auto const& kv : map_)
      snapshot.emplace_back(key_value_type{kv.first, kv.second});
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