#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include <atomic>
#include "key.h"
#include "node.h"

namespace keyvadb {
template <std::uint32_t BITS>
class KeyStore {
 public:
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using key_type = Key<BITS>;

  virtual ~KeyStore() = default;

  virtual node_ptr New(key_type const& start, const key_type& end) = 0;
  virtual node_ptr Get(std::uint64_t const id) = 0;
  virtual void Set(node_ptr const& node) = 0;
  virtual std::size_t Size() const = 0;
};

template <std::uint32_t BITS>
class MemoryKeyStore : public KeyStore<BITS> {
 public:
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using key_type = Key<BITS>;

 private:
  std::uint32_t degree_;
  std::atomic_uint_fast64_t id_;
  std::unordered_map<std::uint64_t, node_ptr> map_;

 public:
  MemoryKeyStore(std::uint32_t const degree) : degree_(degree) {}

  node_ptr New(const key_type& first, const key_type& last) override {
    return std::make_shared<node_type>(id_++, degree_, first, last);
  }
  node_ptr Get(std::uint64_t const id) override { return map_[id]; }
  void Set(node_ptr const& node) override { map_[node->Id()] = node; }
  std::size_t Size() const override { return id_; }
};

template <std::uint32_t BITS>
std::unique_ptr<KeyStore<BITS>> MakeMemoryKeyStore(std::uint32_t const degree) {
  return std::make_unique<MemoryKeyStore<BITS>>(degree);
}

// To be implemented
template <std::uint32_t BITS>
std::unique_ptr<KeyStore<BITS>> MakeFileKeyStore(
    std::size_t const bytesPerBlock, std::size_t const cacheBytes) {}

}  // namespace keyvadb