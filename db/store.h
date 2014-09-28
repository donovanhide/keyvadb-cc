#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include <atomic>
#include "key.h"
#include "node.h"

namespace keyvadb {
template <uint32_t BITS, uint32_t DEGREE>
class KeyStore {
 public:
  virtual std::shared_ptr<Node<BITS, DEGREE>> New(const Key<BITS>& start,
                                                  const Key<BITS>& end) = 0;
  virtual std::shared_ptr<Node<BITS, DEGREE>> Get(const uint64_t id) const = 0;
  // virtual void Set(const std::shared_ptr<Node<BITS, DEGREE>> node) = 0;
  virtual size_t Size() const = 0;
};

template <uint32_t BITS, uint32_t DEGREE>
class MemoryKeyStore : KeyStore<BITS, DEGREE> {
 private:
  std::atomic_uint_fast64_t id_;
  std::unordered_map<uint64_t, Node<BITS, DEGREE>> map_;

 public:
  std::shared_ptr<Node<BITS, DEGREE>> New(const Key<BITS>& first,
                                          const Key<BITS>& last) {
    return std::make_shared<Node<BITS, DEGREE>>(id_++, first, last);
  }
  std::shared_ptr<Node<BITS, DEGREE>> Get(const uint64_t id) const {
    auto node = map_.find(id);
    if (node != map_.end())
      return std::make_shared<Node<BITS, DEGREE>>(node->second);
    return std::shared_ptr<Node<BITS, DEGREE>>();
  }
  // void Set(const std::shared_ptr<Node<BITS, DEGREE>> node) {
  //   map_[node->Id()] = *node;
  // }
  std::size_t Size() const { return id_; }
};

}  // namespace keyvadb