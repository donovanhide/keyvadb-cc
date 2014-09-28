#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include "node.h"

namespace keyvadb {
template <uint32_t BITS>
class Tree {
 protected:
  uint64_t root_;
  // temporary memory store
  std::unordered_map<uint64_t, Node<BITS>> map_;

 public:
  std::size_t Balance(Buffer<BITS>& buffer) {
    auto snapshot = buffer.Snapshot();
    for (auto const& kv : snapshot) std::cout << kv << std::endl;
    // if (node.EmptyKeyCount() == 0) return 0;
    // std::set_union(node.keys_.cbegin(), node.keys_.cend(),
    //                map_.lower_bound(node.first_),
    //                map_.upper_bound(node.last_),
    //                std::back_inserter(v));
    // auto both = buffer.Union(node);
    return 0;
  }
};

}  // namespace keyvadb