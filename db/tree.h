#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include "node.h"
#include "store.h"

namespace keyvadb {
template <uint32_t BITS>
class Tree {
 public:
  using store_ptr = std::unique_ptr<KeyStore<BITS>>&;
  using node_type = Node<BITS>;

 private:
  uint64_t root_;
  store_ptr store_;

 public:
  Tree(store_ptr const& store) : root_(0), store_(store) {
    auto first = Min<BITS>() + 1;
    auto last = Max<BITS>();
    auto root = store_->New(first, last);
    root->AddSyntheticKeyValues();
    store_->Set(root);
  }

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