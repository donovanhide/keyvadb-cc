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
  using key_type = Key<BITS>;
  using store_ptr = std::unique_ptr<KeyStore<BITS>>&;
  using buffer_ptr = std::shared_ptr<Buffer<BITS>>;
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using node_func = std::function<void(node_ptr, std::uint32_t)>;

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

  void Walk(node_func f) const { walk(root_, 0, f); }

  bool IsSane() const {
    bool sane = true;
    Walk([&](node_ptr n, std::uint32_t level) { sane &= n->IsSane(); });
    return sane;
  }

  std::size_t Add(buffer_ptr& buffer) {
    auto snapshot = buffer->Snapshot();
    // for (auto const& kv : snapshot) std::cout << kv << std::endl;
    // if (node.EmptyKeyCount() == 0) return 0;
    // std::set_union(node.keys_.cbegin(), node.keys_.cend(),
    //                map_.lower_bound(node.first_),
    //                map_.upper_bound(node.last_),
    //                std::back_inserter(v));
    // auto both = buffer.Union(node);
    return 0;
  }

  friend std::ostream& operator<<(std::ostream& stream, const Tree& tree) {
    tree.Walk([&](node_ptr n, std::uint32_t level) {
      stream << "Level:\t\t" << level << std::endl << *n;
    });
    return stream;
  }

 private:
  void walk(std::uint64_t const id, std::uint32_t const level,
            node_func f) const {
    auto node = store_->Get(id);
    if (node) {
      f(node, level);
    }
    node->EachChild([&](const key_type& first, const key_type& last,
                        const std::uint64_t cid) {
      if (cid != EmptyChild) walk(cid, level + 1, f);
    });
  }
};

}  // namespace keyvadb