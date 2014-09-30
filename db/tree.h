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
  using key_value_type = KeyValue<BITS>;
  using store_ptr = std::unique_ptr<KeyStore<BITS>>&;
  using buffer_ptr = std::shared_ptr<Buffer<BITS>>;
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using node_func = std::function<void(node_ptr, std::uint32_t)>;
  using result_type = std::multimap<std::uint32_t, node_ptr>;
  using result_ptr = std::unique_ptr<result_type>;

 private:
  static const uint64_t rootId = 0;
  store_ptr store_;

 public:
  Tree(store_ptr const& store) : store_(store) {
    auto root = store_->Get(rootId);
    if (!root) {
      root = store_->New(firstRootKey(), lastRootKey());
      // root->AddSyntheticKeyValues();
      store_->Set(root);
    }
  }

  void Walk(node_func f) const { walk(rootId, 0, f); }

  // Retuns a map of the changed nodes sorted by depth
  // The nodes are copies of the ones in the store (ie. copy on write)
  result_ptr Add(buffer_ptr& buffer) const {
    auto results = std::make_unique<result_type>();
    auto root = store_->Get(rootId);
    if (!root) {
      throw std::domain_error("Trying to get non-existent root");
    }
    add(root, 0, buffer->Snapshot(), results);
    return results;
  }

  bool IsSane() const {
    bool sane = true;
    Walk([&](node_ptr n, std::uint32_t level) { sane &= n->IsSane(); });
    return sane;
  }

  friend std::ostream& operator<<(std::ostream& stream, const Tree& tree) {
    tree.Walk([&](node_ptr n, std::uint32_t level) {
      stream << "Level:\t\t" << level << std::endl << *n;
    });
    return stream;
  }

 private:
  static constexpr key_type firstRootKey() { return Min<BITS>() + 1; }
  static constexpr key_type lastRootKey() { return Max<BITS>(); }

  // This is the only method that mutates a Node
  // Copy on write is employed
  void add(node_ptr node, std::uint32_t level,
           std::vector<KeyValue<BITS>> values, result_ptr& results) const {
    bool dirty = false;
    if (node->EmptyKeyCount() != 0) {
      // Copy on write
      node = std::make_shared<Node<BITS>>(*node);
      std::cout << "Before" << std::endl << "Level:\t\t" << level << std::endl
                << *node << std::endl;
      node->Add(values);
      dirty = true;
      std::cout << "After" << std::endl << "Level:\t\t" << level << std::endl
                << *node << std::endl;
    }
    if (node->EmptyKeyCount() == 0)
      node->EachChild([&](const std::size_t i, const key_type& first,
                          const key_type& last, const std::uint64_t cid) {
        std::cout << ToHex(first) << " " << ToHex(last) << std::endl;
        auto lower = std::upper_bound(std::cbegin(values), std::cend(values),
                                      key_value_type{first, 0});
        auto upper =
            std::lower_bound(lower, std::cend(values), key_value_type{last, 0});
        if (std::distance(lower, upper) <= 1) return;
        std::cout << std::distance(lower, upper) << " " << ToHex(lower->key)
                  << " " << ToHex(upper->key) << std::endl;
        lower++;
        upper--;
        std::cout << std::distance(lower, upper) << " " << ToHex(lower->key)
                  << " " << ToHex(upper->key) << std::endl;
        if (cid == EmptyChild) {
          // Copy on write
          if (!dirty) {
            dirty = true;
            node = std::make_shared<Node<BITS>>(*node);
          }
          auto child = store_->New(first, last);
          node->SetChild(i, child->Id());
          add(child, level + 1, values, results);
        } else {
          auto child = store_->Get(cid);
          if (!child) {
            throw std::domain_error("Trying to get non-existent node");
          }
          add(child, level + 1, values, results);
        }
      });
    if (!node->IsSane()) {
      std::cout << *node << std::endl;
      throw std::domain_error("Insane node");
    }
    if (dirty) results->emplace(level, node);
  }

  void walk(std::uint64_t const id, std::uint32_t const level,
            node_func f) const {
    auto node = store_->Get(id);
    if (!node) {
      throw std::domain_error("Trying to walk non-existent node");
    }
    f(node, level);
    node->EachChild([&](const std::size_t i, const key_type& first,
                        const key_type& last, const std::uint64_t cid) {
      if (cid != EmptyChild) walk(cid, level + 1, f);
    });
  }
};

}  // namespace keyvadb