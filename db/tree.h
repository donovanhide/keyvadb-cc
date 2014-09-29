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
  using result_type = std::multimap<std::uint32_t, node_ptr>;
  using result_ptr = std::unique_ptr<result_type>;

 private:
  uint64_t root_;
  store_ptr store_;

 public:
  Tree(store_ptr const& store) : root_(0), store_(store) {
    auto first = Min<BITS>() + 1;
    auto last = Max<BITS>();
    auto root = store_->New(first, last);
    // root->AddSyntheticKeyValues();
    store_->Set(root);
  }

  void Walk(node_func f) const { walk(root_, 0, f); }

  // Retuns a map of the changed nodes sorted by depth
  // The nodes are copies of the ones in the store (ie. copy on write)
  result_ptr Add(buffer_ptr& buffer) const {
    auto results = std::make_unique<result_type>();
    add(0, 0, buffer->Snapshot(), results);
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
  void add(std::uint64_t const id, std::uint32_t level,
           std::vector<KeyValue<BITS>> values, result_ptr& results) const {
    auto node = store_->Get(id);
    if (!node) {
      throw std::domain_error("Trying to add to non-existent node");
    }
    if (node->EmptyKeyCount() != 0) {
      std::cout << "Before" << std::endl << *node << std::endl;
      results->emplace(level, node->Add(values));
      std::cout << "After" << std::endl << *std::prev(results->end(), 1)->second
                << std::endl;
    }
    node->EachChild([&](const key_type& first, const key_type& last,
                        const std::uint64_t cid) {

    });
  }

  void walk(std::uint64_t const id, std::uint32_t const level,
            node_func f) const {
    auto node = store_->Get(id);
    if (!node) {
      throw std::domain_error("Trying to walk non-existent node");
    }
    f(node, level);
    node->EachChild([&](const key_type& first, const key_type& last,
                        const std::uint64_t cid) {
      if (cid != EmptyChild) walk(cid, level + 1, f);
    });
  }
};

}  // namespace keyvadb