#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include "node.h"
#include "store.h"
#include "journal.h"

namespace keyvadb {

template <uint32_t BITS>
class Tree {
 public:
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;
  using store_ptr = std::shared_ptr<KeyStore<BITS>>;
  using buffer_ptr = std::shared_ptr<Buffer<BITS>>;
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using node_func = std::function<void(node_ptr, std::uint32_t)>;
  using journal_ptr = std::unique_ptr<Journal<BITS>>;
  using snapshot_ptr = std::unique_ptr<Snapshot<BITS>>;

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

  // Retuns a Journal of the changed nodes sorted by depth
  // The nodes are copies of the ones in the store (ie. copy on write)
  journal_ptr Add(buffer_ptr& buffer) const {
    auto journal = MakeJournal<BITS>();
    auto root = store_->Get(rootId);
    if (!root) {
      throw std::domain_error("Trying to get non-existent root");
    }
    auto snapshot = buffer->GetSnapshot();
    add(root, 0, snapshot, journal);
    return journal;
  }

  bool IsSane() const {
    bool sane = true;
    Walk([&](node_ptr n, std::uint32_t level) { sane &= n->IsSane(); });
    return sane;
  }

  std::size_t NonSyntheticKeyCount() const {
    std::size_t count = 0;
    Walk([&](node_ptr n,
             std::uint32_t level) { count += n->NonSyntheticKeyCount(); });
    return count;
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
  void add(node_ptr& node, std::uint32_t level, snapshot_ptr& snapshot,
           journal_ptr& journal) const {
    bool dirty = false;
    std::int64_t added = 0;
    if (node->EmptyKeyCount() != 0) {
      // Copy on write
      node = std::make_shared<Node<BITS>>(*node);
      dirty = true;
      // std::cout << "Before" << std::endl << "Level:\t\t" << level <<
      // std::endl
      // << *node << std::endl;
      added = node->Add(snapshot);
      // std::cout << "After" << std::endl << "Level:\t\t" << level << std::endl
      // << *node << std::endl;
    }
    if (!node->IsSane()) {
      std::cout << *node << std::endl;
      throw std::domain_error("Insane node");
    }
    if (node->EmptyKeyCount() == 0)
      node->EachChild([&](const std::size_t i, const key_type& first,
                          const key_type& last, const std::uint64_t cid) {
        if (!snapshot->ContainsRange(first, last)) return;

        if (cid == EmptyChild) {
          // Copy on write
          if (!dirty) {
            dirty = true;
            node = std::make_shared<Node<BITS>>(*node);
          }
          auto child = store_->New(first, last);
          node->SetChild(i, child->Id());
          add(child, level + 1, snapshot, journal);
        } else {
          auto child = store_->Get(cid);
          if (!child) {
            throw std::domain_error("Trying to get non-existent node");
          }
          add(child, level + 1, snapshot, journal);
        }
      });
    if (!node->IsSane()) {
      std::cout << *node << std::endl;
      throw std::domain_error("Insane node");
    }
    if (dirty) journal->Add(level, node, added);
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