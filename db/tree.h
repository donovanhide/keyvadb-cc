#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include "node.h"
#include "store.h"
#include "delta.h"

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
  using delta_type = Delta<BITS>;

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

  void add(node_ptr& node, std::uint32_t const level, snapshot_ptr& snapshot,
           journal_ptr& journal) const {
    delta_type delta(node);
    delta.AddKeys(snapshot);
    delta.CheckSanity();
    if (delta.Current()->EmptyKeyCount() == 0)
      delta.Current()->EachChild([&](const std::size_t i, const key_type& first,
                                     const key_type& last,
                                     const std::uint64_t cid) {
        if (!snapshot->ContainsRange(first, last)) return;
        if (cid == EmptyChild) {
          auto child = store_->New(first, last);
          delta.SetChild(i, child->Id());
          add(child, level + 1, snapshot, journal);
        } else {
          auto child = store_->Get(cid);
          if (!child) {
            throw std::domain_error("Trying to get non-existent node");
          }
          add(child, level + 1, snapshot, journal);
        }
      });
    delta.CheckSanity();
    if (delta.Dirty()) journal->Add(level, delta);
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