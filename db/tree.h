#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include "db/node.h"
#include "db/store.h"
#include "db/delta.h"

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
  explicit Tree(store_ptr const& store) : store_(store) {
    if (!store_->Has(rootId)) {
      auto root = store_->New(firstRootKey(), lastRootKey());
      // root->AddSyntheticKeyValues();
      store_->Set(root);
    }
  }

  void Walk(node_func f) const { walk(rootId, 0, f); }

  // Retuns a Journal of the changed nodes sorted by depth
  // The nodes are copies of the ones in the store (ie. copy on write)
  journal_ptr Add(buffer_ptr const& buffer) const {
    auto journal = MakeJournal<BITS>();
    auto root = store_->Get(rootId);
    auto snapshot = buffer->GetSnapshot();
    add(root, 0, snapshot, journal);
    return journal;
  }

  std::uint64_t Get(key_type const& key) const { return get(rootId, key); }

  std::uint64_t get(std::uint64_t const id, key_type const& key) const {
    auto node = store_->Get(id);
    auto value = node->Find(key);
    if (value != EmptyValue) return value;
    node->EachChild([&](const std::size_t, const key_type& first,
                        const key_type& last, const std::uint64_t cid) {
      if (first > key) return;
      if (key > first && key < last) value = get(cid, key);
    });
    return value;
  }

  bool IsSane() const {
    bool sane = true;
    Walk([&sane](node_ptr n, std::uint32_t) { sane &= n->IsSane(); });
    return sane;
  }

  std::size_t NonSyntheticKeyCount() const {
    std::size_t count = 0;
    Walk([&count](node_ptr n,
                  std::uint32_t) { count += n->NonSyntheticKeyCount(); });
    return count;
  }

  friend std::ostream& operator<<(std::ostream& stream, const Tree& tree) {
    tree.Walk([&stream](node_ptr n, std::uint32_t level) {
      stream << "Level:\t\t" << level << std::endl << *n;
    });
    return stream;
  }

 private:
  static constexpr key_type firstRootKey() { return Min<BITS>() + 1; }
  static constexpr key_type lastRootKey() { return Max<BITS>(); }

  void add(node_ptr const& node, std::uint32_t const level,
           snapshot_ptr const& snapshot, journal_ptr const& journal) const {
    delta_type delta(node);
    delta.AddKeys(snapshot);
    delta.CheckSanity();
    if (delta.Current()->EmptyKeyCount() == 0) {
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
          add(child, level + 1, snapshot, journal);
        }
      });
    }
    delta.CheckSanity();
    if (delta.Dirty()) journal->Add(level, delta);
  }

  void walk(std::uint64_t const id, std::uint32_t const level,
            node_func f) const {
    auto node = store_->Get(id);
    f(node, level);
    node->EachChild([&](const std::size_t, const key_type&, const key_type&,
                        const std::uint64_t cid) {
      if (cid != EmptyChild) walk(cid, level + 1, f);
    });
  }
};

}  // namespace keyvadb
