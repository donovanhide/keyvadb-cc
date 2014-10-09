#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include <system_error>
#include <utility>
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
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using node_func = std::function<std::error_code(node_ptr, std::uint32_t)>;
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

  std::error_code Walk(node_func f) const { return walk(rootId, 0, f); }

  // Retuns a Journal of the changed nodes sorted by depth
  // The nodes are copies of the ones in the store (ie. copy on write)
  std::pair<journal_ptr, std::error_code> Add(
      snapshot_ptr const& snapshot) const {
    auto journal = MakeJournal<BITS>();
    node_ptr root;
    std::error_code err;
    std::tie(root, err) = store_->Get(rootId);
    if (err) return std::make_pair(std::move(journal), err);
    err = add(root, 0, snapshot, journal);
    return std::make_pair(std::move(journal), err);
  }

  std::pair<std::uint64_t, std::error_code> Get(key_type const& key) const {
    return get(rootId, key);
  }

  std::pair<std::uint64_t, std::error_code> get(std::uint64_t const id,
                                                key_type const& key) const {
    node_ptr node;
    std::error_code err;
    std::tie(node, err) = store_->Get(id);
    if (err) return std::make_pair(EmptyValue, err);
    auto value = node->Find(key);
    if (value != EmptyValue) return std::make_pair(value, err);
    // TODO(DH) Check shadowing...
    err = node->EachChild([&](const std::size_t, const key_type& first,
                              const key_type& last, const std::uint64_t cid) {
      if (key > first && key < last) {
        std::tie(value, err) = get(cid, key);
        if (err) return err;
      }
      return std::error_code();
    });
    return std::make_pair(value, err);
  }

  std::pair<bool, std::error_code> IsSane() const {
    bool sane = true;
    auto err = Walk([&sane](node_ptr n, std::uint32_t) {
      sane &= n->IsSane();
      return std::error_code();
    });
    return std::make_pair(sane, err);
  }

  std::pair<std::size_t, std::error_code> NonSyntheticKeyCount() const {
    std::size_t count = 0;
    auto err = Walk([&count](node_ptr n, std::uint32_t) {
      count += n->NonSyntheticKeyCount();
      return std::error_code();
    });
    return std::make_pair(count, err);
  }

  friend std::ostream& operator<<(std::ostream& stream, const Tree& tree) {
    auto err = tree.Walk([&stream](node_ptr n, std::uint32_t level) {
      stream << "Level:\t\t" << level << std::endl << *n;
    });
    stream << err << std::endl;
    return stream;
  }

 private:
  static constexpr key_type firstRootKey() { return Min<BITS>() + 1; }
  static constexpr key_type lastRootKey() { return Max<BITS>(); }

  std::error_code add(node_ptr const& node, std::uint32_t const level,
                      snapshot_ptr const& snapshot,
                      journal_ptr const& journal) const {
    delta_type delta(node);
    delta.AddKeys(snapshot);
    delta.CheckSanity();
    if (delta.Current()->EmptyKeyCount() == 0) {
      auto err = delta.Current()->EachChild(
          [&](const std::size_t i, const key_type& first, const key_type& last,
              const std::uint64_t cid) {
            if (!snapshot->ContainsRange(first, last)) return std::error_code();
            if (cid == EmptyChild) {
              auto child = store_->New(first, last);
              delta.SetChild(i, child->Id());
              return add(child, level + 1, snapshot, journal);
            } else {
              node_ptr child;
              std::error_code err;
              std::tie(child, err) = store_->Get(cid);
              if (err) return err;
              return add(child, level + 1, snapshot, journal);
            }
          });
      if (err) return err;
    }
    delta.CheckSanity();
    if (delta.Dirty()) journal->Add(level, delta);
    return std::error_code();
  }

  std::error_code walk(std::uint64_t const id, std::uint32_t const level,
                       node_func f) const {
    auto result = store_->Get(id);
    if (result.second) return result.second;
    if (auto err = f(result.first, level)) return err;
    return result.first->EachChild([&](const std::size_t, const key_type&,
                                       const key_type&,
                                       const std::uint64_t cid) {
      if (cid != EmptyChild)
        if (auto err = walk(cid, level + 1, f)) return err;
      return std::error_code();
    });
  }
};

}  // namespace keyvadb
