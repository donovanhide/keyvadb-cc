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
  using util = detail::KeyUtil<BITS>;
  using key_type = typename util::key_type;
  using key_value_type = KeyValue<BITS>;
  using store_ptr = std::shared_ptr<KeyStore<BITS>>;
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using node_func =
      std::function<std::error_condition(node_ptr, std::uint32_t)>;
  using journal_ptr = std::unique_ptr<Journal<BITS>>;
  using snapshot_ptr = std::unique_ptr<Snapshot<BITS>>;
  using delta_type = Delta<BITS>;

 private:
  static const uint64_t rootId = 0;
  store_ptr store_;

 public:
  explicit Tree(store_ptr const& store) : store_(store) {}

  // Build root node if not already present
  std::error_condition Init(bool const addSynthetics) {
    node_ptr root;
    std::error_condition err;
    std::tie(root, err) = store_->Get(rootId);
    if (!err)
      return err;
    root = store_->New(firstRootKey(), lastRootKey());
    if (addSynthetics)
      root->AddSyntheticKeyValues();
    return store_->Set(root);
  }

  std::error_condition Walk(node_func f) const { return walk(rootId, 0, f); }

  // Retuns a Journal of the changed nodes sorted by depth
  // The nodes are copies of the ones in the store (ie. copy on write)
  std::pair<journal_ptr, std::error_condition> Add(
      snapshot_ptr const& snapshot) const {
    auto journal = MakeJournal<BITS>();
    node_ptr root;
    std::error_condition err;
    std::tie(root, err) = store_->Get(rootId);
    if (err)
      return std::make_pair(std::move(journal), err);
    err = add(root, 0, snapshot, journal);
    return std::make_pair(std::move(journal), err);
  }

  std::pair<std::uint64_t, std::error_condition> Get(
      key_type const& key) const {
    return get(rootId, key);
  }

  std::pair<bool, std::error_condition> IsSane() const {
    bool sane = true;
    auto err = Walk([&sane](node_ptr n, std::uint32_t) {
      sane &= n->IsSane();
      return std::error_condition();
    });
    return std::make_pair(sane, err);
  }

  std::pair<std::size_t, std::error_condition> NonSyntheticKeyCount() const {
    std::size_t count = 0;
    auto err = Walk([&count](node_ptr n, std::uint32_t) {
      count += n->NonSyntheticKeyCount();
      return std::error_condition();
    });
    return std::make_pair(count, err);
  }

  friend std::ostream& operator<<(std::ostream& stream, const Tree& tree) {
    auto err = tree.Walk([&stream](node_ptr n, std::uint32_t level) {
      stream << "Level:\t\t" << level << std::endl << *n;
      return std::error_condition();
    });
    if (err)
      stream << err.message() << std::endl;
    return stream;
  }

 private:
  static constexpr key_type firstRootKey() { return util::Min() + 1; }
  static constexpr key_type lastRootKey() { return util::Max(); }

  std::error_condition add(node_ptr const& node, std::uint32_t const level,
                           snapshot_ptr const& snapshot,
                           journal_ptr const& journal) const {
    delta_type delta(node);
    delta.AddKeys(snapshot);
    delta.CheckSanity();
    if (delta.Current()->EmptyKeyCount() == 0) {
      auto err = delta.Current()->EachChild(
          [&](const std::size_t i, const key_type& first, const key_type& last,
              const std::uint64_t cid) {
            if (!snapshot->ContainsRange(first, last))
              return std::error_condition();
            if (cid == EmptyChild) {
              auto child = store_->New(first, last);
              delta.SetChild(i, child->Id());
              return add(child, level + 1, snapshot, journal);
            } else {
              node_ptr child;
              std::error_condition err;
              std::tie(child, err) = store_->Get(cid);
              if (err)
                return err;
              return add(child, level + 1, snapshot, journal);
            }
          });
      if (err)
        return err;
    }
    delta.CheckSanity();
    if (delta.Dirty())
      journal->Add(level, delta);
    return std::error_condition();
  }

  std::pair<std::uint64_t, std::error_condition> get(
      std::uint64_t const id, key_type const& key) const {
    node_ptr node;
    std::error_condition err;
    std::tie(node, err) = store_->Get(id);
    if (err)
      return std::make_pair(EmptyValue, err);
    auto value = node->Find(key);
    if (value != EmptyValue)
      return std::make_pair(value, err);
    // TODO(DH) Check shadowing...
    err = node->EachChild([&](const std::size_t, const key_type& first,
                              const key_type& last, const std::uint64_t cid) {
      if (key > first && key < last) {
        std::tie(value, err) = get(cid, key);
        if (err)
          return err;
      }
      return std::error_condition();
    });
    return std::make_pair(value, err);
  }

  std::error_condition walk(std::uint64_t const id, std::uint32_t const level,
                            node_func f) const {
    node_ptr node;
    std::error_condition err;
    std::tie(node, err) = store_->Get(id);
    if (err)
      return err;
    if (auto err = f(node, level))
      return err;
    return node->EachChild([&](const std::size_t, const key_type&,
                               const key_type&, const std::uint64_t cid) {
      if (cid != EmptyChild)
        if (auto err = walk(cid, level + 1, f))
          return err;
      return std::error_condition();
    });
  }
};

}  // namespace keyvadb
