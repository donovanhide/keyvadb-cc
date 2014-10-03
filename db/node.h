#pragma once

#include <cstdint>
#include <cstddef>
#include <array>
#include <algorithm>
#include <limits>
#include <mutex>
#include "key.h"
#include "snapshot.h"
#include "journal.h"

namespace keyvadb {

// Node invariants:
// 1. keys must always be in sorted order,lowest to highest
// 2. each key is unique, not including zero
// 3. first_ must be lower than last_
// 4. each non-zero key must be greater than first_ and less than last_
// 5. no children must exist unless all keys are populated
template <std::uint32_t BITS>
class Node {
 public:
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;
  using key_values_type = std::vector<key_value_type>;
  using children_type = std::vector<std::uint64_t>;
  using child_func = std::function<void(const std::size_t, const key_type&,
                                        const key_type&, const std::uint64_t)>;
  using snapshot_ptr = std::unique_ptr<Snapshot<BITS>>;
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using iterator = typename std::vector<KeyValue<BITS>>::iterator;
  using const_iterator = typename std::vector<KeyValue<BITS>>::const_iterator;

 private:
  std::uint64_t id_;
  std::uint32_t degree_;
  key_type first_;
  key_type last_;
  key_values_type keys_;
  children_type children_;

 public:
  Node(std::uint64_t const id, std::uint32_t degree, key_type const& first,
       key_type const& last)
      : id_(id),
        degree_(degree),
        first_(first),
        last_(last),
        keys_(degree - 1),
        children_(degree) {
    if (first >= last)
      throw std::domain_error("first must be lower than last:" + ToHex(first) +
                              " " + ToHex(last));
  }

  constexpr std::size_t AddSyntheticKeyValues() {
    auto const stride = Stride();
    auto cursor = first_ + stride;
    std::size_t count = 0;
    for (auto& key : keys_) {
      if (key.IsZero()) {
        key = key_value_type{cursor, SyntheticValue};
        count++;
      }
      cursor += stride;
    }
    return count;
  }

  constexpr void Clear() {
    for (auto& key : keys_) key = key_value_type{0, EmptyValue};
  }

  constexpr void SetChild(std::size_t const i, std::uint64_t const cid) {
    children_.at(i) = cid;
  }
  constexpr std::uint64_t GetChild(std::size_t const i) const {
    return children_.at(i);
  }

  constexpr void EachChild(child_func f) const {
    auto length = Degree();
    for (std::size_t i = 0; i < length; i++) {
      if (i == 0) {
        if (!keys_.at(i).IsZero())
          f(i, first_, keys_.at(i).key, children_.at(i));
      } else if (i == length - 1) {
        if (!keys_.at(i - 1).IsZero())
          f(i, keys_.at(i - 1).key, last_, children_.at(i));
      } else {
        if (!keys_.at(i - 1).IsZero() && !keys_.at(i).IsZero())
          f(i, keys_.at(i - 1).key, keys_.at(i).key, children_.at(i));
      }
    }
  }

  constexpr key_value_type GetKeyValue(std::size_t const i) const {
    return keys_.at(i);
  }
  constexpr void SetKeyValue(std::size_t const i, key_value_type const& kv) {
    keys_.at(i) = kv;
  }

  constexpr bool IsSane() const {
    if (first_ >= last_) return false;
    if (!std::is_sorted(keys_.cbegin(), keys_.cend())) return false;
    for (std::size_t i = 1; i < Degree() - 1; i++) {
      if (!keys_.at(i).IsZero() && keys_.at(i) == keys_.at(i - 1)) return false;
      if (!keys_.at(i).IsZero() &&
          (keys_.at(i).key <= first_ || keys_.at(i).key >= last_))
        return false;
    }
    if (EmptyKeyCount() > 0 && EmptyChildCount() != Degree()) return false;
    return true;
  }

  constexpr std::uint64_t Id() const { return id_; }
  constexpr key_type First() const { return first_; }
  constexpr key_type Last() const { return last_; }

  // Iterators
  constexpr iterator begin() { return std::begin(keys_); }
  constexpr iterator end() { return std::end(keys_); }
  constexpr const_iterator end() const { return std::cend(keys_); }
  constexpr const_iterator cbegin() const { return std::cbegin(keys_); }
  constexpr const_iterator cend() const { return std::cend(keys_); }
  constexpr const_iterator NonZeroBegin() const {
    return std::find_if_not(
        std::cbegin(keys_), std::cend(keys_),
        [](key_value_type const& kv) { return kv.IsZero(); });
  }
  constexpr bool Empty() const { return EmptyKeyCount() == MaxKeys(); }

  constexpr std::size_t NonSyntheticKeyCount() const {
    return std::count_if(std::cbegin(keys_), std::cend(keys_),
                         [](key_value_type const& kv) {
      return !kv.IsZero() && !kv.IsSynthetic();
    });
  }
  constexpr std::size_t NonEmptyKeyCount() const {
    return std::distance(NonZeroBegin(), std::cend(keys_));
  }
  constexpr std::size_t EmptyKeyCount() const {
    return std::distance(std::cbegin(keys_), NonZeroBegin());
  }
  constexpr std::size_t EmptyChildCount() const {
    return std::count(std::cbegin(children_), std::cend(children_), EmptyChild);
  }
  constexpr std::size_t MaxKeys() const { return keys_.size(); }
  constexpr std::size_t Degree() const { return children_.size(); }
  constexpr key_type Distance() const {
    return keyvadb::Distance(first_, last_);
  }
  constexpr key_type Stride() const {
    return keyvadb::Stride(first_, last_, Degree());
  }

  friend std::ostream& operator<<(std::ostream& stream, const Node& node) {
    stream << "Id:\t\t" << node.id_ << std::endl;
    stream << "Keys:\t\t" << node.MaxKeys() - node.EmptyKeyCount() << std::endl;
    stream << "Children:\t" << node.Degree() - node.EmptyChildCount()
           << std::endl;
    stream << "First:\t\t" << ToHex(node.first_) << std::endl;
    stream << "Last:\t\t" << ToHex(node.last_) << std::endl;
    stream << "Stride:\t\t" << ToHex(node.Stride()) << std::endl;
    stream << "Distance:\t" << ToHex(node.Distance()) << std::endl;
    stream << "--------" << std::endl;
    for (std::size_t i = 0; i < node.MaxKeys(); i++) {
      stream << std::setfill('0') << std::setw(3) << i << " ";
      stream << ToHex(node.keys_.at(i).key) << " ";
      if (node.keys_.at(i).value == SyntheticValue) {
        stream << "Synthetic"
               << " ";
      } else {
        stream << node.keys_.at(i).value << " ";
      }
      stream << node.children_.at(i) << " " << node.children_.at(i + 1);
      stream << std::endl;
    }
    stream << "--------" << std::endl;
    return stream;
  }
};

}  // namespace keyvadb