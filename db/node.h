#pragma once

#include <cstdint>
#include <cstddef>
#include <array>
#include <algorithm>
#include <limits>
#include <mutex>
#include "key.h"

namespace keyvadb {

static const std::uint64_t EmptyKey = 0;

static const std::uint64_t SyntheticValue =
    std::numeric_limits<std::uint64_t>::max();
static const std::uint64_t EmptyValue = 0;

static const std::uint64_t EmptyChild = 0;

template <std::uint32_t BITS>
struct KeyValue {
  Key<BITS> key;        // Hash of actual value
  std::uint64_t value;  // offset of actual value in values file

  constexpr bool IsZero() const { return key.is_zero(); }
  constexpr bool operator<(KeyValue<BITS> const& rhs) const {
    return key < rhs.key;
  }
  constexpr bool operator==(KeyValue<BITS> const& rhs) const {
    return key == rhs.key;
  }
  constexpr bool operator!=(KeyValue<BITS> const& rhs) const {
    return key != rhs.key;
  }
  friend std::ostream& operator<<(std::ostream& stream,
                                  KeyValue<BITS> const& kv) {
    stream << "Key: " << ToHex(kv.key) << " Value: " << kv.value;
    return stream;
  }
};

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
  using child_func = std::function<void(const std::size_t, const key_type&,
                                        const key_type&, const std::uint64_t)>;

 private:
  std::uint64_t id_;
  std::uint32_t degree_;
  key_type first_;
  key_type last_;
  std::vector<key_value_type> keys_;
  std::vector<std::uint64_t> children_;
  bool dirty_;

 public:
  Node(std::uint64_t const id, std::uint32_t degree, key_type const& first,
       key_type const& last)
      : id_(id),
        degree_(degree),
        first_(first),
        last_(last),
        keys_(degree - 1),
        children_(degree),
        dirty_(false) {
    if (first >= last) throw std::domain_error("first must be lower than last");
  }

  void AddSyntheticKeyValues() {
    auto const stride = Stride();
    auto cursor = first_ + stride;
    for (auto& key : keys_) {
      key = key_value_type{cursor, SyntheticValue};
      cursor += stride;
    }
  }

  void Add(std::vector<KeyValue<BITS>> values) {
    auto empty = EmptyKeyCount();
    auto length = KeyCount();
    auto first = std::upper_bound(std::cbegin(values), std::cend(values),
                                  key_value_type{first_, 0});
    auto last =
        std::lower_bound(first, std::cend(values), key_value_type{last_, 0});
    first++;
    last--;
    auto count = std::distance(first, last);

    if (empty == length && count <= empty) {
      // Node empty and won't overflow so fill from right
      std::copy_backward(first, last, std::end(keys_));
      return;
    }
    if (empty + count <= length) {
      // Node won't overflow so add left and sort
      std::cout << ToHex(first->key) << " " << ToHex(last->key) << std::endl;
      std::copy(first, last, std::begin(keys_));
      std::sort(std::begin(keys_), std::end(keys_));
      return;
    }
    // Node will overflow so select best keys from union
    // while overwriting synthetic key values and
    // add remainder to values
    std::vector<KeyValue<BITS>> both;
    auto firstNonZero =
        std::find_if_not(std::cbegin(keys_), std::cend(keys_),
                         [](key_value_type const& kv) { return kv.IsZero(); });
    std::set_union(firstNonZero, std::cend(keys_), first, last,
                   std::back_inserter(both));
    AddSyntheticKeyValues();
    auto stride = Stride();
    std::uint32_t index = 0;
    auto bestDistance = Max<BITS>();

    for (auto const& v : both) {
      std::uint32_t nearest;
      key_type distance;
      NearestStride(first_, stride, v.key, ChildCount(), distance, nearest);

      if ((nearest == index && distance < bestDistance) || (nearest != index)) {
        // std::cout << ToHex(v.key) << " " << ToHex(distance) << " " << index
        //           << " " << nearest << " " << length << std::endl;
        keys_.at(nearest) = v;
        bestDistance = distance;
      }
      index = nearest;
    }
  }

  void SetChild(std::size_t i, std::uint64_t cid) { children_[i] = cid; }

  void EachChild(child_func f) const {
    auto length = ChildCount();
    for (std::size_t i = 0; i < length; i++) {
      if (i == 0) {
        if (!keys_[i].IsZero()) f(i, first_, keys_[i].key, children_[i]);
      } else if (i == length - 1) {
        if (!keys_[i].IsZero()) f(i, keys_[i - 1].key, last_, children_[i]);
      } else {
        if (!keys_[i - 1].IsZero() && !keys_[i].IsZero())
          f(i, keys_[i - 1].key, keys_[i].key, children_[i]);
      }
    }
  }

  constexpr key_value_type GetKeyValue(std::size_t const i) const {
    return keys_.at(i);
  }

  bool IsSane() const {
    if (first_ >= last_) return false;
    if (!std::is_sorted(keys_.cbegin(), keys_.cend())) return false;
    for (std::size_t i = 1; i < ChildCount() - 1; i++) {
      if (!keys_[i].IsZero() && keys_[i] == keys_[i - 1]) return false;
      if (!keys_[i].IsZero() &&
          (keys_[i].key <= first_ || keys_[i].key >= last_))
        return false;
    }
    if (EmptyKeyCount() > 0 && EmptyChildCount() != ChildCount()) return false;
    return true;
  }

  constexpr std::uint64_t Id() const { return id_; }
  constexpr key_type First() const { return first_; }
  constexpr key_type Last() const { return last_; }

  constexpr std::size_t EmptyKeyCount() const {
    return std::count_if(keys_.cbegin(), keys_.cend(),
                         [](key_value_type const& k) { return k.IsZero(); });
  }
  constexpr std::size_t EmptyChildCount() const {
    return std::count(children_.cbegin(), children_.cend(), EmptyChild);
  }
  constexpr std::size_t KeyCount() const { return keys_.size(); }
  constexpr std::size_t ChildCount() const { return children_.size(); }
  constexpr key_type Distance() const {
    return keyvadb::Distance(first_, last_);
  }
  constexpr key_type Stride() const {
    return keyvadb::Stride(first_, last_, ChildCount());
  }

  friend std::ostream& operator<<(std::ostream& stream, const Node& node) {
    stream << "Id:\t\t" << node.id_ << std::endl;
    stream << "Keys:\t\t" << node.KeyCount() - node.EmptyKeyCount()
           << std::endl;
    stream << "Children:\t" << node.ChildCount() - node.EmptyChildCount()
           << std::endl;
    stream << "First:\t\t" << ToHex(node.first_) << std::endl;
    stream << "Last:\t\t" << ToHex(node.last_) << std::endl;
    stream << "Stride:\t\t" << ToHex(node.Stride()) << std::endl;
    stream << "Distance:\t" << ToHex(node.Distance()) << std::endl;
    stream << "--------" << std::endl;
    for (std::size_t i = 0; i < node.KeyCount(); i++) {
      stream << std::setfill('0') << std::setw(3) << i << " ";
      stream << ToHex(node.keys_[i].key) << " ";
      if (node.keys_[i].value == SyntheticValue) {
        stream << "Synthetic"
               << " ";
      } else {
        stream << node.keys_[i].value << " ";
      }
      stream << node.children_[i] << " " << node.children_[i + 1];
      stream << std::endl;
    }
    stream << "--------" << std::endl;
    return stream;
  }
};

}  // namespace keyvadb