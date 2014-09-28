#pragma once

#include <cstdint>
#include <cstddef>
#include <array>
#include <algorithm>
#include <limits>
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
// 4. no children must exist unless all keys are populated
template <std::uint32_t BITS, std::uint32_t DEGREE>
class Node {
 public:
  using key_type = Key<BITS>;

 private:
  std::uint64_t id_;
  key_type first_;
  key_type last_;
  std::array<KeyValue<BITS>, DEGREE - 1> keys_;
  std::array<std::uint64_t, DEGREE> children_;

 public:
  Node(std::uint64_t id, key_type const& first, key_type const& last)
      : id_(id), first_(first), last_(last) {
    if (first >= last) throw std::domain_error("first must be lower than last");
    static auto zero = KeyValue<BITS>{EmptyKey, EmptyValue};
    std::fill(keys_.begin(), keys_.end(), zero);
    std::fill(children_.begin(), children_.end(), EmptyChild);
  }

  void AddSyntheticKeyValues() {
    auto const stride = Stride();
    auto cursor = first_ + stride;
    for (auto& key : keys_) {
      key = KeyValue<BITS>{cursor, SyntheticValue};
      cursor += stride;
    }
  }

  constexpr KeyValue<BITS> GetKeyValue(std::size_t const i) const {
    return keys_[i];
  }

  bool IsSane() const {
    if (first_ >= last_) return false;
    if (!std::is_sorted(keys_.cbegin(), keys_.cend())) return false;
    for (std::size_t i = 1; i < DEGREE - 1; i++)
      if (!keys_[i].IsZero() && keys_[i] == keys_[i - 1]) return false;
    if (EmptyKeyCount() > 0 && EmptyChildCount() != ChildCount()) return false;
    return true;
  }

  constexpr std::uint64_t Id() const { return id_; }
  constexpr key_type First() const { return first_; }
  constexpr key_type Last() const { return last_; }

  constexpr std::size_t EmptyKeyCount() const {
    return std::count_if(keys_.cbegin(), keys_.cend(),
                         [](KeyValue<BITS> const& k) { return k.IsZero(); });
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