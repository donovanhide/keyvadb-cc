#pragma once

#include <stdint.h>
#include <array>
#include <algorithm>
#include <limits>
#include "key.h"

namespace keyvadb {

static const uint64_t SyntheticChild = std::numeric_limits<uint64_t>::max();
static const uint64_t EmptyChild = 0;

template <uint32_t BITS>
struct KeyValue {
  Key<BITS> key;   // Hash of actual value
  uint64_t value;  // offset of actual value in values file

  constexpr bool IsZero() { return key.is_zero(); }
  constexpr bool operator<(KeyValue<BITS> const& rhs) { return key < rhs.key; }
  constexpr bool operator==(KeyValue<BITS> const& rhs) {
    return key == rhs.key;
  }
};

// Node invariants:
// 1. keys must always be in sorted order,lowest to highest
// 2. each key is unique, not including zero
// 3. first_ must be lower than last_
// 4. No children must exist unless all keys are populated.
template <uint32_t BITS, uint32_t DEGREE>
class Node {
 private:
  uint64_t id_;
  Key<BITS> first_;
  Key<BITS> last_;
  std::array<KeyValue<BITS>, DEGREE - 1> keys_;
  std::array<uint64_t, DEGREE> children_;

 public:
  Node(uint64_t id, Key<BITS> const& first, Key<BITS> const& last)
      : id_(id), first_(first), last_(last) {
    if (first >= last) throw std::domain_error("first must be lower than last");
    // auto zero = KeyValue<BITS>();
    // std::fill(keys_.begin(), keys_.end(), zero);
    std::fill(children_.begin(), children_.end(), EmptyChild);
  }

  void AddSyntheticKeys() {
    auto stride = Stride(first_, last_, DEGREE);
    auto cursor = first_ + stride;
    for (auto& key : keys_) {
      key = KeyValue<BITS>{cursor, SyntheticChild};
      cursor += stride;
    }
  }

  constexpr bool IsSane() {
    if (first_ >= last_) return false;
    if (!std::is_sorted(keys_.cbegin(), keys_.cend())) return false;
    for (size_t i = 1; i < DEGREE - 1; i++)
      if (!keys_[i].IsZero() && keys_[i] == keys_[i - 1]) return false;
    if (EmptyKeyCount() > 0 && EmptyChildCount() != ChildCount()) return false;
    return true;
  }

  constexpr size_t EmptyKeyCount() {
    return std::count_if(keys_.cbegin(), keys_.cend(),
                         [](KeyValue<BITS> const& k) { return k.IsZero(); });
  }
  constexpr size_t EmptyChildCount() {
    return std::count(children_.cbegin(), children_.cend(), EmptyChild);
  }
  constexpr size_t KeyCount() { return keys_.size(); }
  constexpr size_t ChildCount() { return children_.size(); }

  friend std::ostream& operator<<(std::ostream& stream, const Node& node) {
    auto stride = ToHex(Stride(node.first_, node.last_, node.ChildCount()));
    auto distance = ToHex(Distance(node.first_, node.last_));
    stream << "Id:\t\t" << node.id_ << std::endl;
    stream << "Keys:\t\t" << node.KeyCount() - node.EmptyKeyCount()
           << std::endl;
    stream << "Children:\t" << node.ChildCount() - node.EmptyChildCount()
           << std::endl;
    stream << "First:\t\t" << ToHex(node.first_) << std::endl;
    stream << "Last:\t\t" << ToHex(node.last_) << std::endl;
    stream << "Stride:\t\t" << stride << std::endl;
    stream << "Distance:\t" << distance << std::endl;
    stream << "--------" << std::endl;
    for (size_t i = 0; i < node.KeyCount(); i++) {
      stream << std::setfill('0') << std::setw(3) << i << " ";
      stream << ToHex(node.keys_[i].key) << " ";
      stream << node.keys_[i].value << " " << node.children_[i];
      if (i == node.KeyCount() - 1) stream << " " << node.children_[i + 1];
      stream << std::endl;
    }
  }
};

}  // namespace keyvadb