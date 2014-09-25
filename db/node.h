#pragma once

#include <stdint.h>
#include <array>
#include <algorithm>
#include <limits>
#include "key.h"

namespace keyvadb {

const uint64_t SyntheticKey = std::numeric_limits<uint64_t>::max();

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
  Key<BITS> first_;
  Key<BITS> last_;
  std::array<KeyValue<BITS>, DEGREE - 1> keys_;
  std::array<uint64_t, DEGREE> children_;

 public:
  Node(Key<BITS> const& first, Key<BITS> const& last)
      : first_(first), last_(last) {
    if (first >= last) throw std::domain_error("first must be lower than last");
  }

  void AddSyntheticKeys() {
    auto stride = Stride(first_, last_, DEGREE);
    auto cursor = first_ + stride;
    for (auto& key : keys_) {
      key = KeyValue<BITS>{cursor, SyntheticKey};
      cursor += stride;
    }
  }

  bool IsSane() {
    if (first_ >= last_) return false;
    if (!std::is_sorted(keys_.cbegin(), keys_.cend())) return false;
    for (size_t i = 1; i < DEGREE - 1; i++)
      if (!keys_[i].IsZero() && keys_[i] == keys_[i - 1]) return false;
    if (EmptyKeyCount() > 0 && EmptyChildCount() != ChildCount()) return false;
    return true;
  }

  size_t EmptyKeyCount() {
    return std::count_if(keys_.begin(), keys_.end(),
                         [](KeyValue<BITS> const& k) { return k.IsZero(); });
  }
  size_t EmptyChildCount() {
    return std::count_if(children_.begin(), children_.end(),
                         [](uint64_t const c) { return c == 0; });
  }
  size_t KeyCount() { return keys_.size(); }
  size_t ChildCount() { return children_.size(); }
};

}  // namespace keyvadb