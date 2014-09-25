#pragma once

#include <stdint.h>
#include <array>
#include "key.h"

namespace keyvadb {

template <uint32_t BITS>
struct KeyValue {
  Key<BITS> key;   // Hash of actual value
  uint64_t value;  // offset of actual value in values file
};

// Node invariants:
// 1. keys must always be in sorted order,lowest to highest
// 2. each key is unique
// 3. first must be lower than last
template <uint32_t BITS, uint32_t DEGREE>
struct Node {
  Node(Key<BITS> const& first, Key<BITS> const& last)
      : first(first), last(last) {
    if (first >= last) throw std::domain_error("first must be lower than last");
  }
  Key<BITS> first;
  Key<BITS> last;
  std::array<KeyValue<BITS>, DEGREE - 1> keys;
  std::array<uint64_t, DEGREE> children;
};

}  // namespace keyvadb