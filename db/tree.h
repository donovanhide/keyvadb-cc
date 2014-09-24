#pragma once

#include <stdint.h>
#include <array>
#include <key.h>

namespace keyvadb {

template <uint32_t BITS, size_t KEYS>
class Node {
 protected:
  array<Key<BITS>, KEYS> keys_;

 public:
};

template <uint32_t BITS, size_t KEYS>
class Tree {
 protected:
  uint64_t root_;

 public:
};

}  // namespace keyvadb