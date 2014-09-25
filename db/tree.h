#pragma once

#include <stdint.h>
#include <unordered_map>
#include "node.h"

namespace keyvadb {
template <uint32_t BITS, uint32_t DEGREE>
class Tree {
 protected:
  uint64_t root_;
  // temporary memory store
  std::unordered_map<uint64_t, Node<BITS, DEGREE>> map_;

 public:
};

}  // namespace keyvadb