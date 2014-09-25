#pragma once

#include <stdint.h>
#include "node.h"

namespace keyvadb {
template <uint32_t BITS, size_t KEYS>
class Tree {
 protected:
  uint64_t root_;

 public:
};

}  // namespace keyvadb