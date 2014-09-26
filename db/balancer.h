#pragma once

#include "node.h"
#include "buffer.h"

namespace keyvadb {
template <uint32_t BITS, uint32_t DEGREE>
// Returns the net number of KeyValues inserted
size_t Balance(Node<BITS, DEGREE>& node, Buffer<BITS>& buffer) {
  if (node.EmptyKeyCount() == 0) return 0;
  return 0;
}
}  // namespace keyvadb