#pragma once

#include <stdint.h>
#include <cstddef>
#include <string>
#include <array>
#include <algorithm>

namespace keyvadb {
template <size_t BYTES>
class Key {
  std::array<uint32_t, BYTES / 4> n;

 public:
  Key() { std::fill(n, 0); }

  Key(const std::string& s) {
    if (s.length() != BYTES * 2) {
      throw std::invalid_argument("received wrong length");
    }
    size_t pos = 0;
    for (uint32_t& v : n) {
      v = std::strtoul(s.c_str() + pos, nullptr, 16);
      pos += 8;
    }
  }

  Key& operator=(const Key& b) {
    std::copy(b.n.begin(), b.n.end(), n);
    return *this;
  }

  Key(const Key& b) { std::copy(b.n.begin(), b.n.end(), n.begin()); }

  bool Empty() {
    for (uint32_t& v : n)
      if (v != 0) return false;
    return true;
  }

  bool operator<(const Key& b) { return n < b.n; }
  bool operator>(const Key& b) { return n > b.n; }
  bool operator<=(const Key& b) { return n <= b.n; }
  bool operator>=(const Key& b) { return n >= b.n; }
  bool operator==(const Key& b) { return n == b.n; }
  bool operator!=(const Key& b) { return n != b.n; }
};
}  // namespace keyvadb