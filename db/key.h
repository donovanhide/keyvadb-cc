#pragma once

#include <stdint.h>
#include <cstddef>
#include <string>
#include <array>
#include <algorithm>
#include <boost/multiprecision/cpp_int.hpp>

using namespace boost::multiprecision;

namespace keyvadb {
template <uint32_t BITS>
using Key =
    number<cpp_int_backend<BITS, BITS, unsigned_magnitude, checked, void>>;

template <uint32_t BITS>
extern void FromHex(Key<BITS>& key, std::string& s) {
  key = Key<BITS>("0x" + s);
};

template <uint32_t BITS>
extern std::string ToHex(Key<BITS> const& key) {
  std::stringstream s;
  s << std::setw(BITS / 4) << std::setfill('0') << std::setbase(16) << key;
  return s.str();
};

template <uint32_t BITS>
extern Key<BITS> Distance(Key<BITS> const& a, Key<BITS> const& b) {
  if (a > b) return a - b;
  return b - a;
};

template <uint32_t BITS>
extern Key<BITS> Stride(Key<BITS> const& start, Key<BITS> const& end,
                        uint64_t n) {
  return (end - start) / n;
};

}  // namespace keyvadb