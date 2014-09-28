#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/random.hpp>

using namespace boost::multiprecision;
using namespace boost::random;

namespace keyvadb {
template <std::uint32_t BITS>
using Key =
    number<cpp_int_backend<BITS, BITS, unsigned_magnitude, checked, void>>;

template <std::uint32_t BITS>
extern void FromHex(Key<BITS>& key, std::string& s) {
  key = Key<BITS>("0x" + s);
};

template <std::uint32_t BITS>
extern std::string ToHex(Key<BITS> const& key) {
  std::stringstream ss;
  ss << std::setw(BITS / 4) << std::setfill('0') << std::setbase(16) << key;
  return ss.str();
};

template <std::uint32_t BITS>
extern Key<BITS> Distance(Key<BITS> const& a, Key<BITS> const& b) {
  if (a > b) return a - b;
  return b - a;
};

template <std::uint32_t BITS>
extern Key<BITS> Stride(Key<BITS> const& start, Key<BITS> const& end,
                        std::uint32_t const& n) {
  return (end - start) / n;
};

template <std::uint32_t BITS>
extern void NearestStride(Key<BITS> const& start, Key<BITS> const& stride,
                          Key<BITS> const& value, Key<BITS>& distance,
                          std::uint32_t& nearest) {
  Key<BITS> index;
  divide_qr(value - start, stride, index, distance);
  nearest = static_cast<std::uint32_t>(index);
}

template <std::uint32_t BITS>
using KeyGenerator = independent_bits_engine<mt19937, BITS, Key<BITS>>;

template <std::uint32_t BITS>
extern std::vector<Key<BITS>> RandomKeys(std::uint64_t n) {
  KeyGenerator<BITS> gen;
  std::vector<Key<BITS>> v;
  for (std::uint64_t i = 0; i < n; i++) v.emplace_back(gen());
  return v;
};

}  // namespace keyvadb