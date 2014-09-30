#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <limits>
#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/random.hpp>
#include <boost/math/tools/precision.hpp>

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
extern const Key<BITS> Max() {
  return boost::math::tools::max_value<Key<BITS>>();
}

template <std::uint32_t BITS>
extern const Key<BITS> Min() {
  return boost::math::tools::min_value<Key<BITS>>();
}

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
                          Key<BITS> const& value, const std::uint32_t max,
                          Key<BITS>& distance, std::uint32_t& nearest) {
  Key<BITS> index;
  divide_qr(value - start, stride, index, distance);
  // std::cout << ToHex(start) << " " << ToHex(stride) << " " << ToHex(value)
  //           << " " << ToHex(distance) << std::endl;
  nearest = static_cast<std::uint32_t>(index);
  // Round up first
  if (nearest == 0) {
    nearest++;
    distance = stride - distance;
  }
  // Round down last
  if (nearest == max) {
    nearest--;
    distance = stride - distance;
  }
  // Fit to key indexes
  nearest--;
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

template <std::uint32_t BITS>
struct KeyValue {
  Key<BITS> key;        // Hash of actual value
  std::uint64_t value;  // offset of actual value in values file

  constexpr bool IsZero() const { return key.is_zero(); }
  constexpr bool operator<(KeyValue<BITS> const& rhs) const {
    return key < rhs.key;
  }
  constexpr bool operator==(KeyValue<BITS> const& rhs) const {
    return key == rhs.key;
  }
  constexpr bool operator!=(KeyValue<BITS> const& rhs) const {
    return key != rhs.key;
  }
  friend std::ostream& operator<<(std::ostream& stream,
                                  KeyValue<BITS> const& kv) {
    stream << "Key: " << ToHex(kv.key) << " Value: " << kv.value;
    return stream;
  }
};

}  // namespace keyvadb