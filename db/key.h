#pragma once

#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/random.hpp>
#include <boost/math/tools/precision.hpp>
#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <limits>

namespace keyvadb {
static const std::uint64_t EmptyKey = 0;

static const std::uint64_t SyntheticValue =
    std::numeric_limits<std::uint64_t>::max();
static const std::uint64_t EmptyValue = 0;

template <std::uint32_t BITS>
using Key =
    boost::multiprecision::number<boost::multiprecision::cpp_int_backend<
        BITS, BITS, boost::multiprecision::unsigned_magnitude,
        boost::multiprecision::checked, void>>;

std::string string_to_hex(const std::string& input) {
  static const char* const lut = "0123456789ABCDEF";
  size_t len = input.length();

  std::string output;
  output.reserve(2 * len);
  for (size_t i = 0; i < len; ++i) {
    const unsigned char c = input[i];
    output.push_back(lut[c >> 4]);
    output.push_back(lut[c & 15]);
  }
  return output;
}

// TODO(DH): Check this is portable!
template <std::uint32_t BITS>
extern std::string ToBytes(Key<BITS> const& key) {
  auto bytes = key.backend().limbs();
  auto length = key.backend().size() * sizeof(*key.backend().limbs());
  auto s = std::string(reinterpret_cast<const char*>(bytes), length);
  return s;
}

template <std::uint32_t BITS>
extern Key<BITS> FromBytes(std::string const& str) {
  Key<BITS> key;
  auto length = str.size() / sizeof(*key.backend().limbs());
  key.backend().resize(length, length);
  auto bytes = key.backend().limbs();
  std::memcpy(bytes, str.data(), str.size());
  return key;
}

template <std::uint32_t BITS>
extern std::size_t WriteBytes(Key<BITS> const& key, const std::size_t pos,
                              std::string& str) {
  auto bytes = key.backend().limbs();
  auto length = key.backend().size() * sizeof(*key.backend().limbs());
  std::memcpy(&str[pos], &bytes, length);
  return length;
}

template <std::uint32_t BITS>
extern std::size_t ReadBytes(std::string const& str, const std::size_t pos,
                             const std::size_t length, Key<BITS>& key) {
  auto limbLength = length / sizeof(*key.backend().limbs());
  key.backend().resize(limbLength, limbLength);
  auto bytes = key.backend().limbs();
  std::memcpy(bytes, &str[pos], length);
  return length;
}

template <std::uint32_t BITS>
extern void FromHex(Key<BITS>& key, std::string const& s) {
  key = Key<BITS>("0x" + s);
}

template <std::uint32_t BITS>
extern std::string ToHex(Key<BITS> const& key) {
  std::stringstream ss;
  ss << std::setw(BITS / 4) << std::setfill('0') << std::setbase(16) << key;
  return ss.str();
}

template <std::uint32_t BITS>
extern const Key<BITS> Max() {
  return boost::math::tools::max_value<Key<BITS>>();
}

template <std::uint32_t BITS>
extern std::size_t MaxSize() {
  auto max = Max<BITS>();
  return max.backend().size() * sizeof(*max.backend().limbs());
}

template <std::uint32_t BITS>
extern const Key<BITS> Min() {
  return boost::math::tools::min_value<Key<BITS>>();
}

template <std::uint32_t BITS>
extern Key<BITS> Distance(Key<BITS> const& a, Key<BITS> const& b) {
  if (a > b) return a - b;
  return b - a;
}

template <std::uint32_t BITS>
extern Key<BITS> Stride(Key<BITS> const& start, Key<BITS> const& end,
                        std::uint32_t const& n) {
  return (end - start) / n;
}

template <std::uint32_t BITS>
extern void NearestStride(Key<BITS> const& start, Key<BITS> const& stride,
                          Key<BITS> const& value, Key<BITS>& distance,
                          std::uint32_t& nearest) {
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
  nearest--;
}

template <std::uint32_t BITS>
using KeyGenerator = boost::random::independent_bits_engine<
    boost::random::mt19937, BITS, Key<BITS>>;

template <std::uint32_t BITS>
extern std::vector<Key<BITS>> RandomKeys(std::size_t n, std::uint32_t seed) {
  boost::random::mt19937 base(seed);
  KeyGenerator<BITS> gen(base);
  std::vector<Key<BITS>> v;
  for (std::size_t i = 0; i < n; i++) v.emplace_back(gen());
  return v;
}

template <std::uint32_t BITS>
struct KeyValue {
  Key<BITS> key;        // Hash of actual value
  std::uint64_t value;  // offset of actual value in values file

  constexpr bool IsZero() const { return key.is_zero(); }
  constexpr bool IsSynthetic() const { return value == SyntheticValue; }
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
