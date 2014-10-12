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
class KeyPolicy {
  using key_type =
      boost::multiprecision::number<boost::multiprecision::cpp_int_backend<
          BITS, BITS, boost::multiprecision::unsigned_magnitude,
          boost::multiprecision::checked, void>>;
  using seed_type = boost::random::mt19937;
  using gen_type =
      boost::random::independent_bits_engine<seed_type, BITS, key_type>;

  // protected:
  //  ~KeyPolicy() {}

 public:
  enum { Bits = BITS, HexChars = BITS / 4, Bytes = BITS / 8 };
  key_type MakeKey(const std::uint64_t num) const { return key_type(num); }

  key_type FromHex(char const c) { return FromHex(std::string(HexChars, c)); }
  key_type FromHex(std::size_t const count, char const c) {
    return FromHex(std::string(count, c));
  }
  key_type FromHex(std::string const& s) { return key_type("0x" + s); }

  std::string ToHex(key_type const& key) {
    std::stringstream ss;
    ss << std::setw(BITS / 4) << std::setfill('0') << std::setbase(16) << key;
    return ss.str();
  }

  std::string ToBytes(key_type const& key) {
    auto bytes = key.backend().limbs();
    auto length = key.backend().size() * sizeof(*key.backend().limbs());
    auto s = std::string(reinterpret_cast<const char*>(bytes), length);
    return s;
  }

  key_type FromBytes(std::string const& str) {
    key_type key;
    auto length = str.size() / sizeof(*key.backend().limbs());
    key.backend().resize(length, length);
    auto bytes = key.backend().limbs();
    std::memcpy(bytes, str.data(), str.size());
    return key;
  }

  key_type Distance(key_type const& a, key_type const& b) {
    if (a > b) return a - b;
    return b - a;
  }

  key_type Stride(key_type const& start, key_type const& end,
                  std::uint32_t const& n) {
    return (end - start) / n;
  }

  void NearestStride(key_type const& start, key_type const& stride,
                     key_type const& value, key_type& distance,
                     std::uint32_t& nearest) {
    key_type index;
    divide_qr(value - start, stride, index, distance);
    // std::cout << ToHex(start) << " " << ToHex(stride) << " " <<
    // ToHex(value)
    //           << " " << ToHex(distance) << std::endl;
    nearest = static_cast<std::uint32_t>(index);
    // Round up first
    if (nearest == 0) {
      nearest++;
      distance = stride - distance;
    }
    nearest--;
  }

  const key_type Max() { return boost::math::tools::max_value<key_type>(); }

  const key_type Min() { return boost::math::tools::min_value<key_type>(); }

  std::size_t MaxSize() {
    auto max = Max();
    return max.backend().size() * sizeof(*max.backend().limbs());
  }
  std::vector<key_type> RandomKeys(std::size_t n, std::uint32_t seed) {
    seed_type base(seed);
    gen_type gen(base);
    std::vector<key_type> v;
    for (std::size_t i = 0; i < n; i++) v.emplace_back(gen());
    return v;
  }
};

template <std::uint32_t BITS>
using Key =
    boost::multiprecision::number<boost::multiprecision::cpp_int_backend<
        BITS, BITS, boost::multiprecision::unsigned_magnitude,
        boost::multiprecision::checked, void>>;

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
                              const std::size_t length, std::string& str) {
  auto bytes = key.backend().limbs();
  auto limbLength = key.backend().size() * sizeof(*key.backend().limbs());
  // Copy limbs into rightmost position, probably not portable
  auto offset = length - limbLength;
  std::memcpy(&str[pos + offset], bytes, length - offset);
  return length;
}

template <std::uint32_t BITS>
extern std::size_t ReadBytes(std::string const& str, const std::size_t pos,
                             const std::size_t length, Key<BITS>& key) {
  auto limbLength = length / sizeof(*key.backend().limbs());
  // Make sure we have enough limbs for largest possible value
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
