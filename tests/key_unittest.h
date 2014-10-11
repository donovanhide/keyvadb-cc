#include <string>
#include "tests/common.h"
#include "db/key.h"

using namespace keyvadb;

template <std::uint32_t BITS>
struct KeyTraits {
  enum { Bits = BITS };
};

template <typename T>
class KeyTest : public ::testing::Test {
  using key_type = Key<T::Bits>;

 private:
  T traits_;

 protected:
  key_type GetFromHex(std::string const& s) {
    key_type key;
    FromHex<T::Bits>(key, s);
    return key;
  }
  key_type GetFromBytes(std::string const& s) { return FromBytes<T::Bits>(s); }
  key_type GetMax() { return Max<T::Bits>(); }
  key_type GetMin() { return Min<T::Bits>(); }
  key_type MakeKey(std::uint32_t n) { return key_type(n); }
  key_type MakeKeyFromHex(const char c) {
    return GetFromHex(std::string(T::Bits / 4, c));
  }
  key_type TooBig() { return GetFromHex(std::string(T::Bits, 'F')); }
};

typedef ::testing::Types<KeyTraits<1024>, KeyTraits<256>, KeyTraits<32>,
                         KeyTraits<8>> KeyTypes;
TYPED_TEST_CASE(KeyTest, KeyTypes);

TYPED_TEST(KeyTest, General) {
  auto zero = this->MakeKey(0);
  auto two = this->MakeKey(2);
  auto first = this->MakeKey(1);
  auto last = this->MakeKeyFromHex('F');
  auto ones = this->MakeKeyFromHex('1');
  auto twos = this->MakeKeyFromHex('2');
  auto threes = this->MakeKeyFromHex('3');
  // Min/Max
  ASSERT_EQ(zero, this->GetMin());
  ASSERT_EQ(last, this->GetMax());
  // Comparisons
  ASSERT_TRUE(zero.is_zero());
  ASSERT_TRUE(first < last);
  ASSERT_TRUE(last > first);
  ASSERT_TRUE(first != last);
  // Adddition
  ASSERT_EQ(threes, ones + twos);
  // Exceptions
  ASSERT_THROW(last + first, std::overflow_error);
  ASSERT_THROW(first - last, std::range_error);
  ASSERT_THROW(this->TooBig(), std::overflow_error);
  // Distances
  ASSERT_EQ(ones, Distance(threes, twos));
  ASSERT_EQ(ones, Distance(twos, threes));
  // Strides
  auto stride = Stride(zero, last, 15);
  ASSERT_EQ(ones, stride);
  // Nearest
  uint32_t nearest;
  auto distance = this->MakeKey(0);
  NearestStride(zero, stride, ones, distance, nearest);
  ASSERT_EQ(zero, distance);
  ASSERT_EQ(0UL, nearest);
  NearestStride(zero, stride, twos, distance, nearest);
  ASSERT_EQ(zero, distance);
  ASSERT_EQ(1UL, nearest);
  NearestStride(zero, stride, two, distance, nearest);
  ASSERT_EQ(ones - two, distance);
  ASSERT_EQ(0UL, nearest);
  // From/To bytes
  auto f = ToBytes(first);
  auto f2 = this->GetFromBytes(f);
  ASSERT_EQ(first, f2);
  auto l = ToBytes(last);
  auto l2 = this->GetFromBytes(l);
  ASSERT_EQ(last, l2);
}
