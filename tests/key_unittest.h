#include "tests/common.h"
#include "db/key.h"

using namespace keyvadb;

TEST(KeyTests, General) {
  // Constructors and strings
  Key<256> zero, first, last, ones, twos, threes, two, tooBig;
  FromHex(zero, h0);
  FromHex(first, h1);
  FromHex(last, h2);
  FromHex(ones, h3);
  FromHex(twos, h4);
  FromHex(threes, h6);
  FromHex(two, h7);
  ASSERT_EQ(h0, ToHex(zero));
  ASSERT_EQ(h1, ToHex(first));
  ASSERT_EQ(h2, ToHex(last));
  // Min/Max
  ASSERT_EQ(zero, Min<256>());
  ASSERT_EQ(first, Min<256>() + 1);
  ASSERT_EQ(last, Max<256>());
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
  ASSERT_THROW(FromHex(tooBig, h8), std::overflow_error);
  // Distances
  ASSERT_EQ(ones, Distance(threes, twos));
  ASSERT_EQ(ones, Distance(twos, threes));
  // Strides
  auto stride = Stride(zero, last, 15);
  ASSERT_EQ(ones, stride);
  // Nearest
  uint32_t nearest;
  Key<256> distance;
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
  auto f2 = FromBytes<256>(f);
  ASSERT_EQ(first, f2);
  auto l = ToBytes(last);
  auto l2 = FromBytes<256>(l);
  ASSERT_EQ(last, l2);
}
