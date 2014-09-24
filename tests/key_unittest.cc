#include "gtest/gtest.h"
#include "db/key.h"

using namespace keyvadb;

std::string h0(
    "0000000000000000000000000000000000000000000000000000000000000000");
std::string h1(
    "0000000000000000000000000000000000000000000000000000000000000001");
std::string h2(
    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
std::string h3(
    "1111111111111111111111111111111111111111111111111111111111111111");
std::string h4(
    "2222222222222222222222222222222222222222222222222222222222222222");
std::string h6(
    "3333333333333333333333333333333333333333333333333333333333333333");
std::string h7(
    "0000000000000000000000000000000000000000000000000000000000000002");
std::string h8(
    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");

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
  ASSERT_EQ(ones, Stride(zero, last, 15));
}
