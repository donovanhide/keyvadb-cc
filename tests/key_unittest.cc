#include "gtest/gtest.h"
#include "db/key.h"

using namespace keyvadb;

const char* h0 =
    "0000000000000000000000000000000000000000000000000000000000000000";
const char* h1 =
    "0000000000000000000000000000000000000000000000000000000000000001";
const char* h2 =
    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
const char* h3 =
    "8888888899999999AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDDEEEEEEEEFFFFFFFF";

TEST(KeyTest, General) {
  // Bad string constructor
  ASSERT_THROW(Key<32> bad("BADDBADD"), std::exception);
  ASSERT_NO_THROW(Key<4> bad("BADDBADD"));
  // Constructor
  Key<32> zero(h0);
  ASSERT_TRUE(zero.Empty());
  Key<32> first(h1);
  ASSERT_FALSE(first.Empty());
  Key<32> last(h2);
  ASSERT_FALSE(last.Empty());
  Key<32> various(h3);
  ASSERT_FALSE(various.Empty());
  // Copy constructor
  Key<32> dupe(various);
  ASSERT_FALSE(dupe.Empty());
  ASSERT_TRUE(various == dupe);
  ASSERT_FALSE(zero == dupe);
  // Assignment constructor
  Key<32> dupe2 = various;
  ASSERT_FALSE(dupe2.Empty());
  ASSERT_TRUE(various == dupe2);
  ASSERT_FALSE(zero == dupe2);
  // Comparison
  ASSERT_TRUE(first < last);
  ASSERT_TRUE(first <= last);
  ASSERT_TRUE(first != last);
  ASSERT_FALSE(first > last);
  ASSERT_FALSE(first >= last);
}
