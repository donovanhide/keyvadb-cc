#include "gtest/gtest.h"
#include "common.h"
#include "db/node.h"

using namespace keyvadb;

TEST(NodeTests, General) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
  ASSERT_THROW((Node<256, 84>(last, first)), std::domain_error);
  Node<256, 84> node(first, last);
  ASSERT_EQ(84, node.children.size());
  ASSERT_EQ(83, node.keys.size());
  ASSERT_EQ(Key<256>(), node.keys[0].key);
}
