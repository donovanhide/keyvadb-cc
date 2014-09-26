#include "gtest/gtest.h"
#include "common.h"
#include "db/node.h"

using namespace keyvadb;

TEST(NodeTests, General) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
  ASSERT_THROW((Node<256, 84>(0, last, first)), std::domain_error);
  Node<256, 84> node(0, first, last);
  ASSERT_TRUE(node.IsSane());
  ASSERT_EQ(84, node.ChildCount());
  ASSERT_EQ(84, node.EmptyChildCount());
  ASSERT_EQ(83, node.KeyCount());
  ASSERT_EQ(83, node.EmptyKeyCount());
  node.AddSyntheticKeys();
  ASSERT_TRUE(node.IsSane());
}
