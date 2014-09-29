#include "gtest/gtest.h"
#include "common.h"
#include "db/node.h"

using namespace keyvadb;

TEST(NodeTests, Big) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
  ASSERT_THROW((Node<256>(0, 84, last, first)), std::domain_error);
  Node<256> node(0, 84, first, last);
  ASSERT_TRUE(node.IsSane());
  ASSERT_EQ(84, node.ChildCount());
  ASSERT_EQ(84, node.EmptyChildCount());
  ASSERT_EQ(83, node.KeyCount());
  ASSERT_EQ(83, node.EmptyKeyCount());
  node.AddSyntheticKeyValues();
  ASSERT_TRUE(node.IsSane());
}

TEST(NodeTests, Small) {
  Key<256> first;
  Key<256> last;
  Key<256> middle;
  FromHex(first, h0);
  FromHex(last, h2);
  FromHex(middle, h9);
  Node<256> node(0, 16, first, last);
  ASSERT_TRUE(node.IsSane());
  node.AddSyntheticKeyValues();
  ASSERT_TRUE(node.IsSane());
  ASSERT_EQ(middle, node.GetKeyValue(7).key);
  ASSERT_EQ(SyntheticValue, node.GetKeyValue(7).value);
}

TEST(NodeTests, CopyAssign) {
  Key<256> first;
  Key<256> last;
  Key<256> middle;
  FromHex(first, h0);
  FromHex(last, h2);
  FromHex(middle, h9);
  Node<256> node(0, 16, first, last);
  ASSERT_TRUE(node.IsSane());
  auto copyNode = Node<256>(node);
  ASSERT_TRUE(copyNode.IsSane());
  node.AddSyntheticKeyValues();
  ASSERT_NE(node.GetKeyValue(7), copyNode.GetKeyValue(7));
  auto assignNode = copyNode;
  assignNode.AddSyntheticKeyValues();
  ASSERT_NE(assignNode.GetKeyValue(7), copyNode.GetKeyValue(7));
}