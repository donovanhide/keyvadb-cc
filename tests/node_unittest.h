#include "tests/common.h"
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
  ASSERT_EQ(84UL, node.Degree());
  ASSERT_EQ(84UL, node.EmptyChildCount());
  ASSERT_EQ(83UL, node.MaxKeys());
  ASSERT_EQ(83UL, node.EmptyKeyCount());
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
  node.SetChild(0, 1);
  ASSERT_NE(node.GetKeyValue(7), copyNode.GetKeyValue(7));
  ASSERT_NE(node.GetChild(0), copyNode.GetChild(0));
  auto assignNode = copyNode;
  assignNode.AddSyntheticKeyValues();
  node.SetChild(0, 2);
  ASSERT_NE(assignNode.GetKeyValue(7), copyNode.GetKeyValue(7));
  ASSERT_NE(node.GetChild(0), copyNode.GetChild(0));
}
