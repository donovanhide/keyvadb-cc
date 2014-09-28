#include "gtest/gtest.h"
#include "common.h"
#include "db/tree.h"

using namespace keyvadb;

TEST(TreeTests, General) {
  auto tree = Tree<256>();
  Buffer<256> buffer;
  auto keys = RandomKeys<256>(1000);
  for (auto const& key : keys) buffer.Add(key, 0);
  ASSERT_EQ(keys.size(), buffer.Size());
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
  Node<256> node(0, 16, first, last);
  ASSERT_TRUE(node.IsSane());
  auto inserted = tree.Balance(buffer);
  ASSERT_GT(0, inserted);
}
