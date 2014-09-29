#include "gtest/gtest.h"
#include "common.h"
#include "db/tree.h"

using namespace keyvadb;

TEST(TreeTests, General) {
  auto mem = MakeMemoryKeyStore<256>(16);
  auto tree = Tree<256>(mem);
  ASSERT_EQ(1, mem->Size());
  auto buffer = MakeBuffer<256>();
  buffer->FillRandom(1000);
  ASSERT_EQ(1000, buffer->Size());
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
  Node<256> node(0, 16, first, last);
  ASSERT_TRUE(node.IsSane());
  auto inserted = tree.Balance(buffer);
  std::cout << tree;
  // ASSERT_GT(0, inserted);
}
