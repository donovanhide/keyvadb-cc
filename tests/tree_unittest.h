#include "gtest/gtest.h"
#include "common.h"
#include "db/tree.h"

using namespace keyvadb;

TEST(TreeTests, General) {
  auto mem = MakeMemoryKeyStore<256>(16);
  auto tree = Tree<256>(mem);
  // Check root has been created
  ASSERT_TRUE(tree.IsSane());
  ASSERT_EQ(1, mem->Size());
  // Insert some random values
  auto buffer = MakeBuffer<256>();
  std::size_t n = 10000;
  buffer->FillRandom(n);
  ASSERT_EQ(n, buffer->Size());
  auto inserted = tree.Add(buffer);
  ASSERT_GT(inserted->size(), 0);
  ASSERT_TRUE(tree.IsSane());
  // std::cout << tree;
}
