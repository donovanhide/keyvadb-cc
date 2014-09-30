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
  for (std::size_t i = 0; i < 10; i++) {
    auto buffer = MakeBuffer<256>();
    std::size_t n = 10000;
    buffer->FillRandom(n);
    ASSERT_EQ(n, buffer->Size());
    auto journal = tree.Add(buffer);
    ASSERT_GT(journal->Size(), 0);
    std::cout << *journal;
    ASSERT_TRUE(tree.IsSane());
  }
  // std::cout << tree;
}
