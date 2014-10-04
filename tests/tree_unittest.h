#include "gtest/gtest.h"
#include "common.h"
#include "db/tree.h"

using namespace keyvadb;

TEST(TreeTests, General) {
  auto mem = MakeMemoryKeyStore<256>(16);
  auto tree = Tree<256>(mem);
  // Check root has been created
  ASSERT_TRUE(tree.IsSane());
  ASSERT_EQ(1UL, mem->Size());
  // Insert some random values
  // twice with same seed to insert duplicates
  const std::size_t n = 10;
  const std::size_t rounds = 20;
  for (std::size_t i = 0; i < 2; i++) {
    for (std::size_t j = 0; j < rounds; j++) {
      auto buffer = MakeBuffer<256>();
      // Use j as seed
      buffer->FillRandom(n, j);
      ASSERT_EQ(n, buffer->Size());
      auto journal = tree.Add(buffer);
      if (i == 0)
        ASSERT_GT(journal->Size(), 0UL);
      else if (i == 0)
        ASSERT_EQ(journal->Size(), 0UL);
      std::cout << *journal << "----" << std::endl;
      ASSERT_TRUE(tree.IsSane());
      ASSERT_EQ(n, journal->TotalInsertions());
      journal->Commit(mem);
      ASSERT_TRUE(tree.IsSane());
    }
  }
  ASSERT_EQ(n * rounds, tree.NonSyntheticKeyCount());
  // std::cout << tree;
}
