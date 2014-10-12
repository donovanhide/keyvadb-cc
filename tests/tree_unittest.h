#include "tests/common.h"
#include "db/memory.h"
#include "db/tree.h"

using namespace keyvadb;

void checkTree(Tree<256> const& tree) {
  bool sane;
  std::error_condition err;
  std::tie(sane, err) = tree.IsSane();
  ASSERT_FALSE(err);
  ASSERT_TRUE(sane);
}

void checkCount(Tree<256> const& tree, std::size_t const expected) {
  std::size_t count;
  std::error_condition err;
  std::tie(count, err) = tree.NonSyntheticKeyCount();
  ASSERT_FALSE(err);
  ASSERT_EQ(expected, count);
}

void checkValue(Tree<256> const& tree, KeyValue<256> const kv) {
  std::uint64_t value;
  std::error_condition err;
  std::tie(value, err) = tree.Get(kv.key);
  ASSERT_FALSE(err);
  ASSERT_EQ(kv.value, value);
}

TEST(TreeTests, General) {
  auto mem = MemoryStoragePolicy<256>::CreateKeyStore(16);
  auto tree = Tree<256>(mem);
  tree.Init(false);
  // Check root has been created
  checkTree(tree);
  ASSERT_EQ(1UL, mem->Size());
  // Insert some random values
  // twice with same seed to insert duplicates
  const std::size_t n = 1000;
  const std::size_t rounds = 20;
  for (std::size_t i = 0; i < 2; i++) {
    for (std::size_t j = 0; j < rounds; j++) {
      auto buffer = MakeBuffer<256>();
      // Use j as seed
      buffer->FillRandom(n, j);
      ASSERT_EQ(n, buffer->Size());
      Tree<256>::journal_ptr journal;
      std::error_condition err;
      std::tie(journal, err) = tree.Add(buffer->GetSnapshot());
      ASSERT_FALSE(err);
      checkTree(tree);
      journal->Commit(mem);
      checkTree(tree);
      if (i == 0) {
        ASSERT_GT(journal->Size(), 0UL);
        ASSERT_EQ(n, journal->TotalInsertions());
        checkCount(tree, n * (j + 1));
      } else {
        ASSERT_EQ(journal->Size(), 0UL);
        ASSERT_EQ(0UL, journal->TotalInsertions());
      }
      auto snapshot = buffer->GetSnapshot();
      for (auto const& kv : snapshot->keys) checkValue(tree, kv);

      // std::cout << *journal << "----" << std::endl;
    }
  }
  // std::cout << tree;
}
