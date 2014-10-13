#include "tests/common.h"
#include "db/buffer.h"

using namespace keyvadb;

TEST(BufferTest, General) {
  using util = detail::KeyUtil<256>;
  auto buffer = MakeBuffer<256>();
  ASSERT_EQ(0UL, buffer->Size());
  auto keys = util::RandomKeys(1000, 0);
  // Adds
  for (auto const& key : keys) buffer->Add(key, 0);
  ASSERT_EQ(keys.size(), buffer->Size());
  // Removes
  for (auto const& key : keys) {
    auto kv = KeyValue<256>{key, 0};
    ASSERT_TRUE(buffer->Remove(kv));
    ASSERT_FALSE(buffer->Remove(kv));
  }
  ASSERT_EQ(0UL, buffer->Size());
  // Snapshot
  auto snapshot = buffer->GetSnapshot();
  auto first = util::MakeKey(1);
  auto last = util::FromHex('F');
  auto ones = util::FromHex('1');
  auto threes = util::FromHex('1');
  snapshot->Add(ones, 0);
  snapshot->Add(ones + 1, 0);
  snapshot->Add(ones - 1, 0);
  snapshot->Add(ones + 2, 0);
  snapshot->Add(ones - 2, 0);
  snapshot->Add(threes, 0);
  // ContainsRange
  ASSERT_TRUE(snapshot->ContainsRange(first, last));
  ASSERT_FALSE(snapshot->ContainsRange(first, first));
  ASSERT_FALSE(snapshot->ContainsRange(last, last));
  ASSERT_TRUE(snapshot->ContainsRange(ones, threes));
  ASSERT_FALSE(snapshot->ContainsRange(ones, ones + 1));
  ASSERT_FALSE(snapshot->ContainsRange(ones - 1, ones));
  ASSERT_TRUE(snapshot->ContainsRange(ones, ones + 2));
  ASSERT_TRUE(snapshot->ContainsRange(ones - 2, ones));
  // std::cout << snapshot;
}
