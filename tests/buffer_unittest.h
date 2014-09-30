#include "gtest/gtest.h"
#include "common.h"
#include "db/buffer.h"

using namespace keyvadb;

TEST(BufferTests, General) {
  auto buffer = MakeBuffer<256>();
  ASSERT_EQ(0, buffer->Size());
  auto keys = RandomKeys<256>(1000, 0);
  // Adds
  for (auto const& key : keys) buffer->Add(key, 0);
  ASSERT_EQ(keys.size(), buffer->Size());
  // Removes
  for (auto const& key : keys) {
    auto kv = KeyValue<256>{key, 0};
    ASSERT_TRUE(buffer->Remove(kv));
    ASSERT_FALSE(buffer->Remove(kv));
  };
  ASSERT_EQ(0, buffer->Size());
  // Snapshot
  auto snapshot = buffer->GetSnapshot();
  Key<256> first, last, ones, threes;
  FromHex(first, h0);
  FromHex(last, h2);
  FromHex(ones, h3);
  FromHex(threes, h6);
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
  // EachRange
  snapshot->EachRange(ones, threes, [&](KeyValue<256> const& kv) {
    // std::cout << kv << std::endl;
    ASSERT_NE(ones, kv.key);
    ASSERT_NE(threes, kv.key);
  });
}
