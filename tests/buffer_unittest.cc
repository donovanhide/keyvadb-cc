#include "gtest/gtest.h"
#include "common.h"
#include "db/buffer.h"

using namespace keyvadb;

TEST(BufferTests, General) {
  Buffer<256, 84> buffer;
  ASSERT_EQ(0, buffer.Size());
  auto keys = RandomKeys<256>(1000);
  // Adds
  for (auto const& key : keys) buffer.Add(key, 0);
  ASSERT_EQ(keys.size(), buffer.Size());
  // Removes
  for (auto const& key : keys) {
    auto kv = KeyValue<256>{key, 0};
    ASSERT_TRUE(buffer.Remove(kv));
    ASSERT_FALSE(buffer.Remove(kv));
  };
  ASSERT_EQ(0, buffer.Size());
  // Swaps
  for (size_t i = 0; i < 500; i++) ASSERT_EQ(i + 1, buffer.Add(keys.at(i), i));
  ASSERT_EQ(500, buffer.Size());
  for (size_t i = 0; i < 500; i++) {
    auto existing = KeyValue<256>{keys.at(i), i};
    auto add = KeyValue<256>{keys.at(i + 500), i + 500};
    ASSERT_TRUE(buffer.Swap(existing, add));
  }
  ASSERT_EQ(500, buffer.Size());
}
