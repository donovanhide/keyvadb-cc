#include "gtest/gtest.h"
#include "common.h"
#include "db/buffer.h"

using namespace keyvadb;

TEST(BufferTests, General) {
  Buffer<256> buffer;
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
    buffer.Swap(existing, add);
  }
  ASSERT_EQ(500, buffer.Size());
  // Bad swaps
  auto notPresent = KeyValue<256>{keys.at(0), 0};
  auto present = KeyValue<256>{keys.at(500), 500};
  auto alsoPresent = KeyValue<256>{keys.at(501), 501};
  ASSERT_THROW(buffer.Swap(notPresent, present), std::domain_error);
  ASSERT_THROW(buffer.Swap(present, alsoPresent), std::domain_error);
  ASSERT_THROW(buffer.Swap(present, present), std::domain_error);
}
