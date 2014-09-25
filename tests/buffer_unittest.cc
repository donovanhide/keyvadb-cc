#include "gtest/gtest.h"
#include "common.h"
#include "db/buffer.h"

using namespace keyvadb;

TEST(BufferTests, General) {
  Buffer<256, 84> buffer;
  ASSERT_EQ(0, buffer.Size());
  auto keys = RandomKeys<256>(1000);
  for (auto key : keys) buffer.Add(key, 0);
  ASSERT_EQ(keys.size(), buffer.Size());
}
