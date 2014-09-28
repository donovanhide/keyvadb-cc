#include "gtest/gtest.h"
#include "common.h"
#include "db/store.h"

using namespace keyvadb;

TEST(StoreTests, Memory) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
  auto mem = MakeMemoryKeyStore<256>(16);
  auto root = mem->New(first, last);
  ASSERT_EQ(first, root->First());
  ASSERT_EQ(last, root->Last());
}
