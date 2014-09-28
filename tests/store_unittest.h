#include "gtest/gtest.h"
#include "common.h"
#include "db/store.h"

using namespace keyvadb;

TEST(StoreTests, Memory) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
  MemoryKeyStore<256, 16> mem;
  auto node1 = mem.New(first, last);
}
