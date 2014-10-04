#include "tests/common.h"
#include "db/memory.h"

using namespace keyvadb;

TEST(StoreTests, Memory) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);

  auto key = first + 1;
  auto values = MakeMemoryValueStore<256>();
  auto kv = values->Set(key, "This is a test");
  ASSERT_EQ(1UL, values->Size());
  ASSERT_EQ("This is a test", values->Get(kv.value));
  ASSERT_THROW(values->Get(2), std::out_of_range);

  auto keys = MakeMemoryKeyStore<256>(16);
  auto root = keys->New(first, last);
  ASSERT_EQ(0UL, root->Id());
  ASSERT_EQ(first, root->First());
  ASSERT_EQ(last, root->Last());
  ASSERT_THROW(keys->Get(root->Id()), std::out_of_range);
  ASSERT_FALSE(keys->Has(root->Id()));
  keys->Set(root);
  ASSERT_EQ(root, keys->Get(root->Id()));
  ASSERT_TRUE(keys->Has(root->Id()));
}
