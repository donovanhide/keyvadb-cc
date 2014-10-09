#include <string>
#include "tests/common.h"
#include "db/memory.h"
#include "db/file.h"

using namespace keyvadb;

TEST(StoreTests, Memory) {
  auto values = MakeMemoryValueStore<256>();
  KeyValue<256> kv;
  ASSERT_FALSE(values->Set("This is a test key with 32 chars",
                           "This is a test value", kv));
  std::string value;
  ASSERT_FALSE(values->Get(kv.value, &value));
  ASSERT_EQ("This is a test value", value);
  ASSERT_THROW(values->Get(2, &value), std::out_of_range);

  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);

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

TEST(StoreTests, File) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);

  auto values = MakeFileValueStore<256>("test");
  ASSERT_FALSE(values->Open());
  ASSERT_FALSE(values->Clear());
  KeyValue<256> kv;
  ASSERT_FALSE(values->Set("This is a test key with 32 chars",
                           "This is a test value", kv));
  std::string value;
  ASSERT_FALSE(values->Get(kv.value, &value));
  ASSERT_EQ("This is a test value", value);
  ASSERT_THROW(values->Get(2000, &value), std::out_of_range);
  ASSERT_FALSE(values->Close());
}
