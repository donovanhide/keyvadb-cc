#include <string>
#include <tuple>
#include "tests/common.h"
#include "db/memory.h"
#include "db/file.h"

using namespace keyvadb;

// Helper to pack BITS into a template parameter
template <std::uint32_t BITS>
class TypeValue {
 public:
  static const std::uint32_t value = BITS;
};
template <std::uint32_t BITS>
const std::uint32_t TypeValue<BITS>::value;

template <typename T>
class KeyStoreTest : public ::testing::Test {
 public:
  using key_store_type =
      std::shared_ptr<KeyStore<std::tuple_element<1, T>::type::value>>;
  static const std::uint32_t bits = std::tuple_element<1, T>::type::value;
  key_store_type keys_;

  KeyStoreTest() {
    if (std::is_same<
            typename std::tuple_element<0, T>::type,
            MemoryKeyStore<std::tuple_element<1, T>::type::value>>::value) {
      keys_ = MakeMemoryKeyStore<std::tuple_element<1, T>::type::value>(16);
    } else {
      keys_ = MakeFileKeyStore<std::tuple_element<1, T>::type::value>(
          "test.keys", 4096);
    }
  }
  virtual ~KeyStoreTest() {}
};
template <typename T>
const std::uint32_t KeyStoreTest<T>::bits;

TYPED_TEST_CASE_P(KeyStoreTest);

TYPED_TEST_P(KeyStoreTest, SetAndGet) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
  auto root = this->keys_->New(first, last);
  ASSERT_EQ(0UL, root->Id());
  ASSERT_EQ(first, root->First());
  ASSERT_EQ(last, root->Last());
  ASSERT_THROW(this->keys_->Get(root->Id()), std::out_of_range);
  ASSERT_FALSE(this->keys_->Has(root->Id()));
  ASSERT_FALSE(this->keys_->Set(root));
  ASSERT_TRUE(this->keys_->Has(root->Id()));
  // ASSERT_FALSE(this->keys_->Get(root->Id(), node))
  // ASSERT_EQ(root, node);
}

REGISTER_TYPED_TEST_CASE_P(KeyStoreTest, SetAndGet);

typedef ::testing::Types<std::tuple<MemoryKeyStore<256>, TypeValue<256>>,
                         std::tuple<FileKeyStore<256>, TypeValue<256>>>
    KeyStoreImplementations;
INSTANTIATE_TYPED_TEST_CASE_P(KeyStoreTests, KeyStoreTest,
                              KeyStoreImplementations);

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

  // auto keys = MakeMemoryKeyStore<256>(16);
  // auto root = keys->New(first, last);
  // ASSERT_EQ(0UL, root->Id());
  // ASSERT_EQ(first, root->First());
  // ASSERT_EQ(last, root->Last());
  // Node<256> node;
  // ASSERT_THROW(keys->Get(root->Id(), node), std::out_of_range);
  // ASSERT_FALSE(keys->Has(root->Id(), node));
  // ASSERT_FALSE(keys->Set(root));
  // ASSERT_FALSE(keys->Get(root->Id(), node))
  // ASSERT_EQ(root, node);
  // ASSERT_TRUE(keys->Has(root->Id()));
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
