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
  virtual void SetUp() {
    ASSERT_FALSE(keys_->Open());
    ASSERT_FALSE(keys_->Clear());
  }
  virtual void TearDown() { ASSERT_FALSE(keys_->Close()); }
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
  std::error_condition err;
  Tree<256>::node_ptr node;
  std::tie(node, err) = this->keys_->Get(root->Id());
  ASSERT_EQ(db_error::key_not_found, err.value());
  ASSERT_EQ(nullptr, node);
  ASSERT_FALSE(this->keys_->Set(root));
  std::tie(node, err) = this->keys_->Get(root->Id());
  ASSERT_FALSE(err);
  ASSERT_EQ(ToHex(root->Last()), ToHex(node->Last()));
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
  ASSERT_EQ(values->Get(2, &value), db_error::value_not_found);

  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);
}

TEST(StoreTests, File) {
  Key<256> first;
  Key<256> last;
  FromHex(first, h0);
  FromHex(last, h2);

  auto values = MakeFileValueStore<256>("test.values");
  ASSERT_FALSE(values->Open());
  ASSERT_FALSE(values->Clear());
  KeyValue<256> kv;
  ASSERT_FALSE(values->Set("This is a test key with 32 chars",
                           "This is a test value", kv));
  std::string value;
  ASSERT_FALSE(values->Get(kv.value, &value));
  ASSERT_EQ("This is a test value", value);
  ASSERT_EQ(values->Get(2000, &value), db_error::value_not_found);
  ASSERT_FALSE(values->Close());
}
