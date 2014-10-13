#include <string>
#include <tuple>
#include "tests/common.h"
#include "db/memory.h"
#include "db/file.h"

using namespace keyvadb;

template <typename T>
class StoreTestBase : public ::testing::Test {
 protected:
  using node_ptr = std::shared_ptr<Node<T::Bits>>;
  typename T::KeyStorage keys_;
  typename T::ValueStorage values_;
  virtual void InitStores() {}
  virtual void SetUp() {
    InitStores();
    ASSERT_FALSE(keys_->Open());
    ASSERT_FALSE(keys_->Clear());
    ASSERT_FALSE(values_->Open());
    ASSERT_FALSE(values_->Clear());
  }
  virtual void TearDown() {
    ASSERT_FALSE(keys_->Close());
    ASSERT_FALSE(values_->Close());
  }
  node_ptr EmptyNode() { return nullptr; }
};

template <typename T>
class StoreTest;

template <std::uint32_t BITS>
class StoreTest<MemoryStoragePolicy<BITS>>
    : public StoreTestBase<MemoryStoragePolicy<BITS>> {
  using policy_type = MemoryStoragePolicy<BITS>;

 protected:
  void InitStores() override {
    this->keys_ = policy_type::CreateKeyStore(16);
    this->values_ = policy_type::CreateValueStore();
  }
};

template <std::uint32_t BITS>
class StoreTest<FileStoragePolicy<BITS>>
    : public StoreTestBase<FileStoragePolicy<BITS>> {
  using policy_type = FileStoragePolicy<BITS>;

 protected:
  void InitStores() override {
    this->keys_ = policy_type::CreateKeyStore("test.keys", 4096);
    this->values_ = policy_type::CreateValueStore("test.values");
  }
};

typedef ::testing::Types<MemoryStoragePolicy<256>, FileStoragePolicy<256>>
    StoreTypes;
TYPED_TEST_CASE(StoreTest, StoreTypes);

TYPED_TEST(StoreTest, SetAndGetKeys) {
  auto first = this->keys_->MakeKey(1);
  auto last = this->keys_->FromHex('F');
  auto root = this->keys_->New(first, last);
  ASSERT_EQ(0UL, root->Id());
  ASSERT_EQ(first, root->First());
  ASSERT_EQ(last, root->Last());
  std::error_condition err;
  auto node = this->EmptyNode();
  std::tie(node, err) = this->keys_->Get(root->Id());
  ASSERT_EQ(db_error::key_not_found, err.value());
  ASSERT_EQ(nullptr, node);
  ASSERT_FALSE(this->keys_->Set(root));
  std::tie(node, err) = this->keys_->Get(root->Id());
  ASSERT_FALSE(err);
  ASSERT_EQ(ToHex(root->Last()), ToHex(node->Last()));
}

TYPED_TEST(StoreTest, SetAndGetValues) {
  KeyValue<256> kv;
  ASSERT_FALSE(this->values_->Set("This is a test key with 32 chars",
                                  "This is a test value", kv));
  std::string value;
  ASSERT_FALSE(this->values_->Get(kv.value, &value));
  ASSERT_EQ("This is a test value", value);
}
