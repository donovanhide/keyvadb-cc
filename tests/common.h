#pragma once

#include <string>
#include "gtest/gtest.h"

#include "db/key.h"
#include "db/node.h"
#include "db/tree.h"
#include "db/memory.h"
#include "db/file.h"

using namespace keyvadb;

static std::string h0(
    "0000000000000000000000000000000000000000000000000000000000000000");
static std::string h1(
    "0000000000000000000000000000000000000000000000000000000000000001");
static std::string h2(
    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
static std::string h3(
    "1111111111111111111111111111111111111111111111111111111111111111");
static std::string h4(
    "2222222222222222222222222222222222222222222222222222222222222222");
static std::string h6(
    "3333333333333333333333333333333333333333333333333333333333333333");
static std::string h7(
    "0000000000000000000000000000000000000000000000000000000000000002");
static std::string h8(
    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
static std::string h9(
    "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF8");

template <typename T>
class StoreTestBase : public ::testing::Test {
 protected:
  using node_ptr = std::shared_ptr<Node<T::Bits>>;
  using key_value_type = KeyValue<T::Bits>;
  using tree_type = Tree<T::Bits>;
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
  key_value_type EmptyKeyValue() { return key_value_type(); }
  // Fills a binary key with garbage hex
  std::string HexString(char const c) { return std::string(T::Bits / 8, c); }
  tree_type GetTree() { return tree_type(keys_); }
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
