#pragma once

#include <string>
#include "gtest/gtest.h"

#include "db/key.h"
#include "db/node.h"
#include "db/tree.h"
#include "db/memory.h"
#include "db/file.h"
#include "db/buffer.h"
#include "db/journal.h"

using namespace keyvadb;

template <typename T>
class StoreTestBase : public ::testing::Test, public detail::KeyUtil<T::Bits>
{
   protected:
    using node_ptr = std::shared_ptr<Node<T::Bits>>;
    using key_value_type = KeyValue<T::Bits>;
    using tree_type = Tree<T::Bits>;
    using tree_ptr = std::unique_ptr<tree_type>;
    using cache_type = NodeCache<T::Bits>;
    using journal_type = Journal<T::Bits>;
    using journal_ptr = std::unique_ptr<journal_type>;
    using buffer_type = Buffer<T::Bits>;

    typename T::KeyStorage keys_;
    typename T::ValueStorage values_;
    cache_type cache_;
    buffer_type buffer_;

    virtual void InitStores() {}

    virtual void SetUp()
    {
        InitStores();
        ASSERT_FALSE(keys_->Open());
        ASSERT_FALSE(keys_->Clear());
        ASSERT_FALSE(values_->Open());
        ASSERT_FALSE(values_->Clear());
        buffer_.Clear();
        cache_.Reset();
    }

    virtual void TearDown()
    {
        ASSERT_FALSE(keys_->Close());
        ASSERT_FALSE(values_->Close());
    }

    node_ptr EmptyNode() { return nullptr; }

    key_value_type EmptyKeyValue() { return key_value_type(); }

    // Fills a binary key with garbage hex
    std::string HexString(char const c) { return std::string(T::Bits / 8, c); }

    tree_ptr GetTree() { return std::make_unique<tree_type>(keys_, cache_); }

    journal_ptr GetJournal() { return std::make_unique<journal_type>(); }

    void FillBuffer(std::size_t n, std::uint32_t seed)
    {
        // Add some keys with every other value having an offset
        std::uint64_t offset = 0;
        for (auto const& key : this->RandomKeys(n, seed))
        {
            buffer_.Add(this->ToBytes(key), this->ToBytes(key));
            if (offset % 2 == 0)
                buffer_.SetOffset(key, offset);
            offset++;
        }
    }

    void checkTree(tree_ptr const& tree)
    {
        bool sane;
        std::error_condition err;
        std::tie(sane, err) = tree->IsSane();
        ASSERT_FALSE(err);
        ASSERT_TRUE(sane);
    }

    void checkCount(tree_ptr const& tree, std::size_t const expected)
    {
        std::size_t count;
        std::error_condition err;
        std::tie(count, err) = tree->NonSyntheticKeyCount();
        ASSERT_FALSE(err);
        ASSERT_EQ(expected, count);
    }

    void checkValue(tree_ptr const& tree, KeyValue<256> const kv)
    {
        std::uint64_t value;
        std::error_condition err;
        std::tie(value, err) = tree->Get(kv.key);
        ASSERT_FALSE(err);
        ASSERT_EQ(kv.value, value);
    }
};

template <typename T>
class StoreTest;

template <std::uint32_t BITS>
class StoreTest<MemoryStoragePolicy<BITS>>
    : public StoreTestBase<MemoryStoragePolicy<BITS>>
{
    using policy_type = MemoryStoragePolicy<BITS>;

   protected:
    void InitStores() override
    {
        this->keys_ = policy_type::CreateKeyStore(16);
        this->values_ = policy_type::CreateValueStore();
    }
};

template <std::uint32_t BITS>
class StoreTest<FileStoragePolicy<BITS>>
    : public StoreTestBase<FileStoragePolicy<BITS>>
{
    using policy_type = FileStoragePolicy<BITS>;

   protected:
    void InitStores() override
    {
        this->keys_ = policy_type::CreateKeyStore("test.keys", 4096);
        this->values_ = policy_type::CreateValueStore("test.values");
    }
};

typedef ::testing::Types<MemoryStoragePolicy<256>, FileStoragePolicy<256>>
    StoreTypes;
TYPED_TEST_CASE(StoreTest, StoreTypes);
