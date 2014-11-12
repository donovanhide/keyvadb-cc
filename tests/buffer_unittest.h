#include "tests/common.h"
#include "db/buffer.h"

using namespace keyvadb;

TEST(BufferTest, General)
{
    using util = detail::KeyUtil<256>;
    Buffer<256> buffer;
    ASSERT_EQ(0UL, buffer.Size());
    auto keys = util::RandomKeys(1000, 0);
    // Adds
    for (auto const& key : keys) buffer.Add(key, 0);
    ASSERT_EQ(keys.size(), buffer.Size());
    // Removes
    for (auto const& key : keys)
    {
        auto kv = KeyValue<256>{key, 0};
        ASSERT_TRUE(buffer.Remove(kv));
        ASSERT_FALSE(buffer.Remove(kv));
    }
    ASSERT_EQ(0UL, buffer.Size());
    // Snapshot
    auto snapshot = buffer.GetSnapshot();
    auto first = util::MakeKey(1);
    auto last = util::FromHex('F');
    auto ones = util::FromHex('1');
    auto threes = util::FromHex('3');
    snapshot->Add(ones, 0);
    snapshot->Add(ones + 1, 0);
    snapshot->Add(ones - 1, 0);
    snapshot->Add(ones + 2, 0);
    snapshot->Add(ones - 2, 0);
    snapshot->Add(threes, 0);
    // ContainsRange

    ASSERT_TRUE(snapshot->ContainsRange(first, last));
    ASSERT_THROW(snapshot->ContainsRange(last, first), std::invalid_argument);
    ASSERT_FALSE(snapshot->ContainsRange(first, first));
    ASSERT_FALSE(snapshot->ContainsRange(last, last));
    ASSERT_TRUE(snapshot->ContainsRange(ones, threes));
    ASSERT_FALSE(snapshot->ContainsRange(ones, ones + 1));
    ASSERT_FALSE(snapshot->ContainsRange(ones - 1, ones));
    ASSERT_TRUE(snapshot->ContainsRange(ones, ones + 2));
    ASSERT_TRUE(snapshot->ContainsRange(ones - 2, ones));
    // std::cout << snapshot;
}

TEST(BufferV2Test, General)
{
    using util = detail::KeyUtil<256>;
    BufferV2<256> buffer;
    // Add some known keys
    auto first = util::MakeKey(1);
    auto last = util::FromHex('F');
    auto ones = util::FromHex('1');
    auto threes = util::FromHex('3');
    buffer.Add(util::ToBytes(ones), "ones");
    buffer.Add(util::ToBytes(ones + 1), "ones plus one");
    buffer.Add(util::ToBytes(ones - 1), "ones minus one");
    buffer.Add(util::ToBytes(ones + 2), "ones plus two");
    buffer.Add(util::ToBytes(ones - 2), "ones minus two");
    buffer.Add(util::ToBytes(threes), "threes");

    // ContainsRange
    ASSERT_TRUE(buffer.ContainsRange(first, last));
    ASSERT_THROW(buffer.ContainsRange(last, first), std::invalid_argument);
    ASSERT_FALSE(buffer.ContainsRange(first, first));
    ASSERT_FALSE(buffer.ContainsRange(last, last));
    ASSERT_TRUE(buffer.ContainsRange(ones, threes));
    ASSERT_FALSE(buffer.ContainsRange(ones, ones + 1));
    ASSERT_FALSE(buffer.ContainsRange(ones - 1, ones));
    ASSERT_TRUE(buffer.ContainsRange(ones, ones + 2));
    ASSERT_TRUE(buffer.ContainsRange(ones - 2, ones));
}

TEST(BufferV2Test, Random)
{
    using util = detail::KeyUtil<256>;
    BufferV2<256> buffer;
    ASSERT_EQ(0UL, buffer.Size());
    auto keys = util::RandomKeys(10000, 0);
    // Add some keys with every other value having an offset
    std::uint64_t offset = 0;
    for (auto const& key : keys)
    {
        buffer.Add(util::ToBytes(key), util::ToBytes(key));
        if (offset % 2 == 0)
            buffer.SetOffset(key, offset);
        offset++;
    }
    ASSERT_EQ(keys.size(), buffer.Size());
    auto range = buffer.GetRange(util::FromHex('0'), util::FromHex('8'));
    std::cout << buffer;
}
