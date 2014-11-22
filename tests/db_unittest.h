#include <string>
#include <vector>
#include <set>
#include <random>
#include "tests/common.h"
#include "db/db.h"

using namespace keyvadb;

template <std::uint32_t BITS>
class DBTestBase : public ::testing::Test
{
   public:
    using util = detail::KeyUtil<BITS>;
    auto RandomKeys(std::size_t n, std::uint32_t seed)
    {
        std::vector<std::string> keys;
        for (auto const& key : util::RandomKeys(n, seed))
            keys.emplace_back(util::ToBytes(key));
        return keys;
    }

    void CompareKeys(std::string const& a, std::string const& b)
    {
        // ASSERT_EQ(a, b);
        ASSERT_EQ(util::ToHex(util::FromBytes(a)),
                  util::ToHex(util::FromBytes(b)));
    }
};

template <typename StoragePolicy>
class DBTest : DBTestBase<StoragePolicy::BITS>
{
};

template <std::uint32_t BITS>
class DBTest<MemoryStoragePolicy<BITS>> : public DBTestBase<BITS>
{
   public:
    std::unique_ptr<DB<MemoryStoragePolicy<BITS>>> GetDB()
    {
        return std::make_unique<DB<MemoryStoragePolicy<BITS>>>(16);
    }
};

template <std::uint32_t BITS>
class DBTest<FileStoragePolicy<BITS>> : public DBTestBase<BITS>
{
   public:
    std::unique_ptr<DB<FileStoragePolicy<BITS>>> GetDB()
    {
        return std::make_unique<DB<FileStoragePolicy<BITS>>>(
            "test.keys", "test.values", 4096, 1024 * 1024 * 1024 / 4096);
    }
};

typedef ::testing::Types<MemoryStoragePolicy<256>, FileStoragePolicy<256>>
    StorageTypes;
TYPED_TEST_CASE(DBTest, StorageTypes);

TYPED_TEST(DBTest, General)
{
    // These keys aren't really hex!
    std::string tooLong(
        "A4D71CBF439B2452C4D0A6AA77A24857D29F23300263F142A728D01B674A6A0A");
    std::string tooShort("B674A6A0A");
    std::string key("A4D71CBF439B2452C4D0A6AA77A24857");
    std::string value("testing123");

    auto db = this->GetDB();
    ASSERT_FALSE(db->Open());
    ASSERT_EQ(db_error::key_wrong_length, db->Put(tooLong, value));
    ASSERT_EQ(db_error::key_wrong_length, db->Get(tooLong, &value));
    ASSERT_EQ(db_error::key_wrong_length, db->Put(tooShort, value));
    ASSERT_EQ(db_error::key_wrong_length, db->Get(tooShort, &value));
    ASSERT_EQ(db_error::key_not_found, db->Get(key, &value));
    ASSERT_FALSE(db->Put(key, value));
    ASSERT_FALSE(db->Get(key, &value));
}

TYPED_TEST(DBTest, Bulk)
{
    auto db = this->GetDB();
    ASSERT_FALSE(db->Open());
    std::uniform_int_distribution<size_t> value_length(32, 8000);
    std::mt19937 rng;
    rng.seed(0);
    const std::size_t numKeys = 400000;
    auto keys = this->RandomKeys(numKeys, 0);
    auto f = [&](std::size_t const first, std::size_t const last)
    {
        std::string value;
        for (std::size_t i = first; i < last; i++)
        {
            ASSERT_TRUE(db->Get(keys[i], &value) == db_error::key_not_found);
            // Use key as value
            value = keys[i];
            value.resize(value_length(rng));
            ASSERT_FALSE(db->Put(keys[i], value));
        }

        for (std::size_t i = first; i < last; i++)
        {
            ASSERT_FALSE(db->Get(keys[i], &value));
            this->CompareKeys(keys[i], value.substr(0, 32));
        }
    };
    std::thread t1(f, 0, (numKeys / 4) * 1);
    std::thread t2(f, (numKeys / 4) * 1, (numKeys / 4) * 2);
    std::thread t3(f, (numKeys / 4) * 2, (numKeys / 4) * 3);
    std::thread t4(f, (numKeys / 4) * 3, numKeys);
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    std::set<std::string> unique(keys.begin(), keys.end());
    for (const auto& key : unique)
    {
        std::string value;
        ASSERT_FALSE(db->Get(key, &value));
        this->CompareKeys(key, value.substr(0, 32));
    }
    // Close and reopen to make sure everything is flushed
    // auto dbptr = db.release();
    // delete dbptr;
    // db = this->GetDB();
    // ASSERT_FALSE(db->Open());
    // std::uint32_t i = 0;
    // auto err = db->Each([&](std::string const& key, std::string const& value)
    //                     {
    //                         this->CompareKeys(key, value.substr(0, 32));
    //                         ASSERT_TRUE(unique.find(key) != unique.end());
    //                         i++;
    //                     });
    // std::cout << err.message() << std::endl;
    // ASSERT_FALSE(err);
    // ASSERT_EQ(numKeys, i);
}
