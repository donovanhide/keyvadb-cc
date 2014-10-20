#include <string>
#include <vector>
#include <set>
#include "tests/common.h"
#include "db/db.h"

using namespace keyvadb;

template <std::uint32_t BITS>
class DBTestBase : public ::testing::Test {
 public:
  using util = detail::KeyUtil<BITS>;
  auto RandomKeys(std::size_t n, std::uint32_t seed) {
    std::vector<std::string> keys;
    for (auto const& key : util::RandomKeys(n, seed))
      keys.emplace_back(util::ToBytes(key));
    return keys;
  }

  void CompareKeys(std::string const& a, std::string const& b) {
    // ASSERT_EQ(a, b);
    ASSERT_EQ(util::ToHex(util::FromBytes(a)), util::ToHex(util::FromBytes(b)));
  }
};

template <typename StoragePolicy>
class DBTest : DBTestBase<StoragePolicy::BITS> {};

template <std::uint32_t BITS>
class DBTest<MemoryStoragePolicy<BITS>> : public DBTestBase<BITS> {
 public:
  DB<MemoryStoragePolicy<BITS>> db;
  DBTest() : db(16) {}
  void SetUp() {
    ASSERT_FALSE(db.Open());
    ASSERT_FALSE(db.Clear());
  }
};

template <std::uint32_t BITS>
class DBTest<FileStoragePolicy<BITS>> : public DBTestBase<BITS> {
 public:
  DB<FileStoragePolicy<BITS>> db;
  DBTest() : db("test.keys", "test.values", 4096, 3) {}
  void SetUp() {
    ASSERT_FALSE(db.Open());
    ASSERT_FALSE(db.Clear());
  }
};

typedef ::testing::Types<MemoryStoragePolicy<256>, FileStoragePolicy<256>>
    StorageTypes;
TYPED_TEST_CASE(DBTest, StorageTypes);

TYPED_TEST(DBTest, General) {
  // These keys aren't really hex!
  std::string tooLong(
      "A4D71CBF439B2452C4D0A6AA77A24857D29F23300263F142A728D01B674A6A0A");
  std::string tooShort("B674A6A0A");
  std::string key("A4D71CBF439B2452C4D0A6AA77A24857");
  std::string value("testing123");
  ASSERT_EQ(db_error::key_wrong_length, this->db.Put(tooLong, value));
  ASSERT_EQ(db_error::key_wrong_length, this->db.Get(tooLong, &value));
  ASSERT_EQ(db_error::key_wrong_length, this->db.Put(tooShort, value));
  ASSERT_EQ(db_error::key_wrong_length, this->db.Get(tooShort, &value));
  ASSERT_EQ(db_error::key_not_found, this->db.Get(key, &value));
  ASSERT_FALSE(this->db.Put(key, value));
  ASSERT_FALSE(this->db.Get(key, &value));
}

TYPED_TEST(DBTest, Bulk) {
  auto keys = this->RandomKeys(40000, 0);
  auto f = [&](std::size_t const first, std::size_t const last) {
    // Use key as value
    for (std::size_t i = first; i < last; i++) {
      ASSERT_FALSE(this->db.Put(keys[i], keys[i]));
    }
    std::string value;
    for (std::size_t i = first; i < last; i++) {
      ASSERT_FALSE(this->db.Get(keys[i], &value));
      this->CompareKeys(keys[i], value);
    }
  };
  std::thread t1(f, 0, 10000);
  std::thread t2(f, 10000, 20000);
  std::thread t3(f, 20000, 30000);
  std::thread t4(f, 30000, 40000);
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  std::set<std::string> unique(keys.begin(), keys.end());
  std::uint32_t i = 0;
  this->db.Each([&](std::string const& key, std::string const& value) {
    this->CompareKeys(key, value);
    ASSERT_TRUE(unique.find(key) != unique.end());
    i++;
  });
  ASSERT_EQ(40000UL, i);
}
