#include <string>
#include <vector>
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
  DBTest() : db("test.keys", "test.values", 4096) {}
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
  auto keys = this->RandomKeys(25000, 0);
  // Use key as value
  for (auto const& key : keys) ASSERT_FALSE(this->db.Put(key, key));
  std::string value;
  for (auto const& key : keys) {
    ASSERT_FALSE(this->db.Get(key, &value));
    ASSERT_EQ(key, value);
  }
  std::size_t i = 0;
  this->db.Each([&](std::string const& key, std::string const& value) {
    this->CompareKeys(key, value);
    this->CompareKeys(keys[i], key);
    i++;
  });
}
