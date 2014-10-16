#include "tests/common.h"
#include "db/db.h"
#include <string>

using namespace keyvadb;

template <typename StoragePolicy>
class DBTest : public ::testing::Test {
 public:
  DB<StoragePolicy, StandardLog> db;
  DBTest() : db(16) {}
};

template <std::uint32_t BITS>
class DBTest<MemoryStoragePolicy<BITS>> : public ::testing::Test {
 public:
  DB<MemoryStoragePolicy<BITS>, StandardLog> db;
  DBTest() : db(16) {}
};

template <std::uint32_t BITS>
class DBTest<FileStoragePolicy<BITS>> : public ::testing::Test {
 public:
  DB<FileStoragePolicy<BITS>, StandardLog> db;
  DBTest() : db("test.keys", "test.values", 4096) {}
};

typedef ::testing::Types<MemoryStoragePolicy<256>, FileStoragePolicy<256>>
    StorageTypes;
TYPED_TEST_CASE(DBTest, StorageTypes);

TYPED_TEST(DBTest, General) {
  ASSERT_FALSE(this->db.Open());
  ASSERT_FALSE(this->db.Clear());
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
