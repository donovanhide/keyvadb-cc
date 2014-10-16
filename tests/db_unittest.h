#include "tests/common.h"
#include "db/db.h"
#include <string>

using namespace keyvadb;

TEST(DBTest, Memory) {
  DB<MemoryStoragePolicy<256>> db(16);
  ASSERT_FALSE(db.Open());
  // These keys aren't really hex!
  std::string tooLong(
      "A4D71CBF439B2452C4D0A6AA77A24857D29F23300263F142A728D01B674A6A0A");
  std::string tooShort("B674A6A0A");
  std::string key("A4D71CBF439B2452C4D0A6AA77A24857");
  std::string value("testing123");
  ASSERT_EQ(db_error::key_wrong_length, db.Put(tooLong, value));
  ASSERT_EQ(db_error::key_wrong_length, db.Get(tooLong, &value));
  ASSERT_EQ(db_error::key_wrong_length, db.Put(tooShort, value));
  ASSERT_EQ(db_error::key_wrong_length, db.Get(tooShort, &value));
  ASSERT_EQ(db_error::value_not_found, db.Get(key, &value));
  ASSERT_FALSE(db.Put(key, value));
  ASSERT_FALSE(db.Get(key, &value));
}

// TEST(DBTest, File) {
//   DB<FileStoragePolicy<256>> db("test.keys", "test.values", 4096);
//   ASSERT_FALSE(db.Open());
//   ASSERT_FALSE(db.Close());
// }
