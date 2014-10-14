#include "tests/common.h"
#include "db/db.h"

using namespace keyvadb;

TEST(DBTest, Memory) {
  DB<MemoryStoragePolicy<256>> db(16);
  ASSERT_FALSE(db.Open());
  ASSERT_FALSE(db.Close());
}

TEST(DBTest, File) {
  DB<FileStoragePolicy<256>> db("test.keys", "test.values", 4096);
  ASSERT_FALSE(db.Open());
  ASSERT_FALSE(db.Close());
}
