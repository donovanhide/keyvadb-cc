#include "tests/common.h"
#include "include/db.h"

using namespace keyvadb;

TEST(DBTests, Memory) {
  DB<256> db;
  NewMemoryDB(&db);
}
