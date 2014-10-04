#include "db.h"

int main(int argc, char** argv) {
  keyvadb::DB<256> db;
  keyvadb::NewMemoryDB(&db);
  return 0;
}
