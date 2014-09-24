#include "keyvadb/db"

int main(int argc, char** argv) {
	keyvadb::DB db;
	keyvadb::DB::Open("db",&db)
  	return 0;
}
