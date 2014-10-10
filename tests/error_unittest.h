#include "tests/common.h"
#include "db/error.h"

using namespace keyvadb;

TEST(ErrorTests, General) {
  auto err = make_error_condition(db_error::key_not_found);
  ASSERT_EQ("Key not found", err.message());
  auto err2 = std::generic_category().default_error_condition(1);
  ASSERT_EQ("Operation not permitted", err2.message());
  std::cout << err.category().name() << " " << err2.category().name()
            << std::endl;
}
