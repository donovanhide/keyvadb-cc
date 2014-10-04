#include <stdio.h>
#include "gtest/gtest.h"
#include "tests/key_unittest.h"
#include "tests/node_unittest.h"
#include "tests/buffer_unittest.h"
#include "tests/tree_unittest.h"
#include "tests/store_unittest.h"

GTEST_API_ int main(int argc, char **argv) {
  printf("Running main() from gtest_main.cc\n");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
