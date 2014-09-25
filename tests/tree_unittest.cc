#include "gtest/gtest.h"
#include "db/tree.h"

using namespace keyvadb;

TEST(TreeTests, General) { auto tree = Tree<256, 84>(); }
