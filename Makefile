TEST_DIR = tests

# Paths to the fused gtest files.
GTEST_H = $(TEST_DIR)/gtest/gtest.h
GTEST_ALL_C = $(TEST_DIR)/gtest/gtest-all.cc
GTEST_MAIN_CC = $(TEST_DIR)/gtest/gtest_main.cc


CPPFLAGS += -I$(TEST_DIR) -I. -isystem $(TEST_DIR)/gtest
CXXFLAGS += -O1 -g -Wall -Wextra -std=c++1y -DGTEST_LANG_CXX11=1

all : keyvadb_unittests

valgrind : all
	valgrind --dsymutil=yes --track-origins=yes ./keyvadb_unittests

check : all
	./keyvadb_unittests

clean :
	rm -rf keyvadb_unittests *.o

gtest-all.o : $(GTEST_H) $(GTEST_ALL_C)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/gtest/gtest-all.cc

gtest_main.o : $(GTEST_H) $(GTEST_MAIN_CC)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(GTEST_MAIN_CC)

unittests.o : $(TEST_DIR)/unittests.cc db/*.h tests/*.h $(GTEST_H)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/unittests.cc

keyvadb_unittests : unittests.o gtest-all.o gtest_main.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ -o $@