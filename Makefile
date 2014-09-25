TEST_DIR = tests

# Paths to the fused gtest files.
GTEST_H = $(TEST_DIR)/gtest/gtest.h
GTEST_ALL_C = $(TEST_DIR)/gtest/gtest-all.cc
GTEST_MAIN_CC = $(TEST_DIR)/gtest/gtest_main.cc


CPPFLAGS += -I$(TEST_DIR) -I.
CXXFLAGS += -g -pthread -std=c++11

all : keyvadb_tests

check : all
	./keyvadb_tests

clean :
	rm -rf keyvadb_tests *.o

gtest-all.o : $(GTEST_H) $(GTEST_ALL_C)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/gtest/gtest-all.cc

gtest_main.o : $(GTEST_H) $(GTEST_MAIN_CC)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(GTEST_MAIN_CC)

key_unittest.o : $(TEST_DIR)/key_unittest.cc db/*.h $(GTEST_H)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/key_unittest.cc

buffer_unittest.o : $(TEST_DIR)/buffer_unittest.cc db/*.h $(GTEST_H)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/buffer_unittest.cc

node_unittest.o : $(TEST_DIR)/node_unittest.cc db/*.h  $(GTEST_H)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/node_unittest.cc

tree_unittest.o : $(TEST_DIR)/tree_unittest.cc db/*.h $(GTEST_H)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/tree_unittest.cc

keyvadb_tests : key_unittest.o  buffer_unittest.o node_unittest.o tree_unittest.o gtest-all.o gtest_main.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ -o $@