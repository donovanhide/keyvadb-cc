TEST_DIR = tests

# Paths to the fused gtest files.
GTEST_H = $(TEST_DIR)/gtest/gtest.h
GTEST_ALL_C = $(TEST_DIR)/gtest/gtest-all.cc
GTEST_MAIN_CC = $(TEST_DIR)/gtest/gtest_main.cc

CPPFLAGS += -I$(TEST_DIR) -I.
CXXFLAGS += -g -pthread -std=c++11

all : key_unittest

check : all
	./key_unittest

clean :
	rm -rf key_unittest *.o

gtest-all.o : $(GTEST_H) $(GTEST_ALL_C)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/gtest/gtest-all.cc

gtest_main.o : $(GTEST_H) $(GTEST_MAIN_CC)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(GTEST_MAIN_CC)

sample1.o : $(TEST_DIR)/sample1.cc $(TEST_DIR)/sample1.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/sample1.cc

key_unittest.o : $(TEST_DIR)/key_unittest.cc \
                     db/key.h $(GTEST_H)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/key_unittest.cc

key_unittest : key_unittest.o gtest-all.o gtest_main.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ -o $@