TEST_DIR = tests
TOOLS_DIR = tools

# Paths to the fused gtest files.
GTEST_H = $(TEST_DIR)/gtest/gtest.h
GTEST_ALL_C = $(TEST_DIR)/gtest/gtest-all.cc
GTEST_MAIN_CC = $(TEST_DIR)/gtest/gtest_main.cc


CPPFLAGS += -I$(TEST_DIR) -I. -I./include -isystem $(TEST_DIR)/gtest
CXXFLAGS += -O1 -g -Wall -Wextra -std=c++1y -DGTEST_LANG_CXX11=1

all : keyvadb_unittests kvd

valgrind : all
	valgrind --dsymutil=yes --track-origins=yes ./keyvadb_unittests

check : keyvadb_unittests
	./keyvadb_unittests

clean :
	rm -rf keyvadb_unittests *.o

gtest-all.o : $(GTEST_H) $(GTEST_ALL_C)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/gtest/gtest-all.cc

unittests.o : $(TEST_DIR)/unittests.cc db/*.h include/*.h tests/*.h $(GTEST_H)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/unittests.cc

keyvadb_unittests : unittests.o gtest-all.o 
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ -o $@

kvd.o : $(TOOLS_DIR)/kvd.cc db/*.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TOOLS_DIR)/kvd.cc

kvd : kvd.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ -o $@