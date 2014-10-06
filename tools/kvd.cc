#include <boost/algorithm/hex.hpp>
#include <iostream>
#include <algorithm>
#include <string>
#include <csignal>
#include "db/db.h"

using boost::algorithm::unhex;
using namespace keyvadb;

// call function for each line
// returns an iterator to the first unconsumed character
template <class FwdIt, class Function>
FwdIt for_each_line(FwdIt first, FwdIt last, Function f) {
  for (;;) {
    auto const iter = std::find(first, last, '\n');
    if (iter == last) break;
    f(std::string(first, iter));
    first = iter + 1;
  }
  // std::cout << "tail" << std::string(first, last) << std::endl;
  return first;
}

int main() {
  auto values = MakeMemoryValueStore<256>();
  auto keys = MakeMemoryKeyStore<256>(85);
  DB<256> db(keys, values);
  std::ios_base::sync_with_stdio(false);
  std::array<char, 1048576> str;
  auto start = begin(str);
  for (; !std::cin.eof();) {
    std::cin.read(start, std::distance(start, end(str)));
    auto last = start + std::cin.gcount();
    last = for_each_line(begin(str), last, [&](std::string const& line) {
      // std::cout << line << std::endl;
      if (line.find(':') != 64) throw std::invalid_argument("bad line format");
      auto key = unhex(line.substr(0, 64));
      auto value = unhex(line.substr(65, std::string::npos));
      db.Put(key, value);
    });
    start = std::copy(last, end(str), begin(str));
  }
  db.Close();
  return 0;
}
