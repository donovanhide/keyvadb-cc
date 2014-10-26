#include <boost/algorithm/hex.hpp>
#include <iostream>
#include <algorithm>
#include <vector>
#include <string>
#include <csignal>
#include <chrono>
#include "db/db.h"

using boost::algorithm::unhex;
using boost::algorithm::hex;
using namespace keyvadb;
using namespace std::chrono;

// call function for each line
// returns an iterator to the first unconsumed character
template <class FwdIt, class Function>
FwdIt for_each_line(FwdIt first, FwdIt last, Function f) {
  for (;;) {
    auto const iter = std::find(first, last, '\n');
    if (iter == last)
      break;
    f(std::string(first, iter));
    first = iter + 1;
  }
  return first;
}

int main() {
  DB<FileStoragePolicy<256>> db("kvd.keys", "kvd.values", 4096, 2000);
  // 1024 * 1024 * 1024 / 4096);
  // DB<FileStoragePolicy<256>, StandardLog> db("kvd.keys", "kvd.values",
  // 4096);
  // DB<MemoryStoragePolicy<256>> db(85);
  if (auto err = db.Open()) {
    std::cerr << err.message() << std::endl;
    return 1;
  }
  if (auto err = db.Clear()) {
    std::cerr << err.message() << std::endl;
    return 1;
  }
  std::vector<std::string> inserted;
  std::ios_base::sync_with_stdio(false);
  std::array<char, 1048576> str;
  auto start = high_resolution_clock::now();
  auto first = begin(str);
  std::size_t i = 0;
  for (; !std::cin.eof(); i++) {
    std::cin.read(first, std::distance(first, end(str)));
    auto last = first + std::cin.gcount();
    last = for_each_line(begin(str), last,
                         [&db, &inserted](std::string const& line) {
      if (line.find(':') != 64)
        throw std::invalid_argument("bad line format");
      auto key = unhex(line.substr(0, 64));
      auto value = unhex(line.substr(65, std::string::npos));
      if (auto err = db.Put(key, value)) {
        std::cout << err.message() << std::endl;
      }
      inserted.push_back(key);
    });
    first = std::copy(last, end(str), begin(str));
  }
  auto finish = high_resolution_clock::now();
  auto dur = duration_cast<nanoseconds>(finish - start);
  std::cout << "Puts: " << dur.count() / inserted.size() << " ns/key"
            << std::endl;
  start = high_resolution_clock::now();
  std::string value;
  for (auto const& key : inserted)
    if (auto err = db.Get(key, &value))
      std::cout << hex(key) << ":" << err.message() << std::endl;
  finish = high_resolution_clock::now();
  dur = duration_cast<nanoseconds>(finish - start);
  std::cout << "Gets: " << dur.count() / inserted.size() << " ns/key"
            << std::endl;
  return 0;
}
