#include <boost/algorithm/hex.hpp>
#include <boost/asio/signal_set.hpp>
#include <iostream>
#include <string>
#include <csignal>
#include "db/db.h"

using boost::algorithm::unhex;
using namespace boost::asio;

int main() {
  auto db = keyvadb::MakeMemoryDB<256>(85);
  signal_set signals(io_service, SIGINT, SIGTERM);
  signals.async_wait(
      [&db](const boost::system::error_code& error, int signal_number) {
        if (!error) db.Close();
      });

  for (std::string line; std::getline(std::cin, line);) {
    if (line.find(':') != 64) return 1;
    auto key = unhex(line.substr(0, 64));
    auto value = unhex(line.substr(65, std::string::npos));
    db.Put(key, value);
    std::cout << line.substr(0, 64) << std::endl;
  }

  db.Close();
  return 0;
}
