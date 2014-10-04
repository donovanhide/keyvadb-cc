#pragma once

#include <string>
#include <system_error>

namespace keyvadb {

template <std::uint32_t BITS>
class DB {
 public:
  DB(){};
  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  std::error_code Get(std::string const& key, std::string* value);
  std::error_code Put(std::string const& key, std::string const& value);
  std::error_code Close();
};

template <std::uint32_t BITS>
std::error_code OpenFileDB(std::string const& name, DB<BITS>* db);

template <std::uint32_t BITS>
std::error_code NewMemoryDB(DB<BITS>* db);

}  // namespace keyvadb
