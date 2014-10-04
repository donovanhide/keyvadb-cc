#ifndef KEYVADB_INCLUDE_DB_H_
#define KEYVADB_INCLUDE_DB_H_

#include <string>

namespace keyvadb {

class DB {
 public:
  static std::error_code Open(std::string const& name, DB** dbptr);
  virtual std::error_code Get(std::string const& hash, std::string* value) = 0;
  virtual std::error_code Put(std::string const& hash,
                              std::string const& value) = 0;
  virtual std::error_code Close() = 0;

 private:
  // No copying allowed
  DB(const DB&);
  void operator=(const DB&);
};

}  // namespace keyvadb
#endif  // KEYVADB_INCLUDE_DB_H_
