#ifndef KEYVADB_INCLUDE_DB_H_
#define KEYVADB_INCLUDE_DB_H_

namespace keyvadb {

class DB {
 public:
  static std::error_code Open(const std::string name, DB** dbptr);
  virtual std::error_code Get(const std::string hash, std::string* value) = 0;
  virtual std::error_code Put(const std::string hash,
                              const std::string value) = 0;
  virtual std::error_code Close() = 0;

 private:
  // No copying allowed
  DB(const DB&);
  void operator=(const DB&);
};

}  // namespace keyvadb
#endif  // KEYVADB_INCLUDE_DB_H_
