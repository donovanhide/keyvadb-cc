#pragma once

namespace keyvadb {

class DBImpl : public DB {
 public:
  DBImpl(const std::string& name);
  virtual ~DBImpl();
};

}  // namespace keyvadb
