#pragma once

namespace keyvadb {

template <std::uint32_t BITS>
class DBImpl : public DB {
 public:
  DBImpl(const std::string& name);
  virtual ~DBImpl();
};

}  // namespace keyvadb
