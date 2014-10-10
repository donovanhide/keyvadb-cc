#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <utility>
#include <system_error>
#include "db/key.h"
#include "db/error.h"

namespace keyvadb {

// Forward Declaration
template <std::uint32_t BITS>
class Node;

template <std::uint32_t BITS>
class ValueStore {
  using key_value_type = KeyValue<BITS>;
  using key_type = Key<BITS>;

 public:
  virtual ~ValueStore() = default;

  virtual std::error_condition Open() = 0;   // Not threadsafe
  virtual std::error_condition Close() = 0;  // Not threadsafe
  virtual std::error_condition Clear() = 0;  // Not threadsafe

  virtual std::error_condition Get(std::uint64_t const, std::string*) const = 0;
  virtual std::error_condition Set(std::string const& key, std::string const&,
                                   key_value_type&) = 0;
};

template <std::uint32_t BITS>
class KeyStore {
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using node_result = std::pair<node_ptr, std::error_condition>;
  using key_type = Key<BITS>;

 public:
  virtual ~KeyStore() = default;

  virtual std::error_condition Open() = 0;   // Not threadsafe
  virtual std::error_condition Close() = 0;  // Not threadsafe
  virtual std::error_condition Clear() = 0;  // Not threadsafe

  virtual node_ptr New(key_type const& start, const key_type& end) = 0;
  virtual node_result Get(std::uint64_t const id) const = 0;
  virtual std::error_condition Set(node_ptr const& node) = 0;
  virtual std::uint64_t Size() const = 0;
};

}  // namespace keyvadb
