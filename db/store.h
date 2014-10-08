#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <system_error>
#include "db/key.h"

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

  virtual std::error_code Open() = 0;
  virtual std::error_code Close() = 0;
  virtual std::string Get(std::uint64_t const) const = 0;
  virtual key_value_type Set(key_type const& key, std::string const&) = 0;
};

template <std::uint32_t BITS>
class KeyStore {
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using key_type = Key<BITS>;

 public:
  virtual ~KeyStore() = default;

  virtual std::error_code Open() = 0;
  virtual std::error_code Close() = 0;
  virtual node_ptr New(key_type const& start, const key_type& end) = 0;
  virtual node_ptr Get(std::uint64_t const id) = 0;
  virtual bool Has(std::uint64_t const id) = 0;
  virtual void Set(node_ptr const& node) = 0;
  virtual std::size_t Size() const = 0;
};

}  // namespace keyvadb
