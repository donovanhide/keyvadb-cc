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
  using value_result = std::pair<key_value_type, std::error_condition>;
  using key_value_func =
      std::function<void(std::string const&, std::string const&)>;

 public:
  virtual ~ValueStore() = default;

  virtual std::error_condition Open() = 0;   // Not threadsafe
  virtual std::error_condition Close() = 0;  // Not threadsafe
  virtual std::error_condition Clear() = 0;  // Not threadsafe

  virtual std::error_condition Get(std::uint64_t const, std::string*) const = 0;
  virtual std::error_condition Set(std::string const&, std::string const&,
                                   key_value_type&) = 0;
  virtual std::error_condition Each(key_value_func) const = 0;
};

template <std::uint32_t BITS>
class KeyStore {
  using util = detail::KeyUtil<BITS>;
  using key_type = typename util::key_type;
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using node_result = std::pair<node_ptr, std::error_condition>;

 public:
  virtual ~KeyStore() = default;

  virtual std::error_condition Open() = 0;   // Not threadsafe
  virtual std::error_condition Close() = 0;  // Not threadsafe
  virtual std::error_condition Clear() = 0;  // Not threadsafe

  virtual node_ptr New(std::uint32_t const level, key_type const&,
                       const key_type&) = 0;
  virtual node_result Get(std::uint64_t const) const = 0;
  virtual std::error_condition Set(node_ptr const&) = 0;
  virtual std::uint64_t Size() const = 0;
};

}  // namespace keyvadb
