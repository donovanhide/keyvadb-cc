#pragma once

#include <string>
#include <system_error>
#include <atomic>
#include <unordered_map>
#include <utility>
#include "db/store.h"

namespace keyvadb {

template <std::uint32_t BITS>
class MemoryValueStore : public ValueStore<BITS> {
 public:
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;

 private:
  std::atomic_uint_fast64_t id_;
  std::unordered_map<std::uint64_t, std::string> map_;

 public:
  MemoryValueStore() : id_(0) {}
  std::error_condition Open() override { return std::error_condition(); }
  std::error_condition Close() override { return std::error_condition(); }
  std::error_condition Clear() override {
    map_.clear();
    return std::error_condition();
  }
  std::error_condition Get(std::uint64_t const id,
                           std::string* str) const override {
    try {
      str->assign(map_.at(id));
    } catch (std::out_of_range const&) {
      return make_error_condition(db_error::value_not_found);
    }
    return std::error_condition();
  }

  std::error_condition Set(std::string const& key, std::string const& value,
                           key_value_type& kv) override {
    kv = {FromBytes<BITS>(key), id_++};
    map_[kv.value] = value;
    return std::error_condition();
  };
};

template <std::uint32_t BITS>
class MemoryKeyStore : public KeyStore<BITS> {
 public:
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using node_result = std::pair<node_ptr, std::error_condition>;
  using key_type = Key<BITS>;

 private:
  std::uint32_t degree_;
  std::atomic_uint_fast64_t id_;
  std::unordered_map<std::uint64_t, node_ptr> map_;

 public:
  explicit MemoryKeyStore(std::uint32_t const degree)
      : degree_(degree), id_(0) {}

  std::error_condition Open() override { return std::error_condition(); }
  std::error_condition Close() override { return std::error_condition(); }
  std::error_condition Clear() override {
    map_.clear();
    return std::error_condition();
  }

  node_ptr New(const key_type& first, const key_type& last) override {
    return std::make_shared<node_type>(id_++, degree_, first, last);
  }

  node_result Get(std::uint64_t const id) const override {
    try {
      auto node = map_.at(id);
      return std::make_pair(node, std::error_condition());
    } catch (std::out_of_range const&) {
      return std::make_pair(node_ptr(),
                            make_error_condition(db_error::key_not_found));
    }
  }

  std::error_condition Set(node_ptr const& node) override {
    map_[node->Id()] = node;
    return std::error_condition();
  }

  std::uint64_t Size() const override { return id_; }
};

template <std::uint32_t BITS>
struct MemoryStoragePolicy {
  using KeyStorage = std::shared_ptr<KeyStore<BITS>>;
  using ValueStorage = std::shared_ptr<ValueStore<BITS>>;
  enum { Bits = BITS };
  static KeyStorage CreateKeyStore(std::uint32_t const degree) {
    return std::make_shared<MemoryKeyStore<BITS>>(degree);
  }
  static ValueStorage CreateValueStore() {
    return std::make_shared<MemoryValueStore<BITS>>();
  }
};

}  // namespace keyvadb
