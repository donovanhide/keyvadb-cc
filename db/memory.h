#pragma once

#include <string>
#include <system_error>
#include <atomic>
#include <unordered_map>
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
  std::error_code Open() override { return std::error_code(); }
  std::error_code Close() override { return std::error_code(); }
  std::error_code Clear() override {
    map_.clear();
    return std::error_code();
  }
  std::error_code Get(std::uint64_t const id, std::string* str) const override {
    str->assign(map_.at(id));
    return std::error_code();
  }
  std::error_code Set(std::string const& key, std::string const& value,
                      key_value_type& kv) override {
    kv = {FromBytes<BITS>(key), id_++};
    map_[kv.value] = value;
    return std::error_code();
  };
};

template <std::uint32_t BITS>
std::shared_ptr<ValueStore<BITS>> MakeMemoryValueStore() {
  return std::make_shared<MemoryValueStore<BITS>>();
}

template <std::uint32_t BITS>
class MemoryKeyStore : public KeyStore<BITS> {
 public:
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using key_type = Key<BITS>;

 private:
  std::uint32_t degree_;
  std::atomic_uint_fast64_t id_;
  std::unordered_map<std::uint64_t, node_ptr> map_;

 public:
  explicit MemoryKeyStore(std::uint32_t const degree)
      : degree_(degree), id_(0) {}

  std::error_code Open() override { return std::error_code(); }
  std::error_code Close() override { return std::error_code(); }

  node_ptr New(const key_type& first, const key_type& last) override {
    return std::make_shared<node_type>(id_++, degree_, first, last);
  }
  bool Has(std::uint64_t const id) override {
    return map_.find(id) != map_.end();
  };
  node_ptr Get(std::uint64_t const id) override { return map_.at(id); }
  void Set(node_ptr const& node) override { map_[node->Id()] = node; }
  std::size_t Size() const override { return id_; }
};

template <std::uint32_t BITS>
std::shared_ptr<KeyStore<BITS>> MakeMemoryKeyStore(std::uint32_t const degree) {
  return std::make_shared<MemoryKeyStore<BITS>>(degree);
}

}  // namespace keyvadb
