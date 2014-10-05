#pragma once

#include <string>
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
  std::string Get(std::uint64_t const id) const override { return map_.at(id); }
  key_value_type Set(key_type const& key, std::string const& value) override {
    auto kv = key_value_type{key, id_++};
    map_[kv.value] = value;
    return kv;
  };
  std::size_t Size() const override { return map_.size(); }
  void Close() override {}
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

  node_ptr New(const key_type& first, const key_type& last) override {
    return std::make_shared<node_type>(id_++, degree_, first, last);
  }
  bool Has(std::uint64_t const id) override {
    return map_.find(id) != map_.end();
  };
  node_ptr Get(std::uint64_t const id) override { return map_.at(id); }
  void Set(node_ptr const& node) override { map_[node->Id()] = node; }
  std::size_t Size() const override { return id_; }
  void Close() override {}
};

template <std::uint32_t BITS>
std::shared_ptr<KeyStore<BITS>> MakeMemoryKeyStore(std::uint32_t const degree) {
  return std::make_shared<MemoryKeyStore<BITS>>(degree);
}

}  // namespace keyvadb
