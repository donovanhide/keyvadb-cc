#pragma once

#include <string>
#include <system_error>
#include "db/memory.h"
#include "db/buffer.h"
#include "db/tree.h"

namespace keyvadb {

template <std::uint32_t BITS>
class DB {
  using value_store_type = std::shared_ptr<ValueStore<BITS>>;
  using key_store_type = std::shared_ptr<KeyStore<BITS>>;
  using buffer_type = std::shared_ptr<Buffer<BITS>>;
  using tree_type = Tree<BITS>;

 private:
  tree_type tree_;
  key_store_type keys_;
  value_store_type values_;
  buffer_type buffer_;

 public:
  DB(value_store_type const& values, key_store_type const& keys)
      : tree_(tree_type(keys)),
        keys_(keys),
        values_(values),
        buffer_(MakeBuffer<256>()) {}
  DB& operator=(const DB&) = delete;

  std::string Get(std::string const& key) {
    auto k = FromBytes<BITS>(key);
    std::uint64_t valueId;
    try {
      valueId = buffer_->Get(k);
    } catch (std::out_of_range) {
      valueId = tree_.Get(k);
    }
    return values_->Get(valueId);
  }

  void Put(std::string const& key, std::string const& value) {
    auto k = FromBytes<BITS>(key);
    auto kv = values_->Set(k, value);
    buffer_->Add(kv.key, kv.value);
  }

  void Flush() {
    std::cout << "Flushing" << std::endl;
    auto snapshot = buffer_->GetSnapshot();
    auto journal = tree_.Add(snapshot);
    journal->Commit(keys_);
    buffer_->ClearSnapshot(snapshot);
    std::cout << "Flushed" << std::endl;
  }

  void Close() {
    Flush();
    values_->Close();
    keys_->Close();
  }
};

template <std::uint32_t BITS>
DB<BITS> MakeMemoryDB(std::uint64_t const degree) {
  return DB<BITS>(MakeMemoryValueStore<BITS>(),
                  MakeMemoryKeyStore<BITS>(degree));
}

// template <std::uint32_t BITS>
// std::error_code OpenFileDB(std::string const& name, DB<BITS>* db) {
//   return;
// }

}  // namespace keyvadb
