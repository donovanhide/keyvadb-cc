#pragma once

#include <string>
#include <utility>
#include "db/store.h"
#include "db/env.h"

namespace keyvadb {

template <std::uint32_t BITS>
class FileValueStore : public ValueStore<BITS> {
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;
  using file_type = std::unique_ptr<RandomAccessFile>;

 private:
  file_type file_;
  std::atomic_uint_fast64_t size_;

 public:
  explicit FileValueStore(file_type& file) : file_(std::move(file)) {}
  FileValueStore(const FileValueStore&) = delete;
  FileValueStore& operator=(const FileValueStore&) = delete;

  std::error_code Open() override { return file_->Open(); }
  std::error_code Close() override { return file_->Close(); }
  std::string Get(std::uint64_t const id) const override { return "test"; }
  key_value_type Set(key_type const& key, std::string const& value) override {
    return key_value_type{};
  };
};

template <std::uint32_t BITS>
std::shared_ptr<FileValueStore<BITS>> MakeFileValueStore(
    std::string const& filename, std::size_t const bytesPerBlock,
    std::size_t const cacheBytes) {
  // Put ifdef here!
  auto file = std::unique_ptr<RandomAccessFile>(
      std::make_unique<PosixRandomAccessFile>(filename));
  // endif
  return std::make_shared<FileValueStore<BITS>>(file);
}

}  // namespace keyvadb
