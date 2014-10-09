#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include "db/store.h"
#include "db/env.h"
#include "db/encoding.h"

namespace keyvadb {

template <std::uint32_t BITS>
class FileValueStore : public ValueStore<BITS> {
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;
  using file_type = std::unique_ptr<RandomAccessFile>;

 private:
  file_type file_;
  std::atomic_uint_fast64_t size_;
  const std::size_t value_offset_;

 public:
  explicit FileValueStore(file_type& file)
      : file_(std::move(file)),
        value_offset_(MaxSize<BITS>() + sizeof(std::uint64_t)) {}
  FileValueStore(const FileValueStore&) = delete;
  FileValueStore& operator=(const FileValueStore&) = delete;

  std::error_code Open() override {
    if (auto err = file_->OpenSync()) return err;
    return file_->Size(size_);
  }
  std::error_code Clear() override {
    size_ = 0;
    return file_->Truncate();
  }
  std::error_code Close() override { return file_->Close(); }
  std::error_code Get(std::uint64_t const id,
                      std::string* value) const override {
    std::string str;
    // Optmistically read a block
    str.resize(4096);
    if (auto err = file_->ReadAt(id, str)) return err;
    std::uint64_t length = 0;
    string_read<std::uint64_t>(str, 0, length);
    str.resize(length);
    if (length > 4096) {
      // Get the actual length
      if (auto err = file_->ReadAt(id, str)) return err;
    }
    value->assign(str, value_offset_, std::string::npos);
    return std::error_code();
  }
  std::error_code Set(std::string const& key, std::string const& value,
                      key_value_type& kv) override {
    auto length = value_offset_ + value.size();
    std::string str;
    str.resize(length);
    size_t pos = 0;
    pos += string_replace<std::uint64_t>(length, pos, str);
    str.replace(pos, key.size(), key);
    pos += key.size();
    str.replace(pos, value.size(), value);
    size_ += length;
    kv = {FromBytes<BITS>(key), size_ - length};
    return file_->WriteAt(str, kv.value);
  };
};

template <std::uint32_t BITS>
std::shared_ptr<FileValueStore<BITS>> MakeFileValueStore(
    std::string const& filename) {
  // Put ifdef here!
  auto file = std::unique_ptr<RandomAccessFile>(
      std::make_unique<PosixRandomAccessFile>(filename));
  // endif
  return std::make_shared<FileValueStore<BITS>>(file);
}

template <std::uint32_t BITS>
class FileKeyStore : public KeyStore<BITS> {
  using key_type = Key<BITS>;
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using node_result = std::pair<node_ptr, std::error_code>;
  using file_type = std::unique_ptr<RandomAccessFile>;

 private:
  const std::uint32_t block_size_;
  const std::uint32_t degree_;
  const std::size_t key_size_;
  file_type file_;
  std::atomic_uint_fast64_t size_;

 public:
  FileKeyStore(std::size_t const block_size, file_type& file)
      : block_size_(block_size),
        degree_(84),
        key_size_(MaxSize<BITS>()),
        file_(std::move(file)) {}
  FileKeyStore(const FileKeyStore&) = delete;
  FileKeyStore& operator=(const FileKeyStore&) = delete;

  std::error_code Open() override {
    if (auto err = file_->Open()) return err;
    return file_->Size(size_);
  }

  std::error_code Clear() override {
    size_ = 0;
    return file_->Truncate();
  }

  std::error_code Close() override { return file_->Close(); }
  node_ptr New(const key_type& first, const key_type& last) override {
    size_ += block_size_;
    return std::make_shared<node_type>(size_ - block_size_, degree_, first,
                                       last);
  }

  node_result Get(std::uint64_t const id) const {
    std::string str;
    str.resize(block_size_);
    auto node = std::make_shared<node_type>(size_ - block_size_, degree_, 0, 1);
    if (auto err = file_->ReadAt(id, str)) return std::make_pair(node, err);
    node->Read(str);
    return std::make_pair(node, std::error_code());
  }

  std::error_code Set(node_ptr const& node) {
    std::string str;
    str.resize(block_size_);
    node->Write(str);
    return file_->WriteAt(str, node->Id());
  }

  bool Has(std::uint64_t const id) const { return id < size_; }
  std::uint64_t Size() const { return size_; }
};

template <std::uint32_t BITS>
std::shared_ptr<FileKeyStore<BITS>> MakeFileKeyStore(
    std::string const& filename, std::uint32_t block_size) {
  // Put ifdef here!
  auto file = std::unique_ptr<RandomAccessFile>(
      std::make_unique<PosixRandomAccessFile>(filename));
  // endif
  return std::make_shared<FileKeyStore<BITS>>(block_size, file);
}

}  // namespace keyvadb
