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
  using util = detail::KeyUtil<BITS>;
  using key_type = typename util::key_type;
  using key_value_type = KeyValue<BITS>;
  using file_type = std::unique_ptr<RandomAccessFile>;

 private:
  file_type file_;
  std::atomic_uint_fast64_t size_;
  const std::size_t value_offset_;

 public:
  explicit FileValueStore(file_type& file)
      : file_(std::move(file)),
        value_offset_(util::MaxSize() + sizeof(std::uint64_t)) {}
  FileValueStore(const FileValueStore&) = delete;
  FileValueStore& operator=(const FileValueStore&) = delete;

  std::error_condition Open() override {
    if (auto err = file_->OpenSync()) return err;
    return file_->Size(size_);
  }
  std::error_condition Clear() override {
    size_ = 0;
    return file_->Truncate();
  }
  std::error_condition Close() override { return file_->Close(); }
  std::error_condition Get(std::uint64_t const id,
                           std::string* value) const override {
    std::string str;
    // Optmistically read a block
    str.resize(4096);
    std::size_t bytesRead;
    std::error_condition err;
    std::tie(bytesRead, err) = file_->ReadAt(id, str);
    if (err) return err;
    if (bytesRead == 0) return make_error_condition(db_error::value_not_found);
    if (bytesRead < sizeof(std::uint64_t))
      return make_error_condition(db_error::short_read);
    std::uint64_t length = 0;
    string_read<std::uint64_t>(str, 0, length);
    str.resize(length);
    if (length > 4096) {
      // Get the actual length
      std::tie(bytesRead, err) = file_->ReadAt(id, str);
      if (err) return err;
      if (bytesRead == 0)
        return make_error_condition(db_error::value_not_found);
      if (bytesRead != str.length())
        return make_error_condition(db_error::short_read);
    } else if (bytesRead < str.length()) {
      return make_error_condition(db_error::short_read);
    }
    value->assign(str, value_offset_, std::string::npos);
    return std::error_condition();
  }
  std::error_condition Set(std::string const& key, std::string const& value,
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
    kv = {util::FromBytes(key), size_ - length};
    std::size_t bytesWritten;
    std::error_condition err;
    std::tie(bytesWritten, err) = file_->WriteAt(str, kv.value);
    if (err) return err;
    if (bytesWritten != length)
      return make_error_condition(db_error::short_write);
    return std::error_condition();
  };
};

template <std::uint32_t BITS>
class FileKeyStore : public KeyStore<BITS> {
  using util = detail::KeyUtil<BITS>;
  using key_type = typename util::key_type;
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using node_result = std::pair<node_ptr, std::error_condition>;
  using file_type = std::unique_ptr<RandomAccessFile>;

 private:
  const std::uint32_t block_size_;
  const std::uint32_t degree_;
  file_type file_;
  std::atomic_uint_fast64_t size_;

 public:
  FileKeyStore(std::size_t const block_size, file_type& file)
      : block_size_(block_size), degree_(84), file_(std::move(file)) {}
  FileKeyStore(const FileKeyStore&) = delete;
  FileKeyStore& operator=(const FileKeyStore&) = delete;

  std::error_condition Open() override {
    if (auto err = file_->Open()) return err;
    return file_->Size(size_);
  }

  std::error_condition Clear() override {
    size_ = 0;
    return file_->Truncate();
  }

  std::error_condition Close() override { return file_->Close(); }
  node_ptr New(const key_type& first, const key_type& last) override {
    size_ += block_size_;
    return std::make_shared<node_type>(size_ - block_size_, degree_, first,
                                       last);
  }

  node_result Get(std::uint64_t const id) const {
    std::string str;
    str.resize(block_size_);
    auto node = std::make_shared<node_type>(id, degree_, 0, 1);
    std::size_t bytesRead;
    std::error_condition err;
    std::tie(bytesRead, err) = file_->ReadAt(id, str);
    if (err) return std::make_pair(node_ptr(), err);
    if (bytesRead == 0)
      return std::make_pair(node_ptr(),
                            make_error_condition(db_error::key_not_found));
    if (bytesRead != block_size_)
      return std::make_pair(node_ptr(),
                            make_error_condition(db_error::short_read));
    node->Read(str);
    return std::make_pair(node, std::error_condition());
  }

  std::error_condition Set(node_ptr const& node) {
    std::string str;
    str.resize(block_size_);
    node->Write(str);
    std::size_t bytesWritten;
    std::error_condition err;
    std::tie(bytesWritten, err) = file_->WriteAt(str, node->Id());
    if (err) return err;
    if (bytesWritten != block_size_)
      return make_error_condition(db_error::short_write);
    return std::error_condition();
  }

  std::uint64_t Size() const { return size_; }
};

template <std::uint32_t BITS>
struct FileStoragePolicy {
  using KeyStorage = std::shared_ptr<KeyStore<BITS>>;
  using ValueStorage = std::shared_ptr<ValueStore<BITS>>;
  enum { Bits = BITS };
  static KeyStorage CreateKeyStore(std::string const& filename,
                                   std::uint32_t const blockSize) {
    // Put ifdef here!
    auto file = std::unique_ptr<RandomAccessFile>(
        std::make_unique<PosixRandomAccessFile>(filename));
    // endif
    return std::make_shared<FileKeyStore<BITS>>(blockSize, file);
  }
  static ValueStorage CreateValueStore(std::string const& filename) {
    // Put ifdef here!
    auto file = std::unique_ptr<RandomAccessFile>(
        std::make_unique<PosixRandomAccessFile>(filename));
    // endif
    return std::make_shared<FileValueStore<BITS>>(file);
  }
};

}  // namespace keyvadb
