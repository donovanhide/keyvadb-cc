#pragma once

#include <algorithm>
#include <string>
#include <unordered_map>
#include <utility>
#include "db/store.h"
#include "db/cache.h"
#include "db/env.h"
#include "db/encoding.h"

namespace keyvadb
{
template <std::uint32_t BITS>
class FileValueStore : public ValueStore<BITS>
{
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using value_type = typename Buffer<BITS>::Value;
    using file_type = std::unique_ptr<RandomAccessFile>;
    using key_value_func =
        std::function<void(std::string const&, std::string const&)>;

   private:
    enum
    {
        Bytes = BITS / 8
    };
    file_type file_;
    std::atomic_uint_fast64_t size_;
    static const std::size_t value_offset;

   public:
    explicit FileValueStore(file_type& file) : file_(std::move(file)) {}
    FileValueStore(const FileValueStore&) = delete;
    FileValueStore& operator=(const FileValueStore&) = delete;

    std::error_condition Open() override
    {
        if (auto err = file_->Open())
            return err;
        return file_->Size(size_);
    }
    std::error_condition Clear() override
    {
        size_ = 0;
        return file_->Truncate();
    }
    std::error_condition Close() override { return file_->Close(); }
    std::error_condition Get(std::uint64_t const offset,
                             std::uint64_t const length,
                             std::string* value) const override
    {
        value->resize(length - value_offset);
        std::size_t bytesRead;
        std::error_condition err;
        std::tie(bytesRead, err) = file_->ReadAt(offset + value_offset, *value);
        if (err)
            return err;
        if (bytesRead < value->length())
            return make_error_condition(db_error::short_read);
        return std::error_condition();
    }

    std::error_condition Set(key_type const& key,
                             value_type const& value) override
    {
        assert(value.ReadyForWriting());
        auto length = value_offset + value.value.size();
        std::string str(length, '\0');
        size_t pos = 0;
        pos += string_replace<std::uint64_t>(length, pos, str);
        str.replace(pos, Bytes, util::ToBytes(key));
        pos += Bytes;
        str.replace(pos, value.value.size(), value.value);
        std::size_t bytesWritten;
        std::error_condition err;
        std::tie(bytesWritten, err) = file_->WriteAt(str, *value.offset);
        if (err)
            return err;
        if (bytesWritten != length)
            return make_error_condition(db_error::short_write);
        return std::error_condition();
    }

    std::error_condition Each(key_value_func f) const override
    {
        std::string str(1024 * 64, '\0');
        std::uint64_t filePosition = 0;
        std::string key, value;
        do
        {
            std::error_condition err;
            std::size_t bytesRead;
            std::tie(bytesRead, err) = file_->ReadAt(filePosition, str);
            if (err)
                return err;
            std::uint64_t length = 0;
            for (std::size_t pos = 0; pos < bytesRead;)
            {
                string_read<std::uint64_t>(str, pos, length);
                // tail case
                if (pos + length > bytesRead)
                {
                    break;
                }
                pos += sizeof(length);
                key.assign(str, pos, Bytes);
                pos += Bytes;
                auto valueLength = length - value_offset;
                value.assign(str, pos, valueLength);
                pos += valueLength;
                f(key, value);
                filePosition += length;
            }
        } while (filePosition < size_);
        return std::error_condition();
    }

    std::uint64_t Size() const { return size_; }
};
template <std::uint32_t BITS>
const std::size_t FileValueStore<BITS>::value_offset = util::MaxSize() +
                                                       sizeof(std::uint64_t);

template <std::uint32_t BITS>
class FileKeyStore : public KeyStore<BITS>
{
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
    FileKeyStore(std::uint32_t const block_size, file_type& file)
        : block_size_(block_size),
          degree_(node_type::CalculateDegree(block_size)),
          file_(std::move(file))
    {
    }
    FileKeyStore(const FileKeyStore&) = delete;
    FileKeyStore& operator=(const FileKeyStore&) = delete;

    std::error_condition Open() override
    {
        if (auto err = file_->Open())
            return err;
        return file_->Size(size_);
    }

    std::error_condition Clear() override
    {
        size_ = 0;
        return file_->Truncate();
    }

    std::error_condition Close() override { return file_->Close(); }

    node_ptr New(std::uint32_t const level, key_type const& first,
                 key_type const& last) override
    {
        size_ += block_size_;
        return std::make_shared<node_type>(size_ - block_size_, level, degree_,
                                           first, last);
    }

    node_result Get(std::uint64_t const id) const
    {
        std::string str;
        str.resize(block_size_);
        auto node = std::make_shared<node_type>(id, 0, degree_, 0, 1);
        std::size_t bytesRead;
        std::error_condition err;
        std::tie(bytesRead, err) = file_->ReadAt(id, str);
        if (err)
            return std::make_pair(node_ptr(), err);
        if (bytesRead == 0)
        {
            return std::make_pair(
                node_ptr(), make_error_condition(db_error::key_not_found));
        }
        if (bytesRead != block_size_)
            return std::make_pair(node_ptr(),
                                  make_error_condition(db_error::short_read));
        node->Read(str);
        return std::make_pair(node, std::error_condition());
    }

    std::error_condition Set(node_ptr const& node)
    {
        std::string str;
        str.resize(block_size_);
        node->Write(str);
        std::size_t bytesWritten;
        std::error_condition err;
        std::tie(bytesWritten, err) = file_->WriteAt(str, node->Id());
        if (err)
            return err;
        if (bytesWritten != block_size_)
            return make_error_condition(db_error::short_write);
        return std::error_condition();
    }

    std::uint64_t Size() const { return size_; }
};

template <std::uint32_t BITS>
struct FileStoragePolicy
{
    using KeyStorage = std::shared_ptr<KeyStore<BITS>>;
    using ValueStorage = std::shared_ptr<ValueStore<BITS>>;
    enum
    {
        Bits = BITS
    };
    static KeyStorage CreateKeyStore(std::string const& filename,
                                     std::uint32_t const blockSize)
    {
        // Put ifdef here!
        auto file = std::unique_ptr<RandomAccessFile>(
            std::make_unique<PosixRandomAccessFile>(filename));
        // endif
        return std::make_shared<FileKeyStore<BITS>>(blockSize, file);
    }
    static ValueStorage CreateValueStore(std::string const& filename)
    {
        // Put ifdef here!
        auto file = std::unique_ptr<RandomAccessFile>(
            std::make_unique<PosixRandomAccessFile>(filename));
        // endif
        return std::make_shared<FileValueStore<BITS>>(file);
    }
};

}  // namespace keyvadb
