#pragma once

#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/algorithm/hex.hpp>
#include <boost/bimap.hpp>
#include <boost/bimap/set_of.hpp>
#include <boost/bimap/multiset_of.hpp>
#include <boost/bimap/support/lambda.hpp>
#include <string>
#include <map>
#include <ostream>
#include <mutex>
#include <algorithm>
#include "db/key.h"
#include "db/error.h"

namespace keyvadb
{
template <std::uint32_t BITS>
class ValueStore;  // Forward Declaration

// A threadsafe container for storing keys, values and the offset of the key
// and
// values in the values file. A Value field might contain an empty offset or
// an
// empty string but never both. Values with empty strings are being evicted
// from
// a Node to one of its children, while a Value with an empty offset has not
// been yet assigned an offset in the values file. This complicated state is
// to
// facilitate write-buffering.
// The entries can be iterated in either key order, for when keys are being
// added to the tree, and value offset order, for when the keys and values
// are
// being streamed to the values file.
template <std::uint32_t BITS>
class Buffer
{
   public:
    using offset_type = boost::optional<std::uint64_t>;
    enum class ValueState : std::uint8_t
    {
        Unprocessed,
        Evicted,
        Duplicate,
        NeedsCommitting,
        Committed,
    };

    struct Value
    {
        std::uint64_t offset;
        std::string value;
        ValueState status;

        bool ReadyForWriting() const
        {
            return status == ValueState::NeedsCommitting;
        }

        // Disk format size including length and key
        std::uint64_t Size() const
        {
            return sizeof(std::uint64_t) + BITS / 8 + value.size();
        }
    };

    struct ValueComparer
    {
        bool operator()(Value const &lhs, Value const &rhs) const
        {
            if (lhs.status == rhs.status && lhs.offset == rhs.offset)
                return lhs.value < rhs.value;
            if (lhs.status == rhs.status)
                return lhs.offset < rhs.offset;
            return lhs.status < rhs.status;
        }
    };

   private:
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using value_type = boost::optional<std::string>;
    using map_type =
        boost::bimap<boost::bimaps::set_of<key_type>,
                     boost::bimaps::multiset_of<Value, ValueComparer>>;
    using const_iterator = typename map_type::left_const_iterator;
    using key_value_type = typename map_type::value_type;
    using left_key_value_type = typename map_type::left_value_type;
    using value_store_ptr = std::shared_ptr<ValueStore<BITS>>;

    static const std::string emptyBufferValue;
    static const std::map<ValueState, std::string> valueStates;

    map_type buf_;
    mutable std::mutex mtx_;

   public:
    std::size_t Add(std::string const &key, std::string const &value)
    {
        return add(util::FromBytes(key), 0, value, ValueState::Unprocessed);
    }

    std::size_t AddEvictee(key_type const &key, std::uint64_t const offset)
    {
        return add(key, offset, emptyBufferValue, ValueState::Evicted);
    }

    void SetDuplicate(key_type const &key)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = buf_.left.find(key);
        if (it != buf_.left.end())
            buf_.left.modify_data(it, boost::bimaps::_data = Value{
                                          it->second.offset, it->second.value,
                                          ValueState::Duplicate});
    }

    void SetOffset(key_type const &key, std::uint64_t const offset)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = buf_.left.find(key);
        if (it != buf_.left.end())
            buf_.left.modify_data(
                it, boost::bimaps::_data = Value{offset, it->second.value,
                                                 ValueState::NeedsCommitting});
    }

    value_type Get(std::string const &key) const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto v = buf_.left.find(util::FromBytes(key));
        if (v != buf_.left.end() && !v->second.value.empty())
            return v->second.value;
        return boost::none;
    }

    std::error_condition Commit(value_store_ptr const &values,
                                std::size_t const batchSize)
    {
        while (true)
        {
            // std::cout << *this;
            std::lock_guard<std::mutex> lock(mtx_);

            for (std::size_t i = 0; i < batchSize; i++)
            {
                auto it = buf_.right.lower_bound(
                    Value{0, emptyBufferValue, ValueState::NeedsCommitting});
                if (it == buf_.right.end() ||
                    it->first.status != ValueState::NeedsCommitting)
                    return std::error_condition();
                if (auto err = values->Set(it->second, it->first))
                    return err;

                // std::cout << "Committing: " << i << ":"
                //           << valueStates.at(it->first.status) << ":"
                //           << util::ToHex(it->second) << ":" << buf_.size()
                //           << std::endl;
                auto modified = buf_.right.modify_key(
                    it, boost::bimaps::_key =
                            Value{it->first.offset, it->first.value,
                                  ValueState::Committed});

                if (!modified)
                    return make_error_condition(db_error::bad_commit);
            }
        }
    }

    void Purge()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = buf_.right.lower_bound(
            Value{0, emptyBufferValue, ValueState::Evicted});
        buf_.right.erase(it, buf_.right.end());
    }

    // Returns all
    std::set<KeyValue<BITS>> GetRange(key_type const &firstKey,
                                      key_type const &lastKey)
    {
        std::set<KeyValue<BITS>> range;
        std::lock_guard<std::mutex> lock(mtx_);
        std::for_each(lower(firstKey), upper(lastKey),
                      [&range](left_key_value_type const &kv)
                      {
            range.emplace(KeyValue<BITS>{
                kv.first, 0, std::uint32_t(kv.second.value.size())});
        });
        return range;
    }

    void Clear() { buf_.clear(); }

    std::size_t Size() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        return buf_.size();
    }

    std::size_t ReadyForCommitting() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto first = buf_.right.lower_bound(
            Value{0, emptyBufferValue, ValueState::NeedsCommitting});
        auto last = buf_.right.lower_bound(
            Value{0, emptyBufferValue, ValueState::Committed});
        return std::distance(first, last);
    }

    // Returns true if there are values greater than first and less than
    // last
    bool ContainsRange(key_type const &first, key_type const &last) const
    {
        if (first > last)
            throw std::invalid_argument("First must not be greater than last");
        std::lock_guard<std::mutex> lock(mtx_);
        return lower(first) != upper(last);
    }

    friend std::ostream &operator<<(std::ostream &stream, const Buffer &buffer)
    {
        stream << "Buffer" << std::endl;
        std::lock_guard<std::mutex> lock(buffer.mtx_);
        // for (auto const &kv : buffer.buf_)
        //     stream << util::ToHex(kv.left) << ":" << kv.right.offset << ":"
        //            << valueStates.at(kv.right.status) << ":" <<
        //            kv.right.Size()
        //            << std::endl;
        // stream << "--------" << std::endl;
        for (auto it = buffer.buf_.right.begin(); it != buffer.buf_.right.end();
             ++it)
            stream << util::ToHex(it->second) << ":" << it->first.offset << ":"
                   << valueStates.at(it->first.status) << ":"
                   << it->first.Size() << std::endl;
        stream << "--------" << std::endl;
        return stream;
    }

   private:
    std::size_t add(key_type const &key, std::uint64_t const &offset,
                    std::string const &value, ValueState const &status)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (buf_.left.find(key) == buf_.left.end())
            buf_.insert(key_value_type(key, Value{offset, value, status}));
        return buf_.size();
    }

    const_iterator lower(key_type const &first) const
    {
        return buf_.left.upper_bound(first);
    }

    const_iterator upper(key_type const &last) const
    {
        return buf_.left.lower_bound(last);
    }
};

template <std::uint32_t BITS>
const std::string Buffer<BITS>::emptyBufferValue;

template <std::uint32_t BITS>
const std::map<typename Buffer<BITS>::ValueState, std::string>
    Buffer<BITS>::valueStates{
        {Buffer<BITS>::ValueState::Unprocessed, "Unprocessed"},
        {Buffer<BITS>::ValueState::Evicted, "Evicted"},
        {Buffer<BITS>::ValueState::Duplicate, "Duplicate"},
        {Buffer<BITS>::ValueState::NeedsCommitting, "NeedsCommitting"},
        {Buffer<BITS>::ValueState::Committed, "Committed"},
    };

}  // namespace keyvadb
