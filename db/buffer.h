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

namespace keyvadb
{
// A threadsafe container for storing keys, values and the offset of the key and
// values in the values file. A Value field might contain an empty offset or an
// empty string but never both. Values with empty strings are being evicted from
// a Node to one of its children, while a Value with an empty offset has not
// been yet assigned an offset in the values file. This complicated state is to
// facilitate write-buffering.
// The entries can be iterated in either key order, for when keys are being
// added to the tree, and value offset order, for when the keys and values are
// being streamed to the values file.
template <std::uint32_t BITS>
class Buffer
{
   private:
    using offset_type = boost::optional<std::uint64_t>;

    enum class ValueState : std::uint8_t
    {
        Unprocessed,
        Evicted,
        Duplicated,
        NeedsComitting,
    };

    struct Value
    {
        offset_type offset;
        std::string value;
        ValueState status;
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

    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using value_type = boost::optional<std::string>;
    using map_type =
        boost::bimap<boost::bimaps::set_of<key_type>,
                     boost::bimaps::multiset_of<Value, ValueComparer>>;
    using const_iterator = typename map_type::left_const_iterator;
    using key_value_type = typename map_type::value_type;
    using left_key_value_type = typename map_type::left_value_type;

    static const std::string emptyBufferValue;
    static const std::map<ValueState, std::string> valueStates;

    map_type buf_;
    mutable std::mutex mtx_;

   public:
    std::size_t Add(std::string const &key, std::string const &value)
    {
        return add(util::FromBytes(key), boost::none, value,
                   ValueState::Unprocessed);
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
                                          ValueState::Duplicated});
    }

    void SetOffset(key_type const &key, offset_type const offset)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = buf_.left.find(key);
        if (it != buf_.left.end())
            buf_.left.modify_data(
                it, boost::bimaps::_data = Value{offset, it->second.value,
                                                 ValueState::NeedsComitting});
    }

    value_type Get(std::string const &key) const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto v = buf_.left.find(util::FromBytes(key));
        if (v != buf_.left.end() && !v->second.value.empty())
            return v->second.value;
        return boost::none;
    }

    void Purge()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        offset_type emptyOffset;
        auto it = buf_.right.lower_bound(
            Value{boost::none, emptyBufferValue, ValueState::Evicted});
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
            range.emplace(KeyValue<BITS>{kv.first, 0});
        });
        return range;
    }

    void Clear() { buf_.clear(); }

    std::size_t Size() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        return buf_.size();
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
        for (auto const &kv : buffer.buf_)
            stream << util::ToHex(kv.left) << ":" << kv.right.offset << ":"
                   << boost::algorithm::hex(kv.right.value) << ":"
                   << valueStates.at(kv.right.status) << std::endl;
        stream << "--------" << std::endl;
        for (auto it = buffer.buf_.right.begin(); it != buffer.buf_.right.end();
             ++it)
            stream << util::ToHex(it->second) << ":" << it->first.offset << ":"
                   << boost::algorithm::hex(it->first.value) << ":"
                   << valueStates.at(it->first.status) << std::endl;
        stream << "--------" << std::endl;
        return stream;
    }

   private:
    std::size_t add(key_type const &key, offset_type const &offset,
                    std::string const &value, ValueState const &status)
    {
        std::lock_guard<std::mutex> lock(mtx_);
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
        {Buffer<BITS>::ValueState::Duplicated, "Duplicated"},
        {Buffer<BITS>::ValueState::NeedsComitting, "NeedsComitting"},
    };

}  // namespace keyvadb
