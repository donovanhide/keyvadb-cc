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
#include "db/key.h"

namespace keyvadb
{
template <std::uint32_t BITS>
class BufferV2
{
   private:
    using offset_type = boost::optional<std::uint64_t>;

    struct Value
    {
        offset_type offset;
        std::string value;
    };

    struct ValueComparer
    {
        bool operator()(Value const &lhs, Value const &rhs) const
        {
            if (lhs.offset == rhs.offset)
                return lhs.value < rhs.value;
            return lhs.offset < rhs.offset;
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

    map_type buf_;

   public:
    std::size_t Add(std::string const &key, std::string const &value,
                    offset_type const offset = boost::none)
    {
        buf_.insert(key_value_type(util::FromBytes(key), Value{offset, value}));
        return buf_.size();
    }

    value_type Get(std::string const &key) const
    {
        auto v = buf_.find(util::FromBytes(key));
        if (v != buf_.end())
            return v->second.value;
        return boost::none;
    }

    bool SetOffset(key_type const &key, offset_type const offset)
    {
        auto it = buf_.left.find(key);
        if (it == buf_.left.end())
            return false;
        return buf_.left.modify_data(
            it, boost::bimaps::_data = Value{offset, it->second.value});
    }

    // Returns all
    std::set<KeyValue<BITS>> GetRange(key_type const &firstKey,
                                      key_type const &lastKey)
    {
        std::set<KeyValue<BITS>> range;
        for (auto it = Lower(firstKey), last = Upper(lastKey); it != last; it++)
        {
            if (it->second.offset)
                range.emplace(KeyValue<BITS>{it->first, *it->second.offset});
        }
        return range;
    }

    std::size_t Size() const { return buf_.size(); }

    // Returns true if there are values greater than first and less than
    // last
    bool ContainsRange(key_type const &first, key_type const &last) const
    {
        return CountRange(first, last) > 0;
    }

    size_t CountRange(key_type const &first, key_type const &last) const
    {
        if (first > last)
            throw std::invalid_argument("First must not be greater than last");
        return std::distance(Lower(first), Upper(last));
    }

    friend std::ostream &operator<<(std::ostream &stream,
                                    const BufferV2 &buffer)
    {
        stream << "Buffer" << std::endl;
        for (auto const &kv : buffer.buf_)
            stream << util::ToHex(kv.left) << ":" << kv.right.offset << ":"
                   << boost::algorithm::hex(kv.right.value) << std::endl;
        stream << "--------" << std::endl;
        for (auto it = buffer.buf_.right.begin(); it != buffer.buf_.right.end();
             ++it)
            stream << util::ToHex(it->second) << ":" << it->first.offset << ":"
                   << boost::algorithm::hex(it->first.value) << std::endl;
        stream << "--------" << std::endl;
        return stream;
    }

   private:
    const_iterator Lower(key_type const &first) const
    {
        return buf_.left.upper_bound(first);
    }

    const_iterator Upper(key_type const &last) const
    {
        return buf_.left.lower_bound(last);
    }
};
}  // namespace keyvadb
