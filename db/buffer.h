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

// A threadsafe container for storing keys and values for the period before they
// are committed to disk.
template <std::uint32_t BITS>
class Buffer
{
   public:
    using offset_type = boost::optional<std::uint64_t>;
    enum class ValueState : std::uint8_t
    {
        Unprocessed,
        Evicted,
        NeedsCommitting,
        Committed,
    };

    struct Value
    {
        std::uint64_t offset;
        std::uint32_t length;
        std::string value;
        ValueState status;

        bool ReadyForWriting() const
        {
            return status == ValueState::NeedsCommitting;
        }

        // Disk format size including length and key
        std::uint64_t Size() const
        {
            return sizeof(std::uint64_t) + BITS / 8 + length;
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
    using left_value_type = typename map_type::left_value_type;
    using value_store_ptr = std::shared_ptr<ValueStore<BITS>>;
    using candidate_type = std::set<KeyValue<BITS>>;

    static const std::string emptyBufferValue;
    static const std::map<ValueState, std::string> valueStates;

    map_type buf_;
    mutable std::mutex mtx_;

   public:
    value_type Get(std::string const &key) const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto v = buf_.left.find(util::FromBytes(key));
        if (v != buf_.left.end() && v->second.status != ValueState::Evicted)
            // An Evicted key won't have an associated value
            return v->second.value;
        return boost::none;
    }

    std::size_t Add(std::string const &key, std::string const &value)
    {
        auto k = util::FromBytes(key);
        std::lock_guard<std::mutex> lock(mtx_);
        if (buf_.left.find(k) == buf_.left.end())
        {
            // Don't overwrite an existing key that might not be Unprocessed
            auto v = left_value_type(k, Value{0, std::uint32_t(value.length()),
                                              value, ValueState::Unprocessed});
            buf_.left.insert(v);
        }
        return buf_.size();
    }

    std::size_t AddEvictee(key_type const &key, std::uint64_t const offset,
                           std::uint32_t const length)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        assert(buf_.left.find(key) == buf_.left.end());
        auto v = left_value_type(
            key, Value{offset, length, emptyBufferValue, ValueState::Evicted});
        buf_.left.insert(v);
        return buf_.size();
    }

    void RemoveDuplicate(key_type const &key)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        buf_.left.erase(key);
    }

    void SetOffset(key_type const &key, std::uint64_t const offset)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = buf_.left.find(key);
        assert(it != buf_.left.end());
        auto modified = buf_.left.modify_data(
            it, boost::bimaps::_data =
                    Value{offset, it->second.length, it->second.value,
                          ValueState::NeedsCommitting});
        assert(modified);
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
                auto it = first(ValueState::NeedsCommitting);
                if (it == buf_.right.end() ||
                    it->first.status != ValueState::NeedsCommitting)
                    return std::error_condition();
                if (auto err = values->Set(it->second, it->first))
                    return err;
                auto modified = buf_.right.modify_key(
                    it, boost::bimaps::_key =
                            Value{it->first.offset, it->first.length,
                                  it->first.value, ValueState::Committed});
                if (!modified)
                    throw std::runtime_error("Bad Commit");
            }
        }
    }

    void Purge()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto check = first(ValueState::NeedsCommitting);
        assert(check == buf_.right.end() ||
               check->first.status != ValueState::NeedsCommitting);
        buf_.right.erase(first(ValueState::Evicted), buf_.right.end());
    }

    void GetCandidates(key_type const &firstKey, key_type const &lastKey,
                       candidate_type &candidates, candidate_type &evictions)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        std::for_each(lower(firstKey), upper(lastKey),
                      [&candidates, &evictions](left_value_type const &kv)
                      {
            if (kv.second.status == ValueState::Unprocessed)
                candidates.emplace(KeyValue<BITS>{kv.first, kv.second.offset,
                                                  kv.second.length});
            else if (kv.second.status == ValueState::Evicted)
                evictions.emplace(KeyValue<BITS>{kv.first, kv.second.offset,
                                                 kv.second.length});
        });
    }

    // Returns true if there are values greater than first and less than
    // last
    bool ContainsRange(key_type const &first, key_type const &last) const
    {
        assert(first <= last);
        std::lock_guard<std::mutex> lock(mtx_);
        return std::any_of(lower(first), upper(last),
                           [](left_value_type const &kv)
                           {
            return kv.second.status == ValueState::Unprocessed ||
                   kv.second.status == ValueState::Evicted;
        });
    }

    void Clear()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        buf_.clear();
    }

    std::size_t Size() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        return buf_.size();
    }

    std::size_t ReadyForCommitting() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        return std::distance(first(ValueState::NeedsCommitting),
                             first(ValueState::Committed));
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
        for (auto const &v : buffer.buf_.right)
            stream << util::ToHex(v.second) << ":" << v.first.offset << ":"
                   << v.first.length << ":" << valueStates.at(v.first.status)
                   << ":" << v.first.Size() << std::endl;
        stream << "--------" << std::endl;
        return stream;
    }

   private:
    typename map_type::left_const_iterator lower(key_type const &first) const
    {
        return buf_.left.upper_bound(first);
    }

    typename map_type::left_const_iterator upper(key_type const &last) const
    {
        return buf_.left.lower_bound(last);
    }

    typename map_type::right_const_iterator first(ValueState state) const
    {
        return buf_.right.lower_bound(Value{0, 0, emptyBufferValue, state});
    }

    typename map_type::right_iterator first(ValueState state)
    {
        return buf_.right.lower_bound(Value{0, 0, emptyBufferValue, state});
    }
};

template <std::uint32_t BITS>
const std::string Buffer<BITS>::emptyBufferValue;

template <std::uint32_t BITS>
const std::map<typename Buffer<BITS>::ValueState, std::string>
    Buffer<BITS>::valueStates{
        {Buffer<BITS>::ValueState::Unprocessed, "Unprocessed"},
        {Buffer<BITS>::ValueState::Evicted, "Evicted"},
        {Buffer<BITS>::ValueState::NeedsCommitting, "NeedsCommitting"},
        {Buffer<BITS>::ValueState::Committed, "Committed"},
    };

}  // namespace keyvadb
