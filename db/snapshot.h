#pragma once

#include <cstdint>
#include <cstddef>
#include <set>
#include "db/key.h"

namespace keyvadb
{
template <uint32_t BITS>
struct Snapshot
{
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using key_value_type = KeyValue<BITS>;
    using snapshot_func = std::function<void(key_value_type const)>;
    using const_iterator = typename std::set<key_value_type>::const_iterator;

    std::set<key_value_type> keys;

    // return true if KeyValue did not already exist
    bool Add(const key_type& key, const uint64_t value)
    {
        return keys.emplace(key_value_type{key, value}).second;
    }

    constexpr std::size_t Size() const { return keys.size(); }

    // Returns true if there are values greater than first and less than last
    constexpr bool ContainsRange(const key_type& first,
                                 const key_type& last) const
    {
        return CountRange(first, last) > 0;
    }

    size_t CountRange(const key_type& first, const key_type& last) const
    {
        if (first > last)
            throw std::invalid_argument("First must not be greater than last");
        return std::distance(Lower(first), Upper(last));
    }

    constexpr const_iterator Lower(const key_type& first) const
    {
        return keys.upper_bound(key_value_type{first, 0});
    }

    constexpr const_iterator Upper(const key_type& last) const
    {
        return keys.lower_bound(key_value_type{last, 0});
    }

    friend std::ostream& operator<<(std::ostream& stream,
                                    const Snapshot& snapshot)
    {
        stream << "Snapshot" << std::endl;
        for (auto const& kv : snapshot.keys) stream << kv << std::endl;
        stream << "--------" << std::endl;
        return stream;
    }
};

}  // namespace keyvadb
