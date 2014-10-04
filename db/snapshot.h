#pragma once

#include <cstdint>
#include <cstddef>
#include <set>
#include "db/key.h"

namespace keyvadb {

template <uint32_t BITS>
class Snapshot {
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;
  using snapshot_func = std::function<void(key_value_type const)>;
  using const_iterator = typename std::set<key_value_type>::const_iterator;

 private:
  std::set<key_value_type> set_;

 public:
  // return true if KeyValue did not already exist
  bool Add(const key_type& key, const uint64_t value) {
    return set_.emplace(key_value_type{key, value}).second;
  }

  constexpr std::size_t Size() const { return set_.size(); }

  // Returns true if there are values greater than first and less than last
  constexpr bool ContainsRange(const key_type& first,
                               const key_type& last) const {
    return CountRange(first, last) > 0;
  }

  constexpr size_t CountRange(const key_type& first,
                              const key_type& last) const {
    return std::distance(Lower(first), Upper(last));
  }

  constexpr const_iterator Lower(const key_type& first) const {
    return set_.upper_bound(key_value_type{first, 0});
  }

  constexpr const_iterator Upper(const key_type& last) const {
    return set_.lower_bound(key_value_type{last, 0});
  }

  constexpr const_iterator begin() const { return std::cbegin(set_); }
  constexpr const_iterator end() const { return std::cend(set_); }

  friend std::ostream& operator<<(std::ostream& stream,
                                  const Snapshot& snapshot) {
    stream << "Snapshot" << std::endl;
    for (auto const& kv : snapshot.set_) stream << kv << std::endl;
    stream << "--------" << std::endl;
    return stream;
  }
};

}  // namespace keyvadb
