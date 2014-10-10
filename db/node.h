#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <system_error>
#include <algorithm>
#include "db/key.h"
#include "db/encoding.h"
#include "db/snapshot.h"
#include "db/journal.h"

namespace keyvadb {

static const std::uint64_t EmptyChild = 0;

// Node invariants:
// 1. keys must always be in sorted order,lowest to highest
// 2. each key is unique, not including zero
// 3. first_ must be lower than last_
// 4. each non-zero key must be greater than first_ and less than last_
// 5. no children must exist unless all keys are populated
template <std::uint32_t BITS>
class Node {
 public:
  using key_type = Key<BITS>;
  using key_value_type = KeyValue<BITS>;
  using key_values_type = std::vector<key_value_type>;
  using children_type = std::vector<std::uint64_t>;
  using child_func =
      std::function<std::error_code(const std::size_t, const key_type&,
                                    const key_type&, const std::uint64_t)>;
  using snapshot_ptr = std::unique_ptr<Snapshot<BITS>>;
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using iterator = typename key_values_type::iterator;
  using const_iterator = typename key_values_type::const_iterator;

 private:
  std::uint64_t id_;
  std::uint32_t degree_;
  key_type first_;
  key_type last_;
  children_type children_;

 public:
  key_values_type keys;

  Node(std::uint64_t const id, std::uint32_t degree, key_type const& first,
       key_type const& last)
      : id_(id),
        degree_(degree),
        first_(first),
        last_(last),
        children_(degree),
        keys(degree - 1) {
    if (first >= last)
      throw std::domain_error("first must be lower than last:" + ToHex(first) +
                              " " + ToHex(last));
  }

  std::size_t Write(std::string& str) const {
    size_t pos = 0;
    pos += WriteBytes(first_, pos, str);
    pos += WriteBytes(last_, pos, str);
    for (auto const& kv : keys) {
      pos += WriteBytes(kv.key, pos, str);
      pos += string_replace<std::uint64_t>(kv.value, pos, str);
    }
    for (auto const& cid : children_)
      pos += string_replace<std::uint64_t>(cid, pos, str);
    return pos;
  }

  std::size_t Read(std::string const& str) {
    size_t pos = 0;
    size_t length = MaxSize<BITS>();
    pos += ReadBytes(str, pos, length, first_);
    pos += ReadBytes(str, pos, length, last_);
    for (auto& kv : keys) {
      pos += ReadBytes(str, pos, length, kv.key);
      pos += string_read<std::uint64_t>(str, pos, kv.value);
    }
    for (auto& cid : children_)
      pos += string_read<std::uint64_t>(str, pos, cid);
    return pos;
  }

  std::uint64_t AddSyntheticKeyValues() {
    auto const stride = Stride();
    auto cursor = first_ + stride;
    std::uint64_t count = 0;
    for (auto& key : keys) {
      if (key.IsZero()) {
        key = key_value_type{cursor, SyntheticValue};
        count++;
      }
      cursor += stride;
    }
    return count;
  }

  void Clear() {
    std::fill(keys.begin(), keys.end(), key_value_type{0, EmptyValue});
  }

  void SetChild(std::size_t const i, std::uint64_t const cid) {
    children_.at(i) = cid;
  }

  constexpr std::uint64_t GetChild(std::size_t const i) const {
    return children_.at(i);
  }

  std::error_code EachChild(child_func f) const {
    auto length = Degree();
    for (std::size_t i = 0; i < length; i++) {
      if (i == 0) {
        if (!keys.at(i).IsZero())
          if (auto err = f(i, first_, keys.at(i).key, children_.at(i)))
            return err;
      } else if (i == length - 1) {
        if (!keys.at(i - 1).IsZero())
          if (auto err = f(i, keys.at(i - 1).key, last_, children_.at(i)))
            return err;
      } else {
        if (!keys.at(i - 1).IsZero() && !keys.at(i).IsZero())
          if (auto err =
                  f(i, keys.at(i - 1).key, keys.at(i).key, children_.at(i)))
            return err;
      }
    }
    return std::error_code();
  }
  constexpr std::uint64_t Find(key_type const& key) const {
    auto found = std::find_if(
        keys.cbegin(), keys.cend(),
        [&key](key_value_type const& kv) { return kv.key == key; });
    return (found != keys.cend()) ? found->value : EmptyValue;
  }

  constexpr key_value_type GetKeyValue(std::size_t const i) const {
    return keys.at(i);
  }

  void SetKeyValue(std::size_t const i, key_value_type const& kv) {
    keys.at(i) = kv;
  }

  constexpr bool IsSane() const {
    if (first_ >= last_) return false;
    if (!std::is_sorted(keys.cbegin(), keys.cend())) return false;
    for (std::size_t i = 1; i < Degree() - 1; i++) {
      if (!keys.at(i).IsZero() && keys.at(i) == keys.at(i - 1)) return false;
      if (!keys.at(i).IsZero() &&
          (keys.at(i).key <= first_ || keys.at(i).key >= last_))
        return false;
    }
    if (EmptyKeyCount() > 0 && EmptyChildCount() != Degree()) return false;
    return true;
  }

  constexpr std::uint64_t Id() const { return id_; }
  constexpr key_type First() const { return first_; }
  constexpr key_type Last() const { return last_; }

  constexpr const_iterator NonZeroBegin() const {
    return std::find_if_not(
        keys.cbegin(), keys.cend(),
        [](key_value_type const& kv) { return kv.IsZero(); });
  }
  constexpr bool Empty() const { return EmptyKeyCount() == MaxKeys(); }

  constexpr std::size_t NonSyntheticKeyCount() const {
    return std::count_if(keys.cbegin(), keys.cend(),
                         [](key_value_type const& kv) {
      return !kv.IsZero() && !kv.IsSynthetic();
    });
  }
  constexpr std::size_t NonEmptyKeyCount() const {
    return std::distance(NonZeroBegin(), keys.cend());
  }
  constexpr std::size_t EmptyKeyCount() const {
    return std::distance(keys.cbegin(), NonZeroBegin());
  }
  constexpr std::size_t EmptyChildCount() const {
    return std::count(children_.cbegin(), children_.cend(), EmptyChild);
  }
  constexpr std::size_t MaxKeys() const { return keys.size(); }
  constexpr std::size_t Degree() const { return children_.size(); }
  constexpr key_type Distance() const {
    return keyvadb::Distance(first_, last_);
  }
  constexpr key_type Stride() const {
    return keyvadb::Stride(first_, last_, Degree());
  }

  friend std::ostream& operator<<(std::ostream& stream, const Node& node) {
    stream << "Id:\t\t" << node.id_ << std::endl;
    stream << "Keys:\t\t" << node.MaxKeys() - node.EmptyKeyCount() << std::endl;
    stream << "Children:\t" << node.Degree() - node.EmptyChildCount()
           << std::endl;
    stream << "First:\t\t" << ToHex(node.first_) << std::endl;
    stream << "Last:\t\t" << ToHex(node.last_) << std::endl;
    stream << "Stride:\t\t" << ToHex(node.Stride()) << std::endl;
    stream << "Distance:\t" << ToHex(node.Distance()) << std::endl;
    stream << "--------" << std::endl;
    for (std::size_t i = 0; i < node.MaxKeys(); i++) {
      stream << std::setfill('0') << std::setw(3) << i << " ";
      stream << ToHex(node.keys.at(i).key) << " ";
      if (node.keys.at(i).value == SyntheticValue) {
        stream << "Synthetic"
               << " ";
      } else {
        stream << node.keys.at(i).value << " ";
      }
      stream << node.children_.at(i) << " " << node.children_.at(i + 1);
      stream << std::endl;
    }
    stream << "--------" << std::endl;
    return stream;
  }
};

}  // namespace keyvadb
