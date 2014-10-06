#pragma once

#include <cstdint>
#include <cstddef>
#include <map>
#include "db/key.h"
#include "db/store.h"
#include "db/delta.h"

namespace keyvadb {

template <std::uint32_t BITS>
class Journal {
  using delta_type = Delta<BITS>;
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using key_store_ptr = std::shared_ptr<KeyStore<BITS>>;

 private:
  std::multimap<std::uint32_t, delta_type> deltas_;

 public:
  void Add(std::uint32_t const level, delta_type const& delta) {
    deltas_.emplace(level, delta);
  }

  void Commit(key_store_ptr const& store) {
    // write deepest nodes first so that no parent can refer
    // to a non-existent child
    for (auto it = crbegin(deltas_), end = crend(deltas_); it != end; ++it)
      store->Set(it->second.Current());
  }

  constexpr std::size_t Size() const { return deltas_.size(); }

  std::uint64_t TotalInsertions() const {
    std::uint64_t total = 0;
    for (auto const& kv : deltas_) total += kv.second.Insertions();
    return total;
  }

  friend std::ostream& operator<<(std::ostream& stream,
                                  const Journal& journal) {
    for (auto const& kv : journal.deltas_) {
      std::cout << "Level: " << std::setw(3) << kv.first << " " << kv.second
                << std::endl;
    }

    return stream;
  }
};

template <std::uint32_t BITS>
std::unique_ptr<Journal<BITS>> MakeJournal() {
  return std::make_unique<Journal<BITS>>();
}
}  // namespace keyvadb
