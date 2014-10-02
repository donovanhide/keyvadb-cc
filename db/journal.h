#pragma once
#include <cstdint>
#include <cstddef>
#include <map>
#include "key.h"
#include "store.h"
#include "delta.h"

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

  void Commit(key_store_ptr store) {
    // write deepest nodes first so that no parent can refer
    // to a non-existent child
    for (auto it = std::crbegin(deltas_), end = std::crend(deltas_); it != end;
         ++it)
      store->Set(it->second.Current());
  }

  std::size_t Size() { return deltas_.size(); }

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
