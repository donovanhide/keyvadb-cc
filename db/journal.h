#pragma once
#include <cstdint>
#include <cstddef>
#include <map>
#include "key.h"
#include "store.h"

template <std::uint32_t BITS>
class Journal {
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using key_store_ptr = std::shared_ptr<KeyStore<BITS>>;

 private:
  std::multimap<std::size_t, node_ptr> insertions_;

 public:
  void Add(std::size_t const level, const node_ptr& node) {
    insertions_.emplace(level, node);
  }

  void Commit(key_store_ptr store) {
    // write deepest nodes first so that no parent can refer
    // to a non-existent child
    for (auto it = insertions_.crbegin(), end = insertions_.crend(); it != end;
         ++it)
      store->Set(it->second);
  }

  std::size_t Size() { return insertions_.size(); }

  friend std::ostream& operator<<(std::ostream& stream,
                                  const Journal& journal) {
    std::size_t level = 0;
    std::size_t count = 0;
    for (auto const& kv : journal.insertions_) {
      if (kv.first == level) {
        count++;
      } else {
        if (count > 0)
          stream << "Level:\t" << level << " Count:\t" << count << std::endl;
        level = kv.first;
        count = 0;
      }
    }
    return stream;
  }
};

template <std::uint32_t BITS>
std::unique_ptr<Journal<BITS>> MakeJournal() {
  return std::make_unique<Journal<BITS>>();
}
