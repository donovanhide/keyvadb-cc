#pragma once
#include <cstdint>
#include <cstddef>
#include <map>
#include "key.h"
#include "store.h"

template <std::uint32_t BITS>
struct NodeDelta {
  using node_ptr = std::shared_ptr<Node<BITS>>;
  std::uint32_t level;
  std::uint64_t insertions;
  std::uint64_t evictions;
  node_ptr current;
  node_ptr previous;
};

template <std::uint32_t BITS>
class Journal {
  using node_ptr = std::shared_ptr<Node<BITS>>;
  using key_store_ptr = std::shared_ptr<KeyStore<BITS>>;

 private:
  std::multimap<std::uint32_t, node_ptr> nodes_;
  std::map<std::uint32_t, std::int64_t> keys_;

 public:
  void Add(std::uint32_t const level, node_ptr const& node,
           std::uint64_t const keys) {
    nodes_.emplace(level, node);
    keys_[level] += keys;
  }

  void Commit(key_store_ptr store) {
    // write deepest nodes first so that no parent can refer
    // to a non-existent child
    for (auto it = std::crbegin(nodes_), end = std::crend(nodes_); it != end;
         ++it)
      store->Set(it->second);
  }

  std::size_t Size() { return nodes_.size(); }

  friend std::ostream& operator<<(std::ostream& stream,
                                  const Journal& journal) {
    std::uint64_t totalNodes = 0;
    std::int64_t totalKeys = 0;
    for (auto const& kv : journal.keys_) {
      auto nodeCount = journal.nodes_.count(kv.first);
      totalKeys += kv.second;
      totalNodes += nodeCount;
      std::cout << "Level: " << std::setw(3) << kv.first;
      std::cout << "\tKeys:" << std::setw(10) << kv.second;
      std::cout << "\tNodes:" << std::setw(10) << nodeCount << std::endl;
    }
    std::cout << "Total:\t\tKeys:" << std::setw(10) << totalKeys;
    std::cout << "\tNodes:" << std::setw(10) << totalNodes << std::endl;
    return stream;
  }
};

template <std::uint32_t BITS>
std::unique_ptr<Journal<BITS>> MakeJournal() {
  return std::make_unique<Journal<BITS>>();
}
