#pragma once

#include <boost/bimap.hpp>
#include <boost/bimap/list_of.hpp>
#include <boost/bimap/set_of.hpp>
#include <utility>
#include <mutex>
#include "db/key.h"

namespace keyvadb {

template <std::uint32_t BITS>
class NodeCache {
  using util = detail::KeyUtil<BITS>;
  using key_type = typename util::key_type;
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using key_level_pair = std::pair<key_type, std::uint32_t>;
  using index_type = boost::bimaps::bimap<boost::bimaps::set_of<key_level_pair>,
                                          boost::bimaps::list_of<node_ptr>>;
  using index_value = typename index_type::value_type;

 private:
  std::size_t maxSize_;
  index_type nodes_;
  std::mutex lock_;

 public:
  explicit NodeCache(std::size_t maxSize) : maxSize_(maxSize) {}

  void Add(node_ptr const& node) {
    std::lock_guard<std::mutex> lock(lock_);
    auto it = nodes_.left.find(key_level_pair{node->First(), node->Level()});
    if (it != nodes_.left.end()) {
      nodes_.right.relocate(nodes_.right.end(), nodes_.project_right(it));
    } else {
      nodes_.insert(index_value({node->First(), node->Level()}, node));
      nodes_.right.erase(nodes_.right.begin());
    }
  }

  node_ptr Get(key_type const& key) {
    node_ptr node;
    std::lock_guard<std::mutex> lock(lock_);
    auto it = nodes_.left.lower_bound(key_level_pair{key, 0});
    for (; it != nodes_.left.end(); ++it) {
      if (it->second->First() > key || it->second->Last() < key)
        break;
      node = it->second;
    }
    if (node) {
      it--;
      nodes_.right.relocate(nodes_.right.end(), nodes_.project_right(it));
    }
    return node;
  }
};
}  // namespace keyvadb
