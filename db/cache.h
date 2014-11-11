#pragma once

#include <boost/bimap.hpp>
#include <boost/bimap/list_of.hpp>
#include <boost/bimap/set_of.hpp>
#include <utility>
#include <algorithm>
#include <mutex>
#include "db/key.h"

namespace keyvadb
{
template <std::uint32_t BITS>
class NodeCache
{
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using node_type = Node<BITS>;
    using node_ptr = std::shared_ptr<node_type>;
    using key_level_pair = std::pair<std::uint32_t, key_type>;
    struct compare_kl
    {
        bool operator()(key_level_pair const& lhs,
                        key_level_pair const& rhs) const
        {
            if (lhs.first == rhs.first)
                return lhs.second < rhs.second;
            return lhs.first > rhs.first;
        }
    };
    using index_type =
        boost::bimaps::bimap<boost::bimaps::set_of<key_level_pair, compare_kl>,
                             boost::bimaps::list_of<node_ptr>>;
    using index_value = typename index_type::value_type;

   private:
    std::uint64_t maxSize_ = 0;
    std::uint64_t hits_ = 0;
    std::uint64_t misses_ = 0;
    std::uint64_t inserts_ = 0;
    std::uint64_t updates_ = 0;
    index_type nodes_;
    std::mutex lock_;

   public:
    void SetMaxSize(std::uint64_t maxSize)
    {
        std::lock_guard<std::mutex> lock(lock_);
        maxSize_ = maxSize;
    }

    void Reset()
    {
        std::lock_guard<std::mutex> lock(lock_);
        hits_ = 0;
        misses_ = 0;
        inserts_ = 0;
        updates_ = 0;
        nodes_.clear();
    }

    void Add(node_ptr const& node)
    {
        if (maxSize_ == 0)
            return;
        std::lock_guard<std::mutex> lock(lock_);
        auto it =
            nodes_.left.find(key_level_pair{node->Level(), node->First()});
        if (it != nodes_.left.end())
        {
            updates_++;
            it->second = node;
            nodes_.right.relocate(nodes_.right.end(), nodes_.project_right(it));
        }
        else
        {
            inserts_++;
            nodes_.insert(index_value({node->Level(), node->First()}, node));
            if (nodes_.size() > maxSize_)
                nodes_.right.erase(nodes_.right.begin());
        }
    }

    // Get node lowest in the tree by checking deepest nodes in the cache first.
    // Key 0000...0000 will always return a null node_ptr.
    node_ptr Get(key_type const& key)
    {
        using util = detail::KeyUtil<BITS>;
        std::lock_guard<std::mutex> lock(lock_);
        if (maxSize_ == 0 || nodes_.size() == 0)
            return node_ptr();
        auto level = nodes_.left.begin()->first.first + 1;
        for (; level > 0; level--)
        {
            auto it = nodes_.left.upper_bound(key_level_pair{level, key});
            if (it != nodes_.left.begin())
                it--;
            if (it->second->Level() > level)
                break;
            if (it->second->First() < key && it->second->Last() > key)
            {
                hits_++;
                nodes_.right.relocate(nodes_.right.end(),
                                      nodes_.project_right(it));
                return it->second;
            }
        }
        misses_++;
        // std::cout << "Miss:" << util::ToHex(key) << std::endl;
        return node_ptr();
    }

    friend std::ostream& operator<<(std::ostream& stream, NodeCache& cache)
    {
        std::lock_guard<std::mutex> lock(cache.lock_);
        stream << "Size: " << cache.nodes_.size() << "/" << cache.maxSize_
               << " Hits: " << cache.hits_ << " Misses: " << cache.misses_
               << " Inserts:" << cache.inserts_
               << " Updates: " << cache.updates_ << std::endl;
        // for (auto it = std::cbegin(cache.nodes_); it !=
        // std::cend(cache.nodes_);
        //      ++it)
        //   stream << it->left.first << ":" << util::ToHex(it->left.second)
        //          << std::endl;
        return stream;
    }
};
}  // namespace keyvadb
