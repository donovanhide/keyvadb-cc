#pragma once

#include <boost/bimap.hpp>
#include <boost/bimap/list_of.hpp>
#include <boost/bimap/set_of.hpp>
#include <unordered_map>
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
    using store_type =
        boost::bimaps::bimap<boost::bimaps::set_of<key_level_pair, compare_kl>,
                             boost::bimaps::list_of<node_ptr>>;
    using store_value = typename store_type::value_type;
    using index_type = std::unordered_map<std::uint64_t, key_level_pair>;

   private:
    std::uint64_t maxSize_ = 0;
    std::uint64_t hits_ = 0;
    std::uint64_t misses_ = 0;
    std::uint64_t inserts_ = 0;
    std::uint64_t updates_ = 0;
    store_type nodes_;
    index_type index_;
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
        index_.clear();
    }

    void Add(node_ptr const& node)
    {
        if (maxSize_ == 0)
            return;
        std::lock_guard<std::mutex> lock(lock_);
        auto keyPair = key_level_pair{node->Level(), node->First()};
        auto it = nodes_.left.find(keyPair);
        if (it != nodes_.left.end())
        {
            updates_++;
            assert(it->second->Id() == node->Id());
            it->second = node;
            nodes_.right.relocate(nodes_.right.end(), nodes_.project_right(it));
        }
        else
        {
            inserts_++;
            if (nodes_.size() == maxSize_)
            {
                auto evictee = nodes_.right.begin();
                index_.erase(evictee->first->Id());
                nodes_.right.erase(evictee);
            }
            assert(nodes_.size() <= maxSize_ && index_.size() <= maxSize_);
            nodes_.insert(store_value(keyPair, node));
            index_[node->Id()] = keyPair;
        }
    }

    node_ptr GetById(std::uint64_t const id)
    {
        std::lock_guard<std::mutex> lock(lock_);
        auto found = index_.find(id);
        if (found != index_.end())
            return nodes_.left.at(found->second);
        return node_ptr();
    }

    // Get node lowest in the tree by checking deepest nodes in the cache first.
    // Key 0000...0000 will always return a null node_ptr.
    node_ptr Get(key_type const& key)
    {
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
        return node_ptr();
    }

    std::string ToString()
    {
        std::stringstream ss;
        std::lock_guard<std::mutex> lock(lock_);
        ss << "Size: " << nodes_.size() << "/" << maxSize_ << " Hits: " << hits_
           << " Misses: " << misses_ << " Inserts:" << inserts_
           << " Updates: " << updates_;
        // stream << std::endl;
        // for (auto const& n : nodes_)
        //     stream << n.left.first << ":" << util::ToHex(n.left.second) <<
        //     ":"
        //            << n.right->Id() << std::endl;
        return ss.str();
    }

    friend std::ostream& operator<<(std::ostream& stream, NodeCache& cache)
    {
        stream << cache.ToString();
        return stream;
    }
};
}  // namespace keyvadb
