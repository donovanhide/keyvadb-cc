#pragma once

#include <string>
#include <system_error>
#include <atomic>
#include <unordered_map>
#include <utility>
#include "db/store.h"
#include "db/error.h"

namespace keyvadb
{
template <std::uint32_t BITS>
class MemoryValueStore : public ValueStore<BITS>
{
   public:
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using value_type = typename Buffer<BITS>::Value;
    using key_value_func =
        std::function<void(std::string const&, std::string const&)>;

   private:
    std::atomic_uint_fast64_t size_;
    std::map<std::uint64_t, std::pair<std::string, std::string>> map_;
    mutable std::mutex lock_;

   public:
    MemoryValueStore() : size_(0) {}
    std::error_condition Open() override { return std::error_condition(); }
    std::error_condition Close() override { return std::error_condition(); }
    std::error_condition Clear() override
    {
        map_.clear();
        return std::error_condition();
    }
    std::error_condition Get(std::uint64_t const offset, std::uint64_t const,
                             std::string* value) const override
    {
        std::lock_guard<std::mutex> lock(lock_);
        try
        {
            value->assign(map_.at(offset).second);
        }
        catch (std::out_of_range const&)
        {
            return make_error_condition(db_error::value_not_found);
        }
        return std::error_condition();
    }

    std::error_condition Set(key_type const& key,
                             value_type const& value) override
    {
        assert(value.ReadyForWriting());
        std::lock_guard<std::mutex> lock(lock_);
        map_[*value.offset] = std::make_pair(util::ToBytes(key), value.value);
        size_ += value.Size();
        return std::error_condition();
    };

    std::error_condition Each(key_value_func f) const override
    {
        std::lock_guard<std::mutex> lock(lock_);
        for (auto const& kv : map_) f(kv.second.first, kv.second.second);
        return std::error_condition();
    }

    std::uint64_t Size() const { return size_; }
};

template <std::uint32_t BITS>
class MemoryKeyStore : public KeyStore<BITS>
{
   public:
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using node_type = Node<BITS>;
    using node_ptr = std::shared_ptr<node_type>;
    using node_result = std::pair<node_ptr, std::error_condition>;

   private:
    std::uint32_t degree_;
    std::atomic_uint_fast64_t id_;
    std::unordered_map<std::uint64_t, node_ptr> map_;
    mutable std::mutex lock_;

   public:
    explicit MemoryKeyStore(std::uint32_t const degree)
        : degree_(degree), id_(0)
    {
    }

    std::error_condition Open() override { return std::error_condition(); }
    std::error_condition Close() override { return std::error_condition(); }
    std::error_condition Clear() override
    {
        map_.clear();
        return std::error_condition();
    }

    node_ptr New(std::uint32_t const level, key_type const& first,
                 key_type const& last) override
    {
        return std::make_shared<node_type>(id_++, level, degree_, first, last);
    }

    node_result Get(std::uint64_t const id) const override
    {
        std::lock_guard<std::mutex> lock(lock_);
        try
        {
            auto node = map_.at(id);
            return std::make_pair(node, std::error_condition());
        }
        catch (std::out_of_range const&)
        {
            return std::make_pair(
                node_ptr(), make_error_condition(db_error::key_not_found));
        }
    }

    std::error_condition Set(node_ptr const& node) override
    {
        std::lock_guard<std::mutex> lock(lock_);
        map_[node->Id()] = node;
        return std::error_condition();
    }

    std::uint64_t Size() const override { return id_; }
};

template <std::uint32_t BITS>
struct MemoryStoragePolicy
{
    using KeyStorage = std::shared_ptr<KeyStore<BITS>>;
    using ValueStorage = std::shared_ptr<ValueStore<BITS>>;
    enum
    {
        Bits = BITS
    };
    static KeyStorage CreateKeyStore(std::uint32_t const degree)
    {
        return std::make_shared<MemoryKeyStore<BITS>>(degree);
    }
    static ValueStorage CreateValueStore()
    {
        return std::make_shared<MemoryValueStore<BITS>>();
    }
};

}  // namespace keyvadb
