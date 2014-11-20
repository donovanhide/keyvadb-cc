#pragma once

#include <cstdint>
#include <cstddef>
#include <map>
#include <system_error>
#include "db/key.h"
#include "db/node.h"
#include "db/tree.h"
#include "db/buffer.h"
#include "db/store.h"
#include "db/delta.h"

namespace keyvadb
{
// Journal is the where all changes to the keys and values occur
// and the rollback file is created.
template <class Storage>
class Journal
{
    using util = detail::KeyUtil<Storage::Bits>;
    using key_type = typename util::key_type;
    using key_store_ptr = typename Storage::KeyStorage;
    using value_store_ptr = typename Storage::ValueStorage;
    using key_value_type = KeyValue<Storage::Bits>;
    using delta_type = Delta<Storage::Bits>;
    using node_ptr = std::shared_ptr<Node<Storage::Bits>>;
    using tree_type = Tree<Storage::Bits>;
    using buffer_type = Buffer<Storage::Bits>;

   private:
    buffer_type& buffer_;
    key_store_ptr keys_;
    value_store_ptr values_;
    std::multimap<std::uint32_t, delta_type> deltas_;
    std::uint64_t offset_;

   public:
    Journal(buffer_type& buffer, key_store_ptr& keys, value_store_ptr& values)
        : buffer_(buffer), keys_(keys), values_(values)
    {
    }

    std::error_condition Process(tree_type& tree)
    {
        offset_ = values_->Size();
        std::error_condition err;
        node_ptr root;
        std::tie(root, err) = tree.Root();
        if (err)
            return err;
        return process(root, 0);
    }

    std::error_condition Commit(tree_type& tree, std::size_t const batchSize)
    {
        if (auto err = buffer_.Commit(values_, batchSize))
            return err;
        // write deepest nodes first so that no parent can refer
        // to a non-existent child
        for (auto it = deltas_.crbegin(), end = deltas_.crend(); it != end;
             ++it)
            if (auto err = tree.Update(it->second.Current()))
                return err;
        buffer_.Purge();
        deltas_.clear();
        return std::error_condition();
    }

    constexpr std::size_t Size() const { return deltas_.size(); }

    std::uint64_t TotalInsertions() const
    {
        std::uint64_t total = 0;
        for (auto const& kv : deltas_) total += kv.second.Insertions();
        return total;
    }

    friend std::ostream& operator<<(std::ostream& stream,
                                    const Journal& journal)
    {
        for (auto const& kv : journal.deltas_)
        {
            std::cout << "Level: " << std::setw(3) << kv.first << " "
                      << kv.second << std::endl;
        }

        return stream;
    }

   private:
    std::error_condition process(node_ptr const& node,
                                 std::uint32_t const level)
    {
        delta_type delta(node);
        offset_ = delta.AddKeys(buffer_, offset_);
        delta.CheckSanity();
        if (delta.Current()->EmptyKeyCount() == 0)
        {
            auto err = delta.Current()->EachChild(
                [&](const std::size_t i, const key_type& first,
                    const key_type& last, const std::uint64_t cid)
                {
                    if (!buffer_.ContainsRange(first, last))
                        return std::error_condition();
                    // std::cout << util::ToHex(first) << ":" <<
                    // util::ToHex(last)
                    //           << ":" << node->Id() << ":" << level <<
                    //           std::endl;
                    if (cid == EmptyChild)
                    {
                        auto child = keys_->New(level, first, last);
                        delta.SetChild(i, child->Id());
                        return process(child, level + 1);
                    }
                    else
                    {
                        node_ptr child;
                        std::error_condition err;
                        std::tie(child, err) = keys_->Get(cid);
                        if (err)
                            return err;
                        return process(child, level + 1);
                    }
                });
            if (err)
                return err;
        }
        delta.CheckSanity();
        if (delta.Dirty())
            deltas_.emplace(level, delta);
        return std::error_condition();
    }
};
}  // namespace keyvadb
