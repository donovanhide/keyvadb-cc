#pragma once

#include <boost/optional.hpp>
#include <boost/algorithm/hex.hpp>
#include <string>
#include <map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <system_error>
#include "db/memory.h"
#include "db/file.h"
#include "db/buffer.h"
#include "db/tree.h"
#include "db/journal.h"
#include "db/log.h"

namespace keyvadb
{
template <class Storage, class Log = NullLog>
class DB
{
    using util = detail::KeyUtil<Storage::Bits>;
    using key_store_ptr = typename Storage::KeyStorage;
    using value_store_ptr = typename Storage::ValueStorage;
    using key_value_type = KeyValue<Storage::Bits>;
    using buffer_type = Buffer<Storage::Bits>;
    using journal_type = Journal<Storage>;
    using tree_type = Tree<Storage::Bits>;
    using cache_type = NodeCache<Storage::Bits>;
    using key_value_func =
        std::function<void(std::string const &, std::string const &)>;

    enum
    {
        key_length = Storage::Bits / 8
    };

    Log log_;
    key_store_ptr keys_;
    value_store_ptr values_;
    cache_type cache_;
    tree_type tree_;
    buffer_type buffer_;
    std::atomic<bool> close_;
    std::thread thread_;

   public:
    explicit DB(std::uint32_t const degree)
        : log_(Log{}),
          keys_(Storage::CreateKeyStore(degree)),
          values_(Storage::CreateValueStore()),
          cache_(),
          tree_(keys_, cache_),
          close_(false),
          thread_(&DB::flushThread, this)
    {
    }

    DB(std::string const &valueFileName, std::string const &keyFileName,
       std::uint32_t const blockSize, std::uint64_t const cacheSize)
        : log_(Log{}),
          keys_(Storage::CreateKeyStore(valueFileName, blockSize)),
          values_(Storage::CreateValueStore(keyFileName)),
          tree_(keys_, cache_),
          close_(false),
          thread_(&DB::flushThread, this)
    {
        cache_.SetMaxSize(cacheSize);
    }
    DB(DB const &) = delete;
    DB &operator=(DB const &) = delete;

    ~DB()
    {
        close_ = true;
        try
        {
            thread_.join();
            if (auto err = values_->Close())
                if (log_.error)
                    log_.error << "Closing values file: " << err.message();
            if (auto err = keys_->Close())
                if (log_.error)
                    log_.error << "Closing keys file: " << err.message();
        }
        catch (std::exception &ex)
        {
            if (log_.error)
                log_.error << "Destructor exception: " << ex.what();
        }
    }

    // Not threadsafe
    std::error_condition Open()
    {
        if (auto err = keys_->Open())
            return err;
        if (auto err = tree_.Init(true))
            return err;
        return values_->Open();
    }

    // Not threadsafe
    std::error_condition Clear()
    {
        buffer_.Clear();
        if (auto err = keys_->Clear())
            return err;
        if (auto err = tree_.Init(true))
            return err;
        return values_->Clear();
    }

    std::error_condition Get(std::string const &key, std::string *value)
    {
        if (key.length() != key_length)
            return db_error::key_wrong_length;
        if (auto v = buffer_.Get(key))
        {
            value->assign(*v);
            if (log_.debug)
                log_.debug << "Buffer Get: " << boost::algorithm::hex(key);
            return std::error_condition();
        }
        // Value must be on disk
        std::uint64_t offset;
        std::error_condition err;
        std::tie(offset, err) = tree_.Get(util::FromBytes(key));
        if (err)
            return err;
        if (log_.debug)
            log_.debug << "Get: " << boost::algorithm::hex(key) << ":"
                       << offset;
        return values_->Get(offset, value);
    }

    std::error_condition Put(std::string const &key, std::string const &value)
    {
        if (key.length() != key_length)
            return db_error::key_wrong_length;
        if (value.size() > std::numeric_limits<std::uint32_t>::max())
            return db_error::value_too_long;
        buffer_.Add(key, value);
        return std::error_condition();
    }

    // Returns keys and values in insertion order
    std::error_condition Each(key_value_func f) { return values_->Each(f); }

   private:
    std::error_condition flush()
    {
        if (log_.info)
            log_.info << "Flushing approximately: " << buffer_.Size()
                      << " keys";
        journal_type journal(buffer_, keys_, values_);
        if (auto err = journal.Process(tree_))
            return err;
        if (auto err = journal.Commit(tree_))
            return err;
        if (log_.info)
            log_.info << "Flushed: " << buffer_.Size() << " keys into "
                      << journal.Size() << " nodes";
        return std::error_condition();
    }

    void flushThread()
    {
        for (;;)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            bool stop = close_;
            if (auto err = flush())
                if (log_.error)
                    log_.error << "Flushing Error: " << err.message() << ":"
                               << err.category().name();
            if (stop)
                break;
        }
        // thread exits
    }
};
}  // namespace keyvadb
