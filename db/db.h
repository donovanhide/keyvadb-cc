#pragma once

#include <string>
#include <mutex>
#include <thread>
#include "db/memory.h"
#include "db/buffer.h"
#include "db/tree.h"

namespace keyvadb {

template <std::uint32_t BITS>
class DB {
  using value_store_type = std::shared_ptr<ValueStore<BITS>>;
  using key_store_type = std::shared_ptr<KeyStore<BITS>>;
  using buffer_type = std::shared_ptr<Buffer<BITS>>;
  using tree_type = Tree<BITS>;

 private:
  tree_type tree_;
  key_store_type keys_;
  value_store_type values_;
  buffer_type buffer_;
  bool close_;
  std::thread thread_;
  std::condition_variable cond_;
  std::mutex mutex_;

 public:
  DB(key_store_type const& keys, value_store_type const& values)
      : tree_(tree_type(keys)),
        keys_(keys),
        values_(values),
        buffer_(MakeBuffer<256>()),
        close_(false),
        thread_(&DB::flushThread, this) {}
  DB(DB const&) = delete;
  DB& operator=(const DB&) = delete;

  std::string Get(std::string const& key) {
    auto k = FromBytes<BITS>(key);
    std::uint64_t valueId;
    try {
      valueId = buffer_->Get(k);
    } catch (std::out_of_range) {
      valueId = tree_.Get(k);
    }
    return values_->Get(valueId);
  }

  void Put(std::string const& key, std::string const& value) {
    auto k = FromBytes<BITS>(key);
    auto kv = values_->Set(k, value);
    buffer_->Add(kv.key, kv.value);
  }

  void Close() {
    close_ = true;
    cond_.notify_all();
    thread_.join();
    values_->Close();
    keys_->Close();
  }

 private:
  void flush() {
    auto snapshot = buffer_->GetSnapshot();
    std::cout << "Flushing " << snapshot->Size() << " keys" << std::endl;
    auto journal = tree_.Add(snapshot);
    journal->Commit(keys_);
    buffer_->ClearSnapshot(snapshot);
    std::cout << "Flushed " << snapshot->Size() << " keys into "
              << journal->Size() << " nodes" << std::endl;
  }

  void flushThread() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
      bool stop = cond_.wait_for(lock, std::chrono::seconds(1),
                                 [&]() { return close_; });
      flush();
      if (stop) break;
    }
    // thread exits
  }
};

}  // namespace keyvadb
