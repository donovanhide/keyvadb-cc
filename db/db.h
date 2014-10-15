#pragma once

#include <string>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <system_error>
#include "db/cxx11.h"
#include "db/memory.h"
#include "db/file.h"
#include "db/buffer.h"
#include "db/tree.h"

namespace keyvadb {

template <class Storage>
class DB {
  using util = detail::KeyUtil<Storage::Bits>;
  using key_store_type = typename Storage::KeyStorage;
  using value_store_type = typename Storage::ValueStorage;
  using key_value_type = KeyValue<Storage::Bits>;
  using buffer_type = std::shared_ptr<Buffer<Storage::Bits>>;
  using journal_type = std::unique_ptr<Journal<Storage::Bits>>;
  using tree_type = Tree<Storage::Bits>;

 private:
  key_store_type keys_;
  value_store_type values_;
  tree_type tree_;
  buffer_type buffer_;
  bool close_;
  std::thread thread_;
  std::condition_variable cond_;
  std::mutex mutex_;

 public:
  explicit DB(std::uint32_t degree)
      : keys_(Storage::CreateKeyStore(degree)),
        values_(Storage::CreateValueStore()),
        tree_(tree_type(keys_)),
        buffer_(MakeBuffer<Storage::Bits>()),
        close_(false),
        thread_(&DB::flushThread, this) {}
  DB(std::string valueFileName, std::string keyFileName,
     std::uint32_t blockSize)
      : keys_(Storage::CreateKeyStore(valueFileName, blockSize)),
        values_(Storage::CreateValueStore(keyFileName)),
        tree_(tree_type(keys_)),
        buffer_(MakeBuffer<Storage::Bits>()),
        close_(false),
        thread_(&DB::flushThread, this) {}
  DB(DB const&) = delete;
  DB& operator=(const DB&) = delete;

  std::error_condition Open() {
    if (auto err = keys_->Open()) return err;
    if (auto err = tree_.Init(false)) return err;
    return values_->Open();
  }

  std::error_condition Get(std::string const& key, std::string* value) {
    auto k = util::FromBytes(key);
    std::uint64_t valueId;
    try {
      valueId = buffer_->Get(k);
    } catch (std::out_of_range) {
      std::error_condition err;
      std::tie(valueId, err) = tree_.Get(k);
      if (err) return err;
    }
    return values_->Get(valueId, value);
  }

  std::error_condition Put(std::string const& key, std::string const& value) {
    key_value_type kv;
    if (auto err = values_->Set(key, value, kv)) return err;
    buffer_->Add(kv.key, kv.value);
    return std::error_condition();
  }

  std::error_condition Close() {
    close_ = true;
    cond_.notify_all();
    thread_.join();
    if (auto err = values_->Close()) return err;
    return keys_->Close();
  }

 private:
  std::error_condition flush() {
    auto snapshot = buffer_->GetSnapshot();
    if (snapshot->Size() == 0) return std::error_condition();
    std::cout << "Flushing " << snapshot->Size() << " keys" << std::endl;
    journal_type journal;
    std::error_condition err;
    std::tie(journal, err) = tree_.Add(snapshot);
    if (err) return err;
    journal->Commit(keys_);
    buffer_->ClearSnapshot(snapshot);
    std::cout << "Flushed " << snapshot->Size() << " keys into "
              << journal->Size() << " nodes" << std::endl;
    return std::error_condition();
  }

  void flushThread() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
      bool stop = cond_.wait_for(lock, std::chrono::seconds(1),
                                 [&]() { return close_; });
      if (auto err = flush())
        std::cerr << err.message() << ":" << err.category().name() << std::endl;
      if (stop) break;
    }
    // thread exits
  }
};
}  // namespace keyvadb
