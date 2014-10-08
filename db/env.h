#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <utility>

namespace keyvadb {

class RandomAccessFile {
 public:
  virtual ~RandomAccessFile() = default;
  virtual std::error_code Open() = 0;
  virtual std::error_code ReadAt(std::uint64_t const pos,
                                 std::size_t const length,
                                 const char* buf) const = 0;
  virtual std::error_code WriteAt(std::uint64_t const pos,
                                  std::size_t const length,
                                  const char* buf) = 0;
  virtual std::error_code Size(std::uint64_t* size) const = 0;
  virtual std::error_code Close() = 0;
  virtual std::error_code Sync() const = 0;
};

class PosixRandomAccessFile : public RandomAccessFile {
 private:
  std::string filename_;
  std::int32_t fd_;

 public:
  explicit PosixRandomAccessFile(std::string const& filename)
      : filename_(filename) {}
  PosixRandomAccessFile(const PosixRandomAccessFile&) = delete;
  PosixRandomAccessFile& operator=(const PosixRandomAccessFile&) = delete;

  std::error_code Open() override {
    auto fd = open(filename_.c_str(), O_RDWR | O_CREAT | O_APPEND | O_SYNC);
    if (fd < 0) return std::error_code(errno, std::generic_category());
    return std::error_code();
  }

  std::error_code ReadAt(std::uint64_t const pos, std::size_t const length,
                         const char* buf) const override {
    return std::error_code();
  };
  std::error_code WriteAt(std::uint64_t const pos, std::size_t const length,
                          const char* buf) override {
    return std::error_code();
  };
  std::error_code Size(std::uint64_t* size) const override {
    return std::error_code();
  };
  std::error_code Close() override { return std::error_code(); };
  std::error_code Sync() const override { return std::error_code(); };
};

}  // namespace keyvadb
