#pragma once

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstddef>
#include <string>
#include <utility>
#include <atomic>

namespace keyvadb {

class RandomAccessFile {
 public:
  virtual ~RandomAccessFile() = default;
  virtual std::error_code Open() = 0;
  virtual std::error_code OpenSync() = 0;
  virtual std::error_code Truncate() const = 0;
  virtual std::error_code ReadAt(std::uint64_t const pos,
                                 std::string& str) const = 0;
  virtual std::error_code WriteAt(std::string const& str,
                                  std::uint64_t const pos) = 0;
  virtual std::error_code Size(std::atomic_uint_fast64_t& size) const = 0;
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

  std::error_code Open() override { return open(O_RDWR | O_CREAT); }

  std::error_code OpenSync() override {
    return open(O_RDWR | O_CREAT | O_SYNC);
  }

  std::error_code Truncate() const override {
    return check_error(::ftruncate(fd_, 0));
  }

  std::error_code ReadAt(std::uint64_t const pos,
                         std::string& str) const override {
    return check_error(::pread(fd_, &str[0], str.size(), pos));
  };

  std::error_code WriteAt(std::string const& str,
                          std::uint64_t const pos) override {
    return check_error(::pwrite(fd_, str.data(), str.size(), pos));
  };

  std::error_code Size(std::atomic_uint_fast64_t& size) const override {
    struct stat sb;
    if (auto err = check_error(::fstat(fd_, &sb))) return err;
    size = sb.st_size;
    return std::error_code();
  };

  std::error_code Close() override {
    if (auto err = Sync()) return err;
    return check_error(::close(fd_));
  };

  std::error_code Sync() const override { return check_error(::fsync(fd_)); };

 private:
  std::error_code open(std::int32_t const flags) {
    fd_ = ::open(filename_.c_str(), flags, 0644);
    return check_error(fd_);
  }

  std::error_code check_error(ssize_t err) const {
    if (err < 0) return std::error_code(errno, std::generic_category());
    return std::error_code();
  }
};

// CompressedPosixRandomAccessFile
// WindowsRandomAccessFile
// CompressedWindowsRandomAccessFile
// ....

}  // namespace keyvadb
