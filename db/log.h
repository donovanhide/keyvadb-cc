#pragma once
#include <iostream>
#include <functional>

namespace keyvadb {

class NullLog {
 private:
  struct ScopedOutput {
    template <class T>
    ScopedOutput& operator<<(T const&) {
      return *this;
    }
  };

  struct Level {
    template <class T>
    ScopedOutput operator<<(T const&) const {
      return ScopedOutput{};
    }

    explicit operator bool() const { return false; }
  };

 public:
  Level const info;
  Level const debug;
  Level const error;
};

class StandardLog {
 private:
  // This needs improvement!
  struct ScopedOutput {
    std::string prefix_;
    std::ostringstream buffer_;
    std::reference_wrapper<std::ostream> stream_;
    explicit ScopedOutput(std::string const& prefix, std::ostream& stream)
        : prefix_(prefix), stream_(stream) {}
    ScopedOutput(ScopedOutput& out)
        : prefix_(out.prefix_), stream_(out.stream_) {
      buffer_.swap(out.buffer_);
    }
    ~ScopedOutput() {
      if (buffer_.str().length() > 0)
        stream_.get() << prefix_ << ": " << buffer_.str() << std::endl;
    }

    template <class T>
    ScopedOutput& operator<<(T const& v) {
      buffer_ << v;
      return *this;
    }
  };

  struct Level {
    std::string prefix_;
    std::reference_wrapper<std::ostream> stream_;
    Level(std::string const& prefix, std::ostream& stream)
        : prefix_(prefix), stream_(stream) {}
    template <class T>
    ScopedOutput operator<<(T const& v) const {
      return ScopedOutput(prefix_, stream_) << v;
    }
    explicit operator bool() const { return true; }
  };

 public:
  StandardLog()
      : info("INFO", std::cout),
        debug("DEBUG", std::cout),
        error("ERROR", std::cerr) {}
  Level const info;
  Level const debug;
  Level const error;
};

}  // namespace keyvadb
