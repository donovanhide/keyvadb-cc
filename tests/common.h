#pragma once

#include <string>
#include "gtest/gtest.h"
#include "db/key.h"

using namespace keyvadb;

template <std::uint32_t BITS>
struct KeyTraits {
  enum { Bits = BITS };
};

template <typename T>
class KeyTest : public ::testing::Test {
  using key_type = Key<T::Bits>;

 protected:
  key_type GetFromHex(std::string const& s) {
    key_type key;
    FromHex<T::Bits>(key, s);
    return key;
  }
  key_type GetFromBytes(std::string const& s) { return FromBytes<T::Bits>(s); }
  key_type GetMax() { return Max<T::Bits>(); }
  key_type GetMin() { return Min<T::Bits>(); }
  key_type MakeKey(std::uint32_t n) { return key_type(n); }
  key_type MakeKeyFromHex(const char c) {
    return GetFromHex(std::string(T::Bits / 4, c));
  }
  key_type TooBig() { return GetFromHex(std::string(T::Bits, 'F')); }
};

static std::string h0(
    "0000000000000000000000000000000000000000000000000000000000000000");
static std::string h1(
    "0000000000000000000000000000000000000000000000000000000000000001");
static std::string h2(
    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
static std::string h3(
    "1111111111111111111111111111111111111111111111111111111111111111");
static std::string h4(
    "2222222222222222222222222222222222222222222222222222222222222222");
static std::string h6(
    "3333333333333333333333333333333333333333333333333333333333333333");
static std::string h7(
    "0000000000000000000000000000000000000000000000000000000000000002");
static std::string h8(
    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
static std::string h9(
    "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF8");
