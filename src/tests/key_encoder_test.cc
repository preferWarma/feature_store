#include "../engine/key_encoder.h"

#include <string_view>

#include <gtest/gtest.h>

namespace feature_store {
namespace {

constexpr uint64_t kUid = 0x0102030405060708ULL;
constexpr auto kUidBytes = EncodeKeyBytes(kUid);
static_assert(static_cast<unsigned char>(kUidBytes[0]) == 0x01);
static_assert(static_cast<unsigned char>(kUidBytes[1]) == 0x02);
static_assert(static_cast<unsigned char>(kUidBytes[2]) == 0x03);
static_assert(static_cast<unsigned char>(kUidBytes[3]) == 0x04);
static_assert(static_cast<unsigned char>(kUidBytes[4]) == 0x05);
static_assert(static_cast<unsigned char>(kUidBytes[5]) == 0x06);
static_assert(static_cast<unsigned char>(kUidBytes[6]) == 0x07);
static_assert(static_cast<unsigned char>(kUidBytes[7]) == 0x08);

constexpr std::string_view kUidBytesView(kUidBytes.data(), kUidBytes.size());
static_assert(DecodeKeyBytes(kUidBytesView) == kUid);

TEST(KeyEncoderTest, EncodeDecodeRoundTrip) {
    auto key = EncodeKey(kUid);
    ASSERT_EQ(key.size(), 8U);
    EXPECT_EQ(static_cast<unsigned char>(key[0]), 0x01);
    EXPECT_EQ(static_cast<unsigned char>(key[1]), 0x02);
    EXPECT_EQ(static_cast<unsigned char>(key[2]), 0x03);
    EXPECT_EQ(static_cast<unsigned char>(key[3]), 0x04);
    EXPECT_EQ(static_cast<unsigned char>(key[4]), 0x05);
    EXPECT_EQ(static_cast<unsigned char>(key[5]), 0x06);
    EXPECT_EQ(static_cast<unsigned char>(key[6]), 0x07);
    EXPECT_EQ(static_cast<unsigned char>(key[7]), 0x08);
    EXPECT_EQ(DecodeKey(key), kUid);
}

TEST(KeyEncoderTest, DecodeRejectsInvalidLength) {
    EXPECT_EQ(DecodeKey(std::string_view("abc")), 0ULL);
}

}  // namespace
}  // namespace feature_store
