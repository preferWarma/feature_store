#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace feature_store {

constexpr std::array<char, 8> EncodeKeyBytes(uint64_t uid) {
    return {
        static_cast<char>((uid >> 56U) & 0xFFU),
        static_cast<char>((uid >> 48U) & 0xFFU),
        static_cast<char>((uid >> 40U) & 0xFFU),
        static_cast<char>((uid >> 32U) & 0xFFU),
        static_cast<char>((uid >> 24U) & 0xFFU),
        static_cast<char>((uid >> 16U) & 0xFFU),
        static_cast<char>((uid >> 8U) & 0xFFU),
        static_cast<char>(uid & 0xFFU),
    };
}

constexpr uint64_t DecodeKeyBytes(std::string_view bytes) {
    if (bytes.size() != 8) {
        return 0;
    }
    return (static_cast<uint64_t>(static_cast<unsigned char>(bytes[0])) << 56U) |
           (static_cast<uint64_t>(static_cast<unsigned char>(bytes[1])) << 48U) |
           (static_cast<uint64_t>(static_cast<unsigned char>(bytes[2])) << 40U) |
           (static_cast<uint64_t>(static_cast<unsigned char>(bytes[3])) << 32U) |
           (static_cast<uint64_t>(static_cast<unsigned char>(bytes[4])) << 24U) |
           (static_cast<uint64_t>(static_cast<unsigned char>(bytes[5])) << 16U) |
           (static_cast<uint64_t>(static_cast<unsigned char>(bytes[6])) << 8U) |
           static_cast<uint64_t>(static_cast<unsigned char>(bytes[7]));
}

inline std::string EncodeKey(uint64_t uid) {
    const auto bytes = EncodeKeyBytes(uid);
    return std::string(bytes.data(), bytes.size());
}

constexpr uint64_t DecodeKey(std::string_view bytes) {
    return DecodeKeyBytes(bytes);
}

}  // namespace feature_store
