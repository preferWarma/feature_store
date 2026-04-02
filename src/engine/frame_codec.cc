#include "frame_codec.h"

#include <array>
#include <cstring>
#include <limits>
#include <type_traits>

#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/status.h>

#if defined(__SSE4_2__) && (defined(__x86_64__) || defined(__i386__) ||        \
                            defined(_M_X64) || defined(_M_IX86))
#include <nmmintrin.h>
#endif

namespace feature_store {
namespace {

constexpr std::size_t kMagicOffset = 0;
constexpr std::size_t kVersionOffset = 4;
constexpr std::size_t kReservedOffset = 6;
constexpr std::size_t kTimestampOffset = 8;
constexpr std::size_t kIPCLengthOffset = 16;

template <typename T> using UnsignedT = std::make_unsigned_t<T>;

template <typename T> void AppendLittleEndian(T value, std::string *output) {
  using U = UnsignedT<T>;
  U unsigned_value = static_cast<U>(value);
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    output->push_back(static_cast<char>((unsigned_value >> (i * 8U)) & 0xFFU));
  }
}

template <typename T>
T ReadLittleEndian(std::span<const uint8_t> bytes, std::size_t offset) {
  using U = UnsignedT<T>;
  U value = 0;
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    value |= static_cast<U>(bytes[offset + i]) << (i * 8U);
  }
  return static_cast<T>(value);
}

uint32_t ComputeCRC32CSoftware(std::span<const uint8_t> bytes) {
  static const auto table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < t.size(); ++i) {
      uint32_t crc = i;
      for (int bit = 0; bit < 8; ++bit) {
        crc = (crc & 1U) ? (crc >> 1U) ^ 0x82F63B78U : (crc >> 1U);
      }
      t[i] = crc;
    }
    return t;
  }();

  uint32_t crc = 0xFFFFFFFFU;
  for (uint8_t byte : bytes) {
    crc = (crc >> 8U) ^ table[(crc ^ byte) & 0xFFU];
  }
  return crc ^ 0xFFFFFFFFU;
}

#if defined(__SSE4_2__) && (defined(__x86_64__) || defined(__i386__) ||        \
                            defined(_M_X64) || defined(_M_IX86))
#include <nmmintrin.h>

uint32_t ComputeCRC32CHardware(std::span<const uint8_t> bytes) {
  uint32_t crc = 0xFFFFFFFFU;
  const uint8_t *p = bytes.data();
  size_t len = bytes.size();
#if defined(__x86_64__) || defined(_M_X64)
  // 主循环：8 bytes / iter
  while (len >= 8) {
    uint64_t val;
    std::memcpy(&val, p, 8);
    crc = static_cast<uint32_t>(_mm_crc32_u64(crc, val));
    p += 8;
    len -= 8;
  }
#endif
  // 4 字节尾部
  if (len >= 4) {
    uint32_t val;
    std::memcpy(&val, p, 4);
    crc = _mm_crc32_u32(crc, val);
    p += 4;
    len -= 4;
  }
  // 逐字节尾部
  while (len--) {
    crc = _mm_crc32_u8(crc, *p++);
  }
  return crc ^ 0xFFFFFFFFU;
}
#endif

#if defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>

uint32_t ComputeCRC32CHardware(std::span<const uint8_t> bytes) {
  uint32_t crc = 0xFFFFFFFFU;
  const uint8_t *p = bytes.data();
  size_t len = bytes.size();
  // 主循环：8 bytes / iter
  while (len >= 8) {
    uint64_t val;
    std::memcpy(&val, p, 8);
    crc = __crc32cd(crc, val);
    p += 8;
    len -= 8;
  }
  // 4 字节尾部
  if (len >= 4) {
    uint32_t val;
    std::memcpy(&val, p, 4);
    crc = __crc32cw(crc, val);
    p += 4;
    len -= 4;
  }
  // 2 字节尾部
  if (len >= 2) {
    uint16_t val;
    std::memcpy(&val, p, 2);
    crc = __crc32ch(crc, val);
    p += 2;
    len -= 2;
  }
  // 1 字节尾部
  if (len) {
    crc = __crc32cb(crc, *p);
  }
  return crc ^ 0xFFFFFFFFU;
}
#endif

uint32_t ComputeCRC32C(std::span<const uint8_t> bytes) {
#if (defined(__SSE4_2__) && (defined(__x86_64__) || defined(__i386__) ||       \
                             defined(_M_X64) || defined(_M_IX86))) ||          \
    defined(__ARM_FEATURE_CRC32)
  return ComputeCRC32CHardware(bytes);
#else
  return ComputeCRC32CSoftware(bytes);
#endif
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
SerializeRecordBatch(const arrow::RecordBatch &record_batch) {
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(
      auto writer, arrow::ipc::MakeStreamWriter(sink, record_batch.schema()));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(record_batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
DeserializeRecordBatch(std::span<const uint8_t> ipc_bytes) {
  auto buffer = std::make_shared<arrow::Buffer>(
      ipc_bytes.data(), static_cast<int64_t>(ipc_bytes.size()));
  auto input = std::make_shared<arrow::io::BufferReader>(buffer);
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        arrow::ipc::RecordBatchStreamReader::Open(input));
  ARROW_ASSIGN_OR_RAISE(auto record_batch, reader->Next());
  if (!record_batch) {
    return arrow::Status::SerializationError(
        "IPC stream does not contain a RecordBatch");
  }
  ARROW_ASSIGN_OR_RAISE(auto trailing_batch, reader->Next());
  if (trailing_batch) {
    return arrow::Status::SerializationError(
        "IPC stream contains multiple RecordBatches");
  }
  return record_batch;
}

arrow::Status ValidateExactFrameSize(std::span<const uint8_t> bytes,
                                     const FrameHeader &header) {
  if (bytes.size() != header.total_size()) {
    return arrow::Status::Invalid("frame size mismatch");
  }
  return arrow::Status::OK();
}

} // namespace

std::size_t FrameHeader::total_size() const {
  return kFrameHeaderSize + static_cast<std::size_t>(ipc_length) +
         kFrameCRCSize;
}

arrow::Result<std::string> EncodeFrame(uint16_t schema_version,
                                       int64_t timestamp,
                                       const arrow::RecordBatch &record_batch) {
  ARROW_ASSIGN_OR_RAISE(auto ipc_buffer, SerializeRecordBatch(record_batch));

  const std::size_t total_size = kFrameHeaderSize +
                                 static_cast<std::size_t>(ipc_buffer->size()) +
                                 kFrameCRCSize;
  if (ipc_buffer->size() >
      static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
    return arrow::Status::Invalid("IPC payload exceeds uint32_t range");
  }

  std::string encoded;
  encoded.reserve(total_size);
  AppendLittleEndian<uint32_t>(kFrameMagic, &encoded);
  AppendLittleEndian<uint16_t>(schema_version, &encoded);
  AppendLittleEndian<uint16_t>(0, &encoded);
  AppendLittleEndian<int64_t>(timestamp, &encoded);
  AppendLittleEndian<uint32_t>(static_cast<uint32_t>(ipc_buffer->size()),
                               &encoded);
  encoded.append(reinterpret_cast<const char *>(ipc_buffer->data()),
                 static_cast<std::size_t>(ipc_buffer->size()));

  const auto crc = ComputeCRC32C(std::span<const uint8_t>(
      reinterpret_cast<const uint8_t *>(encoded.data()), encoded.size()));
  AppendLittleEndian<uint32_t>(crc, &encoded);
  return encoded;
}

arrow::Result<FrameHeader> DecodeFrameHeader(std::span<const uint8_t> bytes) {
  if (bytes.size() < kFrameHeaderSize) {
    return arrow::Status::Invalid("frame is smaller than header");
  }

  const uint32_t magic = ReadLittleEndian<uint32_t>(bytes, kMagicOffset);
  if (magic != kFrameMagic) {
    return arrow::Status::Invalid("invalid frame magic");
  }

  FrameHeader header{
      .magic = magic,
      .schema_version = ReadLittleEndian<uint16_t>(bytes, kVersionOffset),
      .reserved = ReadLittleEndian<uint16_t>(bytes, kReservedOffset),
      .timestamp = ReadLittleEndian<int64_t>(bytes, kTimestampOffset),
      .ipc_length = ReadLittleEndian<uint32_t>(bytes, kIPCLengthOffset),
  };

  const std::size_t min_tail_size = kFrameHeaderSize + kFrameCRCSize;
  if (header.total_size() < min_tail_size ||
      header.total_size() > bytes.size()) {
    return arrow::Status::Invalid("frame length exceeds available bytes");
  }
  return header;
}

bool ValidateFrameCRC(std::span<const uint8_t> bytes) {
  auto header_result = DecodeFrameHeader(bytes);
  if (!header_result.ok()) {
    return false;
  }

  const FrameHeader &header = *header_result;
  if (bytes.size() != header.total_size()) {
    return false;
  }

  const auto data_without_crc = bytes.first(bytes.size() - kFrameCRCSize);
  const uint32_t expected_crc = ComputeCRC32C(data_without_crc);
  const uint32_t actual_crc =
      ReadLittleEndian<uint32_t>(bytes, bytes.size() - kFrameCRCSize);
  return expected_crc == actual_crc;
}

arrow::Result<Frame> DecodeFrame(std::span<const uint8_t> bytes) {
  ARROW_ASSIGN_OR_RAISE(const auto header, DecodeFrameHeader(bytes));
  ARROW_RETURN_NOT_OK(ValidateExactFrameSize(bytes, header));

  if (!ValidateFrameCRC(bytes)) {
    return arrow::Status::IOError("CRC32 mismatch");
  }

  const auto ipc_bytes = bytes.subspan(
      kFrameHeaderSize, static_cast<std::size_t>(header.ipc_length));
  ARROW_ASSIGN_OR_RAISE(auto record_batch, DeserializeRecordBatch(ipc_bytes));

  return Frame{
      .schema_version = header.schema_version,
      .timestamp = header.timestamp,
      .record_batch = std::move(record_batch),
  };
}

} // namespace feature_store
