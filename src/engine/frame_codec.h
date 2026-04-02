#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

#include <arrow/record_batch.h>
#include <arrow/result.h>

namespace feature_store {

inline constexpr uint32_t kFrameMagic = 0x41524F57U;
inline constexpr std::size_t kFrameHeaderSize = 20;
inline constexpr std::size_t kFrameCRCSize = 4;

struct FrameHeader {
    uint32_t magic;
    uint16_t schema_version;
    uint16_t reserved;
    int64_t timestamp;
    uint32_t ipc_length;

    [[nodiscard]] std::size_t total_size() const;
};

struct Frame {
    uint16_t schema_version;
    int64_t timestamp;
    std::shared_ptr<arrow::RecordBatch> record_batch;
};

arrow::Result<std::string> EncodeFrame(uint16_t schema_version,
                                       int64_t timestamp,
                                       const arrow::RecordBatch& record_batch);

arrow::Result<FrameHeader> DecodeFrameHeader(std::span<const uint8_t> bytes);

arrow::Result<Frame> DecodeFrame(std::span<const uint8_t> bytes);

bool ValidateFrameCRC(std::span<const uint8_t> bytes);

}  // namespace feature_store
