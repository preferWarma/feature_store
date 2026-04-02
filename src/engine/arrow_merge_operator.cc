#include "arrow_merge_operator.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

#include "frame_codec.h"
#include "metrics.h"

namespace feature_store {
namespace {

std::span<const uint8_t> AsSpan(const rocksdb::Slice& s) {
    return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(s.data()),
                                    static_cast<std::size_t>(s.size()));
}

struct ScanSummary {
    bool parsed_all = true;
    bool all_crc_ok = true;
    bool single_version = true;
    bool has_valid_frame = false;
    uint16_t version = 0;
    std::size_t valid_bytes_total = 0;
};

ScanSummary ScanFrameSequence(std::span<const uint8_t> bytes) {
    ScanSummary summary;
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        auto remaining = bytes.subspan(offset);
        auto header_res = DecodeFrameHeader(remaining);
        if (!header_res.ok()) {
            summary.parsed_all = false;
            break;
        }
        const auto& header = *header_res;
        const std::size_t frame_size = header.total_size();
        if (frame_size > remaining.size()) {
            summary.parsed_all = false;
            break;
        }

        auto frame_span = bytes.subspan(offset, frame_size);
        const bool crc_ok = ValidateFrameCRC(frame_span);
        if (!crc_ok) {
            summary.all_crc_ok = false;
        } else {
            summary.valid_bytes_total += frame_size;
            if (!summary.has_valid_frame) {
                summary.has_valid_frame = true;
                summary.version = header.schema_version;
            } else if (header.schema_version != summary.version) {
                summary.single_version = false;
            }
        }
        offset += frame_size;
    }
    return summary;
}

std::size_t AppendValidFrames(std::span<const uint8_t> bytes, std::string* out) {
    std::size_t appended = 0;
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        auto remaining = bytes.subspan(offset);
        auto header_res = DecodeFrameHeader(remaining);
        if (!header_res.ok()) {
            break;
        }
        const auto& header = *header_res;
        const std::size_t frame_size = header.total_size();
        if (frame_size > remaining.size()) {
            break;
        }

        auto frame_span = bytes.subspan(offset, frame_size);
        if (ValidateFrameCRC(frame_span)) {
            out->append(reinterpret_cast<const char*>(frame_span.data()), frame_span.size());
            appended += frame_span.size();
        } else {
            Metrics::Global()->IncCRCError(1);
        }
        offset += frame_size;
    }
    return appended;
}

bool CanFastConcat(const ScanSummary& left, const ScanSummary& right) {
    if (!left.parsed_all || !right.parsed_all) {
        return false;
    }
    if (!left.all_crc_ok || !right.all_crc_ok) {
        return false;
    }
    if (!left.single_version || !right.single_version) {
        return false;
    }
    if (!left.has_valid_frame || !right.has_valid_frame) {
        return true;
    }
    return left.version == right.version;
}

}  // namespace

bool ArrowMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                    MergeOperationOutput* merge_out) const {
    (void)registry_;
    merge_out->new_value.clear();

    std::size_t reserve_hint = 0;
    if (merge_in.existing_value != nullptr) {
        reserve_hint += merge_in.existing_value->size();
    }
    for (const auto& op : merge_in.operand_list) {
        reserve_hint += op.size();
    }
    merge_out->new_value.reserve(reserve_hint);

    if (merge_in.existing_value != nullptr) {
        AppendValidFrames(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(merge_in.existing_value->data()),
                                     static_cast<std::size_t>(merge_in.existing_value->size())),
            &merge_out->new_value);
    }
    for (const auto& op : merge_in.operand_list) {
        AppendValidFrames(AsSpan(op), &merge_out->new_value);
    }

    return true;
}

bool ArrowMergeOperator::PartialMerge(const rocksdb::Slice&,
                                     const rocksdb::Slice& left,
                                     const rocksdb::Slice& right,
                                     std::string* new_value,
                                     rocksdb::Logger*) const {
    (void)registry_;

    const auto left_span = AsSpan(left);
    const auto right_span = AsSpan(right);

    const auto left_summary = ScanFrameSequence(left_span);
    const auto right_summary = ScanFrameSequence(right_span);

    if (CanFastConcat(left_summary, right_summary)) {
        new_value->assign(left.data(), left.size());
        new_value->append(right.data(), right.size());
        return true;
    }

    new_value->clear();
    new_value->reserve(left_summary.valid_bytes_total + right_summary.valid_bytes_total);
    AppendValidFrames(left_span, new_value);
    AppendValidFrames(right_span, new_value);
    return true;
}

}  // namespace feature_store
