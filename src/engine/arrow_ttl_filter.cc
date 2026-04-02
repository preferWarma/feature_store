#include "arrow_ttl_filter.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

#include "frame_codec.h"
#include "metrics.h"

namespace feature_store {
namespace {

int64_t CurrentTimeMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::span<const uint8_t> AsSpan(const rocksdb::Slice& s) {
    return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(s.data()),
                                    static_cast<std::size_t>(s.size()));
}

struct ScanResult {
    bool parsed_all = true;
    bool has_valid_frame = false;
    bool all_valid_expired = true;
    bool any_valid_kept = false;
};

ScanResult ScanFrames(std::span<const uint8_t> bytes,
                      int64_t now_ms,
                      int64_t ttl_ms,
                      std::string* kept_frames) {
    ScanResult result;
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        auto remaining = bytes.subspan(offset);
        auto header_res = DecodeFrameHeader(remaining);
        if (!header_res.ok()) {
            result.parsed_all = false;
            break;
        }
        const auto& header = *header_res;
        const std::size_t frame_size = header.total_size();
        if (frame_size > remaining.size()) {
            result.parsed_all = false;
            break;
        }

        auto frame_span = bytes.subspan(offset, frame_size);
        if (ValidateFrameCRC(frame_span)) {
            result.has_valid_frame = true;
            const bool expired = (now_ms - header.timestamp) > ttl_ms;
            if (!expired) {
                result.all_valid_expired = false;
                result.any_valid_kept = true;
                if (kept_frames != nullptr) {
                    kept_frames->append(reinterpret_cast<const char*>(frame_span.data()),
                                        frame_span.size());
                }
            } else {
                Metrics::Global()->IncTTLExpiredFrames(1);
            }
        } else {
            Metrics::Global()->IncCRCError(1);
        }
        offset += frame_size;
    }
    if (!result.has_valid_frame) {
        result.all_valid_expired = false;
    }
    return result;
}

}  // namespace

std::unique_ptr<rocksdb::CompactionFilter> ArrowTTLFilterFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context&) {
    return std::make_unique<ArrowTTLFilter>(ttl_ms_.load(std::memory_order_relaxed));
}

ArrowTTLFilter::ArrowTTLFilter(int64_t ttl_ms) : ttl_ms_(ttl_ms), now_ms_(CurrentTimeMs()) {}

bool ArrowTTLFilter::FilterMergeOperand(int,
                                        const rocksdb::Slice&,
                                        const rocksdb::Slice& operand) const {
    const auto bytes = AsSpan(operand);
    const auto scan = ScanFrames(bytes, now_ms_, ttl_ms_, nullptr);
    if (!scan.parsed_all) {
        return false;
    }
    if (!scan.has_valid_frame) {
        return false;
    }
    return scan.all_valid_expired;
}

bool ArrowTTLFilter::Filter(int,
                            const rocksdb::Slice&,
                            const rocksdb::Slice& existing_value,
                            std::string* new_value,
                            bool* value_changed) const {
    const auto bytes = AsSpan(existing_value);

    std::string kept;
    kept.reserve(bytes.size());
    const auto scan = ScanFrames(bytes, now_ms_, ttl_ms_, &kept);
    if (!scan.parsed_all) {
        *value_changed = false;
        return false;
    }
    if (!scan.has_valid_frame) {
        *value_changed = false;
        return false;
    }
    if (!scan.any_valid_kept) {
        *value_changed = false;
        Metrics::Global()->IncTTLExpiredKeys(1);
        return true;
    }

    if (kept.size() == bytes.size()) {
        *value_changed = false;
        return false;
    }

    *new_value = std::move(kept);
    *value_changed = true;
    return false;
}

}  // namespace feature_store
