#pragma once

#include <cstdint>
#include <utility>

#include <arrow/buffer.h>
#include <rocksdb/slice.h>

namespace feature_store {

class PinnableBuffer : public arrow::Buffer {
public:
    explicit PinnableBuffer(rocksdb::PinnableSlice&& pinnable)
        : arrow::Buffer(reinterpret_cast<const uint8_t*>(pinnable.data()),
                        static_cast<int64_t>(pinnable.size())),
          pinnable_(std::move(pinnable)) {}

    bool IsPinned() const { return pinnable_.IsPinned(); }

private:
    rocksdb::PinnableSlice pinnable_;
};

}  // namespace feature_store
