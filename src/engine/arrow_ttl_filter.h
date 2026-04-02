#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include <rocksdb/compaction_filter.h>

namespace feature_store {

class ArrowTTLFilterFactory : public rocksdb::CompactionFilterFactory {
public:
    explicit ArrowTTLFilterFactory(int64_t ttl_ms = 3LL * 24 * 3600 * 1000)
        : ttl_ms_(ttl_ms) {}

    void SetTTL(int64_t ttl_ms) { ttl_ms_.store(ttl_ms, std::memory_order_relaxed); }
    int64_t GetTTL() const { return ttl_ms_.load(std::memory_order_relaxed); }

    std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& ctx) override;

    const char* Name() const override { return "ArrowTTLFilterFactory"; }

private:
    std::atomic<int64_t> ttl_ms_;
};

class ArrowTTLFilter : public rocksdb::CompactionFilter {
public:
    explicit ArrowTTLFilter(int64_t ttl_ms);

    bool FilterMergeOperand(int level,
                            const rocksdb::Slice& key,
                            const rocksdb::Slice& operand) const override;

    bool Filter(int level,
                const rocksdb::Slice& key,
                const rocksdb::Slice& existing_value,
                std::string* new_value,
                bool* value_changed) const override;

    const char* Name() const override { return "ArrowTTLFilter"; }

private:
    int64_t ttl_ms_;
    int64_t now_ms_;
};

}  // namespace feature_store
