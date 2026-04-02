#include "../engine/arrow_ttl_filter.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "../engine/arrow_merge_operator.h"
#include "../engine/frame_codec.h"
#include "../engine/key_encoder.h"
#include "../engine/schema_registry.h"

namespace feature_store {
namespace {

int64_t NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::shared_ptr<arrow::RecordBatch> MakeBatch(int64_t v) {
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.Append(v).ok());
    std::shared_ptr<arrow::Int64Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    auto schema = arrow::schema({arrow::field("x", arrow::int64())});
    return arrow::RecordBatch::Make(schema, 1, {arr});
}

std::vector<std::string> SplitFrames(const std::string& value) {
    std::vector<std::string> frames;
    auto bytes = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(value.data()), value.size());
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        auto header_res = DecodeFrameHeader(bytes.subspan(offset));
        if (!header_res.ok()) {
            break;
        }
        const std::size_t frame_size = header_res->total_size();
        if (frame_size > bytes.size() - offset) {
            break;
        }
        frames.emplace_back(value.substr(offset, frame_size));
        offset += frame_size;
    }
    return frames;
}

TEST(ArrowTTLFilterTest, AllFramesExpiredDeletesKey) {
    const int64_t now = NowMs();
    const int64_t ttl_ms = 1000;

    auto frame1 = EncodeFrame(1, now - 100000, *MakeBatch(1));
    auto frame2 = EncodeFrame(1, now - 200000, *MakeBatch(2));
    ASSERT_TRUE(frame1.ok());
    ASSERT_TRUE(frame2.ok());

    const std::string value = frame1.ValueOrDie() + frame2.ValueOrDie();
    ArrowTTLFilter filter(ttl_ms);

    std::string new_value;
    bool value_changed = false;
    const bool remove =
        filter.Filter(0, rocksdb::Slice("k"), rocksdb::Slice(value), &new_value, &value_changed);
    EXPECT_TRUE(remove);
    EXPECT_FALSE(value_changed);
}

TEST(ArrowTTLFilterTest, PartialExpiredRewritesValue) {
    const int64_t now = NowMs();
    const int64_t ttl_ms = 100000;

    auto expired = EncodeFrame(1, now - 200000, *MakeBatch(1));
    auto live = EncodeFrame(1, now - 1000, *MakeBatch(2));
    ASSERT_TRUE(expired.ok());
    ASSERT_TRUE(live.ok());

    const std::string value = expired.ValueOrDie() + live.ValueOrDie();
    ArrowTTLFilter filter(ttl_ms);

    std::string new_value;
    bool value_changed = false;
    const bool remove =
        filter.Filter(0, rocksdb::Slice("k"), rocksdb::Slice(value), &new_value, &value_changed);
    EXPECT_FALSE(remove);
    EXPECT_TRUE(value_changed);

    auto frames = SplitFrames(new_value);
    ASSERT_EQ(frames.size(), 1U);
    auto decoded = DecodeFrame(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(frames[0].data()), frames[0].size()));
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded->timestamp, now - 1000);
}

TEST(ArrowTTLFilterTest, TTLUpdateTakesEffectForNewFilters) {
    const int64_t now = NowMs();
    auto value = EncodeFrame(1, now - 5000, *MakeBatch(1));
    ASSERT_TRUE(value.ok());
    const std::string bytes = value.ValueOrDie();

    ArrowTTLFilterFactory factory(1000);
    rocksdb::CompactionFilter::Context ctx{
        .is_full_compaction = false,
        .is_manual_compaction = true,
        .column_family_id = 0,
        .reason = rocksdb::TableFileCreationReason::kCompaction,
    };
    {
        auto filter = factory.CreateCompactionFilter(ctx);
        std::string new_value;
        bool value_changed = false;
        const bool remove =
            filter->Filter(0, rocksdb::Slice("k"), rocksdb::Slice(bytes), &new_value, &value_changed);
        EXPECT_TRUE(remove);
    }

    factory.SetTTL(1000000);
    {
        auto filter = factory.CreateCompactionFilter(ctx);
        std::string new_value;
        bool value_changed = false;
        const bool remove =
            filter->Filter(0, rocksdb::Slice("k"), rocksdb::Slice(bytes), &new_value, &value_changed);
        EXPECT_FALSE(remove);
        EXPECT_FALSE(value_changed);
    }
}

TEST(ArrowTTLFilterTest, EndToEndMergeAndCompactRange) {
    SchemaRegistry registry;
    auto merge_op = std::make_shared<ArrowMergeOperator>(&registry);
    auto ttl_factory = std::make_shared<ArrowTTLFilterFactory>(20LL * 24 * 3600 * 1000);

    auto path = (std::filesystem::temp_directory_path() /
                 std::filesystem::path("feature_store_ttl_filter_testdb"))
                    .string();
    std::error_code ec;
    std::filesystem::remove_all(path, ec);

    rocksdb::Options options;
    options.create_if_missing = true;
    options.merge_operator = merge_op;
    options.compaction_filter_factory = ttl_factory;

    rocksdb::DB* raw_db = nullptr;
    auto s = rocksdb::DB::Open(options, path, &raw_db);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::unique_ptr<rocksdb::DB> db(raw_db);

    const int64_t now = NowMs();
    const int64_t old_ts = now - 10LL * 24 * 3600 * 1000;
    const int64_t new_ts = now;

    auto old_frame = EncodeFrame(1, old_ts, *MakeBatch(1));
    auto new_frame = EncodeFrame(1, new_ts, *MakeBatch(2));
    ASSERT_TRUE(old_frame.ok());
    ASSERT_TRUE(new_frame.ok());

    const std::string key = EncodeKey(42);
    s = db->Merge(rocksdb::WriteOptions(), key, old_frame.ValueOrDie());
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = db->Merge(rocksdb::WriteOptions(), key, new_frame.ValueOrDie());
    ASSERT_TRUE(s.ok()) << s.ToString();

    rocksdb::CompactRangeOptions cro;
    s = db->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::string value_before;
    s = db->Get(rocksdb::ReadOptions(), key, &value_before);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(SplitFrames(value_before).size(), 2U);

    ttl_factory->SetTTL(3LL * 24 * 3600 * 1000);
    s = db->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::string value_after;
    s = db->Get(rocksdb::ReadOptions(), key, &value_after);
    ASSERT_TRUE(s.ok()) << s.ToString();
    auto frames = SplitFrames(value_after);
    ASSERT_EQ(frames.size(), 1U);
    auto decoded = DecodeFrame(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(frames[0].data()), frames[0].size()));
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded->timestamp, new_ts);

    db.reset();
    std::filesystem::remove_all(path, ec);
}

}  // namespace
}  // namespace feature_store
