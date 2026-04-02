#include "../engine/arrow_rocks_engine.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "../engine/pinnable_buffer.h"

namespace feature_store {
namespace {

std::shared_ptr<arrow::RecordBatch> MakeV1Batch(int64_t a) {
    arrow::Int64Builder a_builder;
    EXPECT_TRUE(a_builder.Append(a).ok());
    std::shared_ptr<arrow::Int64Array> a_arr;
    EXPECT_TRUE(a_builder.Finish(&a_arr).ok());
    auto schema = arrow::schema({arrow::field("a", arrow::int64())});
    return arrow::RecordBatch::Make(schema, 1, {a_arr});
}

std::shared_ptr<arrow::Schema> SchemaV1() {
    return arrow::schema({arrow::field("a", arrow::int64())});
}

std::shared_ptr<arrow::Schema> SchemaV2() {
    return arrow::schema({
        arrow::field("a", arrow::int64()),
        arrow::field("b", arrow::int32(), true),
    });
}

std::string TempDBPath(const std::string& name) {
    auto path = std::filesystem::temp_directory_path() / std::filesystem::path(name);
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    return path.string();
}

std::shared_ptr<PinnableBuffer> FindPinnedParent(const std::shared_ptr<arrow::Buffer>& buf) {
    std::shared_ptr<arrow::Buffer> cur = buf;
    while (cur) {
        auto pinned = std::dynamic_pointer_cast<PinnableBuffer>(cur);
        if (pinned) {
            return pinned;
        }
        cur = cur->parent();
    }
    return nullptr;
}

TEST(ArrowRocksEngineReadTest, MultiVersionProjectionAddsNulls) {
    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = TempDBPath("feature_store_read_test_mv");
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());

    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());
    ASSERT_TRUE(engine.RegisterSchema(1, 2, SchemaV2()).ok());

    ASSERT_TRUE(engine.AppendFeature(1, 100, 1, *MakeV1Batch(10)).ok());
    ASSERT_TRUE(engine.FlushAll().ok());
    ASSERT_TRUE(engine.CompactAll().ok());

    auto res = engine.GetFeature(1, 100, 2);
    ASSERT_TRUE(res.ok()) << res.status().ToString();
    auto batch = *res;
    ASSERT_EQ(batch->num_columns(), 2);

    auto b = std::static_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("b"));
    ASSERT_NE(b, nullptr);
    EXPECT_TRUE(b->IsNull(0));
}

TEST(ArrowRocksEngineReadTest, ColumnProjectionWorks) {
    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = TempDBPath("feature_store_read_test_proj");
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());

    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());
    ASSERT_TRUE(engine.AppendFeature(1, 100, 1, *MakeV1Batch(10)).ok());
    ASSERT_TRUE(engine.FlushAll().ok());
    ASSERT_TRUE(engine.CompactAll().ok());

    auto res = engine.GetFeature(1, 100, 1, {"a"});
    ASSERT_TRUE(res.ok());
    EXPECT_EQ((*res)->num_columns(), 1);

    auto bad = engine.GetFeature(1, 100, 1, {"missing"});
    ASSERT_FALSE(bad.ok());
    EXPECT_EQ(bad.status().code(), arrow::StatusCode::KeyError);
}

TEST(ArrowRocksEngineReadTest, BatchGetAcrossTablesAndMissingKeys) {
    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = TempDBPath("feature_store_read_test_batch");
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());

    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());
    ASSERT_TRUE(engine.RegisterSchema(2, 1, SchemaV1()).ok());

    ASSERT_TRUE(engine.AppendFeature(1, 1, 1, *MakeV1Batch(1)).ok());
    ASSERT_TRUE(engine.AppendFeature(1, 2, 1, *MakeV1Batch(2)).ok());
    ASSERT_TRUE(engine.AppendFeature(2, 3, 1, *MakeV1Batch(3)).ok());
    ASSERT_TRUE(engine.FlushAll().ok());
    ASSERT_TRUE(engine.CompactAll().ok());

    std::vector<BatchGetRequest> reqs{
        {.table_id = 1, .uid = 1, .target_version = 1, .columns = {}},
        {.table_id = 1, .uid = 2, .target_version = 1, .columns = {}},
        {.table_id = 2, .uid = 3, .target_version = 1, .columns = {}},
        {.table_id = 2, .uid = 999, .target_version = 1, .columns = {}},
    };

    auto results = engine.BatchGetFeature(reqs);
    ASSERT_EQ(results.size(), reqs.size());
    EXPECT_TRUE(results[0].status.ok());
    EXPECT_TRUE(results[1].status.ok());
    EXPECT_TRUE(results[2].status.ok());
    EXPECT_FALSE(results[3].status.ok());
    EXPECT_EQ(results[3].status.code(), arrow::StatusCode::KeyError);
}

TEST(ArrowRocksEngineReadTest, BatchAppendWritesMultipleKeysAcrossTables) {
    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = TempDBPath("feature_store_read_test_batch_append");
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());

    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());
    ASSERT_TRUE(engine.RegisterSchema(2, 1, SchemaV1()).ok());

    std::vector<BatchAppendRequest> reqs{
        {.table_id = 1, .uid = 11, .schema_version = 1, .batch = MakeV1Batch(11)},
        {.table_id = 1, .uid = 12, .schema_version = 1, .batch = MakeV1Batch(12)},
        {.table_id = 2, .uid = 21, .schema_version = 1, .batch = MakeV1Batch(21)},
    };

    ASSERT_TRUE(engine.BatchAppendFeature(reqs).ok());
    ASSERT_TRUE(engine.FlushAll().ok());
    ASSERT_TRUE(engine.CompactAll().ok());

    auto r1 = engine.GetFeature(1, 11, 1);
    auto r2 = engine.GetFeature(1, 12, 1);
    auto r3 = engine.GetFeature(2, 21, 1);
    ASSERT_TRUE(r1.ok());
    ASSERT_TRUE(r2.ok());
    ASSERT_TRUE(r3.ok());
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>((*r1)->GetColumnByName("a"))->Value(0), 11);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>((*r2)->GetColumnByName("a"))->Value(0), 12);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>((*r3)->GetColumnByName("a"))->Value(0), 21);
}

TEST(ArrowRocksEngineReadTest, BatchAppendIsAtomicWhenRequestInvalid) {
    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = TempDBPath("feature_store_read_test_batch_append_atomic");
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());

    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());

    std::vector<BatchAppendRequest> reqs{
        {.table_id = 1, .uid = 11, .schema_version = 1, .batch = MakeV1Batch(11)},
        {.table_id = 2, .uid = 21, .schema_version = 1, .batch = MakeV1Batch(21)},
    };

    auto st = engine.BatchAppendFeature(reqs);
    ASSERT_FALSE(st.ok());
    EXPECT_EQ(st.code(), arrow::StatusCode::KeyError);

    auto r1 = engine.GetFeature(1, 11, 1);
    ASSERT_FALSE(r1.ok());
    EXPECT_EQ(r1.status().code(), arrow::StatusCode::KeyError);
}

TEST(ArrowRocksEngineReadTest, ZeroCopyPinnableBufferIsReachableFromBatchBuffers) {
    EngineConfig cfg;
    cfg.db_path = TempDBPath("feature_store_read_test_zerocopy");
    cfg.block_cache_size_mb = 64;

    ArrowRocksEngine engine;
    ASSERT_TRUE(engine.Init(cfg).ok());

    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());
    ASSERT_TRUE(engine.PutFeature(1, 100, 1, 1234, *MakeV1Batch(10)).ok());
    ASSERT_TRUE(engine.FlushAll().ok());
    ASSERT_TRUE(engine.CompactAll().ok());
    ASSERT_TRUE(engine.Close().ok());

    ASSERT_TRUE(engine.Init(cfg).ok());

    auto res = engine.GetFeature(1, 100, 1);
    ASSERT_TRUE(res.ok()) << res.status().ToString();
    auto batch = *res;

    auto arr = std::static_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("a"));
    ASSERT_NE(arr, nullptr);
    ASSERT_GE(arr->data()->buffers.size(), 2U);
    auto value_buf = arr->data()->buffers[1];
    ASSERT_NE(value_buf, nullptr);

    auto pinned = FindPinnedParent(value_buf);
    ASSERT_NE(pinned, nullptr);
    EXPECT_TRUE(pinned->IsPinned());
    EXPECT_NE(pinned->data(), nullptr);
}

}  // namespace
}  // namespace feature_store
