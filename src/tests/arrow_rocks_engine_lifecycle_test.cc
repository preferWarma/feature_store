#include "../engine/arrow_rocks_engine.h"

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace feature_store {
namespace {

std::string TempDBPath(const std::string& name) {
    auto path = std::filesystem::temp_directory_path() / std::filesystem::path(name);
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    return path.string();
}

std::shared_ptr<arrow::Schema> SchemaV1() {
    return arrow::schema({arrow::field("a", arrow::int64())});
}

std::shared_ptr<arrow::RecordBatch> MakeBatch(int64_t a) {
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.Append(a).ok());
    std::shared_ptr<arrow::Int64Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    auto schema = SchemaV1();
    return arrow::RecordBatch::Make(schema, 1, {arr});
}

TEST(ArrowRocksEngineLifecycleTest, FullLifecycleRegisterAppendGetDropRestart) {
    const auto path = TempDBPath("feature_store_lifecycle_full");

    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = path;
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());

    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());
    ASSERT_TRUE(engine.AppendFeature(1, 10, 1, *MakeBatch(123)).ok());
    ASSERT_TRUE(engine.FlushAll().ok());
    ASSERT_TRUE(engine.CompactAll().ok());

    auto got = engine.GetFeature(1, 10, 1);
    ASSERT_TRUE(got.ok()) << got.status().ToString();
    auto arr = std::static_pointer_cast<arrow::Int64Array>((*got)->GetColumnByName("a"));
    ASSERT_NE(arr, nullptr);
    EXPECT_EQ(arr->Value(0), 123);

    ASSERT_TRUE(engine.DropTable(1).ok());
    auto missing = engine.GetFeature(1, 10, 1);
    ASSERT_FALSE(missing.ok());
    EXPECT_EQ(missing.status().code(), arrow::StatusCode::KeyError);

    ASSERT_TRUE(engine.Close().ok());

    ArrowRocksEngine engine2;
    ASSERT_TRUE(engine2.Init(cfg).ok());
    auto missing2 = engine2.GetFeature(1, 10, 1);
    ASSERT_FALSE(missing2.ok());
    EXPECT_EQ(missing2.status().code(), arrow::StatusCode::KeyError);
}

TEST(ArrowRocksEngineLifecycleTest, ConcurrentAppendAndGet) {
    const auto path = TempDBPath("feature_store_lifecycle_concurrent");

    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = path;
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());
    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());

    std::atomic<bool> stop{false};
    std::thread writer([&] {
        for (int i = 0; i < 200; ++i) {
            (void)engine.AppendFeature(1, static_cast<uint64_t>(i % 10), 1, *MakeBatch(i));
        }
        stop.store(true, std::memory_order_relaxed);
    });

    std::thread reader([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto r = engine.GetFeature(1, 1, 1);
            if (!r.ok()) {
                continue;
            }
            auto arr = std::static_pointer_cast<arrow::Int64Array>((*r)->GetColumnByName("a"));
            if (arr) {
                (void)arr->Value(0);
            }
        }
    });

    writer.join();
    reader.join();
}

TEST(ArrowRocksEngineLifecycleTest, CrashRecoveryRestartPreservesData) {
    const auto path = TempDBPath("feature_store_lifecycle_recovery");

    EngineConfig cfg;
    cfg.db_path = path;
    cfg.block_cache_size_mb = 64;

    {
        ArrowRocksEngine engine;
        ASSERT_TRUE(engine.Init(cfg).ok());
        ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());
        ASSERT_TRUE(engine.AppendFeature(1, 99, 1, *MakeBatch(777)).ok());
        ASSERT_TRUE(engine.FlushAll().ok());
        ASSERT_TRUE(engine.Close().ok());
    }

    ArrowRocksEngine engine2;
    ASSERT_TRUE(engine2.Init(cfg).ok());
    auto got = engine2.GetFeature(1, 99, 1);
    ASSERT_TRUE(got.ok()) << got.status().ToString();
    auto arr = std::static_pointer_cast<arrow::Int64Array>((*got)->GetColumnByName("a"));
    ASSERT_NE(arr, nullptr);
    EXPECT_EQ(arr->Value(0), 777);
}

TEST(ArrowRocksEngineLifecycleTest, InitCleansOrphanColumnFamily) {
    const auto path = TempDBPath("feature_store_lifecycle_orphan");

    {
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* raw_db = nullptr;
        auto s = rocksdb::DB::Open(options, path, &raw_db);
        ASSERT_TRUE(s.ok()) << s.ToString();
        std::unique_ptr<rocksdb::DB> db(raw_db);
        rocksdb::ColumnFamilyHandle* orphan = nullptr;
        s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "t_0007", &orphan);
        ASSERT_TRUE(s.ok()) << s.ToString();
        db->DestroyColumnFamilyHandle(orphan);
    }

    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = path;
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());
    ASSERT_TRUE(engine.Close().ok());

    std::vector<std::string> names;
    rocksdb::Options options;
    auto ls = rocksdb::DB::ListColumnFamilies(options, path, &names);
    ASSERT_TRUE(ls.ok()) << ls.ToString();
    for (const auto& n : names) {
        EXPECT_NE(n, "t_0007");
    }
}

TEST(ArrowRocksEngineLifecycleTest, ArchiveTableExportsParquetAndDropsTable) {
    const auto path = TempDBPath("feature_store_lifecycle_archive");
    const auto archive_path =
        (std::filesystem::temp_directory_path() /
         std::filesystem::path("feature_store_archive_table.parquet"))
            .string();
    std::error_code ec;
    std::filesystem::remove(archive_path, ec);

    ArrowRocksEngine engine;
    EngineConfig cfg;
    cfg.db_path = path;
    cfg.block_cache_size_mb = 64;
    ASSERT_TRUE(engine.Init(cfg).ok());

    ASSERT_TRUE(engine.RegisterSchema(1, 1, SchemaV1()).ok());
    ASSERT_TRUE(engine.PutFeature(1, 10, 1, 111, *MakeBatch(123)).ok());
    ASSERT_TRUE(engine.PutFeature(1, 20, 1, 222, *MakeBatch(456)).ok());
    ASSERT_TRUE(engine.FlushAll().ok());

    ASSERT_TRUE(engine.ArchiveTable(1, archive_path).ok());
    EXPECT_TRUE(std::filesystem::exists(archive_path));

    auto missing = engine.GetFeature(1, 10, 1);
    ASSERT_FALSE(missing.ok());
    EXPECT_EQ(missing.status().code(), arrow::StatusCode::KeyError);

    auto input_result = arrow::io::ReadableFile::Open(archive_path);
    ASSERT_TRUE(input_result.ok()) << input_result.status().ToString();
    auto reader_result = parquet::arrow::OpenFile(input_result.ValueOrDie(),
                                                  arrow::default_memory_pool());
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
    auto reader = std::move(reader_result).ValueOrDie();
    std::shared_ptr<arrow::Table> table;
    ASSERT_TRUE(reader->ReadTable(&table).ok());
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->num_rows(), 2);
    EXPECT_NE(table->schema()->GetFieldIndex("__uid"), -1);
    EXPECT_NE(table->schema()->GetFieldIndex("__timestamp"), -1);
    EXPECT_NE(table->schema()->GetFieldIndex("__schema_version"), -1);
    EXPECT_NE(table->schema()->GetFieldIndex("a"), -1);
}

}  // namespace
}  // namespace feature_store
