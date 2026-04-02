#include "../engine/schema_registry.h"

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace feature_store {
namespace {

struct MetaDB {
    std::unique_ptr<rocksdb::DB> db;
    rocksdb::ColumnFamilyHandle* meta_cf = nullptr;
    std::string path;

    MetaDB() = default;
    MetaDB(const MetaDB&) = delete;
    MetaDB& operator=(const MetaDB&) = delete;
    MetaDB(MetaDB&&) = default;
    MetaDB& operator=(MetaDB&&) = default;

    ~MetaDB() {
        if (db && meta_cf) {
            db->DestroyColumnFamilyHandle(meta_cf);
            meta_cf = nullptr;
        }
        db.reset();
        if (!path.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    }
};

MetaDB OpenTempMetaDB() {
    MetaDB out;
    out.path = (std::filesystem::temp_directory_path() /
                std::filesystem::path("feature_store_schema_registry_testdb"))
                   .string();
    std::error_code ec;
    std::filesystem::remove_all(out.path, ec);

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* raw_db = nullptr;
    auto s = rocksdb::DB::Open(options, out.path, &raw_db);
    EXPECT_TRUE(s.ok()) << s.ToString();
    out.db.reset(raw_db);

    rocksdb::ColumnFamilyHandle* handle = nullptr;
    s = out.db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "__meta__", &handle);
    EXPECT_TRUE(s.ok()) << s.ToString();
    out.meta_cf = handle;
    return out;
}

std::shared_ptr<arrow::Schema> MakeSchemaV1() {
    return arrow::schema({
        arrow::field("a", arrow::int64(), false),
        arrow::field("b", arrow::int32(), true),
    });
}

std::shared_ptr<arrow::Schema> MakeSchemaV2Superset() {
    return arrow::schema({
        arrow::field("a", arrow::int64(), false),
        arrow::field("b", arrow::int32(), true),
        arrow::field("c", arrow::utf8(), true),
    });
}

std::shared_ptr<arrow::Schema> MakeSchemaV2BadNewNonNullable() {
    return arrow::schema({
        arrow::field("a", arrow::int64(), false),
        arrow::field("b", arrow::int32(), true),
        arrow::field("c", arrow::utf8(), false),
    });
}

std::shared_ptr<arrow::Schema> MakeSchemaV2BadTypeChange() {
    return arrow::schema({
        arrow::field("a", arrow::int64(), false),
        arrow::field("b", arrow::int64(), true),
    });
}

TEST(SchemaRegistryTest, RegisterAndGet) {
    auto meta = OpenTempMetaDB();
    SchemaRegistry registry;

    ASSERT_TRUE(registry.Register(1, 1, MakeSchemaV1(), meta.db.get(), meta.meta_cf).ok());
    ASSERT_TRUE(registry.Register(1, 2, MakeSchemaV2Superset(), meta.db.get(), meta.meta_cf).ok());

    auto s1 = registry.Get(1, 1);
    auto s2 = registry.Get(1, 2);
    ASSERT_NE(s1, nullptr);
    ASSERT_NE(s2, nullptr);
    EXPECT_TRUE(s1->Equals(*MakeSchemaV1(), false));
    EXPECT_TRUE(s2->Equals(*MakeSchemaV2Superset(), false));
}

TEST(SchemaRegistryTest, RegisterRejectsNonSuperset) {
    auto meta = OpenTempMetaDB();
    SchemaRegistry registry;

    ASSERT_TRUE(registry.Register(1, 1, MakeSchemaV1(), meta.db.get(), meta.meta_cf).ok());

    auto s_bad = registry.Register(1, 2, MakeSchemaV2BadNewNonNullable(), meta.db.get(), meta.meta_cf);
    ASSERT_FALSE(s_bad.ok());
    EXPECT_EQ(s_bad.code(), arrow::StatusCode::Invalid);

    auto s_bad2 = registry.Register(1, 2, MakeSchemaV2BadTypeChange(), meta.db.get(), meta.meta_cf);
    ASSERT_FALSE(s_bad2.ok());
    EXPECT_EQ(s_bad2.code(), arrow::StatusCode::Invalid);
}

TEST(SchemaRegistryTest, RecoverRoundTrip) {
    auto meta = OpenTempMetaDB();
    {
        SchemaRegistry registry;
        ASSERT_TRUE(registry.Register(10, 1, MakeSchemaV1(), meta.db.get(), meta.meta_cf).ok());
        ASSERT_TRUE(registry.Register(10, 2, MakeSchemaV2Superset(), meta.db.get(), meta.meta_cf).ok());
    }

    SchemaRegistry recovered;
    ASSERT_TRUE(recovered.Recover(meta.db.get(), meta.meta_cf).ok());

    auto s1 = recovered.Get(10, 1);
    auto s2 = recovered.Get(10, 2);
    ASSERT_NE(s1, nullptr);
    ASSERT_NE(s2, nullptr);
    EXPECT_TRUE(s1->Equals(*MakeSchemaV1(), false));
    EXPECT_TRUE(s2->Equals(*MakeSchemaV2Superset(), false));
}

TEST(SchemaRegistryTest, ConcurrentReadWrite) {
    auto meta = OpenTempMetaDB();
    SchemaRegistry registry;
    ASSERT_TRUE(registry.Register(1, 1, MakeSchemaV1(), meta.db.get(), meta.meta_cf).ok());

    std::atomic<bool> stop{false};
    std::atomic<uint16_t> latest{1};

    std::thread writer([&] {
        auto current_fields = MakeSchemaV1()->fields();
        for (uint16_t v = 2; v < 50; ++v) {
            current_fields.push_back(
                arrow::field("x" + std::to_string(v), arrow::int32(), true));
            auto schema = std::make_shared<arrow::Schema>(current_fields);
            auto st = registry.Register(1, v, schema, meta.db.get(), meta.meta_cf);
            if (st.ok()) {
                latest.store(v, std::memory_order_relaxed);
            }
        }
        stop.store(true, std::memory_order_relaxed);
    });

    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) {
        readers.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                auto v = latest.load(std::memory_order_relaxed);
                auto s = registry.Get(1, v);
                if (s) {
                    EXPECT_GE(s->num_fields(), 2);
                }
            }
        });
    }

    writer.join();
    for (auto& t : readers) {
        t.join();
    }

    auto last_schema = registry.Get(1, latest.load());
    ASSERT_NE(last_schema, nullptr);
}

}  // namespace
}  // namespace feature_store
