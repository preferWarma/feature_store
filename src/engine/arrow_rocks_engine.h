#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "engine_config.h"
#include "schema_registry.h"

namespace feature_store {

struct BatchGetRequest {
    uint16_t table_id;
    uint64_t uid;
    uint16_t target_version;
    std::vector<std::string> columns;
};

struct BatchGetResult {
    uint64_t uid;
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> batch;
};

class ArrowRocksEngine {
public:
    ArrowRocksEngine();
    ~ArrowRocksEngine();

    arrow::Status Init(const EngineConfig& config);
    arrow::Status Close();

    arrow::Status RegisterSchema(uint16_t table_id,
                                 uint16_t version,
                                 std::shared_ptr<arrow::Schema> schema);

    arrow::Status AppendFeature(uint16_t table_id,
                                uint64_t uid,
                                uint16_t schema_version,
                                const arrow::RecordBatch& delta_batch);

    arrow::Status PutFeature(uint16_t table_id,
                             uint64_t uid,
                             uint16_t schema_version,
                             int64_t timestamp,
                             const arrow::RecordBatch& record_batch);

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetFeature(
        uint16_t table_id,
        uint64_t uid,
        uint16_t target_version,
        const std::vector<std::string>& columns = {});

    std::vector<BatchGetResult> BatchGetFeature(const std::vector<BatchGetRequest>& requests);

    arrow::Status CompactAll();
    arrow::Status FlushAll();

private:
    std::string CFName(uint16_t table_id) const;
    rocksdb::ColumnFamilyHandle* GetCF(uint16_t table_id) const;
    arrow::Status EnsureCF(uint16_t table_id);

    EngineConfig config_;
    SchemaRegistry registry_;

    std::shared_ptr<rocksdb::MergeOperator> merge_operator_;
    std::shared_ptr<rocksdb::CompactionFilterFactory> ttl_filter_factory_;

    rocksdb::ColumnFamilyOptions cf_options_;

    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::ColumnFamilyHandle* default_cf_ = nullptr;
    rocksdb::ColumnFamilyHandle* meta_cf_ = nullptr;
    std::unordered_map<uint16_t, rocksdb::ColumnFamilyHandle*> cf_handles_;

    rocksdb::ReadOptions read_opts_;
    rocksdb::WriteOptions write_opts_;
};

}  // namespace feature_store
