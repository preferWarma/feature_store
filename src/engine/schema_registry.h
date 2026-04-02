#pragma once

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <rocksdb/db.h>

namespace feature_store {

class SchemaRegistry {
public:
    arrow::Status Register(uint16_t table_id,
                           uint16_t version,
                           std::shared_ptr<arrow::Schema> schema,
                           rocksdb::DB* db,
                           rocksdb::ColumnFamilyHandle* meta_cf);

    arrow::Status Unregister(uint16_t table_id);

    std::shared_ptr<arrow::Schema> Get(uint16_t table_id, uint16_t version) const;

    arrow::Status Recover(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* meta_cf);

private:
    static uint32_t EncodeKey(uint16_t table_id, uint16_t version) {
        return (static_cast<uint32_t>(table_id) << 16U) | static_cast<uint32_t>(version);
    }

    mutable std::shared_mutex mu_;
    std::unordered_map<uint32_t, std::shared_ptr<arrow::Schema>> schemas_;
};

}  // namespace feature_store
