#include "Logger.h"
#include <arrow/api.h>
#include <rocksdb/db.h>

using namespace lyf;

int main() {
  Logger::Instance().InitFromConfig("config.yaml");

  // --- Arrow 测试 ---
  {
    arrow::Int64Builder builder;
    auto s = builder.Append(1);
    if (!s.ok()) {
      ERROR("Failed to append to Arrow builder: {}", s.ToString());
    }
    s = builder.Append(2);
    if (!s.ok()) {
      ERROR("Failed to append to Arrow builder: {}", s.ToString());
    }
    std::shared_ptr<arrow::Array> array;
    auto status = builder.Finish(&array);
    if (status.ok()) {
      INFO("Arrow array created. Length: {}", array->length());
    } else {
      ERROR("Failed to finish Arrow array: {}", status.ToString());
    }
  }

  // --- RocksDB 测试 ---
  {
    rocksdb::DB *db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, "/tmp/testdb", &db);
    if (s.ok()) {
      INFO("RocksDB opened successfully!");
      delete db;
    } else {
      ERROR("RocksDB open failed: {}", s.ToString());
    }
  }

  return 0;
}