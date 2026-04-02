#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <arrow/result.h>
#include <arrow/status.h>

namespace feature_store {

struct EngineConfig {
  std::string db_path;
  bool disable_wal = false;
  std::size_t block_cache_size_mb = 512;
  int64_t ttl_days = 3;
  bool enable_mmap_reads = false;
  uint32_t max_column_families = 1024;
  uint16_t max_active_versions_per_table = 16;
  int max_background_compactions = 4;
  int max_background_flushes = 2;
  std::size_t write_buffer_size = 128ULL << 20;

  [[nodiscard]] arrow::Status Validate() const;
  static arrow::Result<EngineConfig> LoadFromJsonFile(std::string_view path);
  [[nodiscard]] std::string ToString() const;
};

} // namespace feature_store
