#include "../engine/engine_config.h"

#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace feature_store {
namespace {

std::string WriteTempJsonFile(const std::string &content) {
  auto path = std::filesystem::temp_directory_path() /
              std::filesystem::path("feature_store_engine_config_test.json");
  std::ofstream out(path, std::ios::out | std::ios::binary | std::ios::trunc);
  EXPECT_TRUE(out.is_open());
  out << content;
  out.close();
  return path.string();
}

TEST(EngineConfigTest, ValidateAcceptsValidConfig) {
  EngineConfig cfg;
  cfg.db_path = "/tmp/testdb";
  EXPECT_TRUE(cfg.Validate().ok()) << cfg.Validate().ToString();
}

TEST(EngineConfigTest, ValidateRejectsInvalidConfig) {
  EngineConfig cfg;
  EXPECT_FALSE(cfg.Validate().ok());

  cfg.db_path = "/tmp/testdb";
  cfg.ttl_days = 0;
  EXPECT_FALSE(cfg.Validate().ok());

  cfg.ttl_days = 3;
  cfg.max_column_families = 0;
  EXPECT_FALSE(cfg.Validate().ok());

  cfg.max_column_families = 1024;
  cfg.max_active_versions_per_table = 0;
  EXPECT_FALSE(cfg.Validate().ok());

  cfg.max_active_versions_per_table = 16;
  cfg.write_buffer_size = 0;
  EXPECT_FALSE(cfg.Validate().ok());
}

TEST(EngineConfigTest, LoadFromJsonFileParsesFields) {
  const auto path = WriteTempJsonFile(R"json(
{
  "db_path": "/tmp/db_path",
  "disable_wal": true,
  "use_direct_reads": true,
  "fill_cache_on_read": false,
  "block_cache_size_mb": 128,
  "ttl_days": 7,
  "enable_mmap_reads": true,
  "max_column_families": 2048,
  "max_active_versions_per_table": 32,
  "max_background_compactions": 1,
  "max_background_flushes": 3,
  "write_buffer_size": 1048576
}
)json");

  auto cfg_result = EngineConfig::LoadFromJsonFile(path);
  ASSERT_TRUE(cfg_result.ok()) << cfg_result.status().ToString();
  const auto &cfg = cfg_result.ValueOrDie();
  EXPECT_EQ(cfg.db_path, "/tmp/db_path");
  EXPECT_TRUE(cfg.disable_wal);
  EXPECT_TRUE(cfg.use_direct_reads);
  EXPECT_FALSE(cfg.fill_cache_on_read);
  EXPECT_EQ(cfg.block_cache_size_mb, 128U);
  EXPECT_EQ(cfg.ttl_days, 7);
  EXPECT_TRUE(cfg.enable_mmap_reads);
  EXPECT_EQ(cfg.max_column_families, 2048U);
  EXPECT_EQ(cfg.max_active_versions_per_table, 32);
  EXPECT_EQ(cfg.max_background_compactions, 1);
  EXPECT_EQ(cfg.max_background_flushes, 3);
  EXPECT_EQ(cfg.write_buffer_size, 1048576U);

  std::error_code ec;
  std::filesystem::remove(path, ec);
}

TEST(EngineConfigTest, LoadFromJsonFileRejectsInvalidJson) {
  const auto path = WriteTempJsonFile("{");
  auto cfg_result = EngineConfig::LoadFromJsonFile(path);
  EXPECT_FALSE(cfg_result.ok());

  std::error_code ec;
  std::filesystem::remove(path, ec);
}

TEST(EngineConfigTest, LoadFromJsonFileRejectsMissingFile) {
  auto cfg_result =
      EngineConfig::LoadFromJsonFile("/tmp/nonexistent_config_file.json");
  EXPECT_FALSE(cfg_result.ok());
}

TEST(EngineConfigTest, ToStringIncludesDisableWal) {
  EngineConfig cfg;
  cfg.db_path = "/tmp/testdb";
  cfg.disable_wal = true;
  cfg.use_direct_reads = true;
  cfg.fill_cache_on_read = false;
  const auto text = cfg.ToString();
  EXPECT_NE(text.find("\"disable_wal\":true"), std::string::npos);
  EXPECT_NE(text.find("\"use_direct_reads\":true"), std::string::npos);
  EXPECT_NE(text.find("\"fill_cache_on_read\":false"), std::string::npos);
}

} // namespace
} // namespace feature_store
