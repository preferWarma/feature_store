#include "engine_config.h"

#include <exception>
#include <fstream>
#include <limits>
#include <type_traits>

#include "../../LogSystem/third/nlohmann_json.h"

namespace feature_store {
namespace {

template <typename T>
arrow::Result<T> ParseNumberWithRange(const nlohmann::json &value,
                                      std::string_view key_name) {
  try {
    if constexpr (std::is_same_v<T, std::size_t>) {
      auto tmp = value.get<uint64_t>();
      if (tmp >
          static_cast<uint64_t>(std::numeric_limits<std::size_t>::max())) {
        return arrow::Status::Invalid(std::string(key_name) + " out of range");
      }
      return static_cast<std::size_t>(tmp);
    } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      auto tmp = value.get<int64_t>();
      if (tmp < static_cast<int64_t>(std::numeric_limits<T>::min()) ||
          tmp > static_cast<int64_t>(std::numeric_limits<T>::max())) {
        return arrow::Status::Invalid(std::string(key_name) + " out of range");
      }
      return static_cast<T>(tmp);
    } else if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T>) {
      auto tmp = value.get<uint64_t>();
      if (tmp > static_cast<uint64_t>(std::numeric_limits<T>::max())) {
        return arrow::Status::Invalid(std::string(key_name) + " out of range");
      }
      return static_cast<T>(tmp);
    } else {
      return arrow::Status::Invalid(std::string(key_name) +
                                    " unsupported type");
    }
  } catch (const std::exception &e) {
    return arrow::Status::Invalid(std::string(key_name) +
                                  " parse error: " + e.what());
  }
}

} // namespace

arrow::Status EngineConfig::Validate() const {
  if (db_path.empty()) {
    return arrow::Status::Invalid("db_path must not be empty");
  }
  if (block_cache_size_mb == 0) {
    return arrow::Status::Invalid("block_cache_size_mb must be > 0");
  }
  if (ttl_days <= 0) {
    return arrow::Status::Invalid("ttl_days must be > 0");
  }
  if (max_column_families == 0) {
    return arrow::Status::Invalid("max_column_families must be > 0");
  }
  if (max_active_versions_per_table == 0) {
    return arrow::Status::Invalid("max_active_versions_per_table must be > 0");
  }
  if (max_background_compactions < 0) {
    return arrow::Status::Invalid("max_background_compactions must be >= 0");
  }
  if (max_background_flushes < 0) {
    return arrow::Status::Invalid("max_background_flushes must be >= 0");
  }
  if (write_buffer_size == 0) {
    return arrow::Status::Invalid("write_buffer_size must be > 0");
  }
  return arrow::Status::OK();
}

arrow::Result<EngineConfig>
EngineConfig::LoadFromJsonFile(std::string_view path) {
  std::ifstream in(std::string(path), std::ios::in | std::ios::binary);
  if (!in.is_open()) {
    return arrow::Status::IOError("failed to open config file");
  }

  nlohmann::json j;
  try {
    in >> j;
  } catch (const std::exception &e) {
    return arrow::Status::Invalid(std::string("invalid json: ") + e.what());
  }

  if (!j.is_object()) {
    return arrow::Status::Invalid("json root must be an object");
  }

  EngineConfig cfg;
  try {
    if (j.contains("db_path")) {
      cfg.db_path = j.at("db_path").get<std::string>();
    }
    if (j.contains("disable_wal")) {
      cfg.disable_wal = j.at("disable_wal").get<bool>();
    }
    if (j.contains("use_direct_reads")) {
      cfg.use_direct_reads = j.at("use_direct_reads").get<bool>();
    }
    if (j.contains("fill_cache_on_read")) {
      cfg.fill_cache_on_read = j.at("fill_cache_on_read").get<bool>();
    }
    if (j.contains("block_cache_size_mb")) {
      ARROW_ASSIGN_OR_RAISE(
          cfg.block_cache_size_mb,
          ParseNumberWithRange<std::size_t>(j.at("block_cache_size_mb"),
                                            "block_cache_size_mb"));
    }
    if (j.contains("ttl_days")) {
      ARROW_ASSIGN_OR_RAISE(cfg.ttl_days, ParseNumberWithRange<int64_t>(
                                              j.at("ttl_days"), "ttl_days"));
    }
    if (j.contains("enable_mmap_reads")) {
      cfg.enable_mmap_reads = j.at("enable_mmap_reads").get<bool>();
    }
    if (j.contains("max_column_families")) {
      ARROW_ASSIGN_OR_RAISE(
          cfg.max_column_families,
          ParseNumberWithRange<uint32_t>(j.at("max_column_families"),
                                         "max_column_families"));
    }
    if (j.contains("max_active_versions_per_table")) {
      ARROW_ASSIGN_OR_RAISE(
          cfg.max_active_versions_per_table,
          ParseNumberWithRange<uint16_t>(j.at("max_active_versions_per_table"),
                                         "max_active_versions_per_table"));
    }
    if (j.contains("max_background_compactions")) {
      ARROW_ASSIGN_OR_RAISE(
          cfg.max_background_compactions,
          ParseNumberWithRange<int>(j.at("max_background_compactions"),
                                    "max_background_compactions"));
    }
    if (j.contains("max_background_flushes")) {
      ARROW_ASSIGN_OR_RAISE(
          cfg.max_background_flushes,
          ParseNumberWithRange<int>(j.at("max_background_flushes"),
                                    "max_background_flushes"));
    }
    if (j.contains("write_buffer_size")) {
      ARROW_ASSIGN_OR_RAISE(
          cfg.write_buffer_size,
          ParseNumberWithRange<std::size_t>(j.at("write_buffer_size"),
                                            "write_buffer_size"));
    }
  } catch (const std::exception &e) {
    return arrow::Status::Invalid(std::string("config parse error: ") +
                                  e.what());
  }

  ARROW_RETURN_NOT_OK(cfg.Validate());
  return cfg;
}

std::string EngineConfig::ToString() const {
  nlohmann::json j;
  j["db_path"] = db_path;
  j["disable_wal"] = disable_wal;
  j["use_direct_reads"] = use_direct_reads;
  j["fill_cache_on_read"] = fill_cache_on_read;
  j["block_cache_size_mb"] = block_cache_size_mb;
  j["ttl_days"] = ttl_days;
  j["enable_mmap_reads"] = enable_mmap_reads;
  j["max_column_families"] = max_column_families;
  j["max_active_versions_per_table"] = max_active_versions_per_table;
  j["max_background_compactions"] = max_background_compactions;
  j["max_background_flushes"] = max_background_flushes;
  j["write_buffer_size"] = write_buffer_size;
  return j.dump();
}

} // namespace feature_store
