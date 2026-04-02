#include "arrow_rocks_engine.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>

#include <parquet/arrow/writer.h>

#include <rocksdb/cache.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>

#include "arrow_merge_operator.h"
#include "arrow_ttl_filter.h"
#include "column_projection.h"
#include "frame_codec.h"
#include "key_encoder.h"
#include "metrics.h"
#include "pinnable_buffer.h"

namespace feature_store {
namespace {

arrow::Status ToArrowStatus(const rocksdb::Status &s) {
  if (s.ok()) {
    return arrow::Status::OK();
  }
  if (s.IsNotFound()) {
    return arrow::Status::KeyError(s.ToString());
  }
  return arrow::Status::IOError(s.ToString());
}

std::span<const uint8_t> AsSpan(const rocksdb::PinnableSlice &s) {
  return std::span<const uint8_t>(reinterpret_cast<const uint8_t *>(s.data()),
                                  static_cast<std::size_t>(s.size()));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ReadSingleRecordBatchFromIPC(const std::shared_ptr<arrow::Buffer> &ipc) {
  auto input = std::make_shared<arrow::io::BufferReader>(ipc);
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        arrow::ipc::RecordBatchStreamReader::Open(input));
  ARROW_ASSIGN_OR_RAISE(auto batch, reader->Next());
  if (!batch) {
    return arrow::Status::SerializationError(
        "IPC stream does not contain a RecordBatch");
  }
  ARROW_ASSIGN_OR_RAISE(auto trailing, reader->Next());
  if (trailing) {
    return arrow::Status::SerializationError(
        "IPC stream contains multiple RecordBatches");
  }
  return batch;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ProjectToSchema(const std::shared_ptr<arrow::RecordBatch> &batch,
                const std::shared_ptr<arrow::Schema> &target_schema) {
  if (!batch) {
    return arrow::Status::Invalid("batch is null");
  }
  if (!target_schema) {
    return arrow::Status::Invalid("target schema is null");
  }

  std::vector<std::shared_ptr<arrow::Array>> cols;
  cols.reserve(static_cast<std::size_t>(target_schema->num_fields()));

  for (const auto &field : target_schema->fields()) {
    const int idx = batch->schema()->GetFieldIndex(field->name());
    if (idx >= 0) {
      const auto &src_field = batch->schema()->field(idx);
      if (!src_field->type()->Equals(field->type())) {
        return arrow::Status::Invalid("field type mismatch: " + field->name());
      }
      cols.push_back(batch->column(idx));
      continue;
    }

    ARROW_ASSIGN_OR_RAISE(
        auto nulls, arrow::MakeArrayOfNull(field->type(), batch->num_rows()));
    cols.push_back(std::move(nulls));
  }

  return arrow::RecordBatch::Make(target_schema, batch->num_rows(),
                                  std::move(cols));
}

struct SelectedFrame {
  bool found = false;
  std::size_t frame_offset = 0;
  std::size_t frame_size = 0;
  FrameHeader header{};
};

SelectedFrame SelectLatestValidFrame(std::span<const uint8_t> bytes) {
  SelectedFrame selected;
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    auto remaining = bytes.subspan(offset);
    auto header_res = DecodeFrameHeader(remaining);
    if (!header_res.ok()) {
      break;
    }
    const auto &header = *header_res;
    const std::size_t frame_size = header.total_size();
    if (frame_size > remaining.size()) {
      break;
    }
    auto frame_span = bytes.subspan(offset, frame_size);
    if (ValidateFrameCRC(frame_span)) {
      if (!selected.found || header.timestamp >= selected.header.timestamp) {
        selected.found = true;
        selected.frame_offset = offset;
        selected.frame_size = frame_size;
        selected.header = header;
      }
    }
    offset += frame_size;
  }
  return selected;
}

arrow::Result<std::pair<uint16_t, std::shared_ptr<arrow::Schema>>>
FindLatestSchemaForArchive(const SchemaRegistry &registry, uint16_t table_id) {
  for (uint32_t version = std::numeric_limits<uint16_t>::max(); version > 0;
       --version) {
    auto schema = registry.Get(table_id, static_cast<uint16_t>(version));
    if (schema) {
      return std::make_pair(static_cast<uint16_t>(version), std::move(schema));
    }
  }
  return arrow::Status::KeyError("schema not found");
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> AddArchiveMetadataColumns(
    uint64_t uid, const Frame &frame,
    const std::shared_ptr<arrow::RecordBatch> &batch) {
  arrow::UInt64Builder uid_builder;
  arrow::Int64Builder ts_builder;
  arrow::UInt16Builder ver_builder;
  for (int64_t i = 0; i < batch->num_rows(); ++i) {
    ARROW_RETURN_NOT_OK(uid_builder.Append(uid));
    ARROW_RETURN_NOT_OK(ts_builder.Append(frame.timestamp));
    ARROW_RETURN_NOT_OK(ver_builder.Append(frame.schema_version));
  }

  std::shared_ptr<arrow::Array> uid_array;
  std::shared_ptr<arrow::Array> ts_array;
  std::shared_ptr<arrow::Array> ver_array;
  ARROW_RETURN_NOT_OK(uid_builder.Finish(&uid_array));
  ARROW_RETURN_NOT_OK(ts_builder.Finish(&ts_array));
  ARROW_RETURN_NOT_OK(ver_builder.Finish(&ver_array));

  std::vector<std::shared_ptr<arrow::Field>> fields = {
      arrow::field("__uid", arrow::uint64(), false),
      arrow::field("__timestamp", arrow::int64(), false),
      arrow::field("__schema_version", arrow::uint16(), false),
  };
  std::vector<std::shared_ptr<arrow::Array>> columns = {
      uid_array, ts_array, ver_array};

  for (int i = 0; i < batch->num_columns(); ++i) {
    fields.push_back(batch->schema()->field(i));
    columns.push_back(batch->column(i));
  }

  return arrow::RecordBatch::Make(arrow::schema(std::move(fields)),
                                  batch->num_rows(), std::move(columns));
}

bool IsRunningInCgroup() {
  std::ifstream in("/proc/self/cgroup", std::ios::in);
  if (!in.is_open()) {
    return false;
  }
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty()) {
      return true;
    }
  }
  return false;
}

} // namespace

ArrowRocksEngine::ArrowRocksEngine() = default;

ArrowRocksEngine::~ArrowRocksEngine() { (void)Close(); }

arrow::Status ArrowRocksEngine::Init(const EngineConfig &config) {
  ARROW_RETURN_NOT_OK(config.Validate());
  state_ = EngineState::kNotReady;
  config_ = config;

  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.avoid_unnecessary_blocking_io = true;
  options.max_background_compactions = config.max_background_compactions;
  options.max_background_flushes = config.max_background_flushes;
  options.write_buffer_size = static_cast<size_t>(config.write_buffer_size);
  options.allow_mmap_reads = config.enable_mmap_reads;
  if (config.enable_mmap_reads && IsRunningInCgroup()) {
    std::fprintf(stderr, "WARNING: enable_mmap_reads=true under cgroup may "
                         "increase RSS accounting\n");
  }

  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.block_cache =
      rocksdb::NewLRUCache(config.block_cache_size_mb * 1024ULL * 1024ULL);
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

  merge_operator_ = std::make_shared<ArrowMergeOperator>(&registry_);
  options.merge_operator = merge_operator_;

  ttl_filter_factory_ =
      std::make_shared<ArrowTTLFilterFactory>(config.ttl_days * 86400000LL);
  options.compaction_filter_factory = ttl_filter_factory_;

  std::vector<std::string> cf_names;
  auto ls = rocksdb::DB::ListColumnFamilies(options, config.db_path, &cf_names);
  rocksdb::DB *raw_db = nullptr;
  default_cf_ = nullptr;
  meta_cf_ = nullptr;
  cf_handles_.clear();

  rocksdb::Status s;
  if (!ls.ok()) {
    s = rocksdb::DB::Open(options, config.db_path, &raw_db);
    ARROW_RETURN_NOT_OK(ToArrowStatus(s));
    db_.reset(raw_db);
  } else {
    bool has_meta = false;
    for (const auto &n : cf_names) {
      if (n == "__meta__") {
        has_meta = true;
        break;
      }
    }
    if (!has_meta) {
      cf_names.push_back("__meta__");
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> descs;
    descs.reserve(cf_names.size());
    for (const auto &n : cf_names) {
      descs.emplace_back(n, MakeCFOptions());
    }

    std::vector<rocksdb::ColumnFamilyHandle *> handles;
    s = rocksdb::DB::Open(options, config.db_path, descs, &handles, &raw_db);
    ARROW_RETURN_NOT_OK(ToArrowStatus(s));
    db_.reset(raw_db);

    for (std::size_t i = 0; i < handles.size(); ++i) {
      if (descs[i].name == rocksdb::kDefaultColumnFamilyName) {
        default_cf_ = handles[i];
      } else if (descs[i].name == "__meta__") {
        meta_cf_ = handles[i];
      } else if (descs[i].name.rfind("t_", 0) == 0) {
        unsigned int tid = 0;
        if (std::sscanf(descs[i].name.c_str(), "t_%u", &tid) == 1 &&
            tid <= 65535U) {
          cf_handles_.emplace(static_cast<uint16_t>(tid), handles[i]);
        } else {
          db_->DestroyColumnFamilyHandle(handles[i]);
        }
      } else {
        db_->DestroyColumnFamilyHandle(handles[i]);
      }
    }
  }

  if (!meta_cf_) {
    rocksdb::ColumnFamilyHandle *h = nullptr;
    s = db_->CreateColumnFamily(MakeCFOptions(), "__meta__", &h);
    ARROW_RETURN_NOT_OK(ToArrowStatus(s));
    meta_cf_ = h;
  }

  ARROW_RETURN_NOT_OK(registry_.Recover(db_.get(), meta_cf_));
  ARROW_RETURN_NOT_OK(CleanupOrphanCFs());
  active_cf_count_.store(static_cast<uint32_t>(cf_handles_.size()),
                         std::memory_order_relaxed);
  Metrics::Global()->SetActiveCFCount(
      active_cf_count_.load(std::memory_order_relaxed));
  state_ = EngineState::kReady;
  return arrow::Status::OK();
}

arrow::Status ArrowRocksEngine::Close() {
  if (!db_) {
    return arrow::Status::OK();
  }
  for (auto &[_, h] : cf_handles_) {
    db_->DestroyColumnFamilyHandle(h);
  }
  cf_handles_.clear();
  if (meta_cf_) {
    db_->DestroyColumnFamilyHandle(meta_cf_);
    meta_cf_ = nullptr;
  }
  if (default_cf_) {
    db_->DestroyColumnFamilyHandle(default_cf_);
    default_cf_ = nullptr;
  }
  db_.reset();
  state_ = EngineState::kClosed;
  return arrow::Status::OK();
}

std::string ArrowRocksEngine::CFName(uint16_t table_id) const {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "t_%04u",
                static_cast<unsigned int>(table_id));
  return std::string(buf);
}

rocksdb::ColumnFamilyOptions ArrowRocksEngine::MakeCFOptions() const {
  rocksdb::ColumnFamilyOptions opts;
  opts.write_buffer_size = config_.write_buffer_size;
  opts.merge_operator = merge_operator_;
  opts.compaction_filter_factory = ttl_filter_factory_;

  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.block_cache =
      rocksdb::NewLRUCache(config_.block_cache_size_mb * 1024ULL * 1024ULL);
  opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));
  return opts;
}

rocksdb::ColumnFamilyHandle *ArrowRocksEngine::GetCF(uint16_t table_id) const {
  auto it = cf_handles_.find(table_id);
  if (it == cf_handles_.end()) {
    return nullptr;
  }
  return it->second;
}

arrow::Status ArrowRocksEngine::EnsureCF(uint16_t table_id) {
  if (GetCF(table_id) != nullptr) {
    return arrow::Status::OK();
  }
  if (active_cf_count_.load(std::memory_order_relaxed) >=
      config_.max_column_families) {
    return arrow::Status::CapacityError("too many column families");
  }
  if (!db_) {
    return arrow::Status::Invalid("db not initialized");
  }
  rocksdb::ColumnFamilyHandle *h = nullptr;
  auto s = db_->CreateColumnFamily(MakeCFOptions(), CFName(table_id), &h);
  ARROW_RETURN_NOT_OK(ToArrowStatus(s));
  cf_handles_.emplace(table_id, h);
  active_cf_count_.fetch_add(1, std::memory_order_relaxed);
  Metrics::Global()->SetActiveCFCount(
      active_cf_count_.load(std::memory_order_relaxed));
  return arrow::Status::OK();
}

arrow::Status
ArrowRocksEngine::RegisterSchema(uint16_t table_id, uint16_t version,
                                 std::shared_ptr<arrow::Schema> schema) {
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  if (!db_ || !meta_cf_) {
    return arrow::Status::Invalid("db not initialized");
  }
  bool new_cf = false;
  if (GetCF(table_id) == nullptr) {
    ARROW_RETURN_NOT_OK(EnsureCF(table_id));
    new_cf = true;
  }

  rocksdb::WriteBatch batch;
  const std::string schema_key =
      "schema/" + std::to_string(table_id) + "/" + std::to_string(version);

  ARROW_ASSIGN_OR_RAISE(
      auto serialized,
      arrow::ipc::SerializeSchema(*schema, arrow::default_memory_pool()));
  std::string schema_bytes(reinterpret_cast<const char *>(serialized->data()),
                           static_cast<std::size_t>(serialized->size()));

  batch.Put(meta_cf_, schema_key, schema_bytes);
  batch.Put(meta_cf_, "cf_state/" + std::to_string(table_id), "active");
  if (new_cf) {
    batch.Put(meta_cf_, "cf_count", std::to_string(active_cf_count_.load()));
  }

  auto s = db_->Write(write_opts_, &batch);
  if (!s.ok()) {
    if (new_cf) {
      auto *cf = GetCF(table_id);
      if (cf) {
        db_->DropColumnFamily(cf);
        db_->DestroyColumnFamilyHandle(cf);
        cf_handles_.erase(table_id);
        active_cf_count_.fetch_sub(1, std::memory_order_relaxed);
      }
    }
    return ToArrowStatus(s);
  }

  return registry_.RegisterInMemory(table_id, version, std::move(schema));
}

arrow::Status
ArrowRocksEngine::AppendFeature(uint16_t table_id, uint64_t uid,
                                uint16_t schema_version,
                                const arrow::RecordBatch &delta_batch) {
  const auto start = std::chrono::steady_clock::now();
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  if (!db_) {
    return arrow::Status::Invalid("db not initialized");
  }
  auto *cf = GetCF(table_id);
  if (!cf) {
    return arrow::Status::KeyError("table not found");
  }
  ARROW_ASSIGN_OR_RAISE(
      auto frame,
      EncodeFrame(schema_version,
                  static_cast<int64_t>(
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count()),
                  delta_batch));
  auto s = db_->Merge(write_opts_, cf, EncodeKey(uid), frame);
  const auto latency_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
  if (s.ok()) {
    Metrics::Global()->IncAppend(table_id, static_cast<uint64_t>(frame.size()),
                                 latency_us);
  }
  return ToArrowStatus(s);
}

arrow::Status
ArrowRocksEngine::PutFeature(uint16_t table_id, uint64_t uid,
                             uint16_t schema_version, int64_t timestamp,
                             const arrow::RecordBatch &record_batch) {
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  if (!db_) {
    return arrow::Status::Invalid("db not initialized");
  }
  auto *cf = GetCF(table_id);
  if (!cf) {
    return arrow::Status::KeyError("table not found");
  }
  ARROW_ASSIGN_OR_RAISE(auto frame,
                        EncodeFrame(schema_version, timestamp, record_batch));
  auto s = db_->Put(write_opts_, cf, EncodeKey(uid), frame);
  return ToArrowStatus(s);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ArrowRocksEngine::GetFeature(uint16_t table_id, uint64_t uid,
                             uint16_t target_version,
                             const std::vector<std::string> &columns) {
  const auto start = std::chrono::steady_clock::now();
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  if (!db_) {
    return arrow::Status::Invalid("db not initialized");
  }
  auto *cf = GetCF(table_id);
  if (!cf) {
    return arrow::Status::KeyError("table not found");
  }

  auto target_schema = registry_.Get(table_id, target_version);
  if (!target_schema) {
    return arrow::Status::KeyError("schema not found");
  }

  rocksdb::PinnableSlice pinnable;
  auto s = db_->Get(read_opts_, cf, EncodeKey(uid), &pinnable);
  if (!s.ok()) {
    const auto latency_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
    Metrics::Global()->IncGet(table_id, false, latency_us, 1.0);
    return ToArrowStatus(s);
  }

  auto pinned_buffer = std::make_shared<PinnableBuffer>(std::move(pinnable));
  auto bytes = std::span<const uint8_t>(
      pinned_buffer->data(), static_cast<std::size_t>(pinned_buffer->size()));

  const auto selected = SelectLatestValidFrame(bytes);
  if (!selected.found) {
    Metrics::Global()->IncCRCError(1);
    return arrow::Status::SerializationError("no valid frame");
  }

  const std::size_t ipc_offset = selected.frame_offset + kFrameHeaderSize;
  auto ipc_slice = std::make_shared<arrow::Buffer>(
      pinned_buffer, static_cast<int64_t>(ipc_offset),
      static_cast<int64_t>(selected.header.ipc_length));
  ARROW_ASSIGN_OR_RAISE(auto batch, ReadSingleRecordBatchFromIPC(ipc_slice));

  if (!batch->schema()->Equals(*target_schema, false)) {
    ARROW_ASSIGN_OR_RAISE(batch, ProjectToSchema(batch, target_schema));
  }

  if (!columns.empty()) {
    ARROW_ASSIGN_OR_RAISE(batch, ProjectColumns(batch, columns));
  }
  const double projection_ratio =
      static_cast<double>(columns.empty() ? batch->num_columns()
                                          : columns.size()) /
      static_cast<double>(target_schema->num_fields());
  const auto latency_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
  Metrics::Global()->IncGet(table_id, true, latency_us, projection_ratio);
  return batch;
}

std::vector<BatchGetResult> ArrowRocksEngine::BatchGetFeature(
    const std::vector<BatchGetRequest> &requests) {
  std::vector<BatchGetResult> results(requests.size());
  Metrics::Global()->IncBatchGet(1, static_cast<uint64_t>(requests.size()));
  if (state_ != EngineState::kReady) {
    for (std::size_t i = 0; i < requests.size(); ++i) {
      results[i] = {requests[i].uid, arrow::Status::Invalid("engine not ready"),
                    nullptr};
    }
    return results;
  }
  if (!db_) {
    for (std::size_t i = 0; i < requests.size(); ++i) {
      results[i] = {requests[i].uid,
                    arrow::Status::Invalid("db not initialized"), nullptr};
    }
    return results;
  }

  std::unordered_map<uint16_t, std::vector<std::size_t>> groups;
  for (std::size_t i = 0; i < requests.size(); ++i) {
    groups[requests[i].table_id].push_back(i);
  }

  for (auto &[table_id, indices] : groups) {
    const auto group_start = std::chrono::steady_clock::now();
    auto *cf = GetCF(table_id);
    if (!cf) {
      for (auto idx : indices) {
        const auto latency_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - group_start)
                .count());
        Metrics::Global()->IncGet(table_id, false, latency_us, 1.0);
        results[idx] = {requests[idx].uid,
                        arrow::Status::KeyError("table not found"), nullptr};
      }
      continue;
    }

    std::vector<std::string> key_bufs;
    std::vector<rocksdb::Slice> keys;
    key_bufs.reserve(indices.size());
    keys.reserve(indices.size());
    for (auto idx : indices) {
      key_bufs.push_back(EncodeKey(requests[idx].uid));
      keys.emplace_back(key_bufs.back());
    }

    std::vector<rocksdb::PinnableSlice> values(indices.size());
    std::vector<rocksdb::Status> statuses(indices.size());
    db_->MultiGet(read_opts_, cf, static_cast<int>(indices.size()), keys.data(),
                  values.data(), statuses.data());

    for (std::size_t j = 0; j < indices.size(); ++j) {
      const auto idx = indices[j];
      const auto latency_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - group_start)
              .count());
      if (!statuses[j].ok()) {
        Metrics::Global()->IncGet(table_id, false, latency_us, 1.0);
        results[idx] = {requests[idx].uid, ToArrowStatus(statuses[j]), nullptr};
        continue;
      }

      auto target_schema =
          registry_.Get(table_id, requests[idx].target_version);
      if (!target_schema) {
        Metrics::Global()->IncGet(table_id, false, latency_us, 1.0);
        results[idx] = {requests[idx].uid,
                        arrow::Status::KeyError("schema not found"), nullptr};
        continue;
      }

      auto pinned_buffer =
          std::make_shared<PinnableBuffer>(std::move(values[j]));
      auto bytes = std::span<const uint8_t>(
          pinned_buffer->data(),
          static_cast<std::size_t>(pinned_buffer->size()));
      const auto selected = SelectLatestValidFrame(bytes);
      if (!selected.found) {
        Metrics::Global()->IncCRCError(1);
        Metrics::Global()->IncGet(table_id, false, latency_us, 1.0);
        results[idx] = {requests[idx].uid,
                        arrow::Status::SerializationError("no valid frame"),
                        nullptr};
        continue;
      }

      const std::size_t ipc_offset = selected.frame_offset + kFrameHeaderSize;
      auto ipc_slice = std::make_shared<arrow::Buffer>(
          pinned_buffer, static_cast<int64_t>(ipc_offset),
          static_cast<int64_t>(selected.header.ipc_length));

      auto batch_res = ReadSingleRecordBatchFromIPC(ipc_slice);
      if (!batch_res.ok()) {
        Metrics::Global()->IncGet(table_id, false, latency_us, 1.0);
        results[idx] = {requests[idx].uid, batch_res.status(), nullptr};
        continue;
      }
      auto batch = batch_res.ValueOrDie();

      if (!batch->schema()->Equals(*target_schema, false)) {
        auto projected = ProjectToSchema(batch, target_schema);
        if (!projected.ok()) {
          Metrics::Global()->IncGet(table_id, false, latency_us, 1.0);
          results[idx] = {requests[idx].uid, projected.status(), nullptr};
          continue;
        }
        batch = projected.ValueOrDie();
      }

      if (!requests[idx].columns.empty()) {
        auto projected_cols = ProjectColumns(batch, requests[idx].columns);
        if (!projected_cols.ok()) {
          Metrics::Global()->IncGet(table_id, false, latency_us, 1.0);
          results[idx] = {requests[idx].uid, projected_cols.status(), nullptr};
          continue;
        }
        batch = projected_cols.ValueOrDie();
      }

      const double projection_ratio =
          static_cast<double>(requests[idx].columns.empty()
                                  ? batch->num_columns()
                                  : requests[idx].columns.size()) /
          static_cast<double>(target_schema->num_fields());
      Metrics::Global()->IncGet(table_id, true, latency_us, projection_ratio);
      results[idx] = {requests[idx].uid, arrow::Status::OK(), std::move(batch)};
    }
  }

  return results;
}

arrow::Status ArrowRocksEngine::CompactAll() {
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  if (!db_) {
    return arrow::Status::Invalid("db not initialized");
  }
  rocksdb::CompactRangeOptions opts;
  auto s = db_->CompactRange(opts, nullptr, nullptr);
  return ToArrowStatus(s);
}

arrow::Status ArrowRocksEngine::FlushAll() {
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  if (!db_) {
    return arrow::Status::Invalid("db not initialized");
  }
  rocksdb::FlushOptions opts;
  auto s = db_->Flush(opts);
  return ToArrowStatus(s);
}

arrow::Status ArrowRocksEngine::DropTable(uint16_t table_id) {
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  auto *cf = GetCF(table_id);
  if (!cf) {
    return arrow::Status::KeyError("table not found");
  }

  rocksdb::WriteBatch batch;
  batch.Put(meta_cf_, "cf_state/" + std::to_string(table_id), "dropped");
  const uint32_t cur = active_cf_count_.load(std::memory_order_relaxed);
  const uint32_t next = cur > 0 ? (cur - 1) : 0;
  batch.Put(meta_cf_, "cf_count", std::to_string(next));
  auto ws = db_->Write(write_opts_, &batch);
  ARROW_RETURN_NOT_OK(ToArrowStatus(ws));

  ARROW_RETURN_NOT_OK(registry_.Unregister(table_id));
  auto s = db_->DropColumnFamily(cf);
  ARROW_RETURN_NOT_OK(ToArrowStatus(s));
  db_->DestroyColumnFamilyHandle(cf);
  cf_handles_.erase(table_id);
  active_cf_count_.fetch_sub(1, std::memory_order_relaxed);
  Metrics::Global()->SetActiveCFCount(
      active_cf_count_.load(std::memory_order_relaxed));
  Metrics::Global()->IncCfDrop(1);
  return arrow::Status::OK();
}

arrow::Status ArrowRocksEngine::ArchiveTable(uint16_t table_id,
                                             const std::string &archive_path) {
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  auto *cf = GetCF(table_id);
  if (!cf) {
    return arrow::Status::KeyError("table not found");
  }

  ARROW_ASSIGN_OR_RAISE(auto latest_schema_info,
                        FindLatestSchemaForArchive(registry_, table_id));
  const auto &latest_schema = latest_schema_info.second;

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  rocksdb::ReadOptions ro;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro, cf));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const auto key = it->key();
    const uint64_t uid =
        DecodeKey(std::string_view(key.data(), static_cast<std::size_t>(key.size())));

    const auto value = it->value();
    auto bytes = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t *>(value.data()),
        static_cast<std::size_t>(value.size()));
    std::size_t offset = 0;
    while (offset < bytes.size()) {
      auto header_res = DecodeFrameHeader(bytes.subspan(offset));
      if (!header_res.ok()) {
        break;
      }
      const auto frame_size = header_res->total_size();
      if (frame_size > bytes.size() - offset) {
        break;
      }
      auto frame_bytes = bytes.subspan(offset, frame_size);
      if (!ValidateFrameCRC(frame_bytes)) {
        Metrics::Global()->IncCRCError(1);
        offset += frame_size;
        continue;
      }

      ARROW_ASSIGN_OR_RAISE(auto frame, DecodeFrame(frame_bytes));
      std::shared_ptr<arrow::RecordBatch> batch = frame.record_batch;
      if (!batch->schema()->Equals(*latest_schema, false)) {
        ARROW_ASSIGN_OR_RAISE(batch, ProjectToSchema(batch, latest_schema));
      }
      ARROW_ASSIGN_OR_RAISE(auto archive_batch,
                            AddArchiveMetadataColumns(uid, frame, batch));
      batches.push_back(std::move(archive_batch));
      offset += frame_size;
    }
  }
  ARROW_RETURN_NOT_OK(ToArrowStatus(it->status()));

  const auto parent_dir = std::filesystem::path(archive_path).parent_path();
  if (!parent_dir.empty()) {
    std::error_code ec;
    std::filesystem::create_directories(parent_dir, ec);
    if (ec) {
      return arrow::Status::IOError("failed to create archive dir: " + ec.message());
    }
  }

  std::shared_ptr<arrow::Schema> archive_schema = arrow::schema({
      arrow::field("__uid", arrow::uint64(), false),
      arrow::field("__timestamp", arrow::int64(), false),
      arrow::field("__schema_version", arrow::uint16(), false),
  });
  for (const auto &f : latest_schema->fields()) {
    archive_schema = archive_schema->AddField(archive_schema->num_fields(), f).ValueOrDie();
  }

  std::shared_ptr<arrow::Table> table;
  if (batches.empty()) {
    table = arrow::Table::Make(
        archive_schema, std::vector<std::shared_ptr<arrow::ChunkedArray>>{}, 0);
  } else {
    ARROW_ASSIGN_OR_RAISE(table, arrow::Table::FromRecordBatches(archive_schema, batches));
  }

  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::FileOutputStream::Open(archive_path));
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), sink, std::max<int64_t>(table->num_rows(), 1)));
  ARROW_RETURN_NOT_OK(sink->Close());

  return DropTable(table_id);
}

arrow::Status ArrowRocksEngine::CompactRange(const CompactRangeOptions &opts) {
  if (state_ != EngineState::kReady) {
    return arrow::Status::Invalid("engine not ready");
  }
  rocksdb::CompactRangeOptions rocksdb_opts;
  rocksdb_opts.bottommost_level_compaction =
      opts.force_bottommost
          ? rocksdb::BottommostLevelCompaction::kForce
          : rocksdb::BottommostLevelCompaction::kIfHaveCompactionFilter;

  if (opts.table_id.has_value()) {
    auto *cf = GetCF(*opts.table_id);
    if (!cf)
      return arrow::Status::KeyError("table not found");
    std::string start_key, end_key;
    rocksdb::Slice *start = nullptr;
    rocksdb::Slice *end = nullptr;
    if (opts.uid_start) {
      start_key = EncodeKey(*opts.uid_start);
      start = new rocksdb::Slice(start_key);
    }
    if (opts.uid_end) {
      end_key = EncodeKey(*opts.uid_end);
      end = new rocksdb::Slice(end_key);
    }
    auto s = db_->CompactRange(rocksdb_opts, cf, start, end);
    delete start;
    delete end;
    return ToArrowStatus(s);
  }

  for (auto &[tid, cf] : cf_handles_) {
    auto s = db_->CompactRange(rocksdb_opts, cf, nullptr, nullptr);
    if (!s.ok())
      return ToArrowStatus(s);
  }
  return arrow::Status::OK();
}

arrow::Status ArrowRocksEngine::CleanupOrphanCFs() {
  if (!db_ || !meta_cf_) {
    return arrow::Status::OK();
  }

  std::vector<uint16_t> to_drop;
  to_drop.reserve(cf_handles_.size());

  rocksdb::ReadOptions ro;
  for (const auto &[tid, _] : cf_handles_) {
    std::string state;
    auto s = db_->Get(ro, meta_cf_, "cf_state/" + std::to_string(tid), &state);
    if (s.IsNotFound()) {
      to_drop.push_back(tid);
      continue;
    }
    if (!s.ok()) {
      return ToArrowStatus(s);
    }
    if (state != "active") {
      to_drop.push_back(tid);
    }
  }

  for (auto tid : to_drop) {
    auto it = cf_handles_.find(tid);
    if (it == cf_handles_.end()) {
      continue;
    }
    auto *cf = it->second;
    db_->DropColumnFamily(cf);
    db_->DestroyColumnFamilyHandle(cf);
    cf_handles_.erase(it);
  }

  return arrow::Status::OK();
}

} // namespace feature_store
