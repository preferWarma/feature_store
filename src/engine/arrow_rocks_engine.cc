#include "arrow_rocks_engine.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <rocksdb/cache.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>

#include "arrow_merge_operator.h"
#include "arrow_ttl_filter.h"
#include "column_projection.h"
#include "frame_codec.h"
#include "key_encoder.h"
#include "pinnable_buffer.h"

namespace feature_store {
namespace {

arrow::Status ToArrowStatus(const rocksdb::Status& s) {
    if (s.ok()) {
        return arrow::Status::OK();
    }
    if (s.IsNotFound()) {
        return arrow::Status::KeyError(s.ToString());
    }
    return arrow::Status::IOError(s.ToString());
}

std::span<const uint8_t> AsSpan(const rocksdb::PinnableSlice& s) {
    return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(s.data()),
                                    static_cast<std::size_t>(s.size()));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ReadSingleRecordBatchFromIPC(
    const std::shared_ptr<arrow::Buffer>& ipc) {
    auto input = std::make_shared<arrow::io::BufferReader>(ipc);
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(input));
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->Next());
    if (!batch) {
        return arrow::Status::SerializationError("IPC stream does not contain a RecordBatch");
    }
    ARROW_ASSIGN_OR_RAISE(auto trailing, reader->Next());
    if (trailing) {
        return arrow::Status::SerializationError("IPC stream contains multiple RecordBatches");
    }
    return batch;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProjectToSchema(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& target_schema) {
    if (!batch) {
        return arrow::Status::Invalid("batch is null");
    }
    if (!target_schema) {
        return arrow::Status::Invalid("target schema is null");
    }

    std::vector<std::shared_ptr<arrow::Array>> cols;
    cols.reserve(static_cast<std::size_t>(target_schema->num_fields()));

    for (const auto& field : target_schema->fields()) {
        const int idx = batch->schema()->GetFieldIndex(field->name());
        if (idx >= 0) {
            const auto& src_field = batch->schema()->field(idx);
            if (!src_field->type()->Equals(field->type())) {
                return arrow::Status::Invalid("field type mismatch: " + field->name());
            }
            cols.push_back(batch->column(idx));
            continue;
        }

        ARROW_ASSIGN_OR_RAISE(auto nulls,
                             arrow::MakeArrayOfNull(field->type(), batch->num_rows()));
        cols.push_back(std::move(nulls));
    }

    return arrow::RecordBatch::Make(target_schema, batch->num_rows(), std::move(cols));
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
        const auto& header = *header_res;
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

}  // namespace

ArrowRocksEngine::ArrowRocksEngine() = default;

ArrowRocksEngine::~ArrowRocksEngine() {
    (void)Close();
}

arrow::Status ArrowRocksEngine::Init(const EngineConfig& config) {
    ARROW_RETURN_NOT_OK(config.Validate());
    config_ = config;

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.max_background_compactions = config.max_background_compactions;
    options.max_background_flushes = config.max_background_flushes;
    options.write_buffer_size = static_cast<size_t>(config.write_buffer_size);
    options.allow_mmap_reads = config.enable_mmap_reads;

    rocksdb::BlockBasedTableOptions table_opts;
    table_opts.block_cache = rocksdb::NewLRUCache(config.block_cache_size_mb * 1024ULL * 1024ULL);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

    merge_operator_ = std::make_shared<ArrowMergeOperator>(&registry_);
    options.merge_operator = merge_operator_;

    ttl_filter_factory_ = std::make_shared<ArrowTTLFilterFactory>(config.ttl_days * 86400000LL);
    options.compaction_filter_factory = ttl_filter_factory_;

    cf_options_ = rocksdb::ColumnFamilyOptions(options);

    std::vector<std::string> cf_names;
    auto ls = rocksdb::DB::ListColumnFamilies(options, config.db_path, &cf_names);
    if (!ls.ok()) {
        cf_names = {rocksdb::kDefaultColumnFamilyName};
    }

    bool has_meta = false;
    for (const auto& n : cf_names) {
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
    for (const auto& n : cf_names) {
        descs.emplace_back(n, cf_options_);
    }

    rocksdb::DB* raw_db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    auto s = rocksdb::DB::Open(options, config.db_path, descs, &handles, &raw_db);
    ARROW_RETURN_NOT_OK(ToArrowStatus(s));
    db_.reset(raw_db);

    default_cf_ = nullptr;
    meta_cf_ = nullptr;
    cf_handles_.clear();
    for (std::size_t i = 0; i < handles.size(); ++i) {
        if (descs[i].name == rocksdb::kDefaultColumnFamilyName) {
            default_cf_ = handles[i];
        } else if (descs[i].name == "__meta__") {
            meta_cf_ = handles[i];
        } else if (descs[i].name.rfind("t_", 0) == 0) {
            unsigned int tid = 0;
            if (std::sscanf(descs[i].name.c_str(), "t_%u", &tid) == 1 && tid <= 65535U) {
                cf_handles_.emplace(static_cast<uint16_t>(tid), handles[i]);
            } else {
                db_->DestroyColumnFamilyHandle(handles[i]);
            }
        } else {
            db_->DestroyColumnFamilyHandle(handles[i]);
        }
    }

    if (!meta_cf_) {
        rocksdb::ColumnFamilyHandle* h = nullptr;
        s = db_->CreateColumnFamily(cf_options_, "__meta__", &h);
        ARROW_RETURN_NOT_OK(ToArrowStatus(s));
        meta_cf_ = h;
    }

    ARROW_RETURN_NOT_OK(registry_.Recover(db_.get(), meta_cf_));
    return arrow::Status::OK();
}

arrow::Status ArrowRocksEngine::Close() {
    if (!db_) {
        return arrow::Status::OK();
    }
    for (auto& [_, h] : cf_handles_) {
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
    return arrow::Status::OK();
}

std::string ArrowRocksEngine::CFName(uint16_t table_id) const {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "t_%04u", static_cast<unsigned int>(table_id));
    return std::string(buf);
}

rocksdb::ColumnFamilyHandle* ArrowRocksEngine::GetCF(uint16_t table_id) const {
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
    if (!db_) {
        return arrow::Status::Invalid("db not initialized");
    }
    rocksdb::ColumnFamilyHandle* h = nullptr;
    auto s = db_->CreateColumnFamily(cf_options_, CFName(table_id), &h);
    ARROW_RETURN_NOT_OK(ToArrowStatus(s));
    cf_handles_.emplace(table_id, h);
    return arrow::Status::OK();
}

arrow::Status ArrowRocksEngine::RegisterSchema(uint16_t table_id,
                                               uint16_t version,
                                               std::shared_ptr<arrow::Schema> schema) {
    if (!db_ || !meta_cf_) {
        return arrow::Status::Invalid("db not initialized");
    }
    ARROW_RETURN_NOT_OK(EnsureCF(table_id));
    return registry_.Register(table_id, version, std::move(schema), db_.get(), meta_cf_);
}

arrow::Status ArrowRocksEngine::AppendFeature(uint16_t table_id,
                                              uint64_t uid,
                                              uint16_t schema_version,
                                              const arrow::RecordBatch& delta_batch) {
    if (!db_) {
        return arrow::Status::Invalid("db not initialized");
    }
    auto* cf = GetCF(table_id);
    if (!cf) {
        return arrow::Status::KeyError("table not found");
    }
    ARROW_ASSIGN_OR_RAISE(auto frame,
                          EncodeFrame(schema_version,
                                      static_cast<int64_t>(std::chrono::duration_cast<
                                          std::chrono::milliseconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count()),
                                      delta_batch));
    auto s = db_->Merge(write_opts_, cf, EncodeKey(uid), frame);
    return ToArrowStatus(s);
}

arrow::Status ArrowRocksEngine::PutFeature(uint16_t table_id,
                                           uint64_t uid,
                                           uint16_t schema_version,
                                           int64_t timestamp,
                                           const arrow::RecordBatch& record_batch) {
    if (!db_) {
        return arrow::Status::Invalid("db not initialized");
    }
    auto* cf = GetCF(table_id);
    if (!cf) {
        return arrow::Status::KeyError("table not found");
    }
    ARROW_ASSIGN_OR_RAISE(auto frame, EncodeFrame(schema_version, timestamp, record_batch));
    auto s = db_->Put(write_opts_, cf, EncodeKey(uid), frame);
    return ToArrowStatus(s);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ArrowRocksEngine::GetFeature(
    uint16_t table_id,
    uint64_t uid,
    uint16_t target_version,
    const std::vector<std::string>& columns) {
    if (!db_) {
        return arrow::Status::Invalid("db not initialized");
    }
    auto* cf = GetCF(table_id);
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
        return ToArrowStatus(s);
    }

    auto pinned_buffer = std::make_shared<PinnableBuffer>(std::move(pinnable));
    auto bytes = std::span<const uint8_t>(pinned_buffer->data(),
                                          static_cast<std::size_t>(pinned_buffer->size()));

    const auto selected = SelectLatestValidFrame(bytes);
    if (!selected.found) {
        return arrow::Status::SerializationError("no valid frame");
    }

    const std::size_t ipc_offset = selected.frame_offset + kFrameHeaderSize;
    auto ipc_slice = std::make_shared<arrow::Buffer>(pinned_buffer,
                                                     static_cast<int64_t>(ipc_offset),
                                                     static_cast<int64_t>(selected.header.ipc_length));
    ARROW_ASSIGN_OR_RAISE(auto batch, ReadSingleRecordBatchFromIPC(ipc_slice));

    if (!batch->schema()->Equals(*target_schema, false)) {
        ARROW_ASSIGN_OR_RAISE(batch, ProjectToSchema(batch, target_schema));
    }

    if (!columns.empty()) {
        return ProjectColumns(batch, columns);
    }
    return batch;
}

std::vector<BatchGetResult> ArrowRocksEngine::BatchGetFeature(
    const std::vector<BatchGetRequest>& requests) {
    std::vector<BatchGetResult> results(requests.size());
    if (!db_) {
        for (std::size_t i = 0; i < requests.size(); ++i) {
            results[i] = {requests[i].uid, arrow::Status::Invalid("db not initialized"), nullptr};
        }
        return results;
    }

    std::unordered_map<uint16_t, std::vector<std::size_t>> groups;
    for (std::size_t i = 0; i < requests.size(); ++i) {
        groups[requests[i].table_id].push_back(i);
    }

    for (auto& [table_id, indices] : groups) {
        auto* cf = GetCF(table_id);
        if (!cf) {
            for (auto idx : indices) {
                results[idx] = {requests[idx].uid, arrow::Status::KeyError("table not found"), nullptr};
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
        db_->MultiGet(read_opts_,
                      cf,
                      static_cast<int>(indices.size()),
                      keys.data(),
                      values.data(),
                      statuses.data());

        for (std::size_t j = 0; j < indices.size(); ++j) {
            const auto idx = indices[j];
            if (!statuses[j].ok()) {
                results[idx] = {requests[idx].uid, ToArrowStatus(statuses[j]), nullptr};
                continue;
            }

            auto target_schema = registry_.Get(table_id, requests[idx].target_version);
            if (!target_schema) {
                results[idx] = {requests[idx].uid, arrow::Status::KeyError("schema not found"), nullptr};
                continue;
            }

            auto pinned_buffer = std::make_shared<PinnableBuffer>(std::move(values[j]));
            auto bytes = std::span<const uint8_t>(pinned_buffer->data(),
                                                  static_cast<std::size_t>(pinned_buffer->size()));
            const auto selected = SelectLatestValidFrame(bytes);
            if (!selected.found) {
                results[idx] = {requests[idx].uid,
                                arrow::Status::SerializationError("no valid frame"),
                                nullptr};
                continue;
            }

            const std::size_t ipc_offset = selected.frame_offset + kFrameHeaderSize;
            auto ipc_slice = std::make_shared<arrow::Buffer>(
                pinned_buffer,
                static_cast<int64_t>(ipc_offset),
                static_cast<int64_t>(selected.header.ipc_length));

            auto batch_res = ReadSingleRecordBatchFromIPC(ipc_slice);
            if (!batch_res.ok()) {
                results[idx] = {requests[idx].uid, batch_res.status(), nullptr};
                continue;
            }
            auto batch = batch_res.ValueOrDie();

            if (!batch->schema()->Equals(*target_schema, false)) {
                auto projected = ProjectToSchema(batch, target_schema);
                if (!projected.ok()) {
                    results[idx] = {requests[idx].uid, projected.status(), nullptr};
                    continue;
                }
                batch = projected.ValueOrDie();
            }

            if (!requests[idx].columns.empty()) {
                auto projected_cols = ProjectColumns(batch, requests[idx].columns);
                if (!projected_cols.ok()) {
                    results[idx] = {requests[idx].uid, projected_cols.status(), nullptr};
                    continue;
                }
                batch = projected_cols.ValueOrDie();
            }

            results[idx] = {requests[idx].uid, arrow::Status::OK(), std::move(batch)};
        }
    }

    return results;
}

arrow::Status ArrowRocksEngine::CompactAll() {
    if (!db_) {
        return arrow::Status::Invalid("db not initialized");
    }
    rocksdb::CompactRangeOptions opts;
    auto s = db_->CompactRange(opts, nullptr, nullptr);
    return ToArrowStatus(s);
}

arrow::Status ArrowRocksEngine::FlushAll() {
    if (!db_) {
        return arrow::Status::Invalid("db not initialized");
    }
    rocksdb::FlushOptions opts;
    auto s = db_->Flush(opts);
    return ToArrowStatus(s);
}

}  // namespace feature_store
