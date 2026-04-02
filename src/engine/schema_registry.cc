#include "schema_registry.h"

#include <charconv>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>

#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

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

std::string MakeSchemaKey(uint16_t table_id, uint16_t version) {
    return "schema/" + std::to_string(table_id) + "/" + std::to_string(version);
}

arrow::Status ParseUint16(std::string_view token, uint16_t* out) {
    uint32_t tmp = 0;
    auto first = token.data();
    auto last = token.data() + token.size();
    auto [ptr, ec] = std::from_chars(first, last, tmp);
    if (ec != std::errc() || ptr != last || tmp > std::numeric_limits<uint16_t>::max()) {
        return arrow::Status::Invalid("invalid schema key");
    }
    *out = static_cast<uint16_t>(tmp);
    return arrow::Status::OK();
}

arrow::Result<std::pair<uint16_t, uint16_t>> ParseSchemaKey(std::string_view key) {
    constexpr std::string_view kPrefix = "schema/";
    if (!key.starts_with(kPrefix)) {
        return arrow::Status::Invalid("invalid schema key");
    }
    key.remove_prefix(kPrefix.size());
    auto slash = key.find('/');
    if (slash == std::string_view::npos) {
        return arrow::Status::Invalid("invalid schema key");
    }
    auto table_part = key.substr(0, slash);
    auto version_part = key.substr(slash + 1);

    uint16_t table_id = 0;
    uint16_t version = 0;
    ARROW_RETURN_NOT_OK(ParseUint16(table_part, &table_id));
    ARROW_RETURN_NOT_OK(ParseUint16(version_part, &version));
    return std::make_pair(table_id, version);
}

arrow::Result<std::shared_ptr<arrow::Schema>> DeserializeSchemaFromBytes(
    std::string_view bytes) {
    ARROW_ASSIGN_OR_RAISE(auto owned, arrow::AllocateBuffer(static_cast<int64_t>(bytes.size())));
    std::memcpy(owned->mutable_data(), bytes.data(), bytes.size());
    auto buffer = std::shared_ptr<arrow::Buffer>(std::move(owned));
    auto reader = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
    arrow::ipc::DictionaryMemo memo;
    return arrow::ipc::ReadSchema(reader.get(), &memo);
}

arrow::Result<std::string> SerializeSchemaToBytes(const arrow::Schema& schema) {
    ARROW_ASSIGN_OR_RAISE(auto buf,
                          arrow::ipc::SerializeSchema(schema, arrow::default_memory_pool()));
    return std::string(reinterpret_cast<const char*>(buf->data()),
                       static_cast<std::size_t>(buf->size()));
}

arrow::Result<uint16_t> FindMaxVersionForTable(
    uint16_t table_id,
    const std::unordered_map<uint32_t, std::shared_ptr<arrow::Schema>>& schemas) {
    bool found = false;
    uint16_t max_version = 0;
    const uint32_t prefix = static_cast<uint32_t>(table_id) << 16U;
    for (const auto& [k, _] : schemas) {
        if ((k & 0xFFFF0000U) != prefix) {
            continue;
        }
        const uint16_t version = static_cast<uint16_t>(k & 0xFFFFU);
        if (!found || version > max_version) {
            max_version = version;
            found = true;
        }
    }
    if (!found) {
        return arrow::Status::KeyError("table not found");
    }
    return max_version;
}

arrow::Status ValidateSupersetSchema(const arrow::Schema& base, const arrow::Schema& candidate) {
    for (const auto& f : base.fields()) {
        const int idx = candidate.GetFieldIndex(f->name());
        if (idx < 0) {
            return arrow::Status::Invalid("schema is not a superset: missing field");
        }
        const auto& cf = candidate.field(idx);
        if (!f->Equals(*cf, false)) {
            return arrow::Status::Invalid("schema is not a superset: incompatible field");
        }
    }

    for (const auto& cf : candidate.fields()) {
        const int idx = base.GetFieldIndex(cf->name());
        if (idx >= 0) {
            continue;
        }
        if (!cf->nullable()) {
            return arrow::Status::Invalid(
                "schema is not a superset: new field must be nullable");
        }
    }
    return arrow::Status::OK();
}

}  // namespace

arrow::Status SchemaRegistry::Register(uint16_t table_id,
                                       uint16_t version,
                                       std::shared_ptr<arrow::Schema> schema,
                                       rocksdb::DB* db,
                                       rocksdb::ColumnFamilyHandle* meta_cf) {
    if (schema == nullptr) {
        return arrow::Status::Invalid("schema is null");
    }
    if (db == nullptr || meta_cf == nullptr) {
        return arrow::Status::Invalid("db/meta_cf is null");
    }

    ARROW_ASSIGN_OR_RAISE(auto serialized, SerializeSchemaToBytes(*schema));
    const auto key = MakeSchemaKey(table_id, version);
    rocksdb::WriteOptions wopts;
    const rocksdb::Status s = db->Put(wopts, meta_cf, rocksdb::Slice(key), rocksdb::Slice(serialized));
    ARROW_RETURN_NOT_OK(ToArrowStatus(s));

    return RegisterInMemory(table_id, version, std::move(schema));
}

arrow::Status SchemaRegistry::RegisterInMemory(uint16_t table_id,
                                               uint16_t version,
                                               std::shared_ptr<arrow::Schema> schema) {
    if (schema == nullptr) {
        return arrow::Status::Invalid("schema is null");
    }

    std::unique_lock lock(mu_);

    const uint32_t encoded_key = EncodeKey(table_id, version);
    if (schemas_.contains(encoded_key)) {
        return arrow::Status::Invalid("schema already registered");
    }

    auto max_version_res = FindMaxVersionForTable(table_id, schemas_);
    if (max_version_res.ok()) {
        const uint16_t base_version = *max_version_res;
        const auto base_schema = schemas_.at(EncodeKey(table_id, base_version));
        if (version <= base_version) {
            return arrow::Status::Invalid("schema version must be greater than existing max");
        }
        ARROW_RETURN_NOT_OK(ValidateSupersetSchema(*base_schema, *schema));
    }

    schemas_.emplace(encoded_key, std::move(schema));
    return arrow::Status::OK();
}

arrow::Status SchemaRegistry::Unregister(uint16_t table_id) {
    std::unique_lock lock(mu_);
    const uint32_t prefix = static_cast<uint32_t>(table_id) << 16U;
    for (auto it = schemas_.begin(); it != schemas_.end();) {
        if ((it->first & 0xFFFF0000U) == prefix) {
            it = schemas_.erase(it);
        } else {
            ++it;
        }
    }
    return arrow::Status::OK();
}

std::shared_ptr<arrow::Schema> SchemaRegistry::Get(uint16_t table_id, uint16_t version) const {
    const uint32_t encoded_key = EncodeKey(table_id, version);
    std::shared_lock lock(mu_);
    auto it = schemas_.find(encoded_key);
    if (it == schemas_.end()) {
        return nullptr;
    }
    return it->second;
}

arrow::Status SchemaRegistry::Recover(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* meta_cf) {
    if (db == nullptr || meta_cf == nullptr) {
        return arrow::Status::Invalid("db/meta_cf is null");
    }

    rocksdb::ReadOptions ropts;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ropts, meta_cf));
    constexpr std::string_view kPrefix = "schema/";

    std::unordered_map<uint32_t, std::shared_ptr<arrow::Schema>> recovered;
    for (it->Seek(rocksdb::Slice(kPrefix.data(), kPrefix.size())); it->Valid(); it->Next()) {
        const auto k = it->key();
        if (!std::string_view(k.data(), k.size()).starts_with(kPrefix)) {
            break;
        }

        ARROW_ASSIGN_OR_RAISE(auto ids,
                              ParseSchemaKey(std::string_view(k.data(), k.size())));
        const auto value = it->value();
        ARROW_ASSIGN_OR_RAISE(auto schema,
                              DeserializeSchemaFromBytes(
                                  std::string_view(value.data(), value.size())));
        recovered.emplace(EncodeKey(ids.first, ids.second), std::move(schema));
    }
    ARROW_RETURN_NOT_OK(ToArrowStatus(it->status()));

    std::unique_lock lock(mu_);
    schemas_ = std::move(recovered);
    return arrow::Status::OK();
}

}  // namespace feature_store
