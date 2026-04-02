# ArrowRocksEngine 设计文档

> FeatureStore 流引擎 · Arrow + RocksDB 实现方案
> 版本：v2.0 · 状态：Draft

---

## 1. 背景与目标

### 1.1 问题陈述

FeatureStore 流引擎需要满足以下核心矛盾：

| 维度            | 要求                                    |
| --------------- | --------------------------------------- |
| **写吞吐**      | 毫秒级追加，不阻塞上游流水线            |
| **读延迟**      | P99 < 5ms，零拷贝返回 Arrow RecordBatch |
| **Schema 演进** | 运行时动态注册新版本，历史数据仍可查询  |
| **数据淘汰**    | 基于 TTL 自动淘汰过期帧，默认保留 3 天  |

### 1.2 选型理由

- **RocksDB MergeOperator**：将追加操作下推到存储层，AppendFeature 退化为单次 `Merge()` 写入，无 read-modify-write 开销。
- **Apache Arrow IPC**：RecordBatch 的序列化格式与内存格式一致，配合 `PinnableSlice` 可实现真正的零拷贝读。
- **列族（Column Family）隔离**：不同 table_id 使用独立 CF，避免 Bloom Filter / Compaction 相互干扰。
- **CompactionFilter TTL**：在 Compaction 时逐帧淘汰过期数据，无需额外读放大，天然适配流式引擎"保留最近 N 天"的数据生命周期。

---

## 2. 整体架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                         ArrowRocksEngine                             │
│                                                                      │
│  ┌───────────────┐  ┌────────────────┐  ┌─────────────────────────┐  │
│  │SchemaRegistry │  │  WriteRouter   │  │  CompactionFilterFactory│  │
│  │               │  │                │  │                         │  │
│  │ (table,ver)   │  │ IPC Serialize  │  │ TTL-based Frame Filter  │  │
│  │ → Schema      │  │ → Merge()      │  │ CRC32 Integrity Check  │  │
│  └──────┬────────┘  └───────┬────────┘  └───────────┬─────────────┘  │
│         │                   │                       │                │
│  ┌──────▼───────────────────▼───────────────────────▼─────────────┐  │
│  │                      RocksDB Instance                          │  │
│  │                                                                │  │
│  │  CF: __meta__    CF: t_{table_id}   CF: t_{table_id} ...      │  │
│  │  Schema 持久化    └── Key: uid(8B)                              │  │
│  │  CF 元信息           Value: Arrow IPC 帧序列 (CRC32 校验)       │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                        QueryPath                               │  │
│  │  PinnableSlice → ArrowBuffer(零拷贝) → RecordBatch             │  │
│  │  支持 Column Projection / BatchGet(MultiGet)                   │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. 存储模型

### 3.1 Column Family 布局

```
__meta__          # 全局元数据（Schema、CF 元信息）
t_0001            # table_id=1 的数据列族
t_0002            # table_id=2 的数据列族
...
```

每个 table 独占一个 CF，由 `RegisterSchema` 首次调用时创建。

**CF 数量上限**：引擎设定 CF 上限为 **1024**（可通过 `EngineConfig::max_column_families` 配置），`RegisterSchema` 在创建新 CF 前校验数量，超出上限返回 `arrow::Status::CapacityError`。

### 3.2 Table 删除与 CF 归档

```
DropTable(table_id) 流程：
1. 从 SchemaRegistry 中注销所有版本
2. 在 __meta__ CF 中标记 cf_state/{table_id} = "dropped"
3. 异步调用 db_->DropColumnFamily(cf_handle)
4. 从 cf_handles_ 映射中移除
5. 更新 active_cf_count_ 原子计数器
```

**归档模式**（可选）：对于需要保留历史数据的场景，`ArchiveTable` 接口将 CF 数据导出为 Parquet 文件后再执行删除：

```cpp
arrow::Status ArchiveTable(uint16_t table_id, const std::string& archive_path);
arrow::Status DropTable(uint16_t table_id);
```

### 3.3 Key 编码

```
┌──────────────────┐
│   uid  (8 bytes) │   大端序，支持前缀扫描
└──────────────────┘
```

table_id 已由 CF 区分，Key 中无需重复携带。

### 3.4 Value 编码——Arrow IPC 帧序列（含 CRC32 校验）

Value 是多个 `Frame` 的线性拼接，MergeOperator 在 Compaction 时合并，CompactionFilter 淘汰过期帧：

```
┌──────────────────────────────────────────────────────────────┐
│ Frame[0]                                                     │
│  magic      : uint32  = 0x41524F57  ('AROW')                │
│  version    : uint16  (schema version，支持 65535 个版本)     │
│  reserved   : uint16  (保留字段，用于未来扩展，当前置 0)       │
│  timestamp  : int64   (unix ms，写入时间)                     │
│  ipc_len    : uint32  (ipc_data 长度)                        │
│  ipc_data   : bytes   (Arrow IPC RecordBatch)                │
│  crc32      : uint32  (从 magic 到 ipc_data 尾部的 CRC32C)   │
├──────────────────────────────────────────────────────────────┤
│ Frame[1] ...                                                 │
├──────────────────────────────────────────────────────────────┤
│ Frame[N] ...                                                 │
└──────────────────────────────────────────────────────────────┘
```

**Frame Header 总大小**：4 + 2 + 2 + 8 + 4 = **20 bytes**（不含 ipc_data 和 crc32）

**CRC32C 校验规则**：

- **写入时**：对 `[magic, version, reserved, timestamp, ipc_len, ipc_data]` 整段计算 CRC32C，附加到帧尾部。
- **读取时**：`DeserializeFrame` 验证 CRC32C，校验失败返回 `arrow::Status::IOError("CRC32 mismatch")`，并在 Metrics 中计数 `frame_crc_error_total`。
- **Compaction 时**：MergeOperator / CompactionFilter 遇到 CRC 错误帧时跳过该帧并记录告警日志，不中断整个 Compaction 流程。

### 3.5 __meta__ CF 的 Schema 持久化

```
Key:  "schema/{table_id}/{version}"     → Arrow Schema IPC 序列化
Key:  "cf_state/{table_id}"             → "active" | "dropped"
Key:  "cf_count"                        → 当前活跃 CF 数量
```

引擎重启时扫描前缀恢复 `SchemaRegistry` 和 CF 状态。

---

## 4. 核心模块设计

### 4.1 SchemaRegistry

```cpp
class SchemaRegistry {
    // (table_id, version) → Schema
    std::shared_mutex mu_;
    // 版本号扩展为 uint16_t，支持 65535 个历史版本
    std::unordered_map<uint32_t, std::shared_ptr<arrow::Schema>> schemas_;

    // 编码 key：table_id(16bit) + version(16bit) → uint32_t
    static uint32_t EncodeKey(uint16_t table_id, uint16_t version);

public:
    arrow::Status Register(uint16_t table_id, uint16_t version,
                           std::shared_ptr<arrow::Schema> schema,
                           rocksdb::DB* db);

    // 注销指定 table 的所有版本（用于 DropTable）
    arrow::Status Unregister(uint16_t table_id);

    // 支持并发读，无锁快路径
    std::shared_ptr<arrow::Schema> Get(uint16_t table_id, uint16_t version) const;

    // 引擎启动时从 __meta__ CF 重放
    arrow::Status Recover(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* meta_cf);
};
```

**Schema 版本号设计变更**（v1 → v2）：

| 属性 | v1 | v2 | 变更理由 |
|------|-----|-----|---------|
| 版本号类型 | `uint8_t` (0~255) | `uint16_t` (0~65535) | 长生命周期系统中 255 个版本不够用 |
| Frame header 占用 | 1 byte | 2 bytes | 新增 2 bytes reserved 字段保持对齐 |
| 活跃版本限制 | 无 | 每 table ≤ 16 个活跃版本 | 控制读时 schema 合并复杂度 |

**Schema 兼容性检查**：`RegisterSchema` 在已有版本的情况下，验证新 Schema 是否为超集（只允许新增 nullable 列），否则返回 `Invalid` 错误。

### 4.2 ArrowMergeOperator

这是整个引擎的核心设计。RocksDB 的 `MergeOperator` 允许将追加语义下推到存储层。

```
AppendFeature 调用链：
  Serialize(delta_batch) → Frame (含 CRC32C)
  db->Merge(cf, key, frame_bytes)   ← O(1) 写，非阻塞

RocksDB 内部在 Compaction / Get 时调用 MergeOperator：
  FullMergeV2(existing_value, operands[]) → new_value
```

```cpp
class ArrowMergeOperator : public rocksdb::MergeOperator {
    SchemaRegistry* registry_; // 用于跨版本 Schema 合并

public:
    // 增量合并（SST Compaction）
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override;

    // 两个已合并值再合并（Level Compaction 优化路径）
    bool PartialMerge(const rocksdb::Slice& key,
                      const rocksdb::Slice& left,
                      const rocksdb::Slice& right,
                      std::string* new_value,
                      rocksdb::Logger* logger) const override;

    const char* Name() const override { return "ArrowMergeOperator"; }
};
```

**FullMergeV2 逻辑**：

```
1. 解析 existing_value 中所有 Frame → frame_list
   - 逐帧校验 CRC32C，跳过校验失败的帧并记录告警
2. 逐个解析 operands 中的 Frame → 追加到 frame_list
3. 将 frame_list 重新序列化 → new_value
   注：TTL 淘汰由 CompactionFilter 负责，MergeOperator 不做截断
```

**PartialMerge 逻辑**：

```
合并相邻两个已序列化的 Frame 序列：
  1. 解析 left + right 的 Frame header（magic + version + timestamp + crc32）
  2. 验证 CRC32C，校验失败的帧跳过
  3. 若两侧所有 Frame 的 schema version 相同：
       直接拼接字节流（跳过 ipc_data 反序列化，仅需验证 header）
  4. 否则保留各帧独立，线性拼接
  → 写回新序列
```

### 4.3 TTL CompactionFilter（替代行数截断）

v1 设计中采用 MergeOperator 内行数截断，存在触发时机不可控、Get 与 Compaction 结果不一致等问题。v2 改为 **基于 TTL 的 CompactionFilter**，语义更清晰，且完全在 Compaction 线程中执行，不影响读写路径。

```cpp
class ArrowTTLFilterFactory : public rocksdb::CompactionFilterFactory {
    std::atomic<int64_t> ttl_ms_;   // 默认 3 天 = 259200000ms
public:
    explicit ArrowTTLFilterFactory(int64_t ttl_ms = 3 * 24 * 3600 * 1000LL)
        : ttl_ms_(ttl_ms) {}

    // 运行时动态调整 TTL（无需重启）
    void SetTTL(int64_t ttl_ms) { ttl_ms_.store(ttl_ms, std::memory_order_relaxed); }
    int64_t GetTTL() const { return ttl_ms_.load(std::memory_order_relaxed); }

    std::unique_ptr<rocksdb::CompactionFilter>
    CreateCompactionFilter(const rocksdb::CompactionFilter::Context& ctx) override {
        return std::make_unique<ArrowTTLFilter>(ttl_ms_.load());
    }

    const char* Name() const override { return "ArrowTTLFilterFactory"; }
};

class ArrowTTLFilter : public rocksdb::CompactionFilter {
    int64_t ttl_ms_;
    int64_t now_ms_;   // 在构造时快照当前时间

public:
    explicit ArrowTTLFilter(int64_t ttl_ms)
        : ttl_ms_(ttl_ms), now_ms_(CurrentTimeMs()) {}

    // 对 Merge operand 过滤
    Decision FilterMergeOperand(
        int level,
        const rocksdb::Slice& key,
        const rocksdb::Slice& operand) const override;

    // 对完整 value 过滤（Compaction 后的最终值）
    bool Filter(int level,
                const rocksdb::Slice& key,
                const rocksdb::Slice& existing_value,
                std::string* new_value,
                bool* value_changed) const override;

    const char* Name() const override { return "ArrowTTLFilter"; }
};
```

**FilterMergeOperand 逻辑**：

```
1. 解析 operand 中所有 Frame header
2. 检查每个 Frame 的 timestamp：
   - 若 (now_ms_ - frame.timestamp) > ttl_ms_ → 标记为过期
3. 若 operand 中所有 Frame 均过期 → 返回 kRemove
4. 若部分过期 → 返回 kKeep（由 Filter 统一处理）
```

**Filter 逻辑（完整 value）**：

```
1. 逐帧扫描 value 中的 Frame 序列
2. 过滤掉所有 timestamp 过期的帧
3. 若所有帧均过期 → 返回 true（删除整个 key）
4. 若部分过期：
   - 将存活帧重新拼接写入 new_value
   - *value_changed = true
   - 返回 false
```

**TTL 配置**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ttl_days` | 3 | 帧保留天数，支持运行时动态调整 |
| `ttl_check_period_hours` | 1 | CompactionFilter 中时间快照刷新间隔 |

### 4.4 零拷贝 GetFeature（支持 Column Projection）

RocksDB 提供 `GetPinnableSlice`，可在 BlockCache 中原地 pin 住数据，避免 memcpy：

```cpp
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ArrowRocksEngine::GetFeature(uint16_t table_id, uint64_t uid,
                              uint16_t target_version,
                              const std::vector<std::string>& columns = {}) {
    // 1. 获取 CF handle
    auto* cf = GetCF(table_id);

    // 2. 零拷贝读（pin BlockCache）
    rocksdb::PinnableSlice pinnable;
    auto s = db_->Get(read_opts_, cf, EncodeKey(uid), &pinnable);
    if (!s.ok()) return ToArrowStatus(s);

    // 3. 将 PinnableSlice 包装为 Arrow Buffer（无 memcpy）
    //    生命周期：PinnableSlice 与 RecordBatch 共享所有权
    auto buffer = std::make_shared<PinnableBuffer>(std::move(pinnable));

    // 4. 解析 Frame 序列，定位 target_version 对应的最新完整帧
    //    若存在多版本帧，按 Schema 合并（投影/补 null）
    auto full_batch = DeserializeAndProject(
        buffer, target_version,
        registry_->Get(table_id, target_version));

    // 5. 列裁剪（Column Projection）
    if (!columns.empty() && full_batch.ok()) {
        return ProjectColumns(*full_batch, columns);
    }
    return full_batch;
}
```

**Column Projection 实现**：

```cpp
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ProjectColumns(const std::shared_ptr<arrow::RecordBatch>& batch,
               const std::vector<std::string>& columns) {
    std::vector<int> indices;
    indices.reserve(columns.size());
    for (const auto& col : columns) {
        int idx = batch->schema()->GetFieldIndex(col);
        if (idx == -1) {
            return arrow::Status::KeyError("Column not found: " + col);
        }
        indices.push_back(idx);
    }
    return batch->SelectColumns(indices);
}
```

**列裁剪优化路径**：

- **延迟反序列化**：当目标列为 Schema 的子集时，可在 IPC 反序列化阶段直接跳过非目标列的 buffer，进一步降低 CPU 开销（需要 Arrow IPC 的列级跳过特性，Arrow >= 12.0）。
- **Metrics 追踪**：`feature_engine_get_projection_ratio` 直方图记录实际读取列占比，用于评估列裁剪收益。

### 4.5 BatchGet 批量读取接口

单 Key `GetFeature` 在批量查询场景下需要逐个调用，无法利用 RocksDB `MultiGet` 的 IO 合并优化。新增 `BatchGetFeature` 接口：

```cpp
struct BatchGetRequest {
    uint16_t table_id;
    uint64_t uid;
    uint16_t target_version;
    std::vector<std::string> columns;  // 可选列裁剪
};

struct BatchGetResult {
    uint64_t uid;
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> batch;  // 成功时非空
};

std::vector<BatchGetResult>
ArrowRocksEngine::BatchGetFeature(
    const std::vector<BatchGetRequest>& requests) {

    // 1. 按 table_id 分组（同 CF 的请求一起执行 MultiGet）
    std::unordered_map<uint16_t, std::vector<size_t>> groups;
    for (size_t i = 0; i < requests.size(); ++i) {
        groups[requests[i].table_id].push_back(i);
    }

    std::vector<BatchGetResult> results(requests.size());

    // 2. 逐 CF 执行 MultiGet
    for (auto& [table_id, indices] : groups) {
        auto* cf = GetCF(table_id);
        if (!cf) {
            for (auto idx : indices) {
                results[idx] = {requests[idx].uid,
                                arrow::Status::KeyError("table not found"),
                                nullptr};
            }
            continue;
        }

        // 构造 MultiGet 参数
        std::vector<rocksdb::Slice> keys;
        std::vector<std::string> key_bufs;
        keys.reserve(indices.size());
        key_bufs.reserve(indices.size());
        for (auto idx : indices) {
            key_bufs.push_back(EncodeKey(requests[idx].uid));
            keys.emplace_back(key_bufs.back());
        }

        // 3. RocksDB MultiGet（IO 合并，减少磁盘随机读）
        std::vector<rocksdb::PinnableSlice> values(indices.size());
        std::vector<rocksdb::Status> statuses(indices.size());
        db_->MultiGet(read_opts_,
                      cf, indices.size(),
                      keys.data(), values.data(), statuses.data());

        // 4. 逐个反序列化 + 投影
        for (size_t j = 0; j < indices.size(); ++j) {
            auto idx = indices[j];
            if (!statuses[j].ok()) {
                results[idx] = {requests[idx].uid,
                                ToArrowStatus(statuses[j]), nullptr};
                continue;
            }
            auto buffer = std::make_shared<PinnableBuffer>(
                std::move(values[j]));
            auto schema = registry_->Get(table_id, requests[idx].target_version);
            auto batch = DeserializeAndProject(
                buffer, requests[idx].target_version, schema);

            if (batch.ok() && !requests[idx].columns.empty()) {
                batch = ProjectColumns(*batch, requests[idx].columns);
            }
            results[idx] = {requests[idx].uid,
                            batch.status(),
                            batch.ok() ? *batch : nullptr};
        }
    }
    return results;
}
```

**BatchGet 性能优势**：

| 场景 | 单次 Get × N | BatchGet(N) | 提升 |
|------|-------------|-------------|------|
| 100 keys, 同 CF | 100 次磁盘 IO | IO 合并，约 10~20 次 | 5~10× |
| 100 keys, 跨 CF | 100 次磁盘 IO | 按 CF 分组后 IO 合并 | 3~5× |
| 命中 BlockCache | 无显著差异 | 减少函数调用开销 | 1.2× |

### 4.6 PinnableBuffer

自定义 `arrow::Buffer` 子类，持有 `PinnableSlice`：

```cpp
class PinnableBuffer : public arrow::Buffer {
    rocksdb::PinnableSlice pinnable_;
public:
    explicit PinnableBuffer(rocksdb::PinnableSlice&& p)
        : arrow::Buffer(
              reinterpret_cast<const uint8_t*>(p.data()),
              static_cast<int64_t>(p.size())),
          pinnable_(std::move(p)) {}
    // 析构时 pinnable_ 释放 BlockCache pin
};
```

**生命周期约束**：

- `PinnableSlice` 在 BlockCache 中 pin 住对应 Block，长期持有会增加 BlockCache 内存压力。
- **建议使用方在读取后尽快消费 RecordBatch**，若需长期持有应调用 `arrow::RecordBatch::CopyTo` 拷贝一份独立副本。
- 引擎层通过 Metrics `feature_engine_pinnable_active_count` 监控当前活跃 PinnableBuffer 数量，超过阈值时在日志中告警。

**多版本投影规则**：

当 Value 中存在低版本 Frame（schema_v1）而查询 target_version=v2 时：

```
v1 Frame ─→ Project(schema_v1 → schema_v2)
              新增列填 null（类型由 v2 Schema 决定）
              删除列丢弃（schema 约束：v2 必须是 v1 超集）
```

---

## 5. 关键接口实现细节

### 5.1 Init

```
1. 构建 RocksDB Options
   - 设置 BlockBasedTableOptions（block_cache_size_mb）
   - 设置全局 MergeOperator = ArrowMergeOperator
   - 设置 CompactionFilterFactory = ArrowTTLFilterFactory
   - 根据配置决定是否开启 allow_mmap_reads（见 §5.6）
2. 打开已有 CF 列表（含 __meta__）
3. ⚠️ SchemaRegistry::Recover() 重放元数据
   → 必须在 RocksDB Open 之后、任何用户读写请求之前完成
   → 引擎在 Recover 完成前处于 NOT_READY 状态，拒绝所有读写
4. 恢复 CF 状态（active_cf_count_）
5. 启动 Metrics 采集
```

**Init 时序保证**：

```
RocksDB::Open()
       │
       ▼
SchemaRegistry::Recover()     ← MergeOperator 依赖 Registry
       │                         Recover 未完成前禁止读写
       ▼
engine_state_ = READY
       │
       ▼
接受读写请求
```

> **关键约束**：RocksDB 在 Open 阶段如果触发自动 Compaction，会调用 MergeOperator。此时 SchemaRegistry 可能尚未 Recover 完毕。解决方式：Open 时设置 `options.avoid_unnecessary_blocking_io = true` 并延迟手动触发 Compaction，确保 Registry 就绪。

**RocksDB 调优参数**：

```cpp
// 写缓冲：减少小 Merge 落盘频率
options.write_buffer_size = 128 << 20;           // 128 MB
options.max_write_buffer_number = 4;

// 并发 Compaction
options.max_background_compactions = 4;
options.max_background_flushes = 2;

// 针对序列数据：L0→L1 触发阈值放宽
options.level0_file_num_compaction_trigger = 8;

// TTL CompactionFilter
options.compaction_filter_factory =
    std::make_shared<ArrowTTLFilterFactory>(config.ttl_days * 86400000LL);

// Mmap 读配置（见 §5.6）
options.allow_mmap_reads = config.enable_mmap_reads;
```

### 5.2 AppendFeature

```
1. 校验引擎状态为 READY
2. 校验 delta_batch schema 与已注册 schema 兼容
3. 序列化 delta_batch → Frame bytes（含 CRC32C 计算）
4. db_->Merge(cf_handle, uid_key, frame_bytes)
5. 更新 Metrics（写计数、写字节数）
```

序列化采用 Arrow IPC `WriteRecordBatch`，使用 `arrow::io::BufferOutputStream` 写入预分配缓冲区，减少内存分配。CRC32C 使用硬件加速指令（SSE 4.2 / ARMv8 CRC）计算。

### 5.3 RegisterSchema（原子性保证）

```
1. 校验 CF 数量未超上限（max_column_families）
2. 检查已有版本 schema 兼容性（新列必须 nullable）
3. 构建 WriteBatch（原子操作）：
   a. Put(__meta__ CF, schema_key, schema_ipc_bytes)
   b. Put(__meta__ CF, cf_state/{table_id}, "active")
   c. Put(__meta__ CF, cf_count, new_count)
4. 若该 table_id 的 CF 不存在，先 CreateColumnFamily
5. db_->Write(write_opts, &batch)   ← 原子提交
6. SchemaRegistry::Register()（内存态更新）
```

**原子性设计**：将 Schema 元数据写入和 CF 状态更新放在同一个 `WriteBatch` 中，确保 crash 后重启时：

- 要么 Schema + CF 状态都已持久化 → Recover 正常恢复
- 要么都未持久化 → 相当于 Register 未发生

> **注意**：`CreateColumnFamily` 本身不支持事务。极端场景下 CF 已创建但 WriteBatch 未提交时 crash，重启后会存在一个"无主" CF。Init 流程中需检测并清理此类孤儿 CF（`cf_state` 中无记录的 CF 执行 `DropColumnFamily`）。

### 5.4 CompactRange 手动触发接口

提供运维接口，支持在低峰期手动触发指定 table 或全局的 Compaction：

```cpp
struct CompactRangeOptions {
    std::optional<uint16_t> table_id;  // 为空时全局 Compact
    bool force_bottommost = false;      // 是否强制压缩到最底层
    std::optional<uint64_t> uid_start;  // Key 范围起始（可选）
    std::optional<uint64_t> uid_end;    // Key 范围结束（可选）
};

arrow::Status ArrowRocksEngine::CompactRange(const CompactRangeOptions& opts) {
    rocksdb::CompactRangeOptions rocksdb_opts;
    rocksdb_opts.bottommost_level_compaction =
        opts.force_bottommost
            ? rocksdb::BottommostLevelCompaction::kForce
            : rocksdb::BottommostLevelCompaction::kIfHaveCompactionFilter;

    if (opts.table_id.has_value()) {
        // 指定 table 的 CF Compaction
        auto* cf = GetCF(*opts.table_id);
        if (!cf) return arrow::Status::KeyError("table not found");

        rocksdb::Slice* start = nullptr;
        rocksdb::Slice* end = nullptr;
        std::string start_key, end_key;
        if (opts.uid_start) {
            start_key = EncodeKey(*opts.uid_start);
            start = new rocksdb::Slice(start_key);
        }
        if (opts.uid_end) {
            end_key = EncodeKey(*opts.uid_end);
            end = new rocksdb::Slice(end_key);
        }

        auto s = db_->CompactRange(rocksdb_opts, cf, start, end);
        delete start; delete end;
        return ToArrowStatus(s);
    }

    // 全局 Compaction：遍历所有活跃 CF
    for (auto& [tid, cf] : cf_handles_) {
        auto s = db_->CompactRange(rocksdb_opts, cf, nullptr, nullptr);
        if (!s.ok()) return ToArrowStatus(s);
    }
    return arrow::Status::OK();
}
```

**使用场景**：

- **TTL 过期清理**：正常 Compaction 调度可能延迟数小时才触发 TTL 过滤，手动 CompactRange 可立即回收磁盘空间。
- **上线后首次清理**：从 v1（行数截断）迁移到 v2（TTL）后，可手动触发全局 Compaction 完成数据格式升级。
- **运维排查**：强制 Compaction 到最底层可排除 MergeOperator 未执行导致的数据不一致问题。

### 5.5 GetMetrics

指标以 Prometheus Text Format 输出：

```
# 写路径
feature_engine_append_total{table_id="1"}          计数器
feature_engine_append_bytes_total{table_id="1"}    字节计数器
feature_engine_append_latency_us                   直方图

# 读路径
feature_engine_get_total{table_id="1",hit="true"}  计数器
feature_engine_get_latency_us                      直方图
feature_engine_get_projection_ratio                直方图（列裁剪比例）
feature_engine_batch_get_total                     计数器（BatchGet 调用数）
feature_engine_batch_get_keys_total                计数器（BatchGet 涉及 Key 总数）

# 数据完整性
feature_engine_frame_crc_error_total               计数器（CRC 校验失败帧数）

# PinnableBuffer 监控
feature_engine_pinnable_active_count               Gauge（活跃 PinnableBuffer 数量）

# TTL 淘汰
feature_engine_ttl_expired_frames_total            计数器（被 TTL 淘汰的帧数）
feature_engine_ttl_expired_keys_total              计数器（被 TTL 整体淘汰的 Key 数）

# CF 管理
feature_engine_active_cf_count                     Gauge（活跃 CF 数量）
feature_engine_cf_drop_total                       计数器（CF 删除次数）

# RocksDB 透传
rocksdb_block_cache_hit_count
rocksdb_block_cache_miss_count
rocksdb_memtable_bytes
rocksdb_pending_compaction_bytes
```

`GetMetrics()` 通过 `rocksdb::Statistics` 和引擎自身原子计数器聚合，无锁采集。

### 5.6 Mmap 读配置与容器环境兼容性

`allow_mmap_reads=true` 可提升顺序读性能，但在容器化部署中存在兼容性风险：

| 风险 | 说明 | 影响 |
|------|------|------|
| cgroup 内存计量 | mmap 的文件页计入容器 RSS，可能触发 OOM Killer | 进程被杀 |
| NUMA 不感知 | mmap 页可能分配在远端 NUMA 节点 | 读延迟波动 |
| 透明大页（THP） | khugepaged 合并 mmap 页时可能造成延迟尖刺 | P99 劣化 |

**配置开关设计**：

```cpp
struct EngineConfig {
    // Mmap 读开关，默认关闭（容器安全）
    bool enable_mmap_reads = false;

    // 仅在以下条件同时满足时建议开启：
    // 1. 物理机部署（非容器）或 cgroup v2 且 memory.high 充裕
    // 2. BlockCache 与 mmap 总内存不超过可用内存的 70%
    // 3. 已禁用 THP (echo never > /sys/kernel/mm/transparent_hugepage/enabled)
};
```

引擎在 Init 时如果检测到 `enable_mmap_reads=true` 且运行在 cgroup 环境中（通过检测 `/proc/self/cgroup`），会在日志中输出 **WARNING** 提醒运维关注内存水位。

---

## 6. Schema 演进规范

| 操作             | 是否允许          | 处理方式                |
| ---------------- | ----------------- | ----------------------- |
| 新增 nullable 列 | ✅                 | 旧数据读时该列补 `null` |
| 新增 non-null 列 | ❌                 | 返回 `Invalid` 错误     |
| 删除列           | ❌                 | 返回 `Invalid` 错误     |
| 修改列类型       | ❌                 | 返回 `Invalid` 错误     |
| 重命名列         | ❌（需新建 table） | —                       |

版本号（`uint16_t`）单调递增，由调用方维护。同一 table 最多支持 65535 个历史版本，活跃版本数限制为 16 个（可配置）。超过活跃版本限制时，`RegisterSchema` 返回 `CapacityError` 并提示对旧版本数据执行离线迁移。

---

## 7. 并发模型

```
                   多线程上游
                  ┌──┬──┬──┐
                  │  │  │  │  AppendFeature (无锁)
                  ▼  ▼  ▼  ▼
             RocksDB WriteBatch / Merge
             （内部 WAL + MemTable，无额外锁）

                  ┌──┬──┐
                  │  │  │      GetFeature / BatchGetFeature (并发读)
                  ▼  ▼  ▼
             RocksDB Get / MultiGet + PinnableSlice
             （BlockCache 读锁，ShardedLRU 分片）

                  ┌─────┐
                  │ BG  │      RocksDB Compaction + TTL Filter
                  └─────┘      （独立线程池，不阻塞读写路径）
```

- `SchemaRegistry` 使用 `shared_mutex`：写操作（Register/Unregister）独占，读操作（Get）共享。
- `AppendFeature` 全路径无引擎级锁，并发安全由 RocksDB 内部保证。
- `BatchGetFeature` 按 CF 分组后调用 `MultiGet`，共享同一组 PinnableSlice。

---

## 8. 错误处理约定

| 错误场景           | 返回值                                           |
| ------------------ | ------------------------------------------------ |
| 引擎未就绪         | `arrow::Status::Invalid("engine not ready")`     |
| table_id 未注册    | `arrow::Status::KeyError`                        |
| Schema 不兼容      | `arrow::Status::Invalid`                         |
| CF 数量超上限      | `arrow::Status::CapacityError`                   |
| uid 不存在         | `arrow::Result` 含 `NotFound`                    |
| CRC32 校验失败     | `arrow::Status::IOError("CRC32 mismatch")`       |
| RocksDB IO 错误    | `arrow::Status::IOError`（携带原始 Status 信息） |
| Frame 解析失败     | `arrow::Status::SerializationError`              |
| 列裁剪列名不存在   | `arrow::Status::KeyError("Column not found")`    |

RocksDB `Status` → Arrow `Status` 通过 `ToArrowStatus()` 统一转换，保留原始 message。

---

## 9. 监控与运维

### 9.1 关键告警规则

| 告警 | 条件 | 级别 |
|------|------|------|
| CRC 校验失败率 > 0 | `rate(frame_crc_error_total[5m]) > 0` | P1 |
| 活跃 CF 数量接近上限 | `active_cf_count > max_cf * 0.8` | P2 |
| PinnableBuffer 堆积 | `pinnable_active_count > 10000` | P2 |
| TTL 淘汰量突增 | `rate(ttl_expired_frames_total[1h])` 超历史均值 5 倍 | P3 |

### 9.2 运维操作清单

| 操作 | 接口 | 场景 |
|------|------|------|
| 手动 Compaction | `CompactRange()` | 低峰清理、版本迁移 |
| 调整 TTL | `SetTTL(new_ttl_ms)` | 业务需要调整保留时长 |
| 删除 Table | `DropTable(table_id)` | 下线不再使用的特征表 |
| 归档 Table | `ArchiveTable(table_id, path)` | 保留历史数据后删除 |

---

## 10. 目录结构

```
feature_store/src
├── engine/
│   ├── arrow_rocks_engine.h         # 对外接口
│   ├── arrow_rocks_engine.cc        # Init / AppendFeature / GetFeature / BatchGetFeature
│   ├── schema_registry.h/cc         # SchemaRegistry
│   ├── arrow_merge_operator.h/cc    # RocksDB MergeOperator 实现
│   ├── arrow_ttl_filter.h/cc        # TTL CompactionFilter 实现
│   ├── pinnable_buffer.h            # 零拷贝 Arrow Buffer 适配
│   ├── frame_codec.h/cc             # Frame 序列化 / 反序列化（含 CRC32C）
│   ├── column_projection.h/cc       # 列裁剪工具
│   ├── key_encoder.h                # RocksDB Key 编码工具
│   ├── engine_config.h              # 配置结构体（TTL、mmap、CF上限等）
│   └── metrics.h/cc                 # 指标收集
├── tests/
│   ├── CMakeLists.txt
│   ├── merge_operator_test.cc
│   ├── ttl_filter_test.cc           # TTL 淘汰逻辑测试
│   ├── crc32_test.cc                # CRC32C 校验测试
│   ├── schema_evolution_test.cc
│   ├── zero_copy_test.cc
│   ├── batch_get_test.cc            # BatchGet 接口测试
│   ├── column_projection_test.cc    # 列裁剪测试
│   ├── cf_lifecycle_test.cc         # CF 创建/删除/归档测试
│   └── benchmark/
│       └── engine_bench.cc          # Google Benchmark
└── CMakeLists.txt
```

---

## 11. 风险与取舍

| 风险点 | 影响 | 缓解措施 |
| ------ | ---- | -------- |
| MergeOperator 中 Arrow 反序列化 CPU 开销高 | Compaction 变慢 | PartialMerge 走字节拼接快路径（需验证 header CRC），跳过 ipc_data 反序列化 |
| Schema 版本过多导致读时 schema 合并复杂 | 读延迟增加 | 限制每 table 最多 16 个活跃版本；旧版本数据离线迁移 |
| Compaction 期间 MergeOperator 崩溃 | 数据损坏 | FullMergeV2 幂等设计；Frame 含 magic + CRC32C 校验；崩溃后回退到原始 operands |
| TTL CompactionFilter 淘汰不及时 | 磁盘占用偏高 | 提供 CompactRange 手动触发接口；监控 pending_compaction_bytes |
| 容器 mmap OOM | 进程被杀 | 默认关闭 mmap，提供配置开关，Init 时检测 cgroup 环境并告警 |
| CF 数量膨胀 | 启动慢、FD 占用高 | 设定上限 1024，提供 DropTable/ArchiveTable 清理机制 |
| RegisterSchema 非原子 crash | 孤儿 CF | Init 时检测并清理 cf_state 中无记录的 CF |

---

## 12. 版本变更记录

| 版本 | 日期 | 变更内容 |
|------|------|---------|
| v1.0 | — | 初始设计：MergeOperator 追加 + 行数截断 + PinnableSlice 零拷贝 |
| v2.0 | 2026-04-02 | 1) Frame header 添加 CRC32C 校验<br>2) 新增 CompactRange 手动触发接口<br>3) GetFeature 支持 Column Projection<br>4) 新增 BatchGetFeature (MultiGet) 接口<br>5) Mmap 读配置开关，适配容器环境<br>6) Schema 版本号 uint8→uint16<br>7) Init 时序保证 + RegisterSchema 原子性<br>8) CF 数量上限 + DropTable/ArchiveTable<br>9) 取消行数截断，改为 TTL CompactionFilter（默认 3 天） |

---

## 13. 后续扩展方向

| 优先级 | 方向 | 说明 |
|--------|------|------|
| P1 | 多主写入 | 引入 MVCC 版本戳，冲突由 MergeOperator 的时间戳仲裁 |
| P2 | 远程读 gRPC 层 | GetFeature 增加 gRPC 服务层，零拷贝语义通过 `grpc::Slice` 传递 |
| P3 | 列存查询快照 | 对高频全表扫描场景，引入 Parquet 快照文件，与 RocksDB 双轨并存 |
| P3 | 延迟反序列化列裁剪 | Arrow IPC 列级跳过，进一步降低列裁剪的 CPU 开销 |
