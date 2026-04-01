# ArrowRocksEngine 设计文档

> FeatureStore 流引擎 · Arrow + RocksDB 实现方案  
> 版本：v1.0 · 状态：Draft

---

## 1. 背景与目标

### 1.1 问题陈述

FeatureStore 流引擎需要满足以下核心矛盾：

| 维度            | 要求                                    |
| --------------- | --------------------------------------- |
| **写吞吐**      | 毫秒级追加，不阻塞上游流水线            |
| **读延迟**      | P99 < 5ms，零拷贝返回 Arrow RecordBatch |
| **Schema 演进** | 运行时动态注册新版本，历史数据仍可查询  |
| **序列截断**    | 行数超过阈值后自动压缩，防止无限增长    |

### 1.2 选型理由

- **RocksDB MergeOperator**：将追加操作下推到存储层，AppendFeature 退化为单次 `Merge()` 写入，无 read-modify-write 开销。
- **Apache Arrow IPC**：RecordBatch 的序列化格式与内存格式一致，配合 `PinnableSlice` 可实现真正的零拷贝读。
- **列族（Column Family）隔离**：不同 table_id 使用独立 CF，避免 Bloom Filter / Compaction 相互干扰。

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        ArrowRocksEngine                         │
│                                                                 │
│  ┌──────────────┐   ┌────────────────┐   ┌──────────────────┐  │
│  │ SchemaRegistry│   │  WriteRouter   │   │  SlowTrackMgr    │  │
│  │              │   │                │   │                  │  │
│  │ (table,ver)  │   │ IPC Serialize  │   │ BG Compaction    │  │
│  │ → Schema     │   │ → Merge()      │   │ Truncate > N rows│  │
│  └──────┬───────┘   └───────┬────────┘   └────────┬─────────┘  │
│         │                   │                     │            │
│  ┌──────▼───────────────────▼─────────────────────▼─────────┐  │
│  │                     RocksDB Instance                      │  │
│  │                                                           │  │
│  │  CF: __meta__   CF: t_{table_id}  CF: t_{table_id} ...   │  │
│  │  Schema 持久化   └── Key: uid(8B)                         │  │
│  │                      Value: Arrow IPC 序列               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                      QueryPath                           │   │
│  │  PinnableSlice → ArrowBuffer(零拷贝) → RecordBatch        │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 存储模型

### 3.1 Column Family 布局

```
__meta__          # 全局元数据
t_0001            # table_id=1 的数据列族
t_0002            # table_id=2 的数据列族
...
```

每个 table 独占一个 CF，由 `RegisterSchema` 首次调用时创建。

### 3.2 Key 编码

```
┌──────────────────┐
│   uid  (8 bytes) │   大端序，支持前缀扫描
└──────────────────┘
```

table_id 已由 CF 区分，Key 中无需重复携带。

### 3.3 Value 编码——Arrow IPC 序列帧

Value 是多个 `Frame` 的线性拼接，MergeOperator 在 Compaction 时合并：

```
┌────────────────────────────────────────────────────┐
│ Frame[0]                                           │
│  magic    : uint32  = 0x41524F57  ('AROW')        │
│  version  : uint8                                  │
│  timestamp: int64   (unix ms)                      │
│  ipc_len  : uint32                                 │
│  ipc_data : bytes   (Arrow IPC RecordBatch)        │
├────────────────────────────────────────────────────┤
│ Frame[1] ...                                       │
├────────────────────────────────────────────────────┤
│ Frame[N] ...                                       │
└────────────────────────────────────────────────────┘
```

### 3.4 __meta__ CF 的 Schema 持久化

```
Key:  "schema/{table_id}/{version}"
Value: Arrow Schema 的 IPC 序列化（Schema message）
```

引擎重启时扫描前缀恢复 `SchemaRegistry`。

---

## 4. 核心模块设计

### 4.1 SchemaRegistry

```cpp
class SchemaRegistry {
    // (table_id, version) → Schema
    std::shared_mutex mu_;
    std::unordered_map<uint32_t, std::shared_ptr<arrow::Schema>> schemas_;

    // 编码 key
    static uint32_t EncodeKey(uint16_t table_id, uint8_t version);

public:
    arrow::Status Register(uint16_t table_id, uint8_t version,
                           std::shared_ptr<arrow::Schema> schema,
                           rocksdb::DB* db);

    // 支持并发读，无锁快路径
    std::shared_ptr<arrow::Schema> Get(uint16_t table_id, uint8_t version) const;

    // 引擎启动时从 __meta__ CF 重放
    arrow::Status Recover(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* meta_cf);
};
```

**Schema 兼容性检查**：`RegisterSchema` 在已有版本的情况下，验证新 Schema 是否为超集（只允许新增 nullable 列），否则返回 `Invalid` 错误。

### 4.2 ArrowMergeOperator

这是整个引擎的核心设计。RocksDB 的 `MergeOperator` 允许将追加语义下推到存储层。

```
AppendFeature 调用链：
  Serialize(delta_batch) → Frame
  db->Merge(cf, key, frame_bytes)   ← O(1) 写，非阻塞
  
RocksDB 内部在 Compaction / Get 时调用 MergeOperator：
  FullMergeV2(existing_value, operands[]) → new_value
```

```cpp
class ArrowMergeOperator : public rocksdb::MergeOperator {
    size_t max_rows_;          // 慢速轨道触发阈值
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
2. 逐个解析 operands 中的 Frame → 追加到 frame_list
3. 若 total_rows > max_rows_：
      保留最新 max_rows_ 行（时间序列截断）
4. 将 frame_list 重新序列化 → new_value
```

**PartialMerge 逻辑**：

```
合并相邻两个已序列化的 Frame 序列：
  解析 left + right 的 Frame 列表
  直接拼接字节流（若 schema 版本相同可跳过反序列化）
  → 写回新序列
```

### 4.3 零拷贝 GetFeature

RocksDB 提供 `GetPinnableSlice`，可在 BlockCache 中原地 pin 住数据，避免 memcpy：

```cpp
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ArrowRocksEngine::GetFeature(uint16_t table_id, uint64_t uid,
                              uint8_t target_version) {
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
    return DeserializeAndProject(buffer, target_version,
                                 registry_->Get(table_id, target_version));
}
```

**PinnableBuffer**：自定义 `arrow::Buffer` 子类，持有 `PinnableSlice`：

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
   - 开启 allow_mmap_reads=true（配合零拷贝）
2. 打开已有 CF 列表（含 __meta__）
3. SchemaRegistry::Recover() 重放元数据
4. SlowTrackManager::Start()
```

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
```

### 5.2 AppendFeature

```
1. 校验 delta_batch schema 与已注册 schema 兼容
2. 序列化 delta_batch → Frame bytes
3. db_->Merge(cf_handle, uid_key, frame_bytes)
4. 更新 Metrics（写计数、写字节数）
```

序列化采用 Arrow IPC `WriteRecordBatch`，使用 `arrow::io::BufferOutputStream` 写入预分配缓冲区，减少内存分配。

### 5.3 RegisterSchema

```
1. 检查已有版本 schema 兼容性（新列必须 nullable）
2. 序列化 schema → IPC bytes
3. db_->Put(__meta__ CF, schema_key, ipc_bytes)
4. SchemaRegistry::Register()
5. 若该 table_id 的 CF 不存在，创建 CF
```

---

## 6. Schema 演进规范

| 操作             | 是否允许          | 处理方式                |
| ---------------- | ----------------- | ----------------------- |
| 新增 nullable 列 | ✅                 | 旧数据读时该列补 `null` |
| 新增 non-null 列 | ❌                 | 返回 `Invalid` 错误     |
| 删除列           | ❌                 | 返回 `Invalid` 错误     |
| 修改列类型       | ❌                 | 返回 `Invalid` 错误     |
| 重命名列         | ❌（需新建 table） | —                       |

版本号（`uint8_t`）单调递增，由调用方维护。同一 table 最多支持 255 个历史版本。

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
                  │  │  │      GetFeature (并发读)
                  ▼  ▼  ▼
             RocksDB Get + PinnableSlice
             （BlockCache 读锁，ShardedLRU 分片）

                  ┌─────┐
                  │ BG  │      RocksDB Compaction
                  └─────┘      （独立线程池，不阻塞读写路径）
```

- `SchemaRegistry` 使用 `shared_mutex`：写操作（Register）独占，读操作（Get）共享。
- `AppendFeature` 全路径无引擎级锁，并发安全由 RocksDB 内部保证。

---

## 8. 错误处理约定

| 错误场景        | 返回值                                           |
| --------------- | ------------------------------------------------ |
| table_id 未注册 | `arrow::Status::KeyError`                        |
| Schema 不兼容   | `arrow::Status::Invalid`                         |
| uid 不存在      | `arrow::Result` 含 `NotFound`                    |
| RocksDB IO 错误 | `arrow::Status::IOError`（携带原始 Status 信息） |
| Frame 解析失败  | `arrow::Status::SerializationError`              |

RocksDB `Status` → Arrow `Status` 通过 `ToArrowStatus()` 统一转换，保留原始 message。

---

## 9. 监控指标（GetMetrics）

指标以 Prometheus Text Format 输出：

```
# 写路径
feature_engine_append_total{table_id="1"}          计数器
feature_engine_append_bytes_total{table_id="1"}    字节计数器
feature_engine_append_latency_us                   直方图

# 读路径
feature_engine_get_total{table_id="1",hit="true"}  计数器
feature_engine_get_latency_us                      直方图

# RocksDB 透传
rocksdb_block_cache_hit_count
rocksdb_block_cache_miss_count
rocksdb_memtable_bytes
rocksdb_pending_compaction_bytes
```

`GetMetrics()` 通过 `rocksdb::Statistics` 和引擎自身原子计数器聚合，无锁采集。

---

## 10. 目录结构建议

```
feature_store/src
├── engine/
│   ├── arrow_rocks_engine.h       # 对外接口
│   ├── arrow_rocks_engine.cc      # Init / AppendFeature / GetFeature
│   ├── schema_registry.h/cc       # SchemaRegistry
│   ├── arrow_merge_operator.h/cc  # RocksDB MergeOperator 实现
│   ├── pinnable_buffer.h          # 零拷贝 Arrow Buffer 适配
│   ├── slow_track_manager.h/cc    # 后台慢轨道
│   ├── frame_codec.h/cc           # Frame 序列化 / 反序列化
│   ├── key_encoder.h              # RocksDB Key 编码工具
│   └── metrics.h/cc               # 指标收集
├── tests/
│   ├── CMakeLists.txt
│   ├── merge_operator_test.cc
│   ├── schema_evolution_test.cc
│   ├── zero_copy_test.cc
│   └── benchmark/
│       └── engine_bench.cc        # Google Benchmark
└── CMakeLists.txt
```

---

## 11. 风险与取舍

| 风险点                                     | 影响            | 缓解措施                                                                    |
| ------------------------------------------ | --------------- | --------------------------------------------------------------------------- |
| MergeOperator 中 Arrow 反序列化 CPU 开销高 | Compaction 变慢 | PartialMerge 走字节拼接快路径，跳过反序列化                                 |  |  |
| Schema 版本过多导致读时 schema 合并复杂    | 读延迟增加      | 限制每 table 最多 8 个活跃版本；旧版本数据离线迁移                          |
| Compaction 期间 MergeOperator 崩溃         | 数据损坏        | FullMergeV2 幂等设计；Frame 含 magic number 校验；崩溃后回退到原始 operands |

---

## 12. 后续扩展方向

- **TTL 支持**：Frame header 加过期时间戳，MergeOperator 在合并时丢弃过期帧。
- **多主写入**：引入 MVCC 版本戳，冲突由 MergeOperator 的时间戳仲裁。
- **远程读**：GetFeature 增加 gRPC 服务层，零拷贝语义通过 `grpc::Slice` 传递。
- **列存查询**：对高频全表扫描场景，引入 Parquet 快照文件，与 RocksDB 数据双轨并存。