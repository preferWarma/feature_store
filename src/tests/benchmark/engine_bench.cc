#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include <arrow/api.h>

#include "../../engine/arrow_merge_operator.h"
#include "../../engine/arrow_rocks_engine.h"
#include "../../engine/frame_codec.h"
#include "../../engine/schema_registry.h"

namespace feature_store {
namespace {

int64_t NowMs() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
      .count();
}

std::string TempPath(const std::string &name) {
  auto path =
      std::filesystem::temp_directory_path() / std::filesystem::path(name);
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  return path.string();
}

std::shared_ptr<arrow::Schema> MakeSchema(int num_columns) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(static_cast<std::size_t>(num_columns));
  for (int i = 0; i < num_columns; ++i) {
    fields.push_back(
        arrow::field("c" + std::to_string(i), arrow::int64(), true));
  }
  return arrow::schema(std::move(fields));
}

std::shared_ptr<arrow::RecordBatch>
MakeBatch(const std::shared_ptr<arrow::Schema> &schema, int64_t rows,
          int64_t seed) {
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(static_cast<std::size_t>(schema->num_fields()));
  for (int c = 0; c < schema->num_fields(); ++c) {
    arrow::Int64Builder builder;
    for (int64_t r = 0; r < rows; ++r) {
      auto st = builder.Append(seed + c * 1000 + r);
      if (!st.ok()) {
        throw std::runtime_error(st.ToString());
      }
    }
    std::shared_ptr<arrow::Array> arr;
    auto st = builder.Finish(&arr);
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
    columns.push_back(std::move(arr));
  }
  return arrow::RecordBatch::Make(schema, rows, std::move(columns));
}

struct EngineBenchContext {
  ArrowRocksEngine engine;
  std::shared_ptr<arrow::Schema> schema;
  std::vector<std::string> quarter_columns;
  std::vector<uint64_t> uids;
  std::atomic<uint64_t> next_uid{1000000};
  uint16_t table_id = 1;
  uint16_t version = 1;

  EngineBenchContext(const std::string &name, int num_columns,
                     int64_t preload_keys, int64_t rows_per_batch,
                     std::size_t cache_mb, bool flush_compact = true,
                     bool use_direct_reads = false,
                     bool fill_cache_on_read = true) {
    schema = MakeSchema(num_columns);
    for (int i = 0; i < std::max(1, num_columns / 4); ++i) {
      quarter_columns.push_back("c" + std::to_string(i));
    }

    EngineConfig cfg;
    cfg.db_path = TempPath(name);
    cfg.block_cache_size_mb = cache_mb;
    cfg.disable_wal = true;
    cfg.use_direct_reads = use_direct_reads;
    cfg.fill_cache_on_read = fill_cache_on_read;
    cfg.ttl_days = 30;
    auto st = engine.Init(cfg);
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
    st = engine.RegisterSchema(table_id, version, schema);
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }

    for (int64_t i = 0; i < preload_keys; ++i) {
      const uint64_t uid = static_cast<uint64_t>(i + 1);
      auto batch = MakeBatch(schema, rows_per_batch, i * 100);
      st = engine.PutFeature(table_id, uid, version, NowMs() + i, *batch);
      if (!st.ok()) {
        throw std::runtime_error(st.ToString());
      }
      uids.push_back(uid);
    }
    if (flush_compact) {
      st = engine.FlushAll();
      if (!st.ok()) {
        throw std::runtime_error(st.ToString());
      }
      st = engine.CompactAll();
      if (!st.ok()) {
        throw std::runtime_error(st.ToString());
      }
    }
  }

  ~EngineBenchContext() { (void)engine.Close(); }
};

EngineBenchContext &AppendContext() {
  static auto *ctx = new EngineBenchContext("engine_bench_append", 8, 0, 8, 64);
  return *ctx;
}

EngineBenchContext &MemTableHitContext() {
  static auto *ctx = new EngineBenchContext("engine_bench_memtable_hit", 32,
                                            4096, 64, 64, false, false, true);
  return *ctx;
}

EngineBenchContext &BlockCacheHitContext() {
  static auto *ctx = new EngineBenchContext("engine_bench_block_cache_hit", 32,
                                            8192, 64, 64, true, false, true);
  return *ctx;
}

EngineBenchContext &PageCacheReadContext() {
  static auto *ctx = new EngineBenchContext("engine_bench_page_cache_read", 32,
                                            50000, 64, 1, true, false, false);
  return *ctx;
}

EngineBenchContext &DirectReadContext() {
  static auto *ctx = new EngineBenchContext("engine_bench_direct_read", 32,
                                            50000, 64, 1, true, true, false);
  return *ctx;
}

EngineBenchContext &ProjectionContext() {
  static auto *ctx =
      new EngineBenchContext("engine_bench_projection", 64, 2048, 8, 64);
  return *ctx;
}

EngineBenchContext &BatchContext() {
  static auto *ctx =
      new EngineBenchContext("engine_bench_batch", 16, 5000, 8, 64);
  return *ctx;
}

std::string MakeMergedValue(int frame_count, uint16_t schema_version,
                            const std::shared_ptr<arrow::Schema> &schema,
                            int64_t rows_per_frame = 8, int64_t seed_base = 0) {
  std::string out;
  for (int i = 0; i < frame_count; ++i) {
    auto batch = MakeBatch(schema, rows_per_frame, seed_base + i * 10);
    auto encoded = EncodeFrame(schema_version, NowMs() + i, *batch);
    if (!encoded.ok()) {
      throw std::runtime_error(encoded.status().ToString());
    }
    out += encoded.ValueOrDie();
  }
  return out;
}

std::vector<std::string>
NamesOfFirst(const std::shared_ptr<arrow::Schema> &schema, int count) {
  std::vector<std::string> cols;
  if (count <= 0) {
    return cols;
  }
  const int n = std::min<int>(count, schema->num_fields());
  cols.reserve(static_cast<std::size_t>(n));
  for (int i = 0; i < n; ++i) {
    cols.push_back(schema->field(i)->name());
  }
  return cols;
}

static void BM_AppendFeature(benchmark::State &state) {
  auto &ctx = AppendContext();
  for (auto _ : state) {
    const uint64_t uid = ctx.next_uid.fetch_add(1, std::memory_order_relaxed);
    auto batch = MakeBatch(ctx.schema, 8, static_cast<int64_t>(uid));
    auto st = ctx.engine.AppendFeature(ctx.table_id, uid, ctx.version, *batch);
    if (!st.ok()) {
      state.SkipWithError(st.ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(batch);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_GetFeatureMemTableHit(benchmark::State &state) {
  auto &ctx = MemTableHitContext();
  const uint64_t hot_uid = ctx.uids.front();
  auto warm = ctx.engine.GetFeature(ctx.table_id, hot_uid, ctx.version);
  if (!warm.ok()) {
    state.SkipWithError(warm.status().ToString().c_str());
    return;
  }
  for (auto _ : state) {
    auto res = ctx.engine.GetFeature(ctx.table_id, hot_uid, ctx.version);
    if (!res.ok()) {
      state.SkipWithError(res.status().ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(*res);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_GetFeatureBlockCacheHit(benchmark::State &state) {
  auto &ctx = BlockCacheHitContext();
  const uint64_t hot_uid = ctx.uids.front();
  auto warm = ctx.engine.GetFeature(ctx.table_id, hot_uid, ctx.version);
  if (!warm.ok()) {
    state.SkipWithError(warm.status().ToString().c_str());
    return;
  }
  for (auto _ : state) {
    auto res = ctx.engine.GetFeature(ctx.table_id, hot_uid, ctx.version);
    if (!res.ok()) {
      state.SkipWithError(res.status().ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(*res);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_GetFeaturePageCacheRead(benchmark::State &state) {
  auto &ctx = PageCacheReadContext();
  std::size_t index = 0;
  for (auto _ : state) {
    const uint64_t uid = ctx.uids[index++ % ctx.uids.size()];
    auto res = ctx.engine.GetFeature(ctx.table_id, uid, ctx.version);
    if (!res.ok()) {
      state.SkipWithError(res.status().ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(*res);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_GetFeatureDirectRead(benchmark::State &state) {
  auto &ctx = DirectReadContext();
  std::size_t index = 0;
  for (auto _ : state) {
    const uint64_t uid = ctx.uids[index++ % ctx.uids.size()];
    auto res = ctx.engine.GetFeature(ctx.table_id, uid, ctx.version);
    if (!res.ok()) {
      state.SkipWithError(res.status().ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(*res);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_BatchGet(benchmark::State &state) {
  auto &ctx = BatchContext();
  const int batch_size = static_cast<int>(state.range(0));
  std::vector<BatchGetRequest> requests;
  requests.reserve(static_cast<std::size_t>(batch_size));
  for (int i = 0; i < batch_size; ++i) {
    requests.push_back(BatchGetRequest{
        .table_id = ctx.table_id,
        .uid = ctx.uids[static_cast<std::size_t>(i) % ctx.uids.size()],
        .target_version = ctx.version,
        .columns = {},
    });
  }

  for (auto _ : state) {
    auto results = ctx.engine.BatchGetFeature(requests);
    benchmark::DoNotOptimize(results);
  }
  state.SetItemsProcessed(state.iterations() * batch_size);
}

static void BM_BatchAppendFeature(benchmark::State &state) {
  auto &ctx = AppendContext();
  const int batch_size = static_cast<int>(state.range(0));
  for (auto _ : state) {
    std::vector<BatchAppendRequest> requests;
    requests.reserve(static_cast<std::size_t>(batch_size));
    for (int i = 0; i < batch_size; ++i) {
      const uint64_t uid = ctx.next_uid.fetch_add(1, std::memory_order_relaxed);
      requests.push_back(BatchAppendRequest{
          .table_id = ctx.table_id,
          .uid = uid,
          .schema_version = ctx.version,
          .batch = MakeBatch(ctx.schema, 8, static_cast<int64_t>(uid)),
      });
    }
    auto st = ctx.engine.BatchAppendFeature(requests);
    if (!st.ok()) {
      state.SkipWithError(st.ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(requests);
  }
  state.SetItemsProcessed(state.iterations() * batch_size);
}

static void BM_GetWithProjection(benchmark::State &state) {
  auto &ctx = ProjectionContext();
  const bool quarter = state.range(0) == 1;
  const auto &columns =
      quarter ? ctx.quarter_columns : std::vector<std::string>{};
  std::size_t index = 0;
  for (auto _ : state) {
    const uint64_t uid = ctx.uids[index++ % ctx.uids.size()];
    auto res = ctx.engine.GetFeature(ctx.table_id, uid, ctx.version, columns);
    if (!res.ok()) {
      state.SkipWithError(res.status().ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(*res);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_GetWithProjectionCols(benchmark::State &state) {
  auto &ctx = ProjectionContext();
  const int select_cols = static_cast<int>(state.range(0));
  const auto columns = NamesOfFirst(ctx.schema, select_cols);
  std::size_t index = 0;
  for (auto _ : state) {
    const uint64_t uid = ctx.uids[index++ % ctx.uids.size()];
    auto res = ctx.engine.GetFeature(ctx.table_id, uid, ctx.version, columns);
    if (!res.ok()) {
      state.SkipWithError(res.status().ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(*res);
  }
  state.SetItemsProcessed(state.iterations());
}

struct EvolvedBenchContext {
  ArrowRocksEngine engine;
  uint16_t table_id = 1;
  uint16_t v1 = 1;
  uint16_t v2 = 2;
  std::vector<uint64_t> uids;
  std::vector<std::string> new_only_cols{"b"};

  EvolvedBenchContext(const std::string &name, int64_t preload_keys,
                      std::size_t cache_mb) {
    EngineConfig cfg;
    cfg.db_path = TempPath(name);
    cfg.block_cache_size_mb = cache_mb;
    cfg.disable_wal = true;
    cfg.ttl_days = 30;
    auto st = engine.Init(cfg);
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }

    auto schema_v1 = arrow::schema({arrow::field("a", arrow::int64())});
    auto schema_v2 = arrow::schema({arrow::field("a", arrow::int64()),
                                    arrow::field("b", arrow::int32(), true)});
    st = engine.RegisterSchema(table_id, v1, schema_v1);
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
    st = engine.RegisterSchema(table_id, v2, schema_v2);
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }

    for (int64_t i = 0; i < preload_keys; ++i) {
      const uint64_t uid = static_cast<uint64_t>(i + 1);
      arrow::Int64Builder a_builder;
      auto append_st = a_builder.Append(100 + i);
      if (!append_st.ok()) {
        throw std::runtime_error(append_st.ToString());
      }
      std::shared_ptr<arrow::Int64Array> a_arr;
      auto finish_st = a_builder.Finish(&a_arr);
      if (!finish_st.ok()) {
        throw std::runtime_error(finish_st.ToString());
      }
      auto batch = arrow::RecordBatch::Make(schema_v1, 1, {a_arr});
      st = engine.PutFeature(table_id, uid, v1, NowMs() + i, *batch);
      if (!st.ok()) {
        throw std::runtime_error(st.ToString());
      }
      uids.push_back(uid);
    }
    st = engine.FlushAll();
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
    st = engine.CompactAll();
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
  }

  ~EvolvedBenchContext() { (void)engine.Close(); }
};

EvolvedBenchContext &EvolvedProjectionContext() {
  static auto *ctx =
      new EvolvedBenchContext("engine_bench_evolved_projection", 2048, 64);
  return *ctx;
}

static void BM_GetEvolvedProjectionNewOnly(benchmark::State &state) {
  auto &ctx = EvolvedProjectionContext();
  std::size_t index = 0;
  for (auto _ : state) {
    const uint64_t uid = ctx.uids[index++ % ctx.uids.size()];
    auto res =
        ctx.engine.GetFeature(ctx.table_id, uid, ctx.v2, ctx.new_only_cols);
    if (!res.ok()) {
      state.SkipWithError(res.status().ToString().c_str());
      break;
    }
    benchmark::DoNotOptimize(*res);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_PartialMergeFastPath(benchmark::State &state) {
  SchemaRegistry registry;
  ArrowMergeOperator merge_op(&registry);
  auto schema = MakeSchema(32);
  const int frame_count = static_cast<int>(state.range(0));
  std::string left = MakeMergedValue(frame_count, 1, schema, 512, 0);
  std::string right = MakeMergedValue(frame_count, 1, schema, 512, 100000);
  const rocksdb::Slice key("k");

  for (auto _ : state) {
    std::string out;
    bool ok = merge_op.PartialMerge(key, rocksdb::Slice(left),
                                    rocksdb::Slice(right), &out, nullptr);
    if (!ok) {
      state.SkipWithError("PartialMerge failed");
      break;
    }
    benchmark::DoNotOptimize(out);
  }
  state.SetItemsProcessed(state.iterations() * frame_count * 2);
  state.SetBytesProcessed(state.iterations() *
                          static_cast<int64_t>(left.size() + right.size()));
}

static void BM_PartialMergeFallback(benchmark::State &state) {
  SchemaRegistry registry;
  ArrowMergeOperator merge_op(&registry);
  auto schema = MakeSchema(32);
  const int frame_count = static_cast<int>(state.range(0));
  std::string left = MakeMergedValue(frame_count, 1, schema, 512, 0);
  std::string right = MakeMergedValue(frame_count, 2, schema, 512, 100000);
  const rocksdb::Slice key("k");

  for (auto _ : state) {
    std::string out;
    bool ok = merge_op.PartialMerge(key, rocksdb::Slice(left),
                                    rocksdb::Slice(right), &out, nullptr);
    if (!ok) {
      state.SkipWithError("PartialMerge fallback failed");
      break;
    }
    benchmark::DoNotOptimize(out);
  }
  state.SetItemsProcessed(state.iterations() * frame_count * 2);
  state.SetBytesProcessed(state.iterations() *
                          static_cast<int64_t>(left.size() + right.size()));
}

static void BM_FullMergeV2ManyTinyOperands(benchmark::State &state) {
  SchemaRegistry registry;
  ArrowMergeOperator merge_op(&registry);
  auto schema = MakeSchema(8);
  const int operand_count = static_cast<int>(state.range(0));
  std::string existing = MakeMergedValue(1, 1, schema, 8, 0);

  std::vector<std::string> operand_storage;
  operand_storage.reserve(static_cast<std::size_t>(operand_count));
  for (int i = 0; i < operand_count; ++i) {
    operand_storage.push_back(MakeMergedValue(1, 1, schema, 8, i * 1000));
  }

  std::vector<rocksdb::Slice> operands;
  operands.reserve(operand_storage.size());
  for (const auto &op : operand_storage) {
    operands.emplace_back(op);
  }

  const rocksdb::Slice key("k");
  const rocksdb::Slice existing_slice(existing);
  rocksdb::MergeOperator::MergeOperationInput merge_in(key, &existing_slice,
                                                       operands, nullptr);

  for (auto _ : state) {
    std::string merged;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput merge_out(merged,
                                                           existing_operand);
    bool ok = merge_op.FullMergeV2(merge_in, &merge_out);
    if (!ok) {
      state.SkipWithError("FullMergeV2 failed");
      break;
    }
    benchmark::DoNotOptimize(merged);
  }

  int64_t total_bytes = static_cast<int64_t>(existing.size());
  for (const auto &op : operand_storage) {
    total_bytes += static_cast<int64_t>(op.size());
  }
  state.SetItemsProcessed(state.iterations() * operand_count);
  state.SetBytesProcessed(state.iterations() * total_bytes);
}

static void BM_FullMergeV2FewBigOperands(benchmark::State &state) {
  SchemaRegistry registry;
  ArrowMergeOperator merge_op(&registry);
  auto schema = MakeSchema(32);
  const int frames_per_operand = static_cast<int>(state.range(0));
  constexpr int operand_count = 4;
  std::string existing = MakeMergedValue(frames_per_operand, 1, schema, 512, 0);

  std::vector<std::string> operand_storage;
  operand_storage.reserve(operand_count);
  for (int i = 0; i < operand_count; ++i) {
    operand_storage.push_back(
        MakeMergedValue(frames_per_operand, 1, schema, 512, i * 100000));
  }

  std::vector<rocksdb::Slice> operands;
  operands.reserve(operand_storage.size());
  for (const auto &op : operand_storage) {
    operands.emplace_back(op);
  }

  const rocksdb::Slice key("k");
  const rocksdb::Slice existing_slice(existing);
  rocksdb::MergeOperator::MergeOperationInput merge_in(key, &existing_slice,
                                                       operands, nullptr);

  for (auto _ : state) {
    std::string merged;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput merge_out(merged,
                                                           existing_operand);
    bool ok = merge_op.FullMergeV2(merge_in, &merge_out);
    if (!ok) {
      state.SkipWithError("FullMergeV2 few-big-operands failed");
      break;
    }
    benchmark::DoNotOptimize(merged);
  }

  int64_t total_bytes = static_cast<int64_t>(existing.size());
  for (const auto &op : operand_storage) {
    total_bytes += static_cast<int64_t>(op.size());
  }
  state.SetItemsProcessed(state.iterations() * operand_count *
                          frames_per_operand);
  state.SetBytesProcessed(state.iterations() * total_bytes);
}

static void BM_CRC32C(benchmark::State &state) {
  const int payload_bytes = static_cast<int>(state.range(0));
  auto schema = MakeSchema(1);
  const int64_t rows = std::max<int64_t>(1, payload_bytes / 8);
  auto batch = MakeBatch(schema, rows, 1);
  auto encoded = EncodeFrame(1, 123, *batch);
  if (!encoded.ok()) {
    state.SkipWithError(encoded.status().ToString().c_str());
    return;
  }
  const auto &bytes = encoded.ValueOrDie();
  auto span = std::span<const uint8_t>(
      reinterpret_cast<const uint8_t *>(bytes.data()), bytes.size());

  for (auto _ : state) {
    bool ok = ValidateFrameCRC(span);
    benchmark::DoNotOptimize(ok);
  }
  state.SetBytesProcessed(state.iterations() *
                          static_cast<int64_t>(bytes.size()));
}

BENCHMARK(BM_AppendFeature)->UseRealTime();
BENCHMARK(BM_AppendFeature)->Threads(4)->UseRealTime();
BENCHMARK(BM_BatchAppendFeature)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->UseRealTime();

BENCHMARK(BM_GetFeatureMemTableHit);
BENCHMARK(BM_GetFeatureBlockCacheHit);
BENCHMARK(BM_GetFeaturePageCacheRead);
BENCHMARK(BM_GetFeatureDirectRead);

BENCHMARK(BM_BatchGet)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_GetWithProjection)->Arg(0)->Arg(1);
BENCHMARK(BM_GetWithProjectionCols)->Arg(8)->Arg(16)->Arg(32)->Arg(64);
BENCHMARK(BM_GetEvolvedProjectionNewOnly);
BENCHMARK(BM_PartialMergeFastPath)->Arg(1)->Arg(10)->Arg(100);
BENCHMARK(BM_PartialMergeFallback)->Arg(1)->Arg(10)->Arg(100);
BENCHMARK(BM_FullMergeV2ManyTinyOperands)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_FullMergeV2FewBigOperands)->Arg(1)->Arg(10)->Arg(100);
BENCHMARK(BM_CRC32C)->Arg(256)->Arg(4096)->Arg(65536);

} // namespace
} // namespace feature_store

BENCHMARK_MAIN();
