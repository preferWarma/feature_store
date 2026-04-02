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
                     std::size_t cache_mb) {
    schema = MakeSchema(num_columns);
    for (int i = 0; i < std::max(1, num_columns / 4); ++i) {
      quarter_columns.push_back("c" + std::to_string(i));
    }

    EngineConfig cfg;
    cfg.db_path = TempPath(name);
    cfg.block_cache_size_mb = cache_mb;
    cfg.disable_wal = true;
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
    st = engine.FlushAll();
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
    st = engine.CompactAll();
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
  }

  ~EngineBenchContext() { (void)engine.Close(); }
};

EngineBenchContext &AppendContext() {
  static auto *ctx = new EngineBenchContext("engine_bench_append", 8, 0, 8, 64);
  return *ctx;
}

EngineBenchContext &ReadHitContext() {
  static auto *ctx =
      new EngineBenchContext("engine_bench_read_hit", 16, 512, 16, 64);
  return *ctx;
}

EngineBenchContext &ReadMissCacheContext() {
  static auto *ctx =
      new EngineBenchContext("engine_bench_read_miss", 16, 20000, 16, 1);
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
                            const std::shared_ptr<arrow::Schema> &schema) {
  std::string out;
  for (int i = 0; i < frame_count; ++i) {
    auto batch = MakeBatch(schema, 8, i * 10);
    auto encoded = EncodeFrame(schema_version, NowMs() + i, *batch);
    if (!encoded.ok()) {
      throw std::runtime_error(encoded.status().ToString());
    }
    out += encoded.ValueOrDie();
  }
  return out;
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

static void BM_GetFeatureCacheHit(benchmark::State &state) {
  auto &ctx = ReadHitContext();
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

static void BM_GetFeatureCacheMiss(benchmark::State &state) {
  auto &ctx = ReadMissCacheContext();
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

static void BM_MergeOperator(benchmark::State &state) {
  SchemaRegistry registry;
  ArrowMergeOperator merge_op(&registry);
  auto schema = MakeSchema(8);
  const int frame_count = static_cast<int>(state.range(0));
  std::string left = MakeMergedValue(frame_count, 1, schema);
  std::string right = MakeMergedValue(frame_count, 1, schema);
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

BENCHMARK(BM_GetFeatureCacheHit);
BENCHMARK(BM_GetFeatureCacheMiss);

BENCHMARK(BM_BatchGet)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_GetWithProjection)->Arg(0)->Arg(1);
BENCHMARK(BM_MergeOperator)->Arg(1)->Arg(10)->Arg(100);
BENCHMARK(BM_CRC32C)->Arg(256)->Arg(4096)->Arg(65536);

} // namespace
} // namespace feature_store

BENCHMARK_MAIN();
