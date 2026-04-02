#include "Logger.h"
#include "arrow_rocks_engine.h"
#include <arrow/api.h>
#include <filesystem>
#include <tuple>

using namespace feature_store;

int main() {
  // 初始化日志
  lyf::Logger::Instance().InitFromConfig("log_config.toml");
  auto log_path = lyf::Logger::Instance().GetConfig().GetLogPath();
  // 清空但不删除log文件
  std::ofstream(log_path.data()).close();

  // 加载引擎配置
  auto cfg = EngineConfig::LoadFromJsonFile("engine_config.json");
  if (!cfg.ok()) {
    FATAL("Failed to load  engine config: {}", cfg.status().ToString());
    return 1;
  }
  INFO("loaded cfg: {}", cfg->ToString());
  auto s = cfg->Validate();
  if (!s.ok()) {
    FATAL("Failed to validate engine config: {}", s.ToString());
    return 1;
  }

  // 清空数据库目录
  auto dp_path = cfg.ValueOrDie().db_path;
  std::filesystem::remove_all(dp_path);

  // 初始化引擎
  ArrowRocksEngine engine;
  auto st = engine.Init(cfg.ValueOrDie());
  if (!st.ok()) {
    FATAL("Failed to init engine: {}", st.ToString());
    return 1;
  }
  INFO("init engine success");

  // 创建表单
  auto schema_v1 = arrow::schema({
      arrow::field("age", arrow::int32()),
      arrow::field("score", arrow::float64(), true),
  });

  //  注册表单
  st = engine.DropTable(1);
  if (st.ok()) {
    INFO("table 1 exists, drop table 1 success");
  }

  st = engine.RegisterSchema(1, 1, schema_v1);
  if (!st.ok()) {
    FATAL("Failed to register schema_v1: {}", st.ToString());
    return 1;
  }
  INFO("register schema_v1 success, schema: {}", schema_v1->ToString());

  // 创建数据
  arrow::Int32Builder age_builder;
  arrow::DoubleBuilder score_builder;
  std::ignore = age_builder.Append(18);
  std::ignore = score_builder.Append(95.5);

  std::shared_ptr<arrow::Array> age_arr;
  std::shared_ptr<arrow::Array> score_arr;
  std::ignore = age_builder.Finish(&age_arr);
  std::ignore = score_builder.Finish(&score_arr);

  auto batch = arrow::RecordBatch::Make(schema_v1, 1, {age_arr, score_arr});

  // 写入数据
  st = engine.AppendFeature(1, 10001, 1, *batch);
  if (!st.ok()) {
    FATAL("Failed to append feature_v1: {}", st.ToString());
    return 1;
  }
  INFO("append feature success, feature: {}", batch->ToString());

  // 读取数据(只读age列)
  auto res = engine.GetFeature(1, 10001, 1, {"age"});
  if (!res.ok()) {
    FATAL("Failed to get feature_v1: {}", res.status().ToString());
    return 1;
  }
  INFO("get feature age success, feature: {}", res.ValueOrDie()->ToString());

  // 读取数据(读取所有列数据)
  res = engine.GetFeature(1, 10001, 1);
  if (!res.ok()) {
    FATAL("Failed to get feature_v1: {}", res.status().ToString());
    return 1;
  }
  INFO("get all feature_v1 success, feature: {}", res.ValueOrDie()->ToString());

  // schema 变更
  auto schema_v2 = arrow::schema({
      arrow::field("age", arrow::int32()),
      arrow::field("score", arrow::float64(), true),
      arrow::field("name", arrow::utf8()),
  });

  // 注册表单
  st = engine.RegisterSchema(1, 2, schema_v2);
  if (!st.ok()) {
    FATAL("Failed to register schema_v2: {}", st.ToString());
    return 1;
  }
  INFO("register schema_v2 success, schema: {}", schema_v2->ToString());
  // 读取v2数据,自动补name列为null
  res = engine.GetFeature(1, 10001, 2);
  if (!res.ok()) {
    FATAL("Failed to get feature_v2: {}", res.status().ToString());
    return 1;
  }
  INFO("get all feature_v2 success, feature: {}", res.ValueOrDie()->ToString());

  // 写入v2数据
  arrow::StringBuilder name_builder;
  std::ignore = name_builder.Append("lyf");
  std::shared_ptr<arrow::Array> name_arr;
  std::ignore = name_builder.Finish(&name_arr);
  batch =
      arrow::RecordBatch::Make(schema_v2, 1, {age_arr, score_arr, name_arr});
  st = engine.AppendFeature(1, 10002, 2, *batch);
  if (!st.ok()) {
    FATAL("Failed to append feature_v2: {}", st.ToString());
    return 1;
  }
  INFO("append feature_v2 success, feature: {}", batch->ToString());
  // 读取v2数据,自动补name列为lyf
  res = engine.GetFeature(1, 10002, 2);
  if (!res.ok()) {
    FATAL("Failed to get feature_v2: {}", res.status().ToString());
    return 1;
  }
  INFO("get all feature_v2 success, feature: {}", res.ValueOrDie()->ToString());
  // 若读取v1数据，则不包含name列
  res = engine.GetFeature(1, 10002, 1);
  if (!res.ok()) {
    FATAL("Failed to get feature_v1: {}", res.status().ToString());
    return 1;
  }
  INFO("get all feature_v1 success, feature: {}", res.ValueOrDie()->ToString());

  // 关闭引擎
  s = engine.Close();
  if (!s.ok()) {
    FATAL("Failed to close engine: {}", s.ToString());
    return 1;
  }
  INFO("close engine success");
  return 0;
}