#include "../engine/metrics.h"

#include <string>

#include <gtest/gtest.h>

namespace feature_store {
namespace {

TEST(MetricsTest, PrometheusTextFormatContainsAllCoreMetrics) {
    auto* m = Metrics::Global();
    m->IncAppend(1, 128, 1000);
    m->IncGet(1, true, 500, 0.5);
    m->IncGet(1, false, 600, 1.0);
    m->IncBatchGet(1, 3);
    m->IncCRCError();
    m->AddPinnableActive(2);
    m->IncTTLExpiredFrames(4);
    m->IncTTLExpiredKeys(1);
    m->SetActiveCFCount(7);
    m->IncCfDrop(2);

    const std::string out = m->GetMetrics(nullptr, nullptr);

    EXPECT_NE(out.find("feature_engine_append_total{table_id=\"1\"}"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_append_bytes_total{table_id=\"1\"}"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_append_latency_us_bucket"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_get_total{table_id=\"1\",hit=\"true\"}"),
              std::string::npos);
    EXPECT_NE(out.find("feature_engine_get_total{table_id=\"1\",hit=\"false\"}"),
              std::string::npos);
    EXPECT_NE(out.find("feature_engine_get_latency_us_bucket"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_get_projection_ratio_bucket"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_batch_get_total"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_batch_get_keys_total"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_frame_crc_error_total"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_pinnable_active_count"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_ttl_expired_frames_total"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_ttl_expired_keys_total"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_active_cf_count"), std::string::npos);
    EXPECT_NE(out.find("feature_engine_cf_drop_total"), std::string::npos);
    EXPECT_NE(out.find("rocksdb_block_cache_hit_count"), std::string::npos);
    EXPECT_NE(out.find("rocksdb_block_cache_miss_count"), std::string::npos);
    EXPECT_NE(out.find("rocksdb_memtable_bytes"), std::string::npos);
    EXPECT_NE(out.find("rocksdb_pending_compaction_bytes"), std::string::npos);
  }

}  // namespace
}  // namespace feature_store
