#include "metrics.h"

#include <sstream>

#include <rocksdb/statistics.h>

namespace feature_store {

namespace {

void AppendHistogram(std::ostringstream &oss, const std::string &name,
                     const Metrics::Histogram &hist) {
  for (std::size_t i = 0; i < hist.buckets.size(); ++i) {
    oss << name << "_bucket{le=\"" << hist.buckets[i] << "\"} "
        << hist.counts[i].load(std::memory_order_relaxed) << "\n";
  }
  oss << name << "_sum " << hist.sum.load(std::memory_order_relaxed) << "\n";
  oss << name << "_count " << hist.total.load(std::memory_order_relaxed)
      << "\n";
}

uint64_t GetTicker(rocksdb::Statistics *stats, rocksdb::Tickers ticker) {
  if (!stats) {
    return 0;
  }
  return stats->getTickerCount(ticker);
}

uint64_t GetUintProperty(rocksdb::DB *db, const std::string &prop) {
  if (!db) {
    return 0;
  }
  uint64_t v = 0;
  (void)db->GetIntProperty(prop, &v);
  return v;
}

} // namespace

Metrics *Metrics::Global() {
  static Metrics g;
  return &g;
}

void Metrics::IncAppend(uint16_t table_id, uint64_t bytes,
                        uint64_t latency_us) {
  auto &m = per_table_[table_id];
  m.append_total.add(1);
  m.append_bytes_total.add(bytes);
  m.append_latency_us.observe(static_cast<double>(latency_us));
}

void Metrics::IncGet(uint16_t table_id, bool hit, uint64_t latency_us,
                     double projection_ratio) {
  auto &m = per_table_[table_id];
  if (hit) {
    m.get_total_hit.add(1);
  } else {
    m.get_total_miss.add(1);
  }
  m.get_latency_us.observe(static_cast<double>(latency_us));
  m.get_projection_ratio.observe(projection_ratio);
}

void Metrics::IncBatchGet(uint64_t calls, uint64_t keys) {
  batch_get_total_.add(calls);
  batch_get_keys_total_.add(keys);
}

void Metrics::IncCRCError(uint64_t n) { frame_crc_error_total_.add(n); }

void Metrics::AddPinnableActive(int64_t delta) {
  if (delta >= 0) {
    pinnable_active_count_.add(delta);
  } else {
    const uint64_t cur = pinnable_active_count_.get();
    const uint64_t dec = static_cast<uint64_t>(-delta);
    pinnable_active_count_.set(cur > dec ? (cur - dec) : 0);
  }
}

void Metrics::IncTTLExpiredFrames(uint64_t n) {
  ttl_expired_frames_total_.add(n);
}
void Metrics::IncTTLExpiredKeys(uint64_t n) { ttl_expired_keys_total_.add(n); }
void Metrics::SetActiveCFCount(uint32_t n) { active_cf_count_.set(n); }
void Metrics::IncCfDrop(uint64_t n) { cf_drop_total_.add(n); }

std::string Metrics::GetMetrics(rocksdb::DB *db,
                                rocksdb::Statistics *stats) const {
  std::ostringstream oss;

  for (const auto &[tid, m] : per_table_) {
    oss << "feature_engine_append_total{table_id=\"" << tid << "\"} "
        << m.append_total.get() << "\n";
    oss << "feature_engine_append_bytes_total{table_id=\"" << tid << "\"} "
        << m.append_bytes_total.get() << "\n";
    AppendHistogram(oss, "feature_engine_append_latency_us",
                    m.append_latency_us);

    oss << "feature_engine_get_total{table_id=\"" << tid << "\",hit=\"true\"} "
        << m.get_total_hit.get() << "\n";
    oss << "feature_engine_get_total{table_id=\"" << tid << "\",hit=\"false\"} "
        << m.get_total_miss.get() << "\n";
    AppendHistogram(oss, "feature_engine_get_latency_us", m.get_latency_us);
    AppendHistogram(oss, "feature_engine_get_projection_ratio",
                    m.get_projection_ratio);
  }

  oss << "feature_engine_batch_get_total " << batch_get_total_.get() << "\n";
  oss << "feature_engine_batch_get_keys_total " << batch_get_keys_total_.get()
      << "\n";
  oss << "feature_engine_frame_crc_error_total " << frame_crc_error_total_.get()
      << "\n";
  oss << "feature_engine_pinnable_active_count " << pinnable_active_count_.get()
      << "\n";
  oss << "feature_engine_ttl_expired_frames_total "
      << ttl_expired_frames_total_.get() << "\n";
  oss << "feature_engine_ttl_expired_keys_total "
      << ttl_expired_keys_total_.get() << "\n";
  oss << "feature_engine_active_cf_count " << active_cf_count_.get() << "\n";
  oss << "feature_engine_cf_drop_total " << cf_drop_total_.get() << "\n";

  oss << "rocksdb_block_cache_hit_count "
      << GetTicker(stats, rocksdb::Tickers::BLOCK_CACHE_HIT) << "\n";
  oss << "rocksdb_block_cache_miss_count "
      << GetTicker(stats, rocksdb::Tickers::BLOCK_CACHE_MISS) << "\n";
  oss << "rocksdb_memtable_bytes "
      << GetUintProperty(db, "rocksdb.cur-size-all-mem-tables") << "\n";
  oss << "rocksdb_pending_compaction_bytes "
      << GetUintProperty(db, "rocksdb.estimate-pending-compaction-bytes")
      << "\n";

  return oss.str();
}

} // namespace feature_store
