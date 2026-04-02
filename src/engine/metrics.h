#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/status.h>
#include <rocksdb/db.h>
#include <rocksdb/statistics.h>

namespace feature_store {

class Metrics {
public:
  struct Histogram {
    std::vector<double> buckets;
    std::vector<std::atomic<uint64_t>> counts;
    std::atomic<uint64_t> sum{0};
    std::atomic<uint64_t> total{0};

    Histogram() = default;
    explicit Histogram(const std::vector<double> &b)
        : buckets(b), counts(b.size()) {}

    void observe(double x) {
      for (std::size_t i = 0; i < buckets.size(); ++i) {
        if (x <= buckets[i]) {
          counts[i].fetch_add(1, std::memory_order_relaxed);
        }
      }
      sum.fetch_add(static_cast<uint64_t>(x), std::memory_order_relaxed);
      total.fetch_add(1, std::memory_order_relaxed);
    }
  };

  static Metrics *Global();

  void IncAppend(uint16_t table_id, uint64_t bytes, uint64_t latency_us);
  void IncGet(uint16_t table_id, bool hit, uint64_t latency_us,
              double projection_ratio);
  void IncBatchGet(uint64_t calls, uint64_t keys);

  void IncCRCError(uint64_t n = 1);
  void AddPinnableActive(int64_t delta);
  void IncTTLExpiredFrames(uint64_t n);
  void IncTTLExpiredKeys(uint64_t n);
  void SetActiveCFCount(uint32_t n);
  void IncCfDrop(uint64_t n = 1);

  std::string GetMetrics(rocksdb::DB *db, rocksdb::Statistics *stats) const;

private:
  struct Counter64 {
    std::atomic<uint64_t> v{0};
    void add(uint64_t n) { v.fetch_add(n, std::memory_order_relaxed); }
    uint64_t get() const { return v.load(std::memory_order_relaxed); }
  };

  struct Gauge64 {
    std::atomic<uint64_t> v{0};
    void set(uint64_t n) { v.store(n, std::memory_order_relaxed); }
    uint64_t get() const { return v.load(std::memory_order_relaxed); }
    void add(int64_t delta) {
      v.fetch_add(static_cast<uint64_t>(delta), std::memory_order_relaxed);
    }
  };

  struct PerTable {
    Counter64 append_total;
    Counter64 append_bytes_total;
    Histogram append_latency_us{
        std::vector<double>{100, 500, 1000, 2000, 5000, 10000, 50000, 100000}};

    Counter64 get_total_hit;
    Counter64 get_total_miss;
    Histogram get_latency_us{
        std::vector<double>{100, 500, 1000, 2000, 5000, 10000, 50000, 100000}};
    Histogram get_projection_ratio{
        std::vector<double>{0.1, 0.25, 0.5, 0.75, 1.0}};
  };

  mutable std::unordered_map<uint16_t, PerTable> per_table_;

  Counter64 batch_get_total_;
  Counter64 batch_get_keys_total_;

  Counter64 frame_crc_error_total_;

  Gauge64 pinnable_active_count_;

  Counter64 ttl_expired_frames_total_;
  Counter64 ttl_expired_keys_total_;

  Gauge64 active_cf_count_;
  Counter64 cf_drop_total_;
};

} // namespace feature_store
