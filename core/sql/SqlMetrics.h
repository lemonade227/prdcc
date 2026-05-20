#pragma once

#include <atomic>
#include <cstdint>

namespace aria::sql {

struct SqlExecutionMetrics {
  std::uint64_t parse_us = 0;
  std::uint64_t plan_us = 0;
  std::uint64_t exec_us = 0;
  std::uint64_t queue_wait_us = 0;
  std::uint64_t cache_hit = 0;
  std::uint64_t cache_miss = 0;
  std::uint64_t txn_total = 0;
};

struct SqlWorkerMetrics {
  std::atomic<std::uint64_t> parse_us{0};
  std::atomic<std::uint64_t> plan_us{0};
  std::atomic<std::uint64_t> exec_us{0};
  std::atomic<std::uint64_t> queue_wait_us{0};
  std::atomic<std::uint64_t> cache_hit{0};
  std::atomic<std::uint64_t> cache_miss{0};
  std::atomic<std::uint64_t> txn_total{0};
};

inline thread_local SqlWorkerMetrics *tls_worker_metrics = nullptr;

inline void register_worker_metrics(SqlWorkerMetrics *metrics) {
  tls_worker_metrics = metrics;
}

inline void publish_metrics(const SqlExecutionMetrics &metrics) {
  if (tls_worker_metrics == nullptr) {
    return;
  }

  tls_worker_metrics->parse_us.fetch_add(metrics.parse_us,
                                         std::memory_order_relaxed);
  tls_worker_metrics->plan_us.fetch_add(metrics.plan_us,
                                        std::memory_order_relaxed);
  tls_worker_metrics->exec_us.fetch_add(metrics.exec_us,
                                        std::memory_order_relaxed);
  tls_worker_metrics->queue_wait_us.fetch_add(metrics.queue_wait_us,
                                              std::memory_order_relaxed);
  tls_worker_metrics->cache_hit.fetch_add(metrics.cache_hit,
                                          std::memory_order_relaxed);
  tls_worker_metrics->cache_miss.fetch_add(metrics.cache_miss,
                                           std::memory_order_relaxed);
  tls_worker_metrics->txn_total.fetch_add(metrics.txn_total,
                                          std::memory_order_relaxed);
}

struct SqlTransactionState {
  bool initialized = false;
  bool frontend_completed = false;
  std::uint32_t attempt_no = 0;
  std::uint64_t last_digest = 0;
};

inline void on_transaction_reset(SqlTransactionState &state) {
  if (state.initialized) {
    state.attempt_no++;
  } else {
    state.initialized = true;
    state.attempt_no = 0;
  }
  state.frontend_completed = false;
  state.last_digest = 0;
}

inline void on_transaction_reuse(SqlTransactionState &state) {
  state.initialized = true;
  state.attempt_no = 0;
  state.frontend_completed = false;
  state.last_digest = 0;
}

inline bool is_retry_attempt(const SqlTransactionState &state) {
  return state.initialized && state.attempt_no > 0;
}

} // namespace aria::sql
