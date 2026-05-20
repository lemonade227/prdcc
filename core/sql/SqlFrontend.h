#pragma once

#include "core/Context.h"
#include "core/sql/SqlExecKernel.h"
#include "core/sql/SqlMetrics.h"
#include "core/sql/SqlParserLite.h"
#include "core/sql/SqlPlanCache.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace aria::sql {

inline void consume_parser_complexity(const SqlAst &ast,
                                      std::size_t parser_complexity) {
  std::uint64_t sink = ast.lexical_fingerprint;
  const std::size_t rounds =
      std::max<std::size_t>(1, parser_complexity) *
      std::max<std::size_t>(1, ast.token_count);
  for (std::size_t i = 0; i < rounds; i++) {
    sink ^= (sink << 7) + (sink >> 3) +
            static_cast<std::uint64_t>(i * 131 + ast.placeholder_count);
  }
  static thread_local std::uint64_t parser_sink = 0;
  parser_sink ^= sink;
}

inline void burn_optimizer_cpu(const SqlPlan &plan, std::size_t level) {
  std::uint64_t sink = plan.plan_hash;
  const std::size_t rounds =
      std::max<std::size_t>(1, level) *
      (plan.operators.size() + plan.expression_cost + 1);
  for (std::size_t i = 0; i < rounds; i++) {
    sink ^= (sink << 9) + (sink >> 1) +
            static_cast<std::uint64_t>(i * 17 + plan.predicate_cost);
  }
  static thread_local std::uint64_t optimizer_sink = 0;
  optimizer_sink ^= sink;
}

inline std::uint64_t elapsed_us(
    const std::chrono::steady_clock::time_point &start) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

inline StatementKind infer_kind(const std::string &sql) {
  if (sql.rfind("SELECT", 0) == 0) {
    return StatementKind::Select;
  }
  if (sql.rfind("UPDATE", 0) == 0) {
    return StatementKind::Update;
  }
  if (sql.rfind("INSERT", 0) == 0) {
    return StatementKind::Insert;
  }
  return StatementKind::Unknown;
}

inline std::uint64_t stable_statement_hash(const std::string &text) {
  std::uint64_t hash = 1469598103934665603ULL;
  for (unsigned char ch : text) {
    hash ^= ch;
    hash *= 1099511628211ULL;
  }
  return hash;
}

class SqlQueueThrottle {
public:
  class Guard {
  public:
    Guard(SqlQueueThrottle &throttle, std::uint64_t &wait_us_out)
        : throttle(throttle) {
      const auto start = std::chrono::steady_clock::now();
      throttle.acquire();
      wait_us_out +=
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - start)
              .count();
    }

    ~Guard() { throttle.release(); }

  private:
    SqlQueueThrottle &throttle;
  };

  static SqlQueueThrottle &instance() {
    static SqlQueueThrottle throttle;
    return throttle;
  }

private:
  void acquire() {
    for (;;) {
      auto cur = active.load(std::memory_order_relaxed);
      if (cur < limit.load(std::memory_order_relaxed) &&
          active.compare_exchange_weak(cur, cur + 1,
                                       std::memory_order_acq_rel,
                                       std::memory_order_relaxed)) {
        return;
      }
      std::this_thread::yield();
    }
  }

  void release() { active.fetch_sub(1, std::memory_order_acq_rel); }

  friend class Guard;

public:
  void configure(std::size_t max_active) {
    limit.store(static_cast<std::uint32_t>(
                    std::max<std::size_t>(1, max_active)),
                std::memory_order_relaxed);
  }

private:
  std::atomic<std::uint32_t> active{0};
  std::atomic<std::uint32_t> limit{
      std::numeric_limits<std::uint32_t>::max()};
};

template <class Context>
inline std::uint64_t run_frontend_once(
    const Context &context, SqlTransactionState &txn_state,
    const std::vector<SqlStatementSpec> &statements) {
  if (!context.enable_sql_emulator || txn_state.frontend_completed) {
    return txn_state.last_digest;
  }

  SqlExecutionMetrics metrics;
  metrics.txn_total = 1;

  SqlQueueThrottle::instance().configure(context.sql_queue_workers == 0
                                             ? std::numeric_limits<std::uint32_t>::max()
                                             : context.sql_queue_workers);
  SqlQueueThrottle::Guard queue_guard(SqlQueueThrottle::instance(),
                                      metrics.queue_wait_us);

  const bool retry_path =
      context.sql_enable_retry_cache_bias && is_retry_attempt(txn_state);
  SqlParserLite parser;
  SqlPlanner planner(context.sql_optimizer_level);
  SqlExecKernel exec_kernel(context.sql_exec_expr_complexity);

  std::uint64_t digest = txn_state.last_digest;

  for (const auto &statement : statements) {
    SqlAst ast;
    SqlPlan plan;
    bool cache_hit = false;
    const std::string &cache_key = statement.sql;

    if (!retry_path) {
      const auto parse_start = std::chrono::steady_clock::now();
      ast = parser.parse(statement.sql);
      consume_parser_complexity(ast, context.sql_parser_complexity);
      metrics.parse_us += elapsed_us(parse_start);
    } else {
      ast.normalized_sql = statement.sql;
      ast.lexical_fingerprint = stable_statement_hash(statement.sql);
      ast.kind = infer_kind(statement.sql);
    }

    if (retry_path) {
      cache_hit =
          SqlPlanCache::instance().lookup(cache_key, context.sql_plan_cache_size,
                                          plan);
    } else {
      cache_hit =
          SqlPlanCache::instance().lookup(cache_key, context.sql_plan_cache_size,
                                          plan);
    }

    if (cache_hit) {
      metrics.cache_hit++;
    } else {
      const auto plan_start = std::chrono::steady_clock::now();
      if (retry_path) {
        ast = parser.parse(statement.sql);
      }
      plan = planner.plan(ast, context.sql_exec_vector_size);
      burn_optimizer_cpu(plan, context.sql_optimizer_level);
      SqlPlanCache::instance().store(cache_key, plan,
                                     context.sql_plan_cache_size);
      metrics.plan_us += elapsed_us(plan_start);
      metrics.cache_miss++;
    }

    const auto exec_start = std::chrono::steady_clock::now();
    digest ^= exec_kernel.execute(plan, statement, retry_path);
    metrics.exec_us += elapsed_us(exec_start);
  }

  txn_state.frontend_completed = true;
  txn_state.last_digest = digest;
  publish_metrics(metrics);
  return digest;
}

template <class Context, class StatementBuilder>
inline std::uint64_t run_frontend_once_lazy(const Context &context,
                                           SqlTransactionState &txn_state,
                                           StatementBuilder &&builder) {
  if (!context.enable_sql_emulator || txn_state.frontend_completed) {
    return txn_state.last_digest;
  }
  return run_frontend_once(context, txn_state, builder());
}

} // namespace aria::sql
