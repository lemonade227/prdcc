#pragma once

#include "core/sql/SqlPlanner.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace aria::sql {

struct SqlStatementSpec {
  std::string sql;
  std::vector<std::uint64_t> params;
};

class SqlExecKernel {
public:
  explicit SqlExecKernel(std::size_t expr_complexity)
      : expr_complexity(expr_complexity == 0 ? 1 : expr_complexity) {}

  std::uint64_t execute(const SqlPlan &plan, const SqlStatementSpec &statement,
                        bool retry_bias_path) const {
    std::uint64_t digest = seed(plan, statement, retry_bias_path);
    const std::size_t iterations =
        (plan.expression_cost + plan.predicate_cost + plan.operators.size() + 1) *
        expr_complexity;
    const std::size_t lanes = plan.vector_width == 0 ? 1 : plan.vector_width;

    for (std::size_t lane = 0; lane < lanes; lane++) {
      std::uint64_t lane_value = digest ^ (lane + 0x9e3779b97f4a7c15ULL);
      for (std::size_t i = 0; i < iterations; i++) {
        const auto param = statement.params.empty()
                               ? static_cast<std::uint64_t>(i + 1)
                               : statement.params[i % statement.params.size()];
        lane_value ^= param + 0x9e3779b97f4a7c15ULL + (lane_value << 6) +
                      (lane_value >> 2);
        lane_value = rotate_left(lane_value, (i % 13) + 1);
        lane_value += (plan.plan_hash ^ static_cast<std::uint64_t>(i * 17 + lane));
      }
      digest ^= lane_value + 0x517cc1b727220a95ULL + (digest << 7) +
                (digest >> 3);
    }
    return digest;
  }

private:
  static std::uint64_t rotate_left(std::uint64_t value, std::size_t shift) {
    shift &= 63U;
    if (shift == 0) {
      return value;
    }
    return (value << shift) | (value >> (64 - shift));
  }

  static std::uint64_t seed(const SqlPlan &plan, const SqlStatementSpec &statement,
                            bool retry_bias_path) {
    std::uint64_t digest = plan.plan_hash ^
                           (static_cast<std::uint64_t>(statement.sql.size()) << 11);
    for (std::uint64_t param : statement.params) {
      digest ^= param + 0x94d049bb133111ebULL + (digest << 8) + (digest >> 5);
    }
    if (retry_bias_path) {
      digest ^= 0xd6e8feb86659fd93ULL;
    }
    return digest;
  }

  std::size_t expr_complexity;
};

} // namespace aria::sql
