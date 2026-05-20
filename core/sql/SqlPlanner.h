#pragma once

#include "core/sql/SqlAst.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace aria::sql {

enum class PlanOperator {
  Scan,
  IndexLookup,
  PredicateEval,
  Projection,
  Update,
  Insert,
  Materialize
};

struct SqlPlan {
  StatementKind kind = StatementKind::Unknown;
  std::string cache_key;
  std::vector<PlanOperator> operators;
  std::size_t predicate_cost = 0;
  std::size_t expression_cost = 0;
  std::size_t vector_width = 1;
  std::uint64_t plan_hash = 0;
};

class SqlPlanner {
public:
  explicit SqlPlanner(std::size_t optimizer_level)
      : optimizer_level(optimizer_level) {}

  SqlPlan plan(const SqlAst &ast, std::size_t exec_vector_size) const {
    SqlPlan plan;
    plan.kind = ast.kind;
    plan.cache_key = ast.normalized_sql;
    plan.vector_width = exec_vector_size == 0 ? 1 : exec_vector_size;
    plan.predicate_cost =
        ast.predicates.size() * (optimizer_level + 1) + ast.join_count * 2;
    plan.expression_cost = ast.placeholder_count + ast.assignments.size() +
                           ast.projections.size() + ast.aggregate_count * 4;

    switch (ast.kind) {
    case StatementKind::Select:
      plan.operators.push_back(ast.predicates.empty() ? PlanOperator::Scan
                                                      : PlanOperator::IndexLookup);
      if (!ast.predicates.empty()) {
        plan.operators.push_back(PlanOperator::PredicateEval);
      }
      plan.operators.push_back(PlanOperator::Projection);
      plan.operators.push_back(PlanOperator::Materialize);
      break;
    case StatementKind::Update:
      plan.operators.push_back(PlanOperator::IndexLookup);
      plan.operators.push_back(PlanOperator::PredicateEval);
      plan.operators.push_back(PlanOperator::Update);
      plan.operators.push_back(PlanOperator::Materialize);
      break;
    case StatementKind::Insert:
      plan.operators.push_back(PlanOperator::Insert);
      plan.operators.push_back(PlanOperator::Materialize);
      break;
    case StatementKind::Unknown:
      plan.operators.push_back(PlanOperator::Scan);
      plan.operators.push_back(PlanOperator::Materialize);
      break;
    }

    plan.plan_hash = ast.lexical_fingerprint ^
                     (static_cast<std::uint64_t>(plan.expression_cost) << 17) ^
                     (static_cast<std::uint64_t>(plan.predicate_cost) << 9) ^
                     static_cast<std::uint64_t>(plan.operators.size());
    return plan;
  }

private:
  std::size_t optimizer_level;
};

} // namespace aria::sql
