#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace aria::sql {

enum class StatementKind { Select, Update, Insert, Unknown };

struct SqlToken {
  enum class Kind {
    Keyword,
    Identifier,
    Number,
    StringLiteral,
    Parameter,
    Operator,
    Punctuation
  };

  Kind kind = Kind::Identifier;
  std::string text;
};

struct SqlAst {
  StatementKind kind = StatementKind::Unknown;
  std::string normalized_sql;
  std::string table;
  std::vector<std::string> projections;
  std::vector<std::string> predicates;
  std::vector<std::string> assignments;
  std::size_t placeholder_count = 0;
  std::size_t token_count = 0;
  std::size_t join_count = 0;
  std::size_t aggregate_count = 0;
  std::size_t ordering_count = 0;
  std::uint64_t lexical_fingerprint = 0;
};

} // namespace aria::sql
