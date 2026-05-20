#pragma once

#include "core/sql/SqlAst.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace aria::sql {

class SqlParserLite {
public:
  SqlAst parse(const std::string &sql) const {
    SqlAst ast;
    auto tokens = tokenize(sql);
    ast.token_count = tokens.size();
    ast.normalized_sql.reserve(sql.size());

    for (std::size_t i = 0; i < tokens.size(); i++) {
      const auto &token = tokens[i];
      ast.lexical_fingerprint =
          mix_hash(ast.lexical_fingerprint, stable_hash(token.text));

      if (!ast.normalized_sql.empty()) {
        ast.normalized_sql.push_back(' ');
      }
      ast.normalized_sql.append(token.text);

      if (token.kind == SqlToken::Kind::Parameter) {
        ast.placeholder_count++;
      }
    }

    if (tokens.empty()) {
      return ast;
    }

    const auto upper0 = upper(tokens[0].text);
    if (upper0 == "SELECT") {
      ast.kind = StatementKind::Select;
      parse_select(tokens, ast);
    } else if (upper0 == "UPDATE") {
      ast.kind = StatementKind::Update;
      parse_update(tokens, ast);
    } else if (upper0 == "INSERT") {
      ast.kind = StatementKind::Insert;
      parse_insert(tokens, ast);
    }

    return ast;
  }

private:
  static std::vector<SqlToken> tokenize(const std::string &sql) {
    std::vector<SqlToken> tokens;
    std::string current;

    auto flush_identifier = [&]() {
      if (current.empty()) {
        return;
      }
      SqlToken token;
      token.text = current;
      token.kind = classify(current);
      tokens.push_back(std::move(token));
      current.clear();
    };

    for (char ch : sql) {
      if (std::isspace(static_cast<unsigned char>(ch))) {
        flush_identifier();
        continue;
      }

      if (std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' ||
          ch == '.') {
        current.push_back(ch);
        continue;
      }

      flush_identifier();

      SqlToken token;
      token.text.assign(1, ch);
      if (ch == '?') {
        token.kind = SqlToken::Kind::Parameter;
      } else if (ch == '\'' || ch == '"') {
        token.kind = SqlToken::Kind::StringLiteral;
      } else if (ch == '=' || ch == '<' || ch == '>' || ch == '+' ||
                 ch == '-' || ch == '*') {
        token.kind = SqlToken::Kind::Operator;
      } else {
        token.kind = SqlToken::Kind::Punctuation;
      }
      tokens.push_back(std::move(token));
    }
    flush_identifier();
    return tokens;
  }

  static SqlToken::Kind classify(const std::string &token) {
    if (token.empty()) {
      return SqlToken::Kind::Identifier;
    }
    if (std::isdigit(static_cast<unsigned char>(token[0]))) {
      return SqlToken::Kind::Number;
    }

    const auto upper_token = upper(token);
    if (upper_token == "SELECT" || upper_token == "UPDATE" ||
        upper_token == "INSERT" || upper_token == "FROM" ||
        upper_token == "WHERE" || upper_token == "SET" ||
        upper_token == "VALUES" || upper_token == "AND" ||
        upper_token == "ORDER" || upper_token == "BY" ||
        upper_token == "JOIN" || upper_token == "COUNT" ||
        upper_token == "SUM") {
      return SqlToken::Kind::Keyword;
    }
    return SqlToken::Kind::Identifier;
  }

  static std::string upper(const std::string &value) {
    std::string out = value;
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char ch) { return std::toupper(ch); });
    return out;
  }

  static void parse_select(const std::vector<SqlToken> &tokens, SqlAst &ast) {
    bool in_projection = true;
    bool in_predicate = false;

    for (std::size_t i = 1; i < tokens.size(); i++) {
      const auto upper_token = upper(tokens[i].text);
      if (upper_token == "FROM") {
        in_projection = false;
        if (i + 1 < tokens.size()) {
          ast.table = tokens[i + 1].text;
        }
        continue;
      }
      if (upper_token == "WHERE") {
        in_predicate = true;
        continue;
      }
      if (upper_token == "JOIN") {
        ast.join_count++;
        continue;
      }
      if (upper_token == "ORDER") {
        ast.ordering_count++;
        continue;
      }
      if (upper_token == "COUNT" || upper_token == "SUM") {
        ast.aggregate_count++;
      }

      if (in_projection) {
        if (tokens[i].kind == SqlToken::Kind::Identifier) {
          ast.projections.push_back(tokens[i].text);
        }
      } else if (in_predicate &&
                 tokens[i].kind == SqlToken::Kind::Identifier) {
        ast.predicates.push_back(tokens[i].text);
      }
    }
  }

  static void parse_update(const std::vector<SqlToken> &tokens, SqlAst &ast) {
    if (tokens.size() > 1) {
      ast.table = tokens[1].text;
    }

    bool in_set = false;
    bool in_where = false;
    for (std::size_t i = 2; i < tokens.size(); i++) {
      const auto upper_token = upper(tokens[i].text);
      if (upper_token == "SET") {
        in_set = true;
        in_where = false;
        continue;
      }
      if (upper_token == "WHERE") {
        in_where = true;
        in_set = false;
        continue;
      }

      if (in_set && tokens[i].kind == SqlToken::Kind::Identifier) {
        ast.assignments.push_back(tokens[i].text);
      } else if (in_where && tokens[i].kind == SqlToken::Kind::Identifier) {
        ast.predicates.push_back(tokens[i].text);
      }
    }
  }

  static void parse_insert(const std::vector<SqlToken> &tokens, SqlAst &ast) {
    for (std::size_t i = 0; i < tokens.size(); i++) {
      if (upper(tokens[i].text) == "INTO" && i + 1 < tokens.size()) {
        ast.table = tokens[i + 1].text;
      }
      if (tokens[i].kind == SqlToken::Kind::Identifier) {
        ast.assignments.push_back(tokens[i].text);
      }
    }
  }

  static std::uint64_t stable_hash(std::string_view text) {
    std::uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : text) {
      hash ^= ch;
      hash *= 1099511628211ULL;
    }
    return hash;
  }

  static std::uint64_t mix_hash(std::uint64_t lhs, std::uint64_t rhs) {
    lhs ^= rhs + 0x9e3779b97f4a7c15ULL + (lhs << 6) + (lhs >> 2);
    return lhs;
  }
};

} // namespace aria::sql
