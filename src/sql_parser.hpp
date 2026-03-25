#pragma once

#include "common.hpp"

#include <optional>
#include <string>
#include <vector>

namespace jarvisql {

enum class ColumnType {
  Int,
  Decimal,
  Varchar,
  Datetime,
};

struct ColumnDef {
  std::string name;
  ColumnType type;
  bool primary_key = false;
};

struct Predicate {
  std::string column;
  std::string op;
  std::string value;
};

struct SelectQuery {
  std::vector<std::string> columns;
  std::string table;
  std::optional<Predicate> where;
};

struct JoinQuery {
  std::vector<std::string> columns;
  std::string left_table;
  std::string right_table;
  std::string left_join_column;
  std::string right_join_column;
  std::optional<Predicate> where;
};

struct CreateTableQuery {
  std::string table;
  std::vector<ColumnDef> columns;
};

struct InsertQuery {
  std::string table;
  std::vector<std::string> values;
  std::optional<int64_t> expires_at_epoch_ms;
};

struct CreateDatabaseQuery {
  std::string database;
};

struct UseDatabaseQuery {
  std::string database;
};

enum class ParsedKind {
  CreateDatabase,
  UseDatabase,
  Create,
  Insert,
  Select,
  Join,
};

struct ParsedQuery {
  ParsedKind kind;
  CreateDatabaseQuery create_database;
  UseDatabaseQuery use_database;
  CreateTableQuery create;
  InsertQuery insert;
  SelectQuery select;
  JoinQuery join;
};

bool parse_sql(const std::string& sql, ParsedQuery& parsed, std::string& error);

}  // namespace jarvisql
