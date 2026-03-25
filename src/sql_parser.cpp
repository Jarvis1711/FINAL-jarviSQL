#include "sql_parser.hpp"

#include <regex>
#include <sstream>

namespace jarvisql {

namespace {

bool parse_column_type(const std::string& token, ColumnType& out) {
  const std::string type = to_upper(trim(token));
  if (type == "INT") {
    out = ColumnType::Int;
    return true;
  }
  if (type == "DECIMAL") {
    out = ColumnType::Decimal;
    return true;
  }
  if (type == "VARCHAR") {
    out = ColumnType::Varchar;
    return true;
  }
  if (type == "DATETIME") {
    out = ColumnType::Datetime;
    return true;
  }
  return false;
}

bool parse_predicate(const std::string& text, Predicate& out) {
  const std::string upper = to_upper(text);
  if (upper.find(" AND ") != std::string::npos || upper.find(" OR ") != std::string::npos) {
    return false;
  }
  static const std::regex pred_re(R"(^\s*([A-Za-z_][\w\.]*)\s*(=|!=|>=|<=|>|<)\s*(.+?)\s*$)", std::regex::icase);
  std::smatch match;
  if (!std::regex_match(text, match, pred_re)) {
    return false;
  }
  out.column = trim(match[1].str());
  out.op = trim(match[2].str());
  out.value = unquote(trim(match[3].str()));
  return true;
}

std::vector<std::string> parse_columns(const std::string& raw) {
  if (trim(raw) == "*") {
    return {"*"};
  }
  auto cols = split_csv_aware(raw);
  for (auto& col : cols) {
    col = trim(col);
  }
  return cols;
}

std::optional<int64_t> parse_expiry(const std::smatch& match) {
  if (match[3].matched) {
    const int64_t seconds = std::stoll(match[3].str());
    return epoch_ms_now() + seconds * 1000;
  }
  if (match[4].matched) {
    const int64_t epoch_seconds = std::stoll(match[4].str());
    return epoch_seconds * 1000;
  }
  return std::nullopt;
}

}  // namespace

bool parse_sql(const std::string& sql_raw, ParsedQuery& parsed, std::string& error) {
  std::string sql = trim(sql_raw);
  if (sql.empty()) {
    error = "Empty SQL command";
    return false;
  }

  if (sql.back() != ';') {
    error = "SQL must end with ';'";
    return false;
  }

  sql.pop_back();
  sql = trim(sql);
  if (sql.empty()) {
    error = "Empty SQL command";
    return false;
  }

  static const std::regex create_re(R"(^CREATE\s+TABLE\s+([A-Za-z_][\w]*)\s*\((.+)\)$)", std::regex::icase);
  static const std::regex create_db_re(R"(^CREATE\s+DATABASE\s+([A-Za-z_][\w]*)$)", std::regex::icase);
  static const std::regex use_db_re(R"(^USE\s+([A-Za-z_][\w]*)$)", std::regex::icase);
  static const std::regex insert_re(
      R"(^INSERT\s+INTO\s+([A-Za-z_][\w]*)\s+VALUES\s*\((.+)\)\s*(?:EXPIRES\s+(?:IN\s+(\d+)|AT\s+'(\d+)'))?$)",
      std::regex::icase);
  static const std::regex join_re(
      R"(^SELECT\s+(.+)\s+FROM\s+([A-Za-z_][\w]*)\s+INNER\s+JOIN\s+([A-Za-z_][\w]*)\s+ON\s+([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)(?:\s+WHERE\s+(.+))?$)",
      std::regex::icase);
  static const std::regex select_re(R"(^SELECT\s+(.+)\s+FROM\s+([A-Za-z_][\w]*)(?:\s+WHERE\s+(.+))?$)", std::regex::icase);

  std::smatch match;
  if (std::regex_match(sql, match, create_db_re)) {
    CreateDatabaseQuery query;
    query.database = match[1].str();
    parsed = ParsedQuery{};
    parsed.kind = ParsedKind::CreateDatabase;
    parsed.create_database = std::move(query);
    return true;
  }

  if (std::regex_match(sql, match, use_db_re)) {
    UseDatabaseQuery query;
    query.database = match[1].str();
    parsed = ParsedQuery{};
    parsed.kind = ParsedKind::UseDatabase;
    parsed.use_database = std::move(query);
    return true;
  }

  if (std::regex_match(sql, match, create_re)) {
    CreateTableQuery query;
    query.table = match[1].str();
    const auto pieces = split_csv_aware(match[2].str());
    if (pieces.empty()) {
      error = "CREATE TABLE requires at least one column";
      return false;
    }
    bool seen_pk = false;
    for (const auto& part : pieces) {
      std::istringstream iss(part);
      ColumnDef col;
      std::string type_token;
      iss >> col.name >> type_token;
      if (col.name.empty() || type_token.empty()) {
        error = "Invalid column definition: " + part;
        return false;
      }
      if (!parse_column_type(type_token, col.type)) {
        error = "Unsupported column type in: " + part;
        return false;
      }
      std::string maybe_primary;
      std::string maybe_key;
      iss >> maybe_primary >> maybe_key;
      if (!maybe_primary.empty()) {
        if (to_upper(maybe_primary) == "PRIMARY" && to_upper(maybe_key) == "KEY") {
          col.primary_key = true;
          if (seen_pk) {
            error = "Only one PRIMARY KEY is supported";
            return false;
          }
          seen_pk = true;
        } else {
          error = "Invalid column modifier in: " + part;
          return false;
        }
      }
      query.columns.push_back(std::move(col));
    }
    parsed = ParsedQuery{};
    parsed.kind = ParsedKind::Create;
    parsed.create = std::move(query);
    return true;
  }

  if (std::regex_match(sql, match, insert_re)) {
    InsertQuery query;
    query.table = match[1].str();
    auto values = split_csv_aware(match[2].str());
    for (auto& value : values) {
      value = unquote(trim(value));
    }
    query.values = std::move(values);
    query.expires_at_epoch_ms = parse_expiry(match);
    parsed = ParsedQuery{};
    parsed.kind = ParsedKind::Insert;
    parsed.insert = std::move(query);
    return true;
  }

  if (std::regex_match(sql, match, join_re)) {
    JoinQuery query;
    query.columns = parse_columns(match[1].str());
    query.left_table = match[2].str();
    query.right_table = match[3].str();
    const std::string left_alias = match[4].str();
    const std::string right_alias = match[6].str();
    if (to_upper(left_alias) != to_upper(query.left_table) || to_upper(right_alias) != to_upper(query.right_table)) {
      error = "JOIN ON clause must reference the declared table names";
      return false;
    }
    query.left_join_column = match[5].str();
    query.right_join_column = match[7].str();
    if (match[8].matched) {
      Predicate predicate;
      if (!parse_predicate(match[8].str(), predicate)) {
        error = "Invalid WHERE clause";
        return false;
      }
      query.where = std::move(predicate);
    }
    parsed = ParsedQuery{};
    parsed.kind = ParsedKind::Join;
    parsed.join = std::move(query);
    return true;
  }

  if (std::regex_match(sql, match, select_re)) {
    SelectQuery query;
    query.columns = parse_columns(match[1].str());
    query.table = match[2].str();
    if (match[3].matched) {
      Predicate predicate;
      if (!parse_predicate(match[3].str(), predicate)) {
        error = "Invalid WHERE clause";
        return false;
      }
      query.where = std::move(predicate);
    }
    parsed = ParsedQuery{};
    parsed.kind = ParsedKind::Select;
    parsed.select = std::move(query);
    return true;
  }

  error = "Unsupported SQL command";
  return false;
}

}  // namespace jarvisql
