#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <optional>

namespace jarvisql {

struct ColumnDef;
struct Row;

class Persistence {
 public:
  Persistence(const std::string& data_dir = "jarvisql_data");

  // Save a single table to disk
  bool save_table(const std::string& database, const std::string& table,
                  const std::vector<ColumnDef>& schema,
                  const std::vector<std::pair<std::vector<std::string>, int64_t>>& rows,
                  std::string& error);

  // Load a single table from disk
  bool load_table(const std::string& database, const std::string& table,
                  std::vector<ColumnDef>& schema,
                  std::vector<std::pair<std::vector<std::string>, int64_t>>& rows,
                  int& primary_key_index,
                  std::string& error);

  // Get list of databases on disk
  bool list_databases(std::vector<std::string>& databases, std::string& error);

  // Get list of tables in a database
  bool list_tables(const std::string& database, std::vector<std::string>& tables, std::string& error);

  // Check if a table exists on disk
  bool table_exists(const std::string& database, const std::string& table);

  // Check if a database exists on disk
  bool database_exists(const std::string& database);

 private:
  std::string data_dir_;

  std::string get_database_dir(const std::string& database) const;
  std::string get_table_file(const std::string& database, const std::string& table) const;
  std::string get_schema_file(const std::string& database, const std::string& table) const;
};

}  // namespace jarvisql
