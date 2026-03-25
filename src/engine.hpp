#pragma once

#include "sql_parser.hpp"
#include "persistence.hpp"

#include <mutex>
#include <memory>
#include <shared_mutex>
#include <string>
#include <list>
#include <unordered_map>
#include <vector>

namespace jarvisql {

struct QueryResult {
  std::vector<std::string> columns;
  std::vector<std::vector<std::string>> rows;
  bool from_cache = false;
};

class Engine {
 public:
  Engine();
  bool execute(const std::string& sql, QueryResult& result, std::string& error);

 private:
  struct Row {
    std::vector<std::string> values;
    int64_t expires_at_epoch_ms = -1;
  };

  struct Table {
    std::vector<ColumnDef> schema;
    std::unordered_map<std::string, int> column_lookup;
    std::vector<Row> rows;
    int primary_key_index = -1;
    std::unordered_map<std::string, size_t> primary_index;
    std::vector<std::unordered_map<std::string, std::vector<size_t>>> equality_indexes;
    mutable std::shared_mutex mutex;
  };

  struct CacheEntry {
    QueryResult result;
    std::list<std::string>::iterator lru_it;
  };

  using TableMap = std::unordered_map<std::string, std::shared_ptr<Table>>;

  static constexpr size_t kCacheCapacity = 512;

  mutable std::shared_mutex db_mutex_;
  std::unordered_map<std::string, TableMap> databases_;
  std::string active_database_ = "default";
  std::unique_ptr<Persistence> persistence_;

  std::mutex cache_mutex_;
  std::list<std::string> lru_order_;
  std::unordered_map<std::string, CacheEntry> cache_;

  bool create_database(const CreateDatabaseQuery& query, std::string& error);
  bool use_database(const UseDatabaseQuery& query, std::string& error);
  bool create_table(const std::string& database, const CreateTableQuery& query, std::string& error);
  bool insert_row(const std::string& database, const InsertQuery& query, std::string& error);
  bool select_rows(const std::string& database, const SelectQuery& query, QueryResult& result, std::string& error);
  bool select_join(const std::string& database, const JoinQuery& query, QueryResult& result, std::string& error);

  static bool is_expired(const Row& row);

  static int column_index(const Table& table, const std::string& column_name);
  static bool row_matches(const std::optional<Predicate>& where, const Table& table, const Row& row);

  void cache_invalidate_all();
  bool cache_get(const std::string& sql_key, QueryResult& result);

  // Persistence helpers
  void load_databases_from_disk();
  void save_table_to_disk(const std::string& database, const std::string& table, const Table& table_obj);
  void cache_put(const std::string& sql_key, const QueryResult& result);
};

}  // namespace jarvisql
