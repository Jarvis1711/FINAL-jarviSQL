#include "engine.hpp"

#include "common.hpp"

#include <algorithm>

namespace jarvisql {

Engine::Engine() : persistence_(std::make_unique<Persistence>("jarvisql_data")) {
  load_databases_from_disk();
}

bool Engine::execute(const std::string& sql, QueryResult& result, std::string& error) {
  result = QueryResult{};
  ParsedQuery parsed;
  if (!parse_sql(sql, parsed, error)) {
    return false;
  }

  {
    std::unique_lock<std::shared_mutex> lock(db_mutex_);
    databases_.try_emplace("default");
    if (active_database_.empty()) {
      active_database_ = "default";
    }
  }

  const std::string normalized_sql = trim(sql);
  const auto current_database = [&]() {
    std::shared_lock<std::shared_mutex> lock(db_mutex_);
    return active_database_;
  };

  switch (parsed.kind) {
    case ParsedKind::CreateDatabase:
      return create_database(parsed.create_database, error);
    case ParsedKind::UseDatabase:
      return use_database(parsed.use_database, error);
    case ParsedKind::Create:
      return create_table(current_database(), parsed.create, error);
    case ParsedKind::Insert:
      return insert_row(current_database(), parsed.insert, error);
    case ParsedKind::Select: {
      const std::string db_name = current_database();
      const std::string cache_key = db_name + "|" + normalized_sql;
      if (cache_get(cache_key, result)) {
        result.from_cache = true;
        return true;
      }
      if (!select_rows(db_name, parsed.select, result, error)) {
        return false;
      }
      cache_put(cache_key, result);
      return true;
    }
    case ParsedKind::Join: {
      const std::string db_name = current_database();
      const std::string cache_key = db_name + "|" + normalized_sql;
      if (cache_get(cache_key, result)) {
        result.from_cache = true;
        return true;
      }
      if (!select_join(db_name, parsed.join, result, error)) {
        return false;
      }
      cache_put(cache_key, result);
      return true;
    }
  }

  error = "Unknown query type";
  return false;
}

bool Engine::create_database(const CreateDatabaseQuery& query, std::string& error) {
  std::unique_lock<std::shared_mutex> lock(db_mutex_);
  auto [it, inserted] = databases_.try_emplace(query.database);
  if (!inserted) {
    error = "Database already exists: " + query.database;
    return false;
  }
  cache_invalidate_all();
  return true;
}

bool Engine::use_database(const UseDatabaseQuery& query, std::string& error) {
  std::unique_lock<std::shared_mutex> lock(db_mutex_);
  auto it = databases_.find(query.database);
  if (it == databases_.end()) {
    error = "Unknown database: " + query.database;
    return false;
  }
  active_database_ = query.database;
  cache_invalidate_all();
  return true;
}

bool Engine::create_table(const std::string& database, const CreateTableQuery& query, std::string& error) {
  std::unique_lock<std::shared_mutex> lock(db_mutex_);
  auto& tables = databases_[database];
  if (tables.find(query.table) != tables.end()) {
    error = "Table already exists: " + query.table;
    return false;
  }
  auto table = std::make_shared<Table>();
  table->schema = query.columns;
  table->equality_indexes.resize(query.columns.size());
  for (size_t i = 0; i < query.columns.size(); ++i) {
    table->column_lookup[to_upper(query.columns[i].name)] = static_cast<int>(i);
    if (query.columns[i].primary_key && table->primary_key_index < 0) {
      table->primary_key_index = static_cast<int>(i);
    }
  }
  tables.emplace(query.table, std::move(table));
  cache_invalidate_all();
  
  // Save table to disk
  {
    std::shared_lock<std::shared_mutex> table_lock(tables[query.table]->mutex);
    save_table_to_disk(database, query.table, *tables[query.table]);
  }
  return true;
}

bool Engine::insert_row(const std::string& database, const InsertQuery& query, std::string& error) {
  std::shared_lock<std::shared_mutex> db_lock(db_mutex_);
  auto db_it = databases_.find(database);
  if (db_it == databases_.end()) {
    error = "Unknown database: " + database;
    return false;
  }
  auto it = db_it->second.find(query.table);
  if (it == db_it->second.end()) {
    error = "Unknown table: " + query.table;
    return false;
  }
  Table& table = *it->second;

  std::unique_lock<std::shared_mutex> table_lock(table.mutex);
  if (query.values.size() != table.schema.size()) {
    error = "INSERT value count does not match schema";
    return false;
  }

  if (table.primary_key_index >= 0) {
    const std::string& pk = query.values[static_cast<size_t>(table.primary_key_index)];
    if (table.primary_index.find(pk) != table.primary_index.end()) {
      error = "Duplicate PRIMARY KEY value";
      return false;
    }
  }

  Row row;
  row.values = query.values;
  row.expires_at_epoch_ms = query.expires_at_epoch_ms.value_or(-1);
  const size_t row_index = table.rows.size();
  table.rows.push_back(std::move(row));

  if (table.primary_key_index >= 0) {
    const std::string& pk = table.rows.back().values[static_cast<size_t>(table.primary_key_index)];
    table.primary_index[pk] = row_index;
  }

  for (size_t i = 0; i < table.rows.back().values.size(); ++i) {
    table.equality_indexes[i][table.rows.back().values[i]].push_back(row_index);
  }
  cache_invalidate_all();
  
  // Save table to disk
  const std::string curr_db = database;  // Capture for use after lock release
  const std::string curr_table = query.table;
  table_lock.unlock();
  
  {
    std::shared_lock<std::shared_mutex> read_lock(table.mutex);
    save_table_to_disk(curr_db, curr_table, table);
  }
  return true;
}

bool Engine::select_rows(const std::string& database, const SelectQuery& query, QueryResult& result, std::string& error) {
  std::shared_lock<std::shared_mutex> db_lock(db_mutex_);
  auto db_it = databases_.find(database);
  if (db_it == databases_.end()) {
    error = "Unknown database: " + database;
    return false;
  }
  auto it = db_it->second.find(query.table);
  if (it == db_it->second.end()) {
    error = "Unknown table: " + query.table;
    return false;
  }
  const Table& table = *it->second;
  std::shared_lock<std::shared_mutex> table_lock(table.mutex);

  std::vector<int> projection;
  if (query.columns.size() == 1 && query.columns.front() == "*") {
    result.columns.clear();
    for (size_t i = 0; i < table.schema.size(); ++i) {
      projection.push_back(static_cast<int>(i));
      result.columns.push_back(table.schema[i].name);
    }
  } else {
    for (const auto& col : query.columns) {
      const int idx = column_index(table, col);
      if (idx < 0) {
        error = "Unknown column in SELECT: " + col;
        return false;
      }
      projection.push_back(idx);
      result.columns.push_back(table.schema[static_cast<size_t>(idx)].name);
    }
  }

  int where_idx = -1;
  if (query.where.has_value()) {
    where_idx = column_index(table, query.where->column);
    if (where_idx < 0) {
      error = "Unknown column in WHERE: " + query.where->column;
      return false;
    }
    if (table.primary_key_index == where_idx && query.where->op == "=") {
      auto pk_it = table.primary_index.find(query.where->value);
      if (pk_it != table.primary_index.end()) {
        const Row& row = table.rows[pk_it->second];
        if (!is_expired(row) && row_matches(query.where, table, row)) {
          std::vector<std::string> out_row;
          out_row.reserve(projection.size());
          for (int idx : projection) {
            out_row.push_back(row.values[static_cast<size_t>(idx)]);
          }
          result.rows.push_back(std::move(out_row));
        }
      }
      return true;
    }

    if (query.where->op == "=") {
      const auto& eq_index = table.equality_indexes[static_cast<size_t>(where_idx)];
      auto bucket = eq_index.find(query.where->value);
      if (bucket == eq_index.end()) {
        return true;
      }
      for (size_t row_idx : bucket->second) {
        const Row& row = table.rows[row_idx];
        if (is_expired(row)) {
          continue;
        }
        std::vector<std::string> out_row;
        out_row.reserve(projection.size());
        for (int idx : projection) {
          out_row.push_back(row.values[static_cast<size_t>(idx)]);
        }
        result.rows.push_back(std::move(out_row));
      }
      return true;
    }
  }

  for (const Row& row : table.rows) {
    if (is_expired(row)) {
      continue;
    }
    if (query.where.has_value()) {
      if (!check_predicate(row.values[static_cast<size_t>(where_idx)], query.where->op, query.where->value)) {
        continue;
      }
    }
    std::vector<std::string> out_row;
    out_row.reserve(projection.size());
    for (int idx : projection) {
      out_row.push_back(row.values[static_cast<size_t>(idx)]);
    }
    result.rows.push_back(std::move(out_row));
  }
  return true;
}

bool Engine::select_join(const std::string& database, const JoinQuery& query, QueryResult& result, std::string& error) {
  std::shared_lock<std::shared_mutex> db_lock(db_mutex_);
  auto db_it = databases_.find(database);
  if (db_it == databases_.end()) {
    error = "Unknown database: " + database;
    return false;
  }
  auto left_it = db_it->second.find(query.left_table);
  auto right_it = db_it->second.find(query.right_table);
  if (left_it == db_it->second.end() || right_it == db_it->second.end()) {
    error = "Unknown table in JOIN";
    return false;
  }

  const Table& right = *right_it->second;
  const Table& left_ref = *left_it->second;
  std::shared_lock<std::shared_mutex> left_lock(left_ref.mutex);
  std::shared_lock<std::shared_mutex> right_lock(right.mutex);

  const int left_join_idx = column_index(left_ref, query.left_join_column);
  const int right_join_idx = column_index(right, query.right_join_column);
  if (left_join_idx < 0 || right_join_idx < 0) {
    error = "Unknown join columns";
    return false;
  }

  struct Projection {
    bool from_left;
    int idx;
    std::string name;
  };
  std::vector<Projection> projection;

  auto add_projection_col = [&](const std::string& col) -> bool {
    std::string table_name;
    std::string column_name;
    const size_t dot = col.find('.');
    if (dot == std::string::npos) {
      if (int idx = column_index(left_ref, col); idx >= 0) {
        projection.push_back({true, idx, left_ref.schema[static_cast<size_t>(idx)].name});
        return true;
      }
      if (int idx = column_index(right, col); idx >= 0) {
        projection.push_back({false, idx, right.schema[static_cast<size_t>(idx)].name});
        return true;
      }
      return false;
    }
    table_name = col.substr(0, dot);
    column_name = col.substr(dot + 1);

    if (to_upper(table_name) == to_upper(query.left_table)) {
      const int idx = column_index(left_ref, column_name);
      if (idx < 0) return false;
      projection.push_back({true, idx, query.left_table + "." + left_ref.schema[static_cast<size_t>(idx)].name});
      return true;
    }
    if (to_upper(table_name) == to_upper(query.right_table)) {
      const int idx = column_index(right, column_name);
      if (idx < 0) return false;
      projection.push_back({false, idx, query.right_table + "." + right.schema[static_cast<size_t>(idx)].name});
      return true;
    }
    return false;
  };

  if (query.columns.size() == 1 && query.columns.front() == "*") {
    for (size_t i = 0; i < left_ref.schema.size(); ++i) {
      projection.push_back({true, static_cast<int>(i), query.left_table + "." + left_ref.schema[i].name});
    }
    for (size_t i = 0; i < right.schema.size(); ++i) {
      projection.push_back({false, static_cast<int>(i), query.right_table + "." + right.schema[i].name});
    }
  } else {
    for (const auto& col : query.columns) {
      if (!add_projection_col(col)) {
        error = "Unknown projection column in JOIN: " + col;
        return false;
      }
    }
  }

  for (const auto& p : projection) {
    result.columns.push_back(p.name);
  }

  std::unordered_map<std::string, std::vector<const Row*>> right_map;
  right_map.reserve(right.rows.size());
  for (const Row& row : right.rows) {
    if (is_expired(row)) {
      continue;
    }
    right_map[row.values[static_cast<size_t>(right_join_idx)]].push_back(&row);
  }

  for (const Row& left_row : left_ref.rows) {
    if (is_expired(left_row)) {
      continue;
    }
    auto bucket = right_map.find(left_row.values[static_cast<size_t>(left_join_idx)]);
    if (bucket == right_map.end()) {
      continue;
    }
    for (const Row* right_row : bucket->second) {
      if (query.where.has_value()) {
        const std::string& expr_col = query.where->column;
        std::string comp;
        auto extract = [&](const std::string& maybe_qualified) -> std::optional<std::string> {
          const size_t dot = maybe_qualified.find('.');
          if (dot == std::string::npos) {
            int li = column_index(left_ref, maybe_qualified);
            if (li >= 0) return left_row.values[static_cast<size_t>(li)];
            int ri = column_index(right, maybe_qualified);
            if (ri >= 0) return right_row->values[static_cast<size_t>(ri)];
            return std::nullopt;
          }
          const std::string tn = maybe_qualified.substr(0, dot);
          const std::string cn = maybe_qualified.substr(dot + 1);
          if (to_upper(tn) == to_upper(query.left_table)) {
            int li = column_index(left_ref, cn);
            if (li >= 0) return left_row.values[static_cast<size_t>(li)];
            return std::nullopt;
          }
          if (to_upper(tn) == to_upper(query.right_table)) {
            int ri = column_index(right, cn);
            if (ri >= 0) return right_row->values[static_cast<size_t>(ri)];
            return std::nullopt;
          }
          return std::nullopt;
        };

        auto found = extract(expr_col);
        if (!found.has_value()) {
          error = "Unknown WHERE column in JOIN: " + expr_col;
          return false;
        }
        comp = *found;
        if (!check_predicate(comp, query.where->op, query.where->value)) {
          continue;
        }
      }

      std::vector<std::string> out;
      out.reserve(projection.size());
      for (const auto& p : projection) {
        if (p.from_left) {
          out.push_back(left_row.values[static_cast<size_t>(p.idx)]);
        } else {
          out.push_back(right_row->values[static_cast<size_t>(p.idx)]);
        }
      }
      result.rows.push_back(std::move(out));
    }
  }

  return true;
}

bool Engine::is_expired(const Row& row) {
  if (row.expires_at_epoch_ms < 0) {
    return false;
  }
  return row.expires_at_epoch_ms <= epoch_ms_now();
}

int Engine::column_index(const Table& table, const std::string& column_name) {
  auto it = table.column_lookup.find(to_upper(column_name));
  if (it == table.column_lookup.end()) {
    return -1;
  }
  return it->second;
}

bool Engine::row_matches(const std::optional<Predicate>& where, const Table& table, const Row& row) {
  if (!where.has_value()) {
    return true;
  }
  const int idx = column_index(table, where->column);
  if (idx < 0) {
    return false;
  }
  return check_predicate(row.values[static_cast<size_t>(idx)], where->op, where->value);
}

void Engine::cache_invalidate_all() {
  std::lock_guard<std::mutex> lock(cache_mutex_);
  cache_.clear();
  lru_order_.clear();
}

bool Engine::cache_get(const std::string& sql_key, QueryResult& result) {
  std::lock_guard<std::mutex> lock(cache_mutex_);
  auto it = cache_.find(sql_key);
  if (it == cache_.end()) {
    return false;
  }
  lru_order_.erase(it->second.lru_it);
  lru_order_.push_front(sql_key);
  it->second.lru_it = lru_order_.begin();
  result = it->second.result;
  return true;
}

void Engine::cache_put(const std::string& sql_key, const QueryResult& result) {
  std::lock_guard<std::mutex> lock(cache_mutex_);
  auto existing = cache_.find(sql_key);
  if (existing != cache_.end()) {
    lru_order_.erase(existing->second.lru_it);
    cache_.erase(existing);
  }

  lru_order_.push_front(sql_key);
  cache_[sql_key] = CacheEntry{result, lru_order_.begin()};

  if (cache_.size() > kCacheCapacity) {
    const std::string& old_key = lru_order_.back();
    cache_.erase(old_key);
    lru_order_.pop_back();
  }
}

void Engine::load_databases_from_disk() {
  std::unique_lock<std::shared_mutex> lock(db_mutex_);

  std::vector<std::string> db_list;
  std::string error;
  
  // Load databases
  if (!persistence_->list_databases(db_list, error)) {
    return;
  }

  for (const auto& db_name : db_list) {
    databases_.try_emplace(db_name);
    auto& tables = databases_[db_name];

    std::vector<std::string> table_list;
    if (!persistence_->list_tables(db_name, table_list, error)) {
      continue;
    }

    for (const auto& table_name : table_list) {
      auto table = std::make_shared<Table>();
      std::vector<std::pair<std::vector<std::string>, int64_t>> rows;
      int pk_idx = -1;

      if (persistence_->load_table(db_name, table_name, table->schema, rows, pk_idx, error)) {
        table->primary_key_index = pk_idx;
        table->equality_indexes.resize(table->schema.size());

        // Rebuild column lookup and indexes
        for (size_t i = 0; i < table->schema.size(); ++i) {
          table->column_lookup[to_upper(table->schema[i].name)] = static_cast<int>(i);
        }

        // Load rows
        for (const auto& [row_values, expires_at_ms] : rows) {
          Row row;
          row.values = row_values;
          row.expires_at_epoch_ms = expires_at_ms;
          const size_t row_index = table->rows.size();
          table->rows.push_back(std::move(row));

          // Rebuild indexes
          if (pk_idx >= 0) {
            const std::string& pk = table->rows.back().values[static_cast<size_t>(pk_idx)];
            table->primary_index[pk] = row_index;
          }

          for (size_t i = 0; i < table->rows.back().values.size(); ++i) {
            table->equality_indexes[i][table->rows.back().values[i]].push_back(row_index);
          }
        }

        tables[table_name] = std::move(table);
      }
    }
  }
}

void Engine::save_table_to_disk(const std::string& database, const std::string& table_name, const Table& table_obj) {
  std::vector<std::pair<std::vector<std::string>, int64_t>> rows_to_save;
  
  for (const auto& row : table_obj.rows) {
    rows_to_save.push_back({row.values, row.expires_at_epoch_ms});
  }

  std::string error;
  persistence_->save_table(database, table_name, table_obj.schema, rows_to_save, error);
}

}  // namespace jarvisql
