#include "flexql.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

namespace {

int count_rows(void* arg, int, char**, char**) {
  auto* counter = static_cast<size_t*>(arg);
  *counter += 1;
  return 0;
}

bool run_sql(flexql_db* db, const std::string& sql) {
  char* errmsg = nullptr;
  int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errmsg);
  if (rc != FLEXQL_OK) {
    std::cerr << "Failed SQL: " << sql << "\n";
    std::cerr << "  error: " << (errmsg ? errmsg : "unknown") << "\n";
    if (errmsg) {
      flexql_free(errmsg);
    }
    return false;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  std::string host = "127.0.0.1";
  int port = 9000;
  int rows = 100000;
  std::string log_file = "proof/performance_optimization.log";

  if (argc >= 2) host = argv[1];
  if (argc >= 3) port = std::stoi(argv[2]);
  if (argc >= 4) rows = std::stoi(argv[3]);
  if (argc >= 5) log_file = argv[4];

  flexql_db* db = nullptr;
  if (flexql_open(host.c_str(), port, &db) != FLEXQL_OK) {
    std::cerr << "Cannot connect to server\n";
    return 1;
  }

  run_sql(db, "CREATE TABLE bench_users (id INT PRIMARY KEY, score DECIMAL, name VARCHAR);");

  auto insert_start = std::chrono::steady_clock::now();
  for (int i = 1; i <= rows; ++i) {
    std::ostringstream sql;
    sql << "INSERT INTO bench_users VALUES (" << i << ", " << (i * 0.1) << ", 'user" << i << "');";
    if (!run_sql(db, sql.str())) {
      flexql_close(db);
      return 1;
    }
  }
  auto insert_end = std::chrono::steady_clock::now();

  size_t row_count = 0;
  auto select_start = std::chrono::steady_clock::now();
  char* errmsg = nullptr;
  int rc = flexql_exec(db, "SELECT * FROM bench_users WHERE id = 777;", count_rows, &row_count, &errmsg);
  auto select_end = std::chrono::steady_clock::now();

  if (rc != FLEXQL_OK) {
    std::cerr << "Select failed: " << (errmsg ? errmsg : "unknown") << '\n';
    if (errmsg) {
      flexql_free(errmsg);
    }
    flexql_close(db);
    return 1;
  }

  using ms = std::chrono::milliseconds;
  using us = std::chrono::microseconds;

  size_t name_row_count = 0;
  const std::string name_query = "SELECT * FROM bench_users WHERE name = 'user777';";
  auto name_select_start = std::chrono::steady_clock::now();
  rc = flexql_exec(db, name_query.c_str(), count_rows, &name_row_count, &errmsg);
  auto name_select_end = std::chrono::steady_clock::now();

  if (rc != FLEXQL_OK) {
    std::cerr << "Indexed name select failed: " << (errmsg ? errmsg : "unknown") << '\n';
    if (errmsg) {
      flexql_free(errmsg);
    }
    flexql_close(db);
    return 1;
  }

  size_t cached_name_row_count = 0;
  auto cached_name_select_start = std::chrono::steady_clock::now();
  rc = flexql_exec(db, name_query.c_str(), count_rows, &cached_name_row_count, &errmsg);
  auto cached_name_select_end = std::chrono::steady_clock::now();

  if (rc != FLEXQL_OK) {
    std::cerr << "Cached name select failed: " << (errmsg ? errmsg : "unknown") << '\n';
    if (errmsg) {
      flexql_free(errmsg);
    }
    flexql_close(db);
    return 1;
  }

  std::cout << "jarvisql benchmark\n";
  std::cout << "rows_inserted=" << rows << "\n";
  std::cout << "insert_ms=" << std::chrono::duration_cast<ms>(insert_end - insert_start).count() << "\n";
  std::cout << "select_ms=" << std::chrono::duration_cast<ms>(select_end - select_start).count() << "\n";
  std::cout << "indexed_name_select_us=" << std::chrono::duration_cast<us>(name_select_end - name_select_start).count() << "\n";
  std::cout << "cached_name_select_us=" << std::chrono::duration_cast<us>(cached_name_select_end - cached_name_select_start).count() << "\n";
  std::cout << "selected_rows=" << row_count << "\n";
  std::cout << "indexed_name_selected_rows=" << name_row_count << "\n";
  std::cout << "cached_name_selected_rows=" << cached_name_row_count << "\n";

  std::ofstream log_stream(log_file, std::ios::app);
  if (log_stream) {
    log_stream << "[benchmark] rows=" << rows
               << " insert_ms=" << std::chrono::duration_cast<ms>(insert_end - insert_start).count()
               << " pk_select_ms=" << std::chrono::duration_cast<ms>(select_end - select_start).count()
               << " indexed_name_select_us=" << std::chrono::duration_cast<us>(name_select_end - name_select_start).count()
               << " cached_name_select_us=" << std::chrono::duration_cast<us>(cached_name_select_end - cached_name_select_start).count()
               << " selected_rows=" << row_count
               << " indexed_name_selected_rows=" << name_row_count
               << " cached_name_selected_rows=" << cached_name_row_count
               << "\n";
  }

  flexql_close(db);
  return 0;
}
