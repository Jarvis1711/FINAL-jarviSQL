#include "flexql.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>

using namespace std;
using namespace std::chrono;

using FlexQL = flexql_db;

static const long long DEFAULT_INSERT_ROWS = 1000000LL;

struct QueryStats {
  long long rows = 0;
};

static int count_rows_callback(void* data, int argc, char** argv, char** azColName) {
  (void)argc;
  (void)argv;
  (void)azColName;
  auto* stats = static_cast<QueryStats*>(data);
  if (stats) {
    stats->rows++;
  }
  return 0;
}

static bool run_exec(FlexQL* db, const string& sql, const string& label) {
  char* errMsg = nullptr;
  auto start = high_resolution_clock::now();
  int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
  auto end = high_resolution_clock::now();
  long long elapsed = duration_cast<milliseconds>(end - start).count();

  if (rc != FLEXQL_OK) {
    cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
    if (errMsg) {
      flexql_free(errMsg);
    }
    return false;
  }

  cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
  return true;
}

static bool run_query(FlexQL* db, const string& sql, const string& label) {
  QueryStats stats;
  char* errMsg = nullptr;
  auto start = high_resolution_clock::now();
  int rc = flexql_exec(db, sql.c_str(), count_rows_callback, &stats, &errMsg);
  auto end = high_resolution_clock::now();
  long long elapsed = duration_cast<milliseconds>(end - start).count();

  if (rc != FLEXQL_OK) {
    cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
    if (errMsg) {
      flexql_free(errMsg);
    }
    return false;
  }

  cout << "[PASS] " << label << " | rows=" << stats.rows << " | " << elapsed << " ms\n";
  return true;
}

static bool query_rows(FlexQL* db, const string& sql, vector<string>& out_rows) {
  struct Collector {
    vector<string> rows;
  } collector;

  auto cb = [](void* data, int argc, char** argv, char** azColName) -> int {
    (void)azColName;
    auto* c = static_cast<Collector*>(data);
    string row;
    for (int i = 0; i < argc; ++i) {
      if (i > 0) {
        row += " ";
      }
      row += (argv[i] ? argv[i] : "NULL");
    }
    c->rows.push_back(row);
    return 0;
  };

  char* errMsg = nullptr;
  int rc = flexql_exec(db, sql.c_str(), cb, &collector, &errMsg);
  if (rc != FLEXQL_OK) {
    cout << "[FAIL] " << sql << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
    if (errMsg) {
      flexql_free(errMsg);
    }
    return false;
  }

  out_rows = collector.rows;
  return true;
}

static bool assert_rows_equal(const string& label, vector<string> actual, vector<string> expected) {
  sort(actual.begin(), actual.end());
  sort(expected.begin(), expected.end());
  if (actual == expected) {
    cout << "[PASS] " << label << "\n";
    return true;
  }

  cout << "[FAIL] " << label << "\n";
  cout << "Expected (" << expected.size() << "):\n";
  for (const auto& row : expected) {
    cout << "  " << row << "\n";
  }
  cout << "Actual (" << actual.size() << "):\n";
  for (const auto& row : actual) {
    cout << "  " << row << "\n";
  }
  return false;
}

static bool assert_row_count(const string& label, const vector<string>& rows, size_t expected_count) {
  if (rows.size() == expected_count) {
    cout << "[PASS] " << label << "\n";
    return true;
  }

  cout << "[FAIL] " << label << " (expected " << expected_count << ", got " << rows.size() << ")\n";
  return false;
}

static bool expect_query_failure(FlexQL* db, const string& sql, const string& label) {
  char* errMsg = nullptr;
  int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
  if (rc == FLEXQL_OK) {
    cout << "[FAIL] " << label << " (expected failure, got success)\n";
    return false;
  }
  if (errMsg) {
    flexql_free(errMsg);
  }
  cout << "[PASS] " << label << "\n";
  return true;
}

static bool run_data_level_unit_tests(FlexQL* db, const string& db_name) {
  cout << "\n\n[[ Running Unit Tests ]]\n\n";

  bool all_ok = true;
  int total_tests = 0;
  int failed_tests = 0;

  auto record = [&](bool result) {
    total_tests++;
    if (!result) {
      all_ok = false;
      failed_tests++;
    }
  };

  record(run_exec(db, "USE " + db_name + ";", "USE benchmark DB"));

  record(run_exec(
      db,
      "CREATE TABLE TEST_USERS(ID INT PRIMARY KEY, NAME VARCHAR, BALANCE DECIMAL, EXPIRES_AT INT);",
      "CREATE TABLE TEST_USERS"));

  record(run_exec(db, "INSERT INTO TEST_USERS VALUES (1, 'Alice', 1200, 1893456000);", "INSERT user 1"));
  record(run_exec(db, "INSERT INTO TEST_USERS VALUES (2, 'Bob', 450, 1893456000);", "INSERT user 2"));
  record(run_exec(db, "INSERT INTO TEST_USERS VALUES (3, 'Carol', 2200, 1893456000);", "INSERT user 3"));
  record(run_exec(db, "INSERT INTO TEST_USERS VALUES (4, 'Dave', 800, 1893456000);", "INSERT user 4"));

  vector<string> rows;

  bool q1 = query_rows(db, "SELECT NAME, BALANCE FROM TEST_USERS WHERE ID = 2;", rows);
  record(q1);
  if (q1) {
    record(assert_rows_equal("Single-row value validation", rows, {"Bob 450"}));
  }

  bool q2 = query_rows(db, "SELECT NAME FROM TEST_USERS WHERE BALANCE > 1000;", rows);
  record(q2);
  if (q2) {
    record(assert_rows_equal("Filtered rows validation", rows, {"Alice", "Carol"}));
  }

  bool q3 = query_rows(db, "SELECT NAME FROM TEST_USERS WHERE BALANCE <= 2200;", rows);
  record(q3);
  if (q3) {
    record(assert_rows_equal("<= operator validation", rows, {"Alice", "Bob", "Carol", "Dave"}));
  }

  bool q4 = query_rows(db, "SELECT ID FROM TEST_USERS WHERE BALANCE > 5000;", rows);
  record(q4);
  if (q4) {
    record(assert_row_count("Empty result-set validation", rows, 0));
  }

  record(run_exec(
      db,
      "CREATE TABLE TEST_ORDERS(ORDER_ID INT PRIMARY KEY, USER_ID INT, AMOUNT DECIMAL, EXPIRES_AT INT);",
      "CREATE TABLE TEST_ORDERS"));

  record(run_exec(db, "INSERT INTO TEST_ORDERS VALUES (101, 1, 50, 1893456000);", "INSERT order 101"));
  record(run_exec(db, "INSERT INTO TEST_ORDERS VALUES (102, 1, 150, 1893456000);", "INSERT order 102"));
  record(run_exec(db, "INSERT INTO TEST_ORDERS VALUES (103, 3, 500, 1893456000);", "INSERT order 103"));

  bool q5 = query_rows(
      db,
      "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
      "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
      "WHERE TEST_ORDERS.AMOUNT >= 100;",
      rows);
  record(q5);
  if (q5) {
    record(assert_rows_equal("Join result validation", rows, {"Alice 150", "Carol 500"}));
  }

  bool q6 = query_rows(db, "SELECT ORDER_ID FROM TEST_ORDERS WHERE USER_ID = 1;", rows);
  record(q6);
  if (q6) {
    record(assert_rows_equal("Single-condition equality WHERE validation", rows, {"101", "102"}));
  }

  bool q7 = query_rows(
      db,
      "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
      "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
      "WHERE TEST_ORDERS.AMOUNT > 900;",
      rows);
  record(q7);
  if (q7) {
    record(assert_row_count("Join with no matches validation", rows, 0));
  }

  record(expect_query_failure(db, "SELECT UNKNOWN_COLUMN FROM TEST_USERS;", "Invalid SQL should fail"));
  record(expect_query_failure(db, "SELECT * FROM MISSING_TABLE;", "Missing table should fail"));

  int passed_tests = total_tests - failed_tests;
  cout << "\nUnit Test Summary: " << passed_tests << "/" << total_tests << " passed, "
       << failed_tests << " failed.\n\n";

  return all_ok;
}

static bool run_insert_benchmark(FlexQL* db, const string& db_name, long long target_rows) {
  if (!run_exec(db, "USE " + db_name + ";", "USE benchmark DB")) {
    return false;
  }

  if (!run_exec(
          db,
          "CREATE TABLE BIG_USERS(ID INT PRIMARY KEY, NAME VARCHAR, EMAIL VARCHAR, BALANCE DECIMAL, EXPIRES_AT INT);",
          "CREATE TABLE BIG_USERS")) {
    return false;
  }

  cout << "\nStarting insertion benchmark for " << target_rows << " rows...\n";
  auto bench_start = high_resolution_clock::now();

  long long progress_step = target_rows / 10;
  if (progress_step <= 0) {
    progress_step = 1;
  }
  long long next_progress = progress_step;

  for (long long inserted = 1; inserted <= target_rows; ++inserted) {
    stringstream ss;
    ss << "INSERT INTO BIG_USERS VALUES ("
       << inserted
       << ", 'user" << inserted << "'"
       << ", 'user" << inserted << "@mail.com'"
       << ", " << (1000.0 + (inserted % 10000))
       << ", 1893456000);";

    char* errMsg = nullptr;
    if (flexql_exec(db, ss.str().c_str(), nullptr, nullptr, &errMsg) != FLEXQL_OK) {
      cout << "[FAIL] INSERT BIG_USERS row " << inserted << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
      if (errMsg) {
        flexql_free(errMsg);
      }
      return false;
    }

    if (inserted >= next_progress || inserted == target_rows) {
      cout << "Progress: " << inserted << "/" << target_rows << "\n";
      next_progress += progress_step;
    }
  }

  auto bench_end = high_resolution_clock::now();
  long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
  long long throughput = (elapsed > 0) ? (target_rows * 1000LL / elapsed) : target_rows;

  cout << "[PASS] INSERT benchmark complete\n";
  cout << "Rows inserted: " << target_rows << "\n";
  cout << "Elapsed: " << elapsed << " ms\n";
  cout << "Throughput: " << throughput << " rows/sec\n";

  run_query(db, "SELECT * FROM BIG_USERS WHERE ID = 777;", "PK lookup");
  run_query(db, "SELECT * FROM BIG_USERS WHERE BALANCE >= 5000;", ">= filter lookup");

  return true;
}

int main(int argc, char** argv) {
  FlexQL* db = nullptr;
  long long insert_rows = DEFAULT_INSERT_ROWS;
  bool run_unit_tests_only = false;

  if (argc > 1) {
    string arg1 = argv[1];
    if (arg1 == "--unit-test") {
      run_unit_tests_only = true;
    } else {
      insert_rows = atoll(argv[1]);
      if (insert_rows <= 0) {
        cout << "Invalid row count. Use a positive integer or --unit-test.\n";
        return 1;
      }
    }
  }

  if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
    cout << "Cannot open FlexQL\n";
    return 1;
  }

  cout << "Connected to FlexQL\n";

  const auto now_ms = static_cast<long long>(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
  const string bench_db = "benchdb_" + to_string(now_ms) + "_" + to_string(static_cast<long long>(getpid()));

  if (!run_exec(db, "CREATE DATABASE " + bench_db + ";", "CREATE benchmark DB")) {
    flexql_close(db);
    return 1;
  }

  if (!run_exec(db, "USE " + bench_db + ";", "USE benchmark DB")) {
    flexql_close(db);
    return 1;
  }

  if (run_unit_tests_only) {
    bool ok = run_data_level_unit_tests(db, bench_db);
    flexql_close(db);
    return ok ? 0 : 1;
  }

  cout << "Running SQL subset checks plus insertion benchmark...\n";
  cout << "Target insert rows: " << insert_rows << "\n\n";

  if (!run_insert_benchmark(db, bench_db, insert_rows)) {
    flexql_close(db);
    return 1;
  }

  if (!run_data_level_unit_tests(db, bench_db)) {
    flexql_close(db);
    return 1;
  }

  flexql_close(db);
  return 0;
}
