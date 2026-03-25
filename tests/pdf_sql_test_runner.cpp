#include "engine.hpp"

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

namespace {

bool is_database_exists_error(const std::string& error) {
  return error.rfind("Database already exists:", 0) == 0;
}

}  // namespace

struct SqlCase {
  std::string name;
  std::string sql;
  bool expect_ok;
  size_t expect_rows = static_cast<size_t>(-1);
};

int main() {
  jarvisql::Engine engine;
  jarvisql::QueryResult result;
  std::string error;

  std::vector<SqlCase> cases = {
      {"create database analytics", "CREATE DATABASE analytics;", true},
      {"use analytics database", "USE analytics;", true},
      {"create users", "CREATE TABLE users (id INT PRIMARY KEY, score DECIMAL, name VARCHAR, created DATETIME);", true},
      {"create orders", "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL);", true},
      {"insert user 1", "INSERT INTO users VALUES (1, 95.5, 'jade', '2026-03-12T00:00:00');", true},
      {"insert user 2", "INSERT INTO users VALUES (2, 88.2, 'alex', '2026-03-12T00:01:00');", true},
      {"insert user 3", "INSERT INTO users VALUES (3, 77.1, 'sam', '2026-03-12T00:02:00');", true},
      {"insert order 1", "INSERT INTO orders VALUES (100, 1, 250.75);", true},
      {"insert order 2", "INSERT INTO orders VALUES (101, 2, 430.10);", true},
      {"insert order 3", "INSERT INTO orders VALUES (102, 1, 90.00);", true},

      {"select all", "SELECT * FROM users;", true, 3},
      {"select specific columns", "SELECT id,name FROM users;", true, 3},
      {"where equals", "SELECT id,name FROM users WHERE id = 2;", true, 1},
      {"where greater than", "SELECT id,name FROM users WHERE score > 90;", true, 1},
      {"where less than", "SELECT id,name FROM users WHERE score < 90;", true, 2},

      {"inner join", "SELECT users.name,orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id;", true, 3},
      {"inner join with where", "SELECT users.name,orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100;", true, 2},

      {"create database sales", "CREATE DATABASE sales;", true},
      {"use sales database", "USE sales;", true},
      {"sales users should not exist", "SELECT * FROM users;", false},
      {"create users in sales", "CREATE TABLE users (id INT PRIMARY KEY, score DECIMAL, name VARCHAR, created DATETIME);", true},
      {"insert sales user", "INSERT INTO users VALUES (10, 70.0, 'nina', '2026-03-12T01:00:00');", true},
      {"sales user select", "SELECT id,name FROM users WHERE id = 10;", true, 1},

      {"switch back analytics", "USE analytics;", true},
      {"analytics users intact", "SELECT id,name FROM users WHERE id = 1;", true, 1},

      {"where with AND should fail", "SELECT id FROM users WHERE id = 1 AND score > 80;", false},
      {"where with OR should fail", "SELECT id FROM users WHERE id = 1 OR score > 80;", false},
  };

  size_t passed = 0;
  std::cout << "=== JarvisQL SQL Coverage (Engine) ===\n";

  for (size_t i = 0; i < cases.size(); ++i) {
    const auto& test_case = cases[i];
    result = jarvisql::QueryResult{};
    error.clear();

    const bool ok = engine.execute(test_case.sql, result, error);
    bool case_passed = (ok == test_case.expect_ok);

    if (!case_passed && !ok && test_case.expect_ok &&
        (test_case.name == "create database analytics" || test_case.name == "create database sales") &&
        is_database_exists_error(error)) {
      case_passed = true;
    }

    if (case_passed && ok && test_case.expect_rows != static_cast<size_t>(-1)) {
      case_passed = (result.rows.size() == test_case.expect_rows);
    }

    std::cout << std::setw(2) << (i + 1) << ". " << test_case.name << " -> " << (case_passed ? "PASS" : "FAIL") << "\n";
    std::cout << "    query: " << test_case.sql << "\n";
    std::cout << "    expected_ok=" << (test_case.expect_ok ? "true" : "false")
          << " actual_ok=" << (ok ? "true" : "false");
    if (test_case.expect_rows != static_cast<size_t>(-1) && ok) {
      std::cout << " expected_rows=" << test_case.expect_rows << " actual_rows=" << result.rows.size();
    }
    std::cout << "\n";

    if (!ok && !error.empty()) {
      std::cout << "    engine_error: " << error << "\n";
    }

    if (ok && !result.rows.empty()) {
      std::cout << "    sample_row:";
      for (const auto& value : result.rows.front()) {
        std::cout << " " << value;
      }
      std::cout << "\n";
    }

    if (case_passed) {
      ++passed;
    }
  }

  std::cout << "\nResult: " << passed << "/" << cases.size() << " checks passed\n";
  return (passed == cases.size()) ? 0 : 1;
}
