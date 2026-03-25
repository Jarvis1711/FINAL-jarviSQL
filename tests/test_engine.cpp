#include "engine.hpp"

#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

using jarvisql::Engine;
using jarvisql::QueryResult;

void test_create_insert_select() {
  Engine engine;
  QueryResult result;
  std::string error;

  assert(engine.execute("CREATE TABLE users (id INT PRIMARY KEY, score DECIMAL, name VARCHAR);", result, error));
  assert(engine.execute("INSERT INTO users VALUES (1, 95.5, 'jade');", result, error));
  assert(engine.execute("INSERT INTO users VALUES (2, 88.2, 'alex');", result, error));

  assert(engine.execute("SELECT id,name FROM users WHERE id = 2;", result, error));
  assert(result.columns.size() == 2);
  assert(result.rows.size() == 1);
  assert(result.rows[0][0] == "2");
  assert(result.rows[0][1] == "alex");
}

void test_join_and_where() {
  Engine engine;
  QueryResult result;
  std::string error;

  assert(engine.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);", result, error));
  assert(engine.execute("CREATE TABLE scores (uid INT PRIMARY KEY, score DECIMAL);", result, error));
  assert(engine.execute("INSERT INTO users VALUES (1, 'a');", result, error));
  assert(engine.execute("INSERT INTO users VALUES (2, 'b');", result, error));
  assert(engine.execute("INSERT INTO scores VALUES (1, 11.2);", result, error));
  assert(engine.execute("INSERT INTO scores VALUES (2, 77.7);", result, error));

  assert(engine.execute("SELECT users.name,scores.score FROM users INNER JOIN scores ON users.id = scores.uid WHERE scores.score > 50;", result, error));
  assert(result.rows.size() == 1);
  assert(result.rows[0][0] == "b");
  assert(result.rows[0][1] == "77.7");
}

void test_expiration() {
  Engine engine;
  QueryResult result;
  std::string error;

  assert(engine.execute("CREATE TABLE sessions (id INT PRIMARY KEY, token VARCHAR);", result, error));
  assert(engine.execute("INSERT INTO sessions VALUES (1, 'soon_expire') EXPIRES IN 1;", result, error));
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));

  assert(engine.execute("SELECT * FROM sessions;", result, error));
  assert(result.rows.empty());
}

void test_cache_path() {
  Engine engine;
  QueryResult result;
  std::string error;

  assert(engine.execute("CREATE TABLE cache_t (id INT PRIMARY KEY, value VARCHAR);", result, error));
  assert(engine.execute("INSERT INTO cache_t VALUES (1, 'x');", result, error));

  assert(engine.execute("SELECT * FROM cache_t WHERE id = 1;", result, error));
  assert(!result.from_cache);

  assert(engine.execute("SELECT * FROM cache_t WHERE id = 1;", result, error));
  assert(result.from_cache);
}

void test_create_and_use_database() {
  Engine engine;
  QueryResult result;
  std::string error;

  bool ok = engine.execute("CREATE DATABASE analytics;", result, error);
  assert(ok || error.rfind("Database already exists:", 0) == 0);
  assert(engine.execute("USE analytics;", result, error));
  assert(engine.execute("CREATE TABLE events_analytics (id INT PRIMARY KEY, name VARCHAR);", result, error));
  assert(engine.execute("INSERT INTO events_analytics VALUES (1, 'launch');", result, error));
  assert(engine.execute("SELECT * FROM events_analytics WHERE id = 1;", result, error));
  assert(result.rows.size() == 1);
  assert(result.rows[0][1] == "launch");

  ok = engine.execute("CREATE DATABASE sales;", result, error);
  assert(ok || error.rfind("Database already exists:", 0) == 0);
  assert(engine.execute("USE sales;", result, error));
  assert(!engine.execute("SELECT * FROM events_analytics;", result, error));

  assert(engine.execute("CREATE TABLE events_sales (id INT PRIMARY KEY, name VARCHAR);", result, error));
  assert(engine.execute("INSERT INTO events_sales VALUES (1, 'invoice');", result, error));
  assert(engine.execute("SELECT * FROM events_sales WHERE id = 1;", result, error));
  assert(result.rows.size() == 1);
  assert(result.rows[0][1] == "invoice");
}

void test_missing_semicolon_rejected() {
  Engine engine;
  QueryResult result;
  std::string error;

  assert(!engine.execute("SELECT * FROM missing_semicolon", result, error));
  assert(error == "SQL must end with ';'");
}

int main() {
  test_create_insert_select();
  test_join_and_where();
  test_expiration();
  test_cache_path();
  test_create_and_use_database();
  test_missing_semicolon_rejected();
  std::cout << "All jarvisql tests passed\n";
  return 0;
}
