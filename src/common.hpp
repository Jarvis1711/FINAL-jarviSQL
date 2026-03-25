#pragma once

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <deque>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace jarvisql {

inline std::string trim(const std::string& input) {
  size_t start = 0;
  size_t end = input.size();
  while (start < end && std::isspace(static_cast<unsigned char>(input[start]))) {
    ++start;
  }
  while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }
  return input.substr(start, end - start);
}

inline std::string to_upper(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
    return static_cast<char>(std::toupper(c));
  });
  return value;
}

inline std::vector<std::string> split_csv_aware(const std::string& input) {
  std::vector<std::string> parts;
  std::string current;
  bool in_quotes = false;
  for (char ch : input) {
    if (ch == '\'' && (current.empty() || current.back() != '\\')) {
      in_quotes = !in_quotes;
      current.push_back(ch);
      continue;
    }
    if (ch == ',' && !in_quotes) {
      parts.push_back(trim(current));
      current.clear();
      continue;
    }
    current.push_back(ch);
  }
  if (!current.empty()) {
    parts.push_back(trim(current));
  }
  return parts;
}

inline std::string unquote(const std::string& value) {
  if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
    return value.substr(1, value.size() - 2);
  }
  return value;
}

inline int64_t epoch_ms_now() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

inline bool is_numeric(const std::string& value) {
  if (value.empty()) {
    return false;
  }
  char* end = nullptr;
  std::strtod(value.c_str(), &end);
  return end != value.c_str() && *end == '\0';
}

inline int compare_values(const std::string& left, const std::string& right) {
  if (is_numeric(left) && is_numeric(right)) {
    const double l = std::stod(left);
    const double r = std::stod(right);
    if (l < r) return -1;
    if (l > r) return 1;
    return 0;
  }
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

inline bool check_predicate(const std::string& left, const std::string& op, const std::string& right) {
  const int cmp = compare_values(left, right);
  if (op == "=") return cmp == 0;
  if (op == "!=") return cmp != 0;
  if (op == ">") return cmp > 0;
  if (op == "<") return cmp < 0;
  if (op == ">=") return cmp >= 0;
  if (op == "<=") return cmp <= 0;
  return false;
}

}  // namespace jarvisql
