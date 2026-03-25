#pragma once

#include <optional>
#include <string>
#include <vector>

namespace jarvisql {

struct WireResponse {
  bool ok = false;
  bool has_rows = false;
  std::string error;
  std::vector<std::string> columns;
  std::vector<std::vector<std::string>> rows;
};

std::string encode_field(const std::string& input);
std::string decode_field(const std::string& input);

std::vector<std::string> split_protocol_line(const std::string& line);
std::string join_protocol_line(const std::vector<std::string>& parts);

std::optional<std::string> read_line_from_fd(int fd);
bool write_all_to_fd(int fd, const std::string& data);

}  // namespace jarvisql
