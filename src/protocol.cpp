#include "protocol.hpp"

#include <sys/socket.h>
#include <unistd.h>

namespace jarvisql {

std::string encode_field(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  for (char c : input) {
    if (c == '%') {
      out += "%25";
    } else if (c == '|') {
      out += "%7C";
    } else if (c == '\n') {
      out += "%0A";
    } else {
      out.push_back(c);
    }
  }
  return out;
}

std::string decode_field(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  for (size_t i = 0; i < input.size(); ++i) {
    if (input[i] == '%' && i + 2 < input.size()) {
      const std::string code = input.substr(i, 3);
      if (code == "%25") {
        out.push_back('%');
        i += 2;
        continue;
      }
      if (code == "%7C") {
        out.push_back('|');
        i += 2;
        continue;
      }
      if (code == "%0A") {
        out.push_back('\n');
        i += 2;
        continue;
      }
    }
    out.push_back(input[i]);
  }
  return out;
}

std::vector<std::string> split_protocol_line(const std::string& line) {
  std::vector<std::string> parts;
  std::string current;
  for (char c : line) {
    if (c == '|') {
      parts.push_back(decode_field(current));
      current.clear();
    } else {
      current.push_back(c);
    }
  }
  parts.push_back(decode_field(current));
  return parts;
}

std::string join_protocol_line(const std::vector<std::string>& parts) {
  std::string line;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i > 0) {
      line.push_back('|');
    }
    line += encode_field(parts[i]);
  }
  line.push_back('\n');
  return line;
}

std::optional<std::string> read_line_from_fd(int fd) {
  std::string line;
  char ch = '\0';
  while (true) {
    const ssize_t read_bytes = recv(fd, &ch, 1, 0);
    if (read_bytes == 0) {
      if (line.empty()) {
        return std::nullopt;
      }
      return line;
    }
    if (read_bytes < 0) {
      return std::nullopt;
    }
    if (ch == '\n') {
      return line;
    }
    line.push_back(ch);
  }
}

bool write_all_to_fd(int fd, const std::string& data) {
  size_t sent = 0;
  while (sent < data.size()) {
    const ssize_t wrote = send(fd, data.data() + sent, data.size() - sent, 0);
    if (wrote <= 0) {
      return false;
    }
    sent += static_cast<size_t>(wrote);
  }
  return true;
}

}  // namespace jarvisql
