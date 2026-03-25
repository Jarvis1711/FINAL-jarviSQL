#include "flexql.h"

#include "protocol.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>
#include <vector>

struct flexql_db {
  int socket_fd = -1;
};

extern "C" {

int flexql_open(const char* host, int port, flexql_db** db) {
  if (host == nullptr || db == nullptr || port <= 0) {
    return FLEXQL_ERROR;
  }

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo* result = nullptr;
  const std::string port_text = std::to_string(port);
  if (getaddrinfo(host, port_text.c_str(), &hints, &result) != 0) {
    return FLEXQL_ERROR;
  }

  int sock = -1;
  for (addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
    sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sock == -1) {
      continue;
    }
    if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) {
      break;
    }
    close(sock);
    sock = -1;
  }
  freeaddrinfo(result);

  if (sock < 0) {
    return FLEXQL_ERROR;
  }

  *db = new flexql_db{sock};
  return FLEXQL_OK;
}

int flexql_close(flexql_db* db) {
  if (db == nullptr || db->socket_fd < 0) {
    return FLEXQL_ERROR;
  }
  close(db->socket_fd);
  db->socket_fd = -1;
  delete db;
  return FLEXQL_OK;
}

int flexql_exec(flexql_db* db, const char* sql, flexql_callback callback, void* arg, char** errmsg) {
  if (errmsg != nullptr) {
    *errmsg = nullptr;
  }
  if (db == nullptr || db->socket_fd < 0 || sql == nullptr) {
    return FLEXQL_ERROR;
  }

  std::string wire = sql;
  if (wire.empty() || wire.back() != '\n') {
    wire.push_back('\n');
  }
  if (!jarvisql::write_all_to_fd(db->socket_fd, wire)) {
    if (errmsg != nullptr) {
      *errmsg = strdup("network write failed");
    }
    return FLEXQL_ERROR;
  }

  auto first = jarvisql::read_line_from_fd(db->socket_fd);
  if (!first.has_value()) {
    if (errmsg != nullptr) {
      *errmsg = strdup("network read failed");
    }
    return FLEXQL_ERROR;
  }

  auto parts = jarvisql::split_protocol_line(*first);
  if (parts.empty()) {
    if (errmsg != nullptr) {
      *errmsg = strdup("invalid server response");
    }
    return FLEXQL_ERROR;
  }

  if (parts[0] == "OK") {
    return FLEXQL_OK;
  }

  if (parts[0] == "ERR") {
    if (errmsg != nullptr) {
      const char* msg = (parts.size() > 1) ? parts[1].c_str() : "unknown error";
      *errmsg = strdup(msg);
    }
    return FLEXQL_ERROR;
  }

  if (parts[0] != "RESULT" || parts.size() < 3) {
    if (errmsg != nullptr) {
      *errmsg = strdup("malformed RESULT response");
    }
    return FLEXQL_ERROR;
  }

  int expected_columns = std::stoi(parts[1]);
  if (expected_columns <= 0 || static_cast<int>(parts.size()) != expected_columns + 2) {
    if (errmsg != nullptr) {
      *errmsg = strdup("invalid RESULT header");
    }
    return FLEXQL_ERROR;
  }

  std::vector<std::string> columns(parts.begin() + 2, parts.end());
  std::vector<char*> col_ptrs;
  col_ptrs.reserve(columns.size());
  for (auto& col : columns) {
    col_ptrs.push_back(const_cast<char*>(col.c_str()));
  }

  while (true) {
    auto line = jarvisql::read_line_from_fd(db->socket_fd);
    if (!line.has_value()) {
      if (errmsg != nullptr) {
        *errmsg = strdup("unexpected EOF");
      }
      return FLEXQL_ERROR;
    }
    auto row_parts = jarvisql::split_protocol_line(*line);
    if (row_parts.empty()) {
      continue;
    }
    if (row_parts[0] == "END") {
      break;
    }
    if (row_parts[0] != "ROW" || static_cast<int>(row_parts.size()) != expected_columns + 1) {
      if (errmsg != nullptr) {
        *errmsg = strdup("malformed ROW response");
      }
      return FLEXQL_ERROR;
    }

    if (callback != nullptr) {
      std::vector<std::string> values(row_parts.begin() + 1, row_parts.end());
      std::vector<char*> val_ptrs;
      val_ptrs.reserve(values.size());
      for (auto& value : values) {
        val_ptrs.push_back(const_cast<char*>(value.c_str()));
      }
      const int should_abort = callback(arg, expected_columns, val_ptrs.data(), col_ptrs.data());
      if (should_abort == 1) {
        while (true) {
          auto maybe = jarvisql::read_line_from_fd(db->socket_fd);
          if (!maybe.has_value()) {
            break;
          }
          auto drain = jarvisql::split_protocol_line(*maybe);
          if (!drain.empty() && drain[0] == "END") {
            break;
          }
        }
        return FLEXQL_OK;
      }
    }
  }

  return FLEXQL_OK;
}

void flexql_free(void* ptr) {
  free(ptr);
}

}  // extern "C"
