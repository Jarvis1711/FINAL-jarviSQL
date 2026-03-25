#include "flexql.h"

#include <cstring>
#include <iostream>
#include <string>

namespace {

int print_row(void*, int column_count, char** values, char** column_names) {
  for (int i = 0; i < column_count; ++i) {
    std::cout << column_names[i] << "=" << values[i];
    if (i + 1 < column_count) {
      std::cout << " | ";
    }
  }
  std::cout << '\n';
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  std::string host = "127.0.0.1";
  int port = 9000;

  if (argc >= 2) {
    host = argv[1];
  }
  if (argc >= 3) {
    port = std::stoi(argv[2]);
  }

  flexql_db* db = nullptr;
  if (flexql_open(host.c_str(), port, &db) != FLEXQL_OK) {
    std::cerr << "Failed to connect to jarvisql server at " << host << ":" << port << '\n';
    return 1;
  }

  std::cout << "Connected to jarvisql at " << host << ":" << port << "\n";
  std::cout << "Type SQL and press Enter. Type .exit to quit.\n";

  std::string line;
  while (true) {
    std::cout << "jarvisql> ";
    if (!std::getline(std::cin, line)) {
      break;
    }
    if (line == ".exit" || line == "exit" || line == "quit") {
      break;
    }
    if (line.empty()) {
      continue;
    }

    char* errmsg = nullptr;
    int rc = flexql_exec(db, line.c_str(), print_row, nullptr, &errmsg);
    if (rc != FLEXQL_OK) {
      std::cerr << "ERROR: " << (errmsg != nullptr ? errmsg : "unknown") << '\n';
      if (errmsg != nullptr) {
        flexql_free(errmsg);
      }
    } else {
      std::cout << "OK\n";
    }
  }

  flexql_close(db);
  return 0;
}
