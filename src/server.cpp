#include "engine.hpp"
#include "protocol.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_shutdown{false};

void handle_signal(int) {
  g_shutdown.store(true);
}

void serve_client(int client_fd, jarvisql::Engine* engine) {
  while (true) {
    auto line = jarvisql::read_line_from_fd(client_fd);
    if (!line.has_value()) {
      break;
    }

    const std::string sql = jarvisql::trim(*line);
    if (sql.empty()) {
      continue;
    }

    jarvisql::QueryResult result;
    std::string error;
    if (!engine->execute(sql, result, error)) {
      const std::string payload = jarvisql::join_protocol_line({"ERR", error});
      if (!jarvisql::write_all_to_fd(client_fd, payload)) {
        break;
      }
      continue;
    }

    if (result.columns.empty()) {
      if (!jarvisql::write_all_to_fd(client_fd, jarvisql::join_protocol_line({"OK"}))) {
        break;
      }
      continue;
    }

    std::vector<std::string> header{"RESULT", std::to_string(result.columns.size())};
    for (const auto& col : result.columns) {
      header.push_back(col);
    }
    if (!jarvisql::write_all_to_fd(client_fd, jarvisql::join_protocol_line(header))) {
      break;
    }

    for (const auto& row : result.rows) {
      std::vector<std::string> payload{"ROW"};
      for (const auto& cell : row) {
        payload.push_back(cell);
      }
      if (!jarvisql::write_all_to_fd(client_fd, jarvisql::join_protocol_line(payload))) {
        break;
      }
    }

    if (!jarvisql::write_all_to_fd(client_fd, jarvisql::join_protocol_line({"END"}))) {
      break;
    }
  }

  close(client_fd);
}

}  // namespace

int main(int argc, char** argv) {
  int port = 9000;
  if (argc >= 2) {
    port = std::stoi(argv[1]);
  }

  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  int opt = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port));

  if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::cerr << "Failed to bind server socket on port " << port << "\n";
    close(server_fd);
    return 1;
  }

  if (listen(server_fd, 128) != 0) {
    std::cerr << "Failed to listen\n";
    close(server_fd);
    return 1;
  }

  std::cout << "jarvisql server listening on port " << port << "\n";

  jarvisql::Engine engine;
  std::vector<std::thread> workers;

  while (!g_shutdown.load()) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) {
      if (g_shutdown.load()) {
        break;
      }
      continue;
    }
    workers.emplace_back(serve_client, client_fd, &engine);
  }

  close(server_fd);

  for (auto& thread : workers) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  return 0;
}
