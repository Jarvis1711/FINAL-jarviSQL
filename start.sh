#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

PORT="${1:-9000}"
HOST="127.0.0.1"

build_if_needed() {
  if [[ -x "build/jarvisql_server" && -x "build/jarvisql_repl" ]]; then
    return
  fi

  echo "[start.sh] build artifacts missing; building..."
  mkdir -p build

  if command -v cmake >/dev/null 2>&1; then
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
    cmake --build build -j
  else
    g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/engine.cpp src/sql_parser.cpp src/protocol.cpp src/persistence.cpp src/server.cpp -o build/jarvisql_server
    g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/client_api.cpp src/protocol.cpp src/repl.cpp -o build/jarvisql_repl
  fi
}

build_if_needed

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

echo "[start.sh] starting jarvisql_server on port ${PORT}"
./build/jarvisql_server "$PORT" > /tmp/jarvisql_server.log 2>&1 &
SERVER_PID=$!

sleep 1
if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
  echo "[start.sh] server failed to start. Check /tmp/jarvisql_server.log"
  exit 1
fi

echo "[start.sh] opening jarvisql_repl at ${HOST}:${PORT}"
./build/jarvisql_repl "$HOST" "$PORT"
