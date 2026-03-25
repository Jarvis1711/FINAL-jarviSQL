#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p proof build

{
  echo "[build] $(date -u +%FT%TZ)"
  if command -v cmake >/dev/null 2>&1; then
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
    cmake --build build -j
  else
    mkdir -p build
    g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/engine.cpp src/sql_parser.cpp src/protocol.cpp src/persistence.cpp src/server.cpp -o build/jarvisql_server
    g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/client_api.cpp src/protocol.cpp src/repl.cpp -o build/jarvisql_repl
    g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/client_api.cpp src/protocol.cpp src/bench.cpp -o build/jarvisql_bench
    g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/engine.cpp src/sql_parser.cpp src/protocol.cpp src/persistence.cpp tests/test_engine.cpp -o build/jarvisql_tests
  fi
} > proof/build.log 2>&1

{
  echo "[test] $(date -u +%FT%TZ)"
  if command -v ctest >/dev/null 2>&1 && [ -f build/CTestTestfile.cmake ]; then
    ctest --test-dir build --output-on-failure
  else
    ./build/jarvisql_tests
  fi
} > proof/test.log 2>&1

./build/jarvisql_server 9100 > proof/server.log 2>&1 &
SERVER_PID=$!
trap 'kill ${SERVER_PID} >/dev/null 2>&1 || true' EXIT
sleep 1

{
  echo "[repl demo] $(date -u +%FT%TZ)"
  {
    echo "CREATE TABLE users (id INT PRIMARY KEY, score DECIMAL, name VARCHAR);"
    echo "INSERT INTO users VALUES (1, 91.5, 'jade');"
    echo "INSERT INTO users VALUES (2, 88.0, 'alex');"
    echo "SELECT id,name FROM users WHERE id = 2;"
    echo "exit"
  } | ./build/jarvisql_repl 127.0.0.1 9100
} > proof/repl_demo.log 2>&1

{
  echo "[benchmark] $(date -u +%FT%TZ)"
  ./build/jarvisql_bench 127.0.0.1 9100 50000
} > proof/benchmark.log 2>&1

python3 scripts/render_proof.py proof/test.log proof/test-proof.png "jarvisql tests"
python3 scripts/render_proof.py proof/benchmark.log proof/benchmark-proof.png "jarvisql benchmark"

echo "Artifacts generated in proof/"
