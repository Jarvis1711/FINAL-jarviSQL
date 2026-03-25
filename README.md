# JarvisQL (FlexQL-compatible)


Author: Jitender Singh  
Roll No: 25CS60D05

## Features
- Multithreaded TCP server (`jarvisql_server`)
- Interactive REPL client (`jarvisql_repl`)
- C API (`flexql_open`, `flexql_close`, `flexql_exec`, `flexql_free`)
- Persistent storage to local files
- Primary-key hash index and equality indexes
- LRU cache for repeated `SELECT` / `JOIN`
- Benchmark tool (`jarvisql_bench`)

## SQL Notes
- Every SQL statement must end with `;`
- Supported statements:
  - `CREATE DATABASE <name>;`
  - `USE <database>;`
  - `CREATE TABLE ...;`
  - `INSERT INTO ... VALUES (...);`
  - `INSERT ... EXPIRES IN <seconds>;`
  - `INSERT ... EXPIRES AT '<epoch_seconds>';`
  - `SELECT ... FROM ... [WHERE ...];`
  - `SELECT ... INNER JOIN ... ON ... [WHERE ...];`

## Build
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

If `cmake` is not installed, use:
```bash
mkdir -p build
g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/engine.cpp src/sql_parser.cpp src/protocol.cpp src/persistence.cpp src/server.cpp -o build/jarvisql_server
g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/client_api.cpp src/protocol.cpp src/repl.cpp -o build/jarvisql_repl
g++ -std=c++20 -O3 -pthread -Iinclude -Isrc src/client_api.cpp src/protocol.cpp src/bench.cpp -o build/jarvisql_bench
```

## One-command Start (SQL shell)
```bash
./start.sh
```
Optional custom port:
```bash
./start.sh 9100
```
This script builds if needed, starts the server, opens REPL, and stops the server when REPL exits.

## Run server + REPL
```bash
./build/jarvisql_server 9000
./build/jarvisql_repl 127.0.0.1 9000
```

## Run tests
```bash
ctest --test-dir build --output-on-failure
```

## Benchmark
By default, benchmark output is also appended to `proof/performance_optimization.log`.

Standard run:
```bash
./build/jarvisql_bench 127.0.0.1 9000 100000
```

10 million rows (long run):
```bash
./build/jarvisql_bench 127.0.0.1 9000 10000000 proof/performance_optimization.log
```

It measures insert time, PK lookup time, indexed equality lookup time, and cache-hit lookup time.

## Proof artifacts
Run:
```bash
./scripts/run_all.sh
```
This creates:
- `proof/build.log`
- `proof/test.log`
- `proof/repl_demo.log`
- `proof/benchmark.log`
- `proof/test-proof.png`
- `proof/benchmark-proof.png`

## Implementation Summary
- O(1) expected lookup via hash primary index
- Hash join for `INNER JOIN`
- Shared mutexes for read-heavy concurrency
- Cache invalidation on writes for correctness
