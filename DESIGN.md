# JarvisQL Design Document
Github Repo- https://github.com/Jarvis1711/FINAL-jarviSQL/edit/master/DESIGN.md

Author: Jitender Singh  
Roll No: 25CS60D05

All SQL statements are expected to end with `;`.

## 1. Project goal
JarvisQL is a lightweight SQL-style engine with a TCP server, REPL client, and C API. The design is focused on fast lookups, simple concurrency, and predictable behavior for course-level workloads.

## 2. Data model and storage
The engine keeps table data in memory using row-oriented storage.

Each table contains:
- schema (column name, type, primary-key flag)
- rows (`vector<string>` values + expiry timestamp)
- column lookup map
- primary-key hash index (if defined)
- per-column equality indexes

Rows are stored append-only, which keeps inserts simple and cache-friendly.

## 3. SQL support and parsing
The parser handles these command families:
- database: `CREATE DATABASE`, `USE`
- table: `CREATE TABLE`
- data: `INSERT`, `SELECT`, `INNER JOIN`

`WHERE` currently supports one predicate with operators:
`=`, `!=`, `>`, `<`, `>=`, `<=`

Logical `AND` / `OR` in `WHERE` are intentionally out of scope in the current version.

## 4. Query execution strategy
### SELECT
- If predicate is `primary_key = value`, engine uses primary index directly.
- If predicate is equality on another column, engine can use equality index.
- For range predicates, engine scans rows and applies predicate.

### INNER JOIN
Join uses a hash-join style flow:
1. Build hash map on join key for one side.
2. Probe with rows from the other side.
3. Apply optional `WHERE` filter after join match.

## 5. Expiration handling
Each row has `expires_at_epoch_ms`.

Supported insert forms:
- `EXPIRES IN <seconds>`
- `EXPIRES AT '<epoch_seconds>'`

Expired rows are ignored during read paths (`SELECT` / `JOIN`).

## 6. Persistence model
Persistence is file-based under `jarvisql_data/`.

- Schema is written to `<table>.schema`
- Rows are written to `<table>.csv`

On startup, persisted databases/tables are loaded and in-memory indexes are rebuilt.

## 7. Cache design
The engine has an LRU cache for `SELECT` and `JOIN` results.

- Key: normalized SQL + current DB context
- Capacity: 512 entries
- Invalidation: full cache clear on writes (`CREATE`, `INSERT`, DB switch)

This keeps correctness simple and still speeds up repeated reads.

## 8. Concurrency model
Server runs one worker thread per client connection.

Locking approach:
- DB map: shared mutex
- Table internals: shared mutex per table
- Cache: mutex

This allows concurrent readers while keeping writes safe.

## 9. C API behavior
Public API functions:
- `flexql_open` — open TCP connection
- `flexql_exec` — execute SQL and stream rows via callback
- `flexql_close` — close connection
- `flexql_free` — free API-allocated memory

## 10. Performance notes
Main choices that improve latency:
- hash primary-key index for point lookups
- equality indexes for `col = value`
- hash join instead of nested loop join
- release build flags and low-overhead protocol framing

## 11. Build and run
Build and run commands are documented in README.md.
