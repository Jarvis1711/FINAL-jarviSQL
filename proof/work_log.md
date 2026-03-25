# jarvisql optimization work log

Date: 2026-03-12

## Summary of work completed
- Added secondary per-column hash equality indexes in the engine for fast `WHERE column = value` search across large tables.
- Kept primary-key hash indexing and LRU query-result cache path in place.
- Optimized column resolution to O(1) with precomputed column lookup map.
- Updated benchmark to include indexed non-primary equality lookup and warm-cache lookup timings.
- Added benchmark append logging to `proof/performance_optimization.log`.

## Files changed
- `src/engine.hpp`
- `src/engine.cpp`
- `src/bench.cpp`
- `README.md`

## How to run 10M timing test
1. Start server:
   - `./build/jarvisql_server 9000`
2. Run benchmark:
   - `./build/jarvisql_bench 127.0.0.1 9000 10000000 proof/performance_optimization.log`

## Notes
- Equality indexing targets `WHERE column = value` and avoids full-table scans in that common path.
- Non-equality predicates (`>`, `<`, `>=`, `<=`, `!=`) still use row scans.
- Query result cache is invalidated on writes to preserve correctness.

## 10M benchmark run result (completed)
- Command: `./build/jarvisql_bench 127.0.0.1 9000 10000000 proof/performance_optimization.log`
- rows_inserted=10000000
- insert_ms=875802
- pk_select_ms=40
- indexed_name_select_us=42051
- cached_name_select_us=41009
- selected_rows=1
- indexed_name_selected_rows=1
- cached_name_selected_rows=1

## Database query support update (completed)
- Added SQL support for `CREATE DATABASE <name>`.
- Added SQL support for `USE <name>`.
- Engine now routes `CREATE TABLE`, `INSERT`, `SELECT`, and `JOIN` within the active database context.
- Added isolation test coverage to verify same table name can exist independently in different databases.
