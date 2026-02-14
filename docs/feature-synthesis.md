# GrapheneDB Feature Synthesis

Combined feature design drawing from PostgreSQL, SQL Server, DragonflyDB, and modern database
innovations (DuckDB, ClickHouse, ScyllaDB, TigerBeetle, CockroachDB). The goal: well-thought-out
standards with extreme performance, without compromising usability.

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [Storage Engine](#2-storage-engine)
3. [Memory Management & Buffer Pool](#3-memory-management--buffer-pool)
4. [Concurrency & MVCC](#4-concurrency--mvcc)
5. [Indexing](#5-indexing)
6. [Query Processing & Optimization](#6-query-processing--optimization)
7. [Execution Engine](#7-execution-engine)
8. [Compression](#8-compression)
9. [WAL & Recovery](#9-wal--recovery)
10. [Threading & I/O Model](#10-threading--io-model)
11. [Network Protocol](#11-network-protocol)
12. [Type System](#12-type-system)
13. [SQL Feature Surface](#13-sql-feature-surface)
14. [Observability & Self-Tuning](#14-observability--self-tuning)
15. [Testing Strategy](#15-testing-strategy)
16. [Feature Roadmap Priorities](#16-feature-roadmap-priorities)

---

## 1. Design Philosophy

### Core Principles

1. **Standards-first**: PostgreSQL-compatible SQL dialect where possible. Users should feel at home.
2. **Performance by default**: Zero-config performance. No knob-turning required for common cases.
3. **Predictable latency**: Consistent P99, not just good averages. No spikes from compaction,
   checkpointing, or GC pauses.
4. **Memory-efficient**: Every byte counts. Compact data structures, minimal per-row overhead.
5. **Zig-native**: Leverage Zig's strengths -- comptime code generation, explicit memory control,
   no hidden allocations, first-class SIMD via `@Vector`.

### Inspiration Sources

| Feature Area | Primary Inspiration | Why |
|---|---|---|
| SQL dialect & type system | PostgreSQL | Best standards compliance, most loved by developers |
| Storage architecture | SQL Server | Proven extent-based allocation, page format |
| Buffer pool eviction | DragonflyDB | 2Q algorithm, zero-overhead per item |
| Execution engine | DuckDB | Vectorized push-based execution, morsel-driven parallelism |
| Threading model | ScyllaDB/DragonflyDB | Thread-per-core, shared-nothing |
| Compression | DuckDB/ClickHouse | Lightweight columnar compression |
| Snapshotting | DragonflyDB | Version-based snapshots without memory spikes |
| Testing | TigerBeetle | Deterministic simulation testing |
| Data structures | DragonflyDB | Compact hash tables, B+ tree sorted sets |
| Adaptive features | SQL Server IQP | Adaptive joins, memory grant feedback |

---

## 2. Storage Engine

### 2.1 Page Format (Keep & Enhance)

**Current**: SQL Server-inspired 8KB slotted pages with 96-byte header, row offset array.

**Enhancements**:

- **Dual-format pages**: Support both row-store and mini-columnar formats within the same table.
  For cold data or analytical scans, pages can be transparently converted to a columnar layout
  within the same 8KB page boundary (inspired by SQL Server's batch mode on rowstore).
- **Page-level compression metadata**: Reserve space in the page header for compression scheme
  indicators (dictionary ID, encoding type). Each page can independently choose its compression.
- **Inline TOAST**: For values exceeding ~2KB, store a pointer to overflow pages (PostgreSQL TOAST
  concept). Keep the main row compact. Use Zstd for large values.

### 2.2 Extent-Based Allocation (Keep & Enhance)

**Current**: SQL Server-style GAM/SGAM/PFS/IAM with 8-page (64KB) extents.

**Enhancements**:

- **Large extents for sequential workloads**: Support 64-page (512KB) "super-extents" for bulk
  loads and large sequential scans. Reduces allocation map overhead and improves I/O coalescing.
- **Extent-level statistics**: Track min/max values per extent for BRIN-like index elimination
  (inspired by ClickHouse's sparse primary indexes). Zero cost -- maintained during writes.
- **Per-extent bloom filters**: Optional small bloom filter per extent for point lookups on
  non-indexed columns. Avoids scanning extents that definitely don't contain the target value.

### 2.3 Row Format

**Current**: Fixed + variable layout with null bitmap.

**Enhancements**:

- **Compact null bitmap**: Use run-length encoding for the null bitmap when >16 columns. Most
  rows have few nulls, so a compressed bitmap saves space.
- **Small string optimization**: Inspired by DragonflyDB's CompactObj. Strings <=15 bytes are
  stored inline in the tuple without a separate allocation. Uses a tag byte to distinguish inline
  vs. pointer.
- **Column ordering optimization**: Automatically reorder columns in physical storage to minimize
  padding (align fixed-width columns first, then variable-width). Transparent to the user.

---

## 3. Memory Management & Buffer Pool

### 3.1 Buffer Pool Architecture

**Current**: Basic buffer pool with clock-sweep eviction.

**Target architecture** (combining best ideas):

- **2Q eviction policy** (from DragonflyDB): Instead of simple clock-sweep, use a 2Q algorithm
  that distinguishes between "new" pages (probationary) and "hot" pages (protected). New pages
  enter a FIFO probationary queue. Only pages accessed a second time are promoted to the
  protected LRU region. This prevents large sequential scans from evicting frequently-used OLTP
  pages.

- **Ring buffers for scans** (from PostgreSQL): Sequential scans and bulk operations use a
  dedicated small ring buffer (256KB) instead of the main buffer pool. This completely isolates
  analytical scans from OLTP cache.

- **Per-thread buffer pool segments** (from ScyllaDB's shard-per-core): Each thread owns a
  segment of the buffer pool. No cross-thread contention on buffer pool data structures. Pages
  are homed to the thread that owns the relevant data partition.

- **NUMA-aware allocation** (from SQL Server): On multi-socket systems, allocate buffer pool
  memory from the local NUMA node. Each thread's buffer segment uses local memory.

### 3.2 Memory Accounting

- **Per-query memory budgets**: Each query gets a memory budget based on available memory and
  estimated needs. If exceeded, gracefully spill to disk (sort spill, hash spill).
- **Memory grant feedback** (from SQL Server IQP): Track actual vs. estimated memory usage per
  query pattern. Adjust future grants based on observed behavior. No DBA intervention needed.
- **Cooperative memory pressure**: When the system approaches memory limits, background tasks
  (compaction, prefetch) voluntarily reduce their memory footprint before eviction becomes
  aggressive.

---

## 4. Concurrency & MVCC

### 4.1 MVCC Design

**Current**: Tuple-header MVCC with xmin/xmax and undo log.

**Target design** (PostgreSQL-style visibility + SQL Server version store benefits):

- **In-row versioning** (keep current approach): Store xmin/xmax in the tuple header. This avoids
  the tempdb version store bottleneck that SQL Server suffers from.
- **Undo log for old versions** (keep current approach): Old versions go to the undo log, not
  inline like PostgreSQL. This avoids PostgreSQL's heap bloat and vacuum dependency.
- **Snapshot isolation by default**: Like PostgreSQL's Repeatable Read. Each transaction sees a
  consistent snapshot. Readers never block writers.
- **Serializable Snapshot Isolation (SSI)**: Implement PostgreSQL's SSI for true serializability.
  Track read-write dependencies using lightweight predicate locks. Detect dangerous cycles and
  abort one transaction. No performance penalty for read-only transactions.

### 4.2 Isolation Levels

| Level | Behavior | Use Case |
|---|---|---|
| Read Committed | New snapshot per statement | Default, general OLTP |
| Repeatable Read | Transaction-level snapshot | Consistency-sensitive reads |
| Serializable (SSI) | Full serializability via dependency tracking | Financial, safety-critical |

### 4.3 Lock-Free Concurrency (No Locks)

DragonflyDB proved that a shared-nothing, thread-per-core architecture eliminates the need for
traditional locking entirely. GrapheneDB adopts this principle fully:

- **No lock manager, no lock table, no mutexes on the hot path**.
- Every shard is owned by exactly one thread. Single-shard operations execute with zero
  synchronization -- no locks, no atomics, no contention.

**How every "lock use case" is handled without locks:**

| Traditional Lock Use Case | Lock-Free Solution |
|---|---|
| Read-write isolation | MVCC snapshots (xmin/xmax). Readers see a snapshot, never block writers. |
| Write-write conflicts (same row) | Optimistic version check. Second writer detects xmax was set, aborts and retries. |
| Cross-shard coordination | Message passing (VLL-style). Each shard-thread has a command queue; coordinator dispatches messages. No shared mutable state. |
| DDL vs DML (schema changes) | Epoch-based quiescence. Bump epoch, wait for in-flight operations to drain, apply schema change. |
| Serializable isolation (SSI) | Dependency tracking via rw-conflict graph, not predicate locks. Detect dangerous cycles and abort one transaction. |
| Application-level coordination | Advisory "locks" implemented as key-value entries with optimistic CAS semantics (no actual OS/mutex locks). |

**Write-write conflict resolution**:

When two transactions try to update the same row concurrently (within the same shard-thread,
which processes them sequentially via fiber scheduling):

1. Transaction A reads row, sees xmax = 0 (live). Writes new version, sets old row's xmax = A.
2. Transaction B reads row, sees xmax = A.
   - If A is committed: B aborts with serialization error (under Repeatable Read/Serializable).
   - If A is still in-progress: B waits for A to commit/abort (via fiber yield, not a lock).
   - If A is aborted: B proceeds as if A never existed.

This is pure MVCC conflict detection -- no lock acquisition, no lock table, no deadlocks.

**Cross-shard transactions** (inspired by DragonflyDB's VLL framework):

For multi-row operations spanning multiple shards (e.g., `UPDATE ... WHERE` affecting rows on
different shard-threads):

1. Coordinator determines which shards are involved.
2. Sends intent messages to each shard-thread's queue.
3. Each shard-thread processes its portion when it reaches the message.
4. Shard-threads signal completion back to coordinator.
5. Coordinator commits or aborts atomically.

No mutexes, no spinlocks, no shared memory between threads. Just message queues.

**Why no deadlocks are possible**:

- Single-shard operations are sequential within a thread (fiber-based) -- no circular waits.
- Cross-shard operations use a coordinator pattern with a fixed ordering -- no cycles.
- Write-write conflicts are resolved by abort, not by waiting for a lock.

---

## 5. Indexing

### 5.1 B+Tree Indexes (Keep & Enhance)

**Current**: B+Tree with i32 keys, leaf chain for range scans.

**Enhancements**:

- **Variable-width keys**: Support string keys, composite keys, and arbitrary key types. Use
  prefix compression within index pages (store common prefix once per page).
- **Included columns** (from SQL Server): Store additional non-key columns in the leaf level
  only. Creates covering indexes without bloating the tree structure.
- **Suffix truncation** (from PostgreSQL): In non-leaf pages, store only enough of the key to
  distinguish between children. Dramatically reduces non-leaf page size for string keys.
- **Bottom-up deletion**: When deleting from a B+Tree, use bottom-up deletion to reclaim empty
  pages efficiently, reducing tree height when data shrinks.

### 5.2 Hash Indexes

- **Bucket-chained hash index**: For equality-only lookups (WHERE id = X). O(1) point lookups,
  no ordering support.
- **Extendible hashing**: Grow the hash table incrementally by splitting one bucket at a time
  (no full rehash). Inspired by DragonflyDB's Dashtable segmented design.
- **Concurrency**: Lock-free reads with optimistic validation on the bucket chain.

### 5.3 BRIN-Style Indexes (New)

Inspired by PostgreSQL's Block Range INdex and ClickHouse's sparse primary indexes:

- Store min/max (and optionally bloom filter) per range of pages (e.g., per extent).
- Tiny index size (1000x smaller than B+Tree for large tables).
- Ideal for append-only tables with timestamp columns, log data, time-series.
- Maintained automatically during writes -- no explicit creation needed for the extent-level
  version. Explicit `CREATE INDEX ... USING BRIN` for custom configurations.

### 5.4 Filtered Indexes (New)

From SQL Server: Create an index with a WHERE clause that indexes only a subset of rows.

```sql
CREATE INDEX idx_active_orders ON orders (customer_id)
WHERE status = 'active';
```

Smaller, faster to maintain, better statistics for the filtered subset. Extremely useful for
columns with skewed distributions.

### 5.5 Adaptive Indexing (Future)

Inspired by database cracking and adaptive merging:

- **Query-driven organization**: Incrementally sort/partition data in response to query patterns.
  First query on a column does initial partitioning; subsequent queries refine it.
- **Automatic index suggestions**: Track query patterns and suggest (or auto-create) indexes
  based on observed workload. Display suggestions via `EXPLAIN ANALYZE`.

---

## 6. Query Processing & Optimization

### 6.1 Parser (Keep & Enhance)

**Current**: Recursive-descent parser with operator precedence climbing.

**Enhancements**:

- **Expanded SQL surface**: See section 13 for full SQL feature coverage.
- **Better error messages**: Include position information, expected tokens, and suggestions.
  "Expected expression, got ')' at line 3, column 15. Did you mean to close the function call?"
- **Prepared statement improvements**: Support both positional ($N) and named (@name) parameters
  (already done). Add prepared statement caching (parse once, execute many).

### 6.2 Query Optimizer

**Current**: Cost-based optimizer comparing seq scan vs index scan with join ordering.

**Target architecture** (incremental, pragmatic approach):

**Phase 1 (Current+)**:
- **Statistics collection**: Maintain per-column histograms (up to 200 buckets), most common
  values, n_distinct, null fraction, and correlation (physical vs. logical ordering). Collect
  automatically during bulk load and periodically via background ANALYZE.
- **Join ordering**: Dynamic programming for up to 12 tables (like PostgreSQL). For larger joins,
  use a greedy heuristic (not genetic algorithm -- too unpredictable).
- **Join method selection**: Cost-based choice between nested-loop, hash join, and merge join.
  Consider index availability for index nested-loop joins.

**Phase 2 (Adaptive)**:
- **Adaptive joins** (from SQL Server IQP): At runtime, a hash join can switch to nested-loop
  based on actual input cardinality. Buffer input rows up to a threshold; if exceeded, use hash
  join.
- **Cardinality feedback**: Track actual vs estimated cardinalities per query pattern. Adjust
  future estimates based on observed errors. Store feedback persistently.
- **Interleaved execution** (from SQL Server): For subqueries and CTEs, execute them first to
  get actual cardinalities before optimizing the rest of the query.

**Phase 3 (Advanced)**:
- **Extended statistics**: Multi-column statistics for functional dependencies and correlated
  predicates (from PostgreSQL). `CREATE STATISTICS` syntax.
- **Predicate pushdown**: Push WHERE clauses through joins, subqueries, and views.
- **Aggregate pushdown**: Push aggregates below joins when possible.
- **Partition pruning**: At plan time and execution time, eliminate partitions that cannot
  contain matching rows.

### 6.3 Plan Cache

- Cache compiled query plans keyed by SQL text + schema version.
- **Prepared statements**: Always cache. Re-plan only when statistics change significantly or
  schema changes.
- **Ad-hoc queries**: Cache with LRU eviction. Use parameterized plan matching -- recognize
  that `SELECT * FROM t WHERE id = 1` and `SELECT * FROM t WHERE id = 2` can share a plan.

---

## 7. Execution Engine

### 7.1 Vectorized Execution (New -- Top Priority)

Replace the current Volcano (row-at-a-time) model with **vectorized push-based execution**
inspired by DuckDB.

**Core design**:

- **Data chunks**: Process batches of 2048 rows at a time. Each chunk contains column vectors
  (arrays of values for each column) and a selection vector (bit mask of active rows).
- **Column-at-a-time operators**: Each operator processes one column at a time. A filter on
  column A never touches columns B, C, D.
- **Push-based**: Operators push results to their parent (producer-driven) rather than parents
  pulling results (consumer-driven). Eliminates per-row virtual function call overhead.
- **SIMD-accelerated primitives**: Use Zig's `@Vector` types for:
  - Comparisons (filter evaluation)
  - Aggregations (SUM, MIN, MAX, COUNT)
  - Hash computation (for hash joins and hash aggregation)
  - String operations (prefix matching, LIKE patterns)
- **Late materialization**: Pass column references and selection vectors through the pipeline.
  Only materialize full result tuples at the very end when sending to the client.
- **Branch-free filter evaluation**: Convert comparison results to 0/1 without branches,
  avoiding branch misprediction penalties.

**Example of the performance difference**:

```
Row-at-a-time (current):
  for each row:
    call GetNext() -- virtual function call
    evaluate filter -- branch per row
    if passes, call GetNext() on parent -- another virtual call
  ~3 function calls + 1 branch per row

Vectorized (target):
  get chunk of 2048 rows
  evaluate filter on column vector -- SIMD, branch-free
  pass selection vector to next operator
  ~1 SIMD instruction per 8-16 values, no branches
```

### 7.2 Morsel-Driven Parallelism (New)

Inspired by DuckDB's morsel-driven parallelism:

- Divide input data into **morsels** (chunks of ~10K rows).
- Worker threads grab morsels from a shared work queue.
- Each operator maintains thread-local state (hash table partition, sort run, etc.).
- At pipeline boundaries (blocking operators like sort, hash build), merge thread-local states.
- **Elastic parallelism**: Threads can be reassigned between queries dynamically based on load.

Benefits over traditional exchange-operator parallelism:
- No fixed partitioning at plan time
- Work stealing balances load automatically
- Can adapt degree of parallelism at runtime

### 7.3 Pipeline Execution

Organize the query plan into **pipelines** -- sequences of non-blocking operators that can
process data in a streaming fashion:

```
Pipeline 1: Scan -> Filter -> Hash Build (for join)
Pipeline 2: Scan -> Filter -> Hash Probe -> Project -> Aggregate
```

Pipeline breaks occur at blocking operators (sort, hash build, aggregate with grouping). Each
pipeline is independently parallelizable.

---

## 8. Compression

### 8.1 Row-Store Compression

- **Page-level dictionary encoding**: For pages with repeated string values, build a per-page
  dictionary and replace values with short codes. Transparent to reads (decompress on access).
- **Prefix compression for indexes**: Common key prefixes stored once per page.
- **Null compression**: Run-length encode null bitmaps.

### 8.2 Columnar Compression (for Vectorized Engine)

When data is processed in columnar vectors, apply lightweight compression:

| Technique | Best For | Compression | Speed |
|---|---|---|---|
| Dictionary encoding | Low-cardinality strings | 5-20x | Decode: ~2GB/s |
| Run-Length Encoding | Sorted columns, booleans | 10-100x | Decode: ~4GB/s |
| Bit-packing | Small integers, codes | 2-8x | Decode: ~5GB/s (SIMD) |
| Frame of Reference (FOR) | Clustered numerics | 2-4x | Decode: ~5GB/s |
| Delta encoding | Timestamps, sequences | 3-10x | Decode: ~4GB/s |
| FSST | Variable-length strings | 2-4x | Decode: ~3GB/s |

**Adaptive compression selection**: At comptime, generate specialized compression/decompression
kernels for each encoding type using Zig's comptime. At runtime, select the best encoding per
column vector based on data characteristics (cardinality, sortedness, value range).

### 8.3 Large Value Compression

For TOAST-ed values (>2KB), use Zstd compression with dictionary training. The dictionary is
trained on a sample of the column's values and stored in the catalog. Typical 3-5x compression
on text data.

---

## 9. WAL & Recovery

### 9.1 WAL Design (Keep & Enhance)

**Current**: 3-phase ARIES-style WAL with redo, undo, and checkpoint.

**Enhancements**:

- **Group commit**: Batch multiple transaction commits into a single WAL flush. Dramatically
  improves throughput for small transactions (from ~1000 TPS to ~50,000 TPS on SSD).
- **WAL compression**: Compress WAL records before writing. Reduces WAL volume by 2-4x,
  reducing both disk I/O and replication bandwidth.
- **Parallel WAL replay**: During recovery, identify independent WAL records (affecting different
  pages) and replay them in parallel across multiple threads.
- **Full-page writes on first modification**: After each checkpoint, the first write to any page
  includes the full page image (PostgreSQL approach). Protects against torn pages. Subsequent
  writes to the same page before the next checkpoint write only the diff.

### 9.2 Checkpoint Design

- **Fuzzy checkpoints**: Don't flush all dirty pages at once. Spread writes over the checkpoint
  interval to avoid I/O spikes (inspired by SQL Server's indirect checkpoint and PostgreSQL's
  checkpoint_completion_target).
- **Incremental checkpoints**: Only write pages that have been modified since the last checkpoint.
  Track dirty pages via a dirty page bitmap.

### 9.3 Snapshotting (from DragonflyDB)

For point-in-time snapshots (backups, replication):

- **Version-based snapshots**: Assign a version counter to each entry. Capture the current epoch.
  A background fiber iterates over all data, serializing entries with version <= epoch. Concurrent
  writes trigger a hook that serializes the old value before modification.
- **No memory spikes**: Unlike PostgreSQL's fork-based or Redis's copy-on-write approach, this
  doesn't duplicate memory. Old values are serialized to the snapshot stream immediately.
- **Two modes**: Conservative (point-in-time semantics, serializes old values before mutation) and
  relaxed (streams changes as they happen, for replication).

---

## 10. Threading & I/O Model

### 10.1 Thread-Per-Core Architecture

Inspired by ScyllaDB's Seastar and DragonflyDB's Helio:

- **One thread per CPU core**: No context switching between threads. Each thread owns its data
  partition, its buffer pool segment, and its I/O queue.
- **Shared-nothing within a process**: Data is partitioned across threads. Each thread processes
  queries on its own partition without locks. Cross-partition queries use message passing.
- **Fiber-based concurrency within each thread**: Use Zig's async or a lightweight fiber library
  for cooperative multitasking within each thread. Multiple connections are served concurrently
  via fibers, not OS threads.

### 10.2 I/O Model

**Current**: QUIC networking with MsQuic.

**Target I/O architecture**:

- **io_uring for disk I/O** (Linux): Batch I/O submissions, registered buffers, kernel polling
  mode for NVMe. Inspired by TigerBeetle's io_uring usage.
  - Pre-register buffer pool pages with io_uring to avoid per-I/O DMA mapping
  - Batch dirty page flushes into a single io_uring_submit call
  - Use direct I/O (O_DIRECT) to bypass the OS page cache (we manage our own buffer pool)
- **Windows I/O Completion Ports**: For Windows, use IOCP with the same batching patterns.
- **QUIC networking** (keep): Already implemented. QUIC provides multiplexed streams, built-in
  TLS, and connection migration. Unique advantage over TCP-based databases.

### 10.3 Connection Handling

- **Fiber per connection**: Each client connection gets a lightweight fiber (not an OS thread).
  Thousands of concurrent connections with minimal overhead.
- **Connection affinity**: Pin connections to the thread that owns most of their data. Migrate
  connections between threads when access patterns change (inspired by DragonflyDB's connection
  migration for Lua scripts).
- **No external pooler needed**: The fiber-per-connection model handles thousands of connections
  natively, unlike PostgreSQL which requires pgBouncer.

---

## 11. Network Protocol

### 11.1 QUIC-Based Protocol (Keep & Enhance)

**Current**: Custom binary protocol over QUIC with TLS and TOFU certificate pinning.

**Enhancements**:

- **Multiplexed query streams**: Use QUIC streams to multiplex multiple in-flight queries on a
  single connection. Each query gets its own stream -- no head-of-line blocking.
- **Server-side cursors**: Stream large result sets back to the client without buffering the
  entire result in memory. The client requests rows in chunks.
- **Columnar result format**: Optionally send results in columnar format (column vectors instead
  of row tuples). Reduces serialization overhead for analytical queries with many rows.
- **Compressed transport**: Use QUIC's stream-level flow control combined with Zstd compression
  for large result sets.

### 11.2 PostgreSQL Wire Protocol Compatibility (Future)

For tool compatibility, support a PostgreSQL wire protocol frontend:

- Accept connections from psql, pgAdmin, JDBC/ODBC PostgreSQL drivers.
- Translate PostgreSQL wire protocol messages to internal query execution.
- This dramatically expands the ecosystem of tools that work with GrapheneDB.

---

## 12. Type System

### 12.1 Core Types

Inspired by PostgreSQL's extensible type system:

| Category | Types | Notes |
|---|---|---|
| Integer | i8, i16, i32, i64 | Fixed-width, SIMD-friendly |
| Float | f32, f64 | IEEE 754 |
| Decimal | DECIMAL(p, s) | Exact arithmetic for financial data |
| String | TEXT, VARCHAR(n) | UTF-8, small string optimization |
| Binary | BYTEA | Arbitrary binary data |
| Boolean | BOOL | 1 byte storage |
| Temporal | TIMESTAMP, TIMESTAMPTZ, DATE, TIME, INTERVAL | Microsecond precision |
| UUID | UUID | 16 bytes, native support |
| JSON | JSONB | Binary JSON with indexing support |
| Array | T[] | Arrays of any type |
| Network | INET, CIDR | IP address types |

### 12.2 Key Design Decisions

- **JSONB with path indexing**: Store JSON in a decomposed binary format (like PostgreSQL's JSONB).
  Support GIN-style indexing on JSON paths. Enable containment queries (`@>`), existence (`?`),
  and path queries (`->`, `->>`).
- **Range types**: Support range types for temporal and numeric ranges. Enable exclusion
  constraints for non-overlapping ranges (e.g., room bookings). Unique PostgreSQL feature.
- **Composite types**: Every table implicitly defines a composite type. Can be used as column
  types and function parameters.
- **Domain types**: Constrained subtypes with CHECK constraints. `CREATE DOMAIN email AS TEXT
  CHECK (VALUE ~ '^.+@.+$')`.

---

## 13. SQL Feature Surface

### 13.1 Current SQL Support

SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX, JOINs (INNER, LEFT,
RIGHT, CROSS), WHERE, ORDER BY, LIMIT, GROUP BY, HAVING, aggregates (COUNT, SUM, AVG, MIN, MAX),
subqueries, prepared statements, EXPLAIN.

### 13.2 Priority SQL Features to Add

**Tier 1 (Essential)**:
- ALTER TABLE (add/drop/rename columns, add/drop constraints)
- UPSERT (INSERT ... ON CONFLICT DO UPDATE/NOTHING)
- Common Table Expressions (WITH, WITH RECURSIVE)
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM OVER, etc.)
- CASE expressions
- DISTINCT, DISTINCT ON
- UNION, UNION ALL, INTERSECT, EXCEPT
- EXISTS, IN (subquery)
- COALESCE, NULLIF, CAST
- String functions (LIKE, ILIKE, SUBSTRING, TRIM, LOWER, UPPER, etc.)
- Date/time functions (NOW, EXTRACT, DATE_TRUNC, AGE, etc.)
- Transaction control (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)

**Tier 2 (Competitive)**:
- Declarative partitioning (PARTITION BY RANGE, LIST, HASH)
- Materialized views (with manual REFRESH)
- Generated columns (STORED)
- LATERAL joins
- Array operations and unnesting
- JSON operators and functions
- COPY for bulk import/export
- CREATE VIEW

**Tier 3 (Advanced)**:
- Temporal tables (system-versioned, inspired by SQL Server)
- Foreign data wrappers (query external data sources)
- Exclusion constraints (non-overlapping ranges)
- Full-text search
- Logical replication
- Advisory locks
- Automatic index suggestions

---

## 14. Observability & Self-Tuning

### 14.1 Query Store (from SQL Server)

Persist query execution history:

- Store execution plans, runtime statistics (duration, rows, I/O, memory), and wait statistics
  per query.
- **Plan regression detection**: Automatically identify when a query gets slower after a plan
  change.
- **Plan pinning**: Allow users to force a known-good plan for a specific query.
- **Automatic tuning**: Detect regressions and automatically revert to the previous plan.

### 14.2 EXPLAIN ANALYZE Enhancements

- **Per-operator timing**: Show actual time spent in each operator, not just estimated.
- **Buffer hit/miss statistics**: Show how many pages were found in the buffer pool vs. read
  from disk.
- **Memory usage**: Show actual memory consumed by each operator (hash tables, sort buffers).
- **Cardinality accuracy**: Show estimated vs actual row counts at each operator, highlighting
  estimation errors.

### 14.3 System Catalog Views

Expose internal state via queryable views:

- `graphene_stat_tables` -- Row counts, dead tuples, last analyze time
- `graphene_stat_indexes` -- Index usage statistics, scan counts
- `graphene_stat_queries` -- Query execution history (like pg_stat_statements)
- `graphene_stat_buffers` -- Buffer pool hit rates, eviction counts
- `graphene_stat_wal` -- WAL write rates, checkpoint statistics
- `graphene_stat_connections` -- Active connections, queries, wait states

### 14.4 Automatic Index Suggestions

Based on observed query patterns:

```sql
EXPLAIN ANALYZE SELECT * FROM orders WHERE customer_id = 42;
-- Output includes:
-- Suggestion: CREATE INDEX idx_orders_customer ON orders (customer_id)
--             would improve this query by ~100x (seq scan -> index scan)
```

---

## 15. Testing Strategy

### 15.1 Deterministic Simulation Testing (from TigerBeetle)

Since GrapheneDB is written in Zig (like TigerBeetle), adopt deterministic simulation testing:

- **Abstract all non-determinism**: I/O, time, networking, random number generation all go
  through interfaces. In test mode, replace with deterministic simulations.
- **Seed-based reproducibility**: Every test run is deterministic given a seed. A failing test
  can be reproduced exactly.
- **Fault injection**: Simulate disk corruption, network partitions, crashes at arbitrary points,
  slow I/O, out-of-memory conditions.
- **Invariant checking**: After every operation, verify structural invariants (B-tree ordering,
  page consistency, WAL integrity, MVCC visibility correctness).

### 15.2 Correctness Testing

- **SQL logic tests**: Port SQLite's sqllogictest suite for SQL correctness validation.
- **Crash recovery tests**: Kill the database at random points, verify that recovery produces a
  consistent state.
- **Concurrency tests**: Run many concurrent transactions with randomized operations, verify
  isolation guarantees (no dirty reads, no phantom reads under serializable).

### 15.3 Performance Regression Testing

- **Continuous benchmarking**: Run the benchmark suite (TPC-B, single/multi-thread point lookups,
  range scans, joins, aggregations) on every commit.
- **P99 latency tracking**: Not just throughput -- track tail latency to detect spikes from
  checkpointing, compaction, or GC.

---

## 16. Feature Roadmap Priorities

### Phase 1: Foundation (Current + Near Term)

Focus: Make the core engine solid and fast.

1. **Vectorized execution engine** -- Single biggest performance win
2. **Group commit** -- Essential for write throughput
3. **Statistics collection & histogram-based optimizer** -- Better query plans
4. **ALTER TABLE, UPSERT, CTEs, window functions** -- Essential SQL surface
5. **Transaction control (BEGIN/COMMIT/ROLLBACK)** -- Usability requirement

### Phase 2: Competitiveness

Focus: Match the features developers expect.

6. **Filtered indexes, included columns** -- Index flexibility
7. **BRIN-style extent indexes** -- Large table scan optimization
8. **Hash indexes** -- Point lookup optimization
9. **Adaptive joins and memory grant feedback** -- Self-tuning
10. **Query store** -- Observability and plan management
11. **Declarative partitioning** -- Large table management
12. **JSONB with indexing** -- Modern application needs

### Phase 3: Excellence

Focus: Unique advantages.

13. **Morsel-driven parallelism** -- Multi-core scaling
14. **io_uring / IOCP integration** -- I/O optimization
15. **Columnar compression** -- Analytical query acceleration
16. **PostgreSQL wire protocol** -- Ecosystem compatibility
17. **Deterministic simulation testing** -- Reliability assurance
18. **Temporal tables** -- Built-in history tracking
19. **Automatic index suggestions** -- Self-tuning DBA-free operation

---

## Appendix A: Key Technical Decisions & Rationale

### Why PostgreSQL SQL dialect over MySQL?

- PostgreSQL has superior SQL standard compliance
- Better type system (JSONB, range types, arrays, composite types)
- CTEs, window functions, lateral joins are first-class
- Developer community strongly prefers PostgreSQL semantics
- Serializable Snapshot Isolation is unique and valuable

### Why vectorized over Volcano execution?

- 10-50x faster for analytical queries (SIMD + no per-row overhead)
- Still fast for OLTP (vectorized point lookups have minimal overhead vs row-at-a-time)
- DuckDB proved this architecture works for embedded databases
- Zig's `@Vector` types make SIMD natural

### Why thread-per-core over thread pool?

- Eliminates lock contention entirely within each thread's partition
- Better cache locality (each core's data stays in its cache hierarchy)
- Proven by ScyllaDB (10x throughput vs Cassandra) and DragonflyDB (25x vs Redis)
- Zig's explicit memory model makes shared-nothing natural

### Why 2Q over LRU/ARC for buffer eviction?

- LRU is vulnerable to scan pollution (one large scan evicts everything)
- ARC is patented (IBM) and complex
- 2Q has been proven more robust with higher hit rates than LRU
- DragonflyDB achieved zero per-item overhead with their 2Q implementation
- Ring buffers for scans provide additional protection

### Why fully lock-free over traditional locking?

- DragonflyDB proved shared-nothing + message passing eliminates locks entirely at 3.8M+ QPS
- Thread-per-core means single-shard operations have zero contention by design
- MVCC already provides read-write isolation without locks
- Write-write conflicts are resolved by version checks and aborts, not lock waits
- No locks means no deadlocks -- eliminates an entire class of bugs and DBA headaches
- Cross-shard coordination via message passing (VLL) scales linearly with cores
- Lock managers are complex, memory-hungry, and add latency to every operation

### Why undo-log MVCC over in-heap versioning?

- PostgreSQL's in-heap versioning causes bloat and requires vacuum
- SQL Server's tempdb version store is a single-instance bottleneck
- Undo log keeps the heap compact (current version always in-place)
- Old versions in undo log are naturally cleaned up in order (FIFO)
- Undo log can be on separate storage for I/O isolation

### Why QUIC over TCP?

- Built-in TLS (no separate TLS layer needed)
- Multiplexed streams (no head-of-line blocking)
- Connection migration (client IP changes don't break connections)
- 0-RTT connection resumption (lower latency for reconnects)
- Already implemented and working in GrapheneDB

---

## Appendix B: Competitive Comparison

| Feature | GrapheneDB (Target) | PostgreSQL | SQL Server | DragonflyDB |
|---|---|---|---|---|
| Execution model | Vectorized push | Volcano (row) | Row + Batch | N/A (KV) |
| SIMD acceleration | Yes (@Vector) | No (JIT only) | Yes (columnstore) | Limited |
| Threading | Thread-per-core | Process-per-conn | Thread pool | Thread-per-core |
| Buffer eviction | 2Q + ring buffers | Clock-sweep + ring | LRU-K | 2Q (zero overhead) |
| MVCC | Undo log | In-heap | tempdb version store | Version-based |
| Concurrency | Lock-free (MVCC + VLL) | Locks + MVCC | Lock manager + RCSI | Lock-free (shared-nothing) |
| Serializable | SSI | SSI | Lock-based only | N/A |
| Network | QUIC | TCP | TCP/TDS | TCP/RESP |
| Compression | Adaptive columnar | TOAST/pglz | Columnstore/page | CompactObj |
| IO model | io_uring/IOCP | buffered I/O | SQLOS async I/O | io_uring |
| Self-tuning | Adaptive joins, etc | No | IQP suite | No |
| Type system | PostgreSQL-like | Extensible | Fixed | Redis types |
| Testing | Deterministic sim | Standard | Standard | Standard |
