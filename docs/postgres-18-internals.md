# PostgreSQL 18 Internals

PostgreSQL 18 was released on September 25, 2025. This document covers the internal architecture changes, new features, and code-level details.

---

## Table of Contents

1. [Asynchronous I/O (AIO) Subsystem](#1-asynchronous-io-aio-subsystem)
2. [Query Planner / Optimizer](#2-query-planner--optimizer)
3. [Query Executor](#3-query-executor)
4. [Storage Engine](#4-storage-engine)
5. [Index Improvements](#5-index-improvements)
6. [VACUUM and Maintenance](#6-vacuum-and-maintenance)
7. [SQL Features and Syntax](#7-sql-features-and-syntax)
8. [System Catalog Changes](#8-system-catalog-changes)
9. [Type System](#9-type-system)
10. [Transaction Management / MVCC](#10-transaction-management--mvcc)
11. [Replication](#11-replication)
12. [Wire Protocol 3.2](#12-wire-protocol-32)
13. [Authentication and Security](#13-authentication-and-security)
14. [Monitoring and Observability](#14-monitoring-and-observability)
15. [pg_upgrade](#15-pg_upgrade)
16. [libpq Changes](#16-libpq-changes)
17. [Contrib Modules](#17-contrib-modules)
18. [Partitioning](#18-partitioning)
19. [Foreign Data Wrappers](#19-foreign-data-wrappers)
20. [New GUC Parameters Summary](#20-new-guc-parameters-summary)

---

## 1. Asynchronous I/O (AIO) Subsystem

The single most significant architectural change in PG18. Replaces the traditional synchronous blocking I/O model with an async one for read operations. Led by Andres Freund, Thomas Munro, Melanie Plageman, and Nazir Bilal Yavuz.

### I/O Methods

Controlled by the `io_method` GUC (requires restart):

| Method | Description |
|--------|-------------|
| `sync` | Legacy synchronous behavior (same as PG17). Uses `posix_fadvise()` for read-ahead hints. Uses `preadv`/`pwritev` on the backend process. |
| `worker` **(default)** | Dedicated background I/O worker processes handle async reads. Backend writes request into shared queue; an idle worker makes the blocking `read()` call. Data is placed in shared buffers and the backend is notified via latch. |
| `io_uring` | Linux's `io_uring` interface (kernel 5.1+). Eliminates worker processes. Uses shared ring buffers (submission queue + completion queue) between Postgres and the kernel. One `io_uring` instance per backend. Requires `--with-liburing` build flag. |

### Supported Operations

- Sequential scans
- Bitmap heap scans
- VACUUM operations
- ANALYZE operations

**Not yet supported:** Index scans, write operations (including WAL writes remain synchronous).

### Internal Data Structures

Defined in:
- `src/include/storage/aio.h` -- main AIO interface header
- `src/backend/storage/aio/README.md` -- design documentation
- `src/backend/storage/aio/method_io_uring.c` -- io_uring implementation
- `src/backend/storage/aio/method_worker.c` -- worker implementation

Key concepts:
- **AIO handles** (`PgAioHandle`): Acquired at a higher level, passed to lower levels to be fully defined. Lowest-level functions: `pgaio_io_start_*()`.
- Handles can be reused as soon as completed. Acquisition must always succeed (limited pool).
- State updates via **AIO Completion callbacks**.
- The issuing backend provides a backend-local variable to receive the I/O result.

### Read Stream API

PG17 introduced the `ReadStream` API as an internal abstraction. PG18 removes the indirection by enabling true asynchronous reads directly into shared buffers.

Key functions:
- `read_stream_begin_relation()` -- initializes streaming reads, takes a callback to get the next block
- `heap_scan_stream_read_next()` -- streaming read callback for heap scans
- `heapgettup_initial_block()`, `heapgettup_advance_block()` -- block tracking helpers

**Bitmap Heap Scan integration:** Uses the read stream API instead of `ReadBuffer()` per block. A read stream callback uses the bitmap iterator to return the next required block. Explicit prefetching removed from bitmap heap scan code.

**Sequential Scan integration:** `heapgettup()` and `heapgettup_pagemode()` now wait for valid buffers (not `BlockNumber`s). Streaming reads prefetch ahead of the currently-processed block.

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `io_method` | `worker` | I/O dispatch method |
| `io_combine_limit` | 128kB (16 pages) | Adjacent blocks combined into single I/O |
| `io_max_combine_limit` | 128kB (up to 1MB on Unix) | Maximum combined I/O size |
| `io_workers` | 3 | Dedicated AIO worker processes (worker mode) |
| `io_max_concurrency` | -1 (auto, max 64) | Max concurrent I/O operations per process |
| `effective_io_concurrency` | **16** (was 1) | Expected concurrent disk I/O |
| `maintenance_io_concurrency` | **16** (was 1) | Same for maintenance operations |

### Monitoring

- `pg_aios` system view: shows file handles and in-flight async I/O operations
- Enhanced `pg_stat_io` view (see Monitoring section)
- Wait event `IO / AioIoCompletion` when backend waits for I/O worker

### Performance

- 2-4x speedups on NVMe/cloud EBS for read-heavy analytic workloads
- Formula: max read-ahead = `effective_io_concurrency * io_combine_limit`

### Direct I/O Status

`debug_io_direct` developer parameter exists (`data`, `wal`, `wal_init`) but is DEBUG-only, not for production. Full Direct I/O support expected in PG19+.

---

## 2. Query Planner / Optimizer

### B-tree Skip Scan (Peter Geoghegan)

Allows multicolumn B-tree indexes to be used even when leading columns lack equality constraints.

**Mechanism:**
- Generates **dynamic equality constraints** internally for each distinct value in the omitted leading column(s)
- Core function: `_bt_advance_array_keys()` manages progression through index values as non-overlapping accesses in key space order
- Scan repositions via repeated index searches from end of one grouping to start of the next
- Preprocessing transforms `low_compare` and `high_compare` nbtree skip array inequalities

**Skip support function infrastructure:**
- Registered under **support function number 6** in B-tree operator families
- API in `src/include/utils/skipsupport.h`
- Iterates through every representable value in key space order
- Operator classes without skip support use a fallback strategy
- Cross-type skipsupport functions are not allowed
- Not sensible for continuous types

**EXPLAIN:** Reports count of **Index Searches** per index scan node. No new plan node type -- standard `Index Scan` / `Index Only Scan` in output.

### Self-Join Elimination (Alena Rybakina, Alexander Korotkov)

Removes inner joins of a plain table to itself when provably redundant. Over 7 years in development.

**Algorithm:**
1. Collect all merge-joinable join quals of the form `a.x = b.x`
2. Add the inner table's `baserestrictinfo` to the list
3. Call `innerrel_is_unique()` -- SJE proof is based on this machinery
4. Inner and outer clauses must have exact match

**Replacement procedure:**
- Inner relation replaced with outer in query tree, equivalence classes, and planner info
- Inner restrictlist moves to outer with duplicate clause removal
- Partly combined with the useless-left-join removal procedure

**GUC:** `enable_self_join_elimination` (boolean, default: `on`)

### OR-Clause to ScalarArrayOp (Alexander Korotkov, Andrey Lepikhov)

Transforms `(indexkey op C1) OR (indexkey op C2) ... OR (indexkey op CN)` into `indexkey op ANY(ARRAY[C1, C2, ...])` during index clause matching. Fix applied for `RelabelType` node handling.

### IN(VALUES...) to ANY Conversion (Alena Rybakina, Andrei Lepikhov)

Converts `x IN (VALUES (a), (b), (c))` to `x = ANY(ARRAY[a, b, c])` for better selectivity estimation.

### SELECT DISTINCT Key Reordering

Planner internally reorders DISTINCT keys to match existing index pathkeys, avoiding unnecessary sorts. Also applies to `DISTINCT ON`.

**GUC:** `enable_distinct_reordering` (boolean, default: `on`)

### GROUP BY Functionally Dependent Column Elimination (Zhang Mingli, Jian He, David Rowley)

If `GROUP BY` includes all columns of a unique index plus additional columns from the same table, the additional columns are dropped as redundant. Extended from non-deferred primary keys to any unique index.

### Disabled Plan Node Handling Overhaul (David Rowley)

**Previous:** `enable_*` parameters added `disable_cost = 1e10`, distorting cost statistics.

**New:** Planner keeps a **count of disabled nodes** per path. Path selection: fewest disabled nodes first, then lowest cost. EXPLAIN shows `Disabled: true` with **normal cost statistics**.

### Hash Right Semi Join in Parallel Mode

Planner can choose which table is hashed for semi-joins, enabling `Hash Right Semi Join` in parallel plans.

### Materialization for Parallel Nested Loop Joins

Previously never considered materializing for parallel nested loops. PG18 allows the Materialize node within parallel nested loop joins.

### Merge Join with Incremental Sort

Merge joins can use incremental sorting when outer set is partially sorted.

### Planner Support for generate_series

`SupportRequestRows` support functions for `generate_series` (including timestamp and numeric variants) for accurate row count estimation.

### Statistics Preservation Through Upgrades

`pg_upgrade` preserves optimizer statistics. `pg_dump`/`pg_dumpall` can dump/restore basic statistics.

### PlaceHolderVar Statistics Lookup

Planner looks through `PlaceHolderVar` nodes when searching for expression statistics from pulled-up subqueries.

### Partial Hash Index Support

Allows indexscans on partial hash indexes when the predicate implies WHERE clause truth.

### GROUPING SETS Optimization

Can push HAVING conditions down to WHERE when appropriate.

---

## 3. Query Executor

### ExecScan() Refactoring and Inlining (Amit Langote, David Rowley)

`ExecScan()` logic moved into **inline-able** `ExecScanExtended()` in `src/include/executor/execScan.h`.

**Signature:** Accepts parameters for `EvalPlanQual` state, qualifiers (`ExprState *`), and projection (`ProjectionInfo *`).

**Optimization:** Specialized scan variants (e.g., `ExecSeqScan()`) pass `const-NULL` for unused parameters. Compiler inlines and eliminates branches, generating specialized code paths.

**Files:** `src/backend/executor/execScan.c`, `src/backend/executor/nodeSeqscan.c`, `src/include/executor/execScan.h` (365 insertions, 203 deletions).

**Performance:** ~5% reduction in execution time for sequential-scan-heavy queries.

### Hash Join Improvements (David Rowley, Jeff Davis)

Improved performance and reduced memory usage, especially on multiple columns. 30-50% improvements for complex sorting on partially indexed columns.

### INTERSECT/EXCEPT/Window Aggregate Speedups (Tom Lane, David Rowley)

Improved hash set operations and hash lookups of subplan values.

### Tuplestore Memory Optimization

Switched `tuplestore.c` to use **Generation memory contexts** instead of ExecutorState `aset.c` contexts. Reduced maximum storage by roughly half (e.g., 16577kB to 8577kB). New `inMem` boolean tracks whether `maxSpace` represents memory or disk. EXPLAIN shows storage type ("Memory" or "Disk") for Materialize, Window Aggregate, and CTE nodes.

### Hardware Acceleration

- **ARM NEON and SVE** CPU intrinsics for `popcount` (used by `bit_count`): >3x speedup for larger buffers
- **x86 AVX-512** for CRC32 calculations (page checksums)
- New `crc32()` and `crc32c()` built-in functions

---

## 4. Storage Engine

### Data Checksums Enabled by Default

`initdb` now enables page checksums by default (`--no-data-checksums` to disable).

- Each data page includes a checksum updated on write, verified on read
- Uses **FNV-1a hash** (CRC32C for WAL records)
- Block address is part of the calculation (detects bit flips and misplaced pages)
- Performance overhead: typically <2%
- `pg_upgrade` requires source and target to match checksum settings

### Virtual Generated Columns

Compute values at read time; occupy **no storage** on disk. Now the **default** for generated columns.

- Stored internally as null values in tuples (space-efficient compromise)
- Expression expansion happens in the **optimizer** (not rewriter) due to outer join complications
- Adding a virtual column is instant (metadata-only, no table rewrite)
- `ExecBuildSlotValueDescription()` now accepts `Relation` (not just `Oid`) for generation expression in error messages

**Catalog:** `pg_attribute.attgenerated`: `'v'` (virtual), `'s'` (stored), `'\0'` (non-generated)

**Restrictions:** Cannot have user-defined types, cannot use user-defined functions, cannot be indexed (work in progress for PG19), cannot be logically replicated.

### Heap Storage

- Heap remains the default table access method
- HOT (Heap-Only Tuples) optimization unchanged
- Every table and index stored as array of fixed-size pages (usually 8 kB)
- Table access method API requires each tuple to have a TID (block number + item number)

---

## 5. Index Improvements

### B-tree Skip Scan

See [Query Planner section](#b-tree-skip-scan-peter-geoghegan). Implementation in `src/backend/access/nbtree/`.

### Parallel GIN Index Build (Tomas Vondra, Matthias van de Meent)

**Algorithm:**
1. Each worker builds index entries on a table subset via parallel scan
2. Workers use a **local tuplesort** to sort/merge entries for the same key
3. TID lists don't overlap per key; merge sort concatenates lists
4. Merged entries written to **shared tuplesort** for the leader
5. Leader merges sorted entries and writes into the index

**Performance:** ~45% faster parallel GIN creation vs. serial; 2-4x speedups on large datasets. Configured via `maintenance_work_mem` and `max_parallel_maintenance_workers`.

New `gin_index_check()` in `amcheck` for GIN consistency verification.

### GiST and btree_gist Sorted Build

Range type sortsupport added: GiST indexes can use the **sorted build** method (much faster than the buffered one-by-one insertion method). `btree_gist` builds in sorted mode by default. Revert to buffered via the `buffering` parameter.

### BRIN Parallel Build

Supports parallel builds. Includes backfilling empty ranges at end of table. Bug fixes for `numeric_minmax_multi_ops` and integer-overflow in scans near 2^32 pages.

### Temporal Constraint Indexes

`WITHOUT OVERLAPS` constraints use **GiST indexes** (not B-tree) -- essentially exclusion constraints with `=` for scalar parts and `&&` for the temporal range part. Requires range type columns (`daterange`, `tstzrange`, `tsrange`).

### Non-B-tree Unique Indexes

Can now be used as partition keys and in materialized views (must still support equality).

---

## 6. VACUUM and Maintenance

### Eager Freeze Optimization

Normal VACUUM proactively freezes tuples on all-visible pages, spreading freeze workload.

**Mechanism:**
- When VACUUM encounters an all-visible page, it attempts to freeze it
- If all tuples are old enough, the page is marked `all-frozen` in the visibility map
- Successful page freezes capped at **20% of all-visible but not all-frozen pages**
- Only freeze failures count against the cap

**Control:** `vacuum_max_eager_freeze_failure_rate` (default `0.03` / 3%). Set to `0` to disable. Per-table configurable:
```sql
ALTER TABLE events SET (vacuum_max_eager_freeze_failure_rate = 0.10);
```

### Dynamic Autovacuum Worker Management

- `autovacuum_worker_slots` (default 16): reserves shared memory at startup (requires restart)
- `autovacuum_max_workers`: controls active slots, **now reloadable** at runtime via `ALTER SYSTEM` + `pg_reload_conf()`
- `autovacuum_max_workers` capped at `autovacuum_worker_slots`

### Hard Cap for Large Tables

`autovacuum_vacuum_max_threshold`: caps the calculated threshold, forcing autovacuum to trigger earlier on big tables regardless of percentage-based calculation. Per-table configurable.

### Recursive Processing

VACUUM and ANALYZE now process inheritance children by default. Use `ONLY` keyword to target parent-level only:
```sql
VACUUM ONLY t;
ANALYZE ONLY t;
```

### AIO-Accelerated VACUUM

VACUUM benefits from AIO, overlapping reads with processing. Up to 2-3x faster for I/O-bound operations.

### Enhanced VACUUM Observability

New columns in `pg_stat_all_tables`:
- `total_vacuum_time`, `total_autovacuum_time`
- `total_analyze_time`, `total_autoanalyze_time`

VACUUM/ANALYZE VERBOSE output includes full WAL buffer count, CPU, WAL, and average read statistics.

### Additional GUCs

- `vacuum_truncate`: controls file truncation during VACUUM (requires `ACCESS EXCLUSIVE` lock)
- `track_cost_delay_timing`: records actual sleep time from cost-based delays in `pg_stat_progress_vacuum`

---

## 7. SQL Features and Syntax

### RETURNING with OLD/NEW Aliases (commit 80feb727c8, Dean Rasheed)

```sql
-- Basic
UPDATE t SET col = val WHERE ... RETURNING OLD.col AS old_val, NEW.col AS new_val;

-- Custom aliases
DELETE FROM t WHERE ... RETURNING WITH (OLD AS before, NEW AS after) before.*, after.*;

-- MERGE with merge_action()
MERGE INTO target USING source ON ...
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT ...
  RETURNING merge_action(), OLD.*, NEW.*;
```

| Operation | OLD | NEW |
|-----------|-----|-----|
| INSERT | NULL | populated |
| DELETE | populated | NULL |
| UPDATE | pre-modification | post-modification |
| MERGE | depends on action per row | depends on action per row |

### Temporal Constraints

**PRIMARY KEY / UNIQUE with WITHOUT OVERLAPS:**
```sql
CREATE TABLE bookings (
    room_id int,
    booking_period tstzrange,
    PRIMARY KEY (room_id, booking_period WITHOUT OVERLAPS)
);
```

**FOREIGN KEY with PERIOD:**
```sql
CREATE TABLE assignments (
    emp_id int,
    assignment_period daterange,
    FOREIGN KEY (emp_id, PERIOD assignment_period)
        REFERENCES employees (emp_id, PERIOD valid_period)
);
```

- Automatically creates GiST indexes (uses `&&` overlap operator)
- Temporal FKs check range **containment** (not equality)
- ON DELETE/ON UPDATE actions not supported for temporal FKs
- Catalog: `conperiod` boolean in `pg_constraint`

### Virtual Generated Columns

```sql
CREATE TABLE t (
    a int,
    b int GENERATED ALWAYS AS (a * 2)          -- virtual by default in PG18
);
CREATE TABLE t (
    a int,
    b int GENERATED ALWAYS AS (a * 2) VIRTUAL  -- explicit virtual
);
CREATE TABLE t (
    a int,
    b int GENERATED ALWAYS AS (a * 2) STORED   -- explicit stored (old behavior)
);
```

### NOT NULL Constraint Rework

NOT NULL constraints are now first-class entries in `pg_constraint` (`contype = 'n'`).

```sql
-- Named NOT NULL
CREATE TABLE t (
    id int CONSTRAINT id_not_null NOT NULL
);

-- NOT NULL as NOT VALID (zero-downtime migration)
ALTER TABLE large_table ADD CONSTRAINT col_nn NOT NULL col NOT VALID;
ALTER TABLE large_table VALIDATE CONSTRAINT col_nn;  -- no ACCESS EXCLUSIVE lock
```

### NOT ENFORCED Constraints (SQL:2023)

```sql
ALTER TABLE t ADD CONSTRAINT chk CHECK (col > 0) NOT ENFORCED;
ALTER TABLE t ADD FOREIGN KEY (col) REFERENCES other(id) NOT ENFORCED;
ALTER TABLE t ALTER CONSTRAINT chk ENFORCED;
ALTER TABLE t ALTER CONSTRAINT chk NOT ENFORCED;
```

- Default is ENFORCED
- NOT ENFORCED skips all checks (different from NOT VALID which skips existing data, and DEFERRABLE which delays)
- For FKs: NOT ENFORCED skips trigger creation; switching to ENFORCED recreates them
- Catalog: `conenforced` boolean in `pg_constraint`

### COPY FROM Improvements

```sql
COPY t FROM 'file.csv' WITH (
    ON_ERROR 'ignore',       -- discard bad rows
    REJECT_LIMIT 100,        -- fail after 100 bad rows
    LOG_VERBOSITY 'verbose'  -- NOTICE per discarded row with line/column
);
```

### EXPLAIN Improvements

- `EXPLAIN ANALYZE` now includes **BUFFERS by default**
- `SERIALIZE TEXT | SERIALIZE BINARY`: measures serialization time
- Buffer info included for subplans, triggers, and functions
- EXPLAIN ANALYZE VERBOSE includes CPU time, WAL writes, average read times
- Index scan nodes report count of index lookups

### New Functions

```sql
-- UUIDv7 (timestamp-ordered)
SELECT uuidv7();
SELECT uuidv7('5 minutes'::interval);   -- time offset
SELECT uuid_extract_timestamp(uuidv7());
SELECT uuid_extract_version(uuidv7());  -- returns 7
SELECT uuidv4();                         -- alias for gen_random_uuid()

-- Array operations
SELECT array_sort(ARRAY[3,1,2]);         -- {1,2,3}
SELECT array_sort(ARRAY[3,1,2], 'desc'); -- {3,2,1}
SELECT array_reverse(ARRAY[1,2,3]);      -- {3,2,1}

-- Other
SELECT casefold(text);                   -- Unicode case folding
SELECT reverse(bytea_val);              -- reverse bytea bytes
SELECT crc32(data);
SELECT crc32c(data);
```

### New Aggregates

- `MIN(anyarray)`, `MAX(anyarray)` -- lexicographic comparison
- `MIN(record)`, `MAX(record)` -- field-by-field comparison
- `MIN(bytea)`, `MAX(bytea)`

---

## 8. System Catalog Changes

### pg_constraint

New columns:
| Column | Type | Description |
|--------|------|-------------|
| `conenforced` | `bool NOT NULL` | Whether constraint is enforced |
| `conperiod` | `bool` | Whether constraint is temporal |

NOT NULL constraints now stored as `contype = 'n'`.

Constraint types (`contype`):
- `'c'` = check, `'f'` = foreign key, `'p'` = primary key, `'u'` = unique
- `'t'` = constraint trigger, `'x'` = exclusion
- **`'n'` = not null (new in PG18)**

### pg_attribute

| Column | Values | Notes |
|--------|--------|-------|
| `attgenerated` | `'v'` (virtual), `'s'` (stored), `'\0'` (none) | `'v'` new in PG18 |
| `attnotnull` | bool | Still present but `pg_constraint` is now authoritative |

### pg_proc Additions

New built-in functions:
- `uuidv7()`, `uuidv4()`, `array_sort()`, `array_reverse()`
- `reverse(bytea)`, `casefold(text)`, `crc32()`, `crc32c()`
- `pg_get_acl(oid, oid)`, `pg_stat_get_backend_wal()`, `pg_stat_get_backend_io()`
- `pg_ls_summariesdir()`

---

## 9. Type System

### Integer <-> Bytea Casting

```sql
SELECT 255::integer::bytea;           -- '\x000000ff' (4 bytes, big-endian)
SELECT 255::smallint::bytea;          -- '\x00ff' (2 bytes)
SELECT 255::bigint::bytea;            -- '\x00000000000000ff' (8 bytes)
SELECT '\x000000ff'::bytea::integer;  -- 255
```

Big-endian, two's complement. Fixed output size per integer type. Error if bytea exceeds integer width.

### UUIDv7 Implementation

- 48-bit millisecond timestamp (Unix epoch)
- 12-bit sub-millisecond fraction (Postgres-specific, ~250ns precision; ~1us on macOS)
- 60 bits total for timestamp (48 + 12)
- `rand_a` field stores sub-millisecond precision (not purely random)
- Within same backend, `rand_a` incremented per generation for monotonic ordering
- Handles ~4 million generations/second
- Uses neither mutex nor atomic variables
- Optional `interval` parameter for time shifting with overflow protection

### Unicode/Collation

**PG_UNICODE_FAST collation:**
- Built-in collation for UTF-8 databases
- Code point sort order (fast, `memcmp`-based) with full Unicode character semantics
- Full case mapping: `upper('ss')` correctly returns `'SS'`

**casefold() function:** Unicode case folding (handles multi-character case variations).

**LIKE with nondeterministic collations:** Now supported. Implementation in `match_pattern_prefix()` in `like_support.c`. Enables case-insensitive LIKE with ICU collations.

---

## 10. Transaction Management / MVCC

### Eager Freezing

See [VACUUM section](#eager-freeze-optimization). Amortizes freezing across normal vacuums, preventing anti-wraparound VACUUM storms.

### Fast-Path Lock Improvements

**Previous:** Fixed 16 fast-path lock slots per backend (in `PGPROC`). Partitioned tables with many indexes quickly exhausted this.

**PG18:** Fast-path locks stored in **variable-sized arrays in separate shared memory** (referenced via pointers from `PGPROC`). Array size determined at startup based on `max_locks_per_transaction` (default: 64). Eliminates the 16-slot bottleneck, reducing `LWLock:LockManager` contention.

### log_lock_failures GUC

New GUC to log lock acquisition failures. Currently supports `SELECT ... NOWAIT` failures. Useful for analyzing lock contention.

---

## 11. Replication

### Logical Replication Conflict Detection

New conflict counter columns in `pg_stat_subscription_stats`:

| Column | Description |
|--------|-------------|
| `confl_insert_exists` | INSERT violated NOT DEFERRABLE unique constraint |
| `confl_update_origin_differs` | UPDATE row has different origin |
| `confl_update_exists` | UPDATE row already exists with different key |
| `confl_update_missing` | UPDATE target row missing |
| `confl_delete_origin_differs` | DELETE row has different origin |
| `confl_delete_missing` | DELETE target row missing |
| `confl_multiple_unique_conflicts` | Multiple unique constraints violated simultaneously |

When `track_commit_timestamp` is enabled, detailed conflict logging includes origin and commit timestamp.

### Generated Column Replication

`publish_generated_columns` publication option controls replication of generated columns (previously never replicated).

### Streaming Default Changed

`CREATE SUBSCRIPTION` streaming default: `off` -> `parallel`. Large transactions (> `logical_decoding_work_mem`) applied via parallel apply workers.

### Idle Slot Cleanup

`idle_replication_slot_timeout` GUC (default: 0/disabled). Slots inactive beyond this interval are invalidated at next checkpoint.

### max_active_replication_origins

New GUC decoupling origin count from `max_replication_slots` (default: 10). Only settable at server start.

### pg_createsubscriber Enhancements

- `--all`: Creates one subscription per database
- `--clean`: Removes specified object types during conversion
- `--enable-two-phase`: Enables two-phase commit for subscriptions

### pg_recvlogical

New `--enable-failover` option for failover slot specification (with `--create-slot`).

### pg_logicalinspect Extension

New contrib module for inspecting logical decoding:
- `pg_get_logical_snapshot_meta(filename)`: Snapshot metadata from `$PGDATA/pg_logical/snapshots/`
- `pg_get_logical_snapshot_info(filename)`: Detailed snapshot info with xmin/xmax, LSN

---

## 12. Wire Protocol 3.2

First protocol version change since 3.0 in PostgreSQL 7.4 (2003). Fully backward compatible.

### 256-Bit Cancel Request Keys

- **Protocol 3.0:** Fixed 4-byte (32-bit) key (~4.3 billion values)
- **Protocol 3.2:** Variable-length key up to 256 bytes; server sends 32-byte (256-bit) keys (~1.1 x 10^77 values)

Prevents brute-force cancellation attacks feasible with 32-bit keys.

### BackendKeyData Changes

Key length is now variable (min 4, max 256 bytes). Larger maximum allows connection poolers to augment keys with routing metadata.

### CancelRequest Changes

Secret key field is variable-length, extending to end of message. Cancel request code unchanged: high 16 bits = 1234, low 16 bits = 5678.

---

## 13. Authentication and Security

### OAuth 2.0 Authentication

Server acts as resource server validating bearer tokens from external identity providers.

**pg_hba.conf:**
```
host all all 0.0.0.0/0 oauth issuer="https://idp.example.com" scope="openid"
```

**postgresql.conf:**
```
oauth_validator_libraries = 'your_oauth_validator'
```

**Validator Module API:**
```c
typedef bool (*ValidatorValidateCB)(
    const ValidatorModuleState *state,
    const char *token,
    const char *role,
    ValidatorModuleResult *result
);

typedef struct {
    bool authorized;
    char *authn_id;  // palloc'd authenticated user name (or NULL)
} ValidatorModuleResult;
```

Parameters: `issuer` (must exactly match token), `scope`, `map` (for pg_ident.conf), `delegate_ident_mapping`.

Requires `--with-libcurl` build flag.

### TLS 1.3 Cipher Configuration

`ssl_tls13_ciphers` GUC. Default: `TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256`.

### MD5 Deprecation

MD5 password auth deprecated. `CREATE ROLE` / `ALTER ROLE` emit deprecation warnings. Controlled by `md5_password_warnings` GUC.

### SCRAM Passthrough

`postgres_fdw` and `dblink` support `use_scram_passthrough` to pass client SCRAM auth to remote servers without storing credentials.

### FIPS Mode

pgcrypto's `builtin_crypto_enabled` parameter controls built-in crypto functions. SHA-2 encryption supported for password hashing.

---

## 14. Monitoring and Observability

### pg_stat_io Enhancements

- WAL I/O now tracked (previously only in `pg_stat_wal`)
- New columns: `read_bytes`, `write_bytes`, `extend_bytes` (replaced `op_bytes` since operations vary in size)
- WAL receiver activity included
- `track_wal_io_timing` controls WAL timing in `pg_stat_io`

### Per-Backend Statistics

- `pg_stat_get_backend_io(pid)`: I/O statistics per backend
- `pg_stat_get_backend_wal(pid)`: WAL statistics per backend
- `pg_stat_reset_backend_stats()`: Clear per-backend stats

### Vacuum/Analyze Timing

`pg_stat_all_tables` new columns: `total_vacuum_time`, `total_autovacuum_time`, `total_analyze_time`, `total_autoanalyze_time`.

### pg_stat_statements

Inner queries from `CREATE TABLE AS` and `DECLARE CURSOR` now get query IDs and appear in `pg_stat_statements`.

### pg_overexplain Extension

New contrib module providing `DEBUG` option for EXPLAIN showing internal plan tree details.

**Per plan node** (from `Plan` struct in `nodes/plannodes.h`):
- `Disabled Nodes`, `Parallel Safe`, `Scan RTI`, `Nominal RTI`, `Exclude Relation RTI`, `Append RTIs`

**Per query** (from `PlannedStmt` struct):
- `hasReturning`, `hasModifyingCTE`, `canSetTag`, `transientPlan`, `dependsOnRole`, `parallelModeNeeded`

Also provides an extension API for custom EXPLAIN output.

---

## 15. pg_upgrade

### --swap Transfer Mode (New, Fastest)

Swaps directories from old cluster to new, then replaces catalog files. Requires same filesystem. Old cluster becomes inaccessible once transfer begins.

### All Transfer Modes

1. `--copy`: Traditional file copy
2. `--copy-file-range`: Uses `copy_file_range()` syscall (Linux/FreeBSD)
3. `--clone`: Filesystem reflinks
4. `--link`: Hard links
5. **`--swap`**: Directory swap (new in PG18)

### Parallel Processing

`--jobs` enables parallel database/tablespace processing (not just data transfer).

### Statistics Preservation

Runs `pg_dump --with-statistics` by default. Transfers basic optimizer statistics (tables, columns, expression indexes). Extended statistics not preserved (need `ANALYZE`).

New `pg_dump`/`pg_dumpall` options: `--with-statistics`, `--no-statistics`, `--statistics-only`.
New `vacuumdb` option: `--missing-stats-only`.

---

## 16. libpq Changes

### Protocol Version Negotiation

- `min_protocol_version` / `max_protocol_version` connection parameters
- `PGMINPROTOCOLVERSION` / `PGMAXPROTOCOLVERSION` environment variables
- Auto-downgrade if server doesn't support requested version
- libpq defaults to protocol 3.0; drivers must opt into 3.2

### New Functions

- `PQfullProtocolVersion()`: Returns full version (e.g., 3.2)
- `PQprotocolVersion()`: Returns major only (3) -- not suitable for 3.0 vs 3.2

### Cancel Changes

`PGgetCancel()` and `PGcancelCreate()` return error (not non-functional object) when server didn't send `BackendKeyData`. libpq no longer sends `CancelRequest` for such connections.

---

## 17. Contrib Modules

### amcheck
- New `gin_index_check()` for GIN indexes
- Fixes for btree parent checks, "half-dead" pages, incomplete root page splits

### pg_trgm
- Clusters using non-libc collation (ICU, builtin) may need to **reindex all pg_trgm indexes**

### pgcrypto
- Fixed buffer overrun in PGP decryption
- `builtin_crypto_enabled` parameter
- SHA-2 password hashing support

### intarray
- Fixed selectivity estimation exploitable for arbitrary code execution

### ltree
- Fixed inconsistent case-insensitive matching. Requires reindexing ltree indexes.

### passwordcheck
- New `min_password_length` configurable variable

### pg_logicalinspect
- New module for inspecting logical decoding snapshots

### pg_overexplain
- New module for internal EXPLAIN plan tree details

---

## 18. Partitioning

- NOT VALID foreign keys on partitioned tables now allowed
- Dropping constraints ONLY on partitioned tables now permitted
- Non-B-tree unique indexes as partition keys
- Improved partition-wise join support with reduced memory
- VACUUM/ANALYZE automatically process children (use `ONLY` for parent-only)
- Unlogged partitioned tables disallowed

---

## 19. Foreign Data Wrappers

### postgres_fdw
- SCRAM passthrough (`use_scram_passthrough`)
- New columns: `used_in_xact`, `closed`, `user_name`, `remote_backend_pid`

### file_fdw
- `on_error`, `log_verbosity`, `reject_limit` options

### General
- `CREATE FOREIGN TABLE ... LIKE` for schema creation from local tables
- NOT NULL constraints on foreign tables
- `COPY FREEZE` on foreign tables explicitly disallowed

---

## 20. New GUC Parameters Summary

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `io_method` | enum | worker | AIO method: sync, worker, io_uring |
| `io_workers` | integer | 3 | AIO background workers |
| `io_combine_limit` | integer | 128kB | I/O request combining |
| `io_max_combine_limit` | integer | 128kB | Max I/O combine |
| `io_max_concurrency` | integer | -1 (auto) | Max concurrent I/O per process |
| `effective_io_concurrency` | integer | 16 | Concurrent async read-ahead |
| `maintenance_io_concurrency` | integer | 16 | I/O concurrency for maintenance |
| `enable_self_join_elimination` | bool | on | Self-join removal |
| `enable_distinct_reordering` | bool | on | DISTINCT key reordering |
| `vacuum_max_eager_freeze_failure_rate` | float | 0.03 | Eager freeze failure rate cap |
| `autovacuum_vacuum_max_threshold` | integer | - | Hard cap for large table vacuum threshold |
| `vacuum_truncate` | bool | true | File truncation during VACUUM |
| `track_cost_delay_timing` | bool | off | Track vacuum cost-delay sleep |
| `log_lock_failures` | bool | off | Log lock acquisition failures |
| `idle_replication_slot_timeout` | duration | 0 | Auto-invalidate idle slots |
| `max_active_replication_origins` | integer | 10 | Max active replication origins |
| `oauth_validator_libraries` | string | - | OAuth token validator libraries |
| `ssl_tls13_ciphers` | string | (see above) | TLS 1.3 cipher suites |
| `md5_password_warnings` | bool | on | MD5 deprecation warnings |

---

## Sources

- [PostgreSQL 18 Release Notes](https://www.postgresql.org/docs/current/release-18.html)
- [PostgreSQL 18 Released! (Announcement)](https://www.postgresql.org/about/news/postgresql-18-released-3142/)
- [Crunchy Data: Get Excited About Postgres 18](https://www.crunchydata.com/blog/get-excited-about-postgres-18)
- [Neon: PostgreSQL 18 New Features](https://neon.com/postgresql/postgresql-18-new-features)
- [Better Stack: What's New in PostgreSQL 18](https://betterstack.com/community/guides/databases/postgresql-18-new-features/)
- [Bytebase: What's New in PostgreSQL 18](https://www.bytebase.com/blog/what-is-new-in-postgres-18/)
- [Xata: Going Down the Rabbit Hole of Postgres 18](https://xata.io/blog/going-down-the-rabbit-hole-of-postgres-18-features)
- [pganalyze: Accelerating Disk Reads with AIO](https://pganalyze.com/blog/postgres-18-async-io)
- [CYBERTEC: PostgreSQL 18 AIO](https://www.cybertec-postgresql.com/en/postgresql-18-better-i-o-performance-with-aio/)
- [CYBERTEC: From AIO to Direct IO](https://www.cybertec-postgresql.com/en/postgresql-18-and-beyond-from-aio-to-direct-io/)
- [credativ: PostgreSQL 18 AIO Deep Dive](https://www.credativ.de/en/blog/postgresql-en/postgresql-18-asynchronous-disk-i-o-deep-dive-into-implementation/)
- [Aiven: PostgreSQL 18 AIO](https://aiven.io/blog/exploring-why-postgresql-18-put-asynchronous-io-in-your-database)
- [pgEdge: Skip Scan](https://www.pgedge.com/blog/postgres-18-skip-scan-breaking-free-from-the-left-most-index-limitation)
- [CYBERTEC: Index Skip Scans](https://www.cybertec-postgresql.com/en/postgresql-18-more-performance-with-index-skip-scans/)
- [CYBERTEC: btree_gist Improvements](https://www.cybertec-postgresql.com/en/btree_gist-improvements-in-postgresql-18/)
- [Microsoft: Vacuuming Improvements](https://techcommunity.microsoft.com/blog/adforpostgresql/postgresql-18-vacuuming-improvements-explained/4459484)
- [pgMustard: Enable Parameters in PG18](https://www.pgmustard.com/blog/enable-parameters-work-differently-in-postgres-18)
- [pgMustard: Index Searches in EXPLAIN](https://www.pgmustard.com/blog/what-do-index-searches-in-explain-mean)
- [Phoronix: Self-Join Elimination](https://www.phoronix.com/news/PostgreSQL-Self-Join-Eliminate)
- [EDB: Virtual Generated Columns](https://www.enterprisedb.com/blog/journey-virtual-generated-columns)
- [EDB: NOT NULL Rework](https://www.enterprisedb.com/blog/changes-not-null-postgres-18)
- [Crunchy Data: OLD and NEW in RETURNING](https://www.crunchydata.com/blog/postgres-18-old-and-new-in-the-returning-clause)
- [Crunchy Data: Data Checksums Default](https://www.crunchydata.com/blog/postgres-18-new-default-for-data-checksums-and-how-to-deal-with-upgrades)
- [CYBERTEC: Statistics Preserved During Upgrade](https://www.cybertec-postgresql.com/en/preserve-optimizer-statistics-during-major-upgrades-with-postgresql-v18/)
- [credativ: Data Checksums by Default](https://www.credativ.de/en/blog/postgresql-en/postgresql-18-enables-datachecksums-by-default/)
- [Aiven: Temporal Constraints](https://aiven.io/blog/exploring-how-postgresql-18-conquered-time-with-temporal-constraints)
- [DBI Services: pg_upgrade Swap Mode](https://www.dbi-services.com/blog/postgresql-18-swap-mode-for-pg_upgrade/)
- [DBI Services: REJECT_LIMIT for COPY](https://www.dbi-services.com/blog/postgresql-18-reject_limit-for-copy/)
- [OpenSourceDB: PG_UNICODE_FAST](https://opensource-db.com/pg18-hacktober-31-days-of-new-features-internationalization-pg_unicode_fast-collation/)
- [OpenSourceDB: Conflict Detection](https://opensource-db.com/postgresql-18-breaking-the-conflict-deadlock-in-logical-replication/)
- [PostgresAI: Fast-path Locking](https://postgres.ai/blog/20251008-postgres-marathon-2-004)
- [PostgresPro CommitFest Reports](https://postgrespro.com/blog/pgsql/5971632)
- [depesz: Various PG18 Feature Posts](https://www.depesz.com/)
- [pgPedia: PostgreSQL 18](https://pgpedia.info/postgresql-versions/postgresql-18.html)
- [PostgreSQL Doxygen: aio.h](https://doxygen.postgresql.org/aio_8h_source.html)
- [GitHub: Self-Join Elimination Commit](https://github.com/postgres/postgres/commit/fc069a3a6319b5bf40d2f0f1efceae1c9b7a68a8)
