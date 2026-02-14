# GrapheneDB Architecture

GrapheneDB is an embedded relational database engine written in Zig with a QUIC-based network protocol. It implements a full SQL database stack from storage to network layer.

## Project Structure

```
GrapheneDB/
  build.zig              # Build configuration (links msquic + OpenSSL)
  build.zig.zon          # Package manifest (min Zig 0.16.0-dev)
  src/
    main.zig             # CLI entry point: server, client, REPL modes
    engine.zig            # Module re-exports (no network deps)
    storage/
      disk_manager.zig    # Raw page I/O (read/write 8KB pages)
      page.zig            # Slotted page format with header + slot directory
      buffer_pool.zig     # LRU page cache with pin counting
      alloc_map.zig       # Extent-based allocation (GAM/PFS/SGAM)
      iam.zig             # Index Allocation Map (extent ownership)
      filegroup.zig       # Logical file grouping
      file_header.zig     # Database file header (page 0)
      data_dir.zig        # Directory layout + PID locking
      catalog.zig         # System catalog (tables, columns, indexes)
      table.zig           # Heap-organized table with extent storage
      tuple.zig           # Value types, schema, tuple serialization
      wal.zig             # Write-Ahead Log (segment-based)
      mvcc.zig            # Transaction manager + snapshot isolation
      undo_log.zig        # In-memory undo log for rollback
    parser/
      lexer.zig           # SQL tokenizer (~85 token types)
      ast.zig             # Abstract Syntax Tree definitions
      parser.zig          # Recursive-descent SQL parser
    planner/
      plan.zig            # Plan node types (seq_scan, index_scan, filter, sort, limit)
      planner.zig         # Cost-based query optimizer
    executor/
      executor.zig        # SQL execution engine (~4000 lines)
    index/
      btree.zig           # B+Tree implementation
      btree_page.zig      # B+Tree on-disk node format
    network/
      msquic.zig          # MsQuic C FFI bindings
      tls.zig             # OpenSSL FFI for cert generation
      protocol.zig        # GP wire protocol (framed messages)
      server.zig          # QUIC server with per-connection executors
      client.zig          # QUIC client with TOFU cert pinning
  bench/
    zig/                  # Native benchmarks (direct executor)
    python/               # Network benchmarks (QUIC client via aioquic)
    results/              # CSV benchmark results
```

## Approximate Line Counts

| Layer | Files | ~Lines |
|-------|-------|--------|
| Storage | 14 files | ~9,500 |
| Parser | 3 files | ~2,000 |
| Planner | 2 files | ~1,060 |
| Executor | 1 file | ~4,000 |
| Index | 2 files | ~1,500 |
| Network | 5 files | ~1,750 |
| Entry points | 2 files | ~950 |
| Bench (Zig) | 6 files | ~790 |
| Bench (Python) | 5 files | ~660 |
| Build | 2 files | ~185 |
| **Total** | **42 files** | **~22,400** |

---

## Storage Layer

### Disk Manager (`disk_manager.zig`)

Low-level file I/O. Reads and writes 8KB pages at specific offsets. Manages file open/close/delete and tracks page count.

- `PAGE_SIZE`: 8192 bytes
- `INVALID_PAGE_ID`: max u32
- `readPage()` / `writePage()`: Single-page I/O at calculated offset
- `allocatePage()`: Extend file with a zeroed page
- `allocateExtent()`: Allocate 8 contiguous pages
- `flush()`: fsync to disk

### Page (`page.zig`)

In-memory slotted page with a header, slot directory growing forward, and tuple data growing backward.

```
[PageHeader][Slot 0][Slot 1]...  →→  ←←  ...[Tuple 1][Tuple 0]
```

- `PageHeader`: page_id, LSN, slot_count, free_space_offset, flags
- `Slot`: offset + length pointing to tuple data
- `insertTuple()`: Allocates from the end, adds slot entry
- `getTuple()` / `deleteTuple()` / `updateTupleData()`: Slot-based access
- `tupleIterator()`: Iterates valid (non-empty) slots

### Buffer Pool (`buffer_pool.zig`)

LRU page cache sitting between disk and all upper layers.

- `Frame`: Holds page data, pin count, dirty flag, page_id
- `LruReplacer`: Doubly-linked list + hash map for O(1) eviction
- `fetchPage()`: Returns cached page or loads from disk, pins it
- `unpinPage()`: Decrements pin count; adds to LRU when count hits 0
- `flushPage()`: Writes dirty page to disk
- `newPage()`: Allocates fresh page via disk manager
- `checkpoint()`: Flushes all dirty unpinned pages

### Allocation Manager (`alloc_map.zig`)

Extent-based space management using SQL Server-style bitmaps:

- **GAM** (Global Allocation Map): 1 bit per extent, tracks free/used
- **PFS** (Page Free Space): Per-page entry with free space category (0-4), page type, allocation flags
- **SGAM** (Shared Global Allocation Map): Tracks mixed extents (partially used)

Constants:
- `EXTENT_SIZE`: 8 pages (64KB)
- `PFS_ENTRIES_PER_PAGE`: ~8188
- `GAM_EXTENTS_PER_PAGE`: ~65472

Two allocation models:
- **Uniform extents**: 8 contiguous pages for large tables
- **Mixed extents**: Individual pages from shared extents for small tables

### IAM (`iam.zig`)

Index Allocation Map pages track which extents belong to a specific table or index.

- Bitmap-based extent ownership tracking
- `MAX_SINGLE_PAGE_SLOTS`: 8 (for mixed extent pages)
- `IamChainIterator`: Walks the IAM chain yielding all owned extent IDs
- Supports chaining multiple IAM pages for large objects

### File Header (`file_header.zig`)

Page 0 of every data file. Stores bootstrap metadata.

- Magic: `"GDB\x01"` (0x47444201)
- `FILE_FORMAT_VERSION`: 1
- `SYSTEM_PAGES`: 6 (reserved pages 0-5)
- `FIRST_DATA_PAGE`: 6
- Persists catalog table IDs for recovery

### Data Directory (`data_dir.zig`)

Manages the physical directory layout on the filesystem:

```
<root>/
  GDB_VERSION           # Format version check
  graphenedb.pid        # PID lock file
  graphenedb.conf       # Configuration
  data/primary.gdb      # Main data file
  wal/                  # WAL segments
  wal/archive/          # Archived segments
  tls/server.p12        # TLS certificate
```

- `initNew()` / `openExisting()` / `openOrInit()`: Lifecycle management
- `acquirePidLock()` / `releasePidLock()`: Mutual exclusion

### Catalog (`catalog.zig`)

System metadata manager using three internal heap tables:

- **gp_tables**: (table_id, name, column_count)
- **gp_columns**: (table_id, ordinal, name, type, max_length, nullable)
- **gp_indexes**: (index_id, table_id, index_name, column_ordinal, is_unique)

Key operations:
- `createTable()` / `dropTable()`: DDL with cascade delete of indexes
- `openTable()`: Reconstruct schema from catalog, return Table handle
- `createIndex()` / `dropIndex()`: B-tree index lifecycle
- `listTables()`: Enumerate all user tables
- Bootstrap IDs persisted in file header for recovery

### Table (`table.zig`)

Heap-organized storage with IAM-based extent tracking and optional MVCC.

- `TableMeta`: IAM page ID + tuple count (stored in slot 0 of first page)
- `insertTuple()`: Finds page with space (hint page -> IAM walk -> new extent)
- `getTuple()` / `getTupleTxn()`: Retrieve with optional MVCC visibility
- `deleteTuple()`: Sets xmax (MVCC) or zeros slot (legacy)
- `updateTuple()`: Delete + insert pattern
- `scan()` / `scanWithTxn()`: IAM chain iteration with optional visibility filtering
- `rollback()`: Replays undo chain to restore pre-transaction state
- `MAX_TUPLE_SIZE`: ~7.5KB

### Tuple (`tuple.zig`)

Value types, schema definitions, and serialization.

**Value** (tagged union):
- `null_value`, `boolean`, `integer` (i32), `bigint` (i64), `float` (f64), `bytes` ([]const u8)

**Schema**: Array of `Column` (name, col_type, max_length, nullable)

**TupleHeader** (16 bytes, for MVCC):
- `xmin`: Creator transaction ID
- `xmax`: Deleter transaction ID (0 = live)
- `undo_ptr`: Pointer into undo log

**Serialization format**: `[null_bitmap][col1_data][col2_data]...`
- Fixed types encoded directly (bool=1B, int=4B, bigint/float=8B)
- Variable types length-prefixed with u16

**Hash join support**:
- `Value.hash()`: Canonicalized hashing (int/bigint/float produce same hash for equal values)
- `Value.eqlForJoin()`: Cross-type numeric equality

### WAL (`wal.zig`)

Write-Ahead Log for crash recovery, segment-based.

- `DEFAULT_SEGMENT_MAX_SIZE`: 64MB
- Record types: BEGIN, COMMIT, ABORT, UPDATE, CHECKPOINT
- `LogRecordHeader`: LSN, txn_id, prev_lsn, record_type, data_length, checksum
- `logBegin()` / `logCommit()` / `logAbort()` / `logUpdate()`: Transaction logging
- `flush()`: Write buffered records to disk

**Recovery protocol** (3-phase):
1. **Analysis**: Build committed/active transaction sets
2. **Redo**: Replay committed transaction updates
3. **Undo**: Reverse uncommitted changes

### MVCC (`mvcc.zig`)

Multi-Version Concurrency Control with snapshot isolation.

- `Transaction`: txn_id, state, snapshot, undo chain head
- `Snapshot`: xmin, xmax, active transaction list
- `TransactionManager`: Allocates txn IDs, manages active/committed sets

Isolation levels:
- `SNAPSHOT_ISOLATION` (default): Point-in-time snapshot at BEGIN
- `READ_COMMITTED`: Refreshes snapshot each statement

Visibility rule: A tuple is visible if xmin is committed and xmax is not committed (from the snapshot's perspective).

### Undo Log (`undo_log.zig`)

In-memory undo log for transaction rollback.

- Record types: INSERT (undo = mark dead), DELETE (undo = resurrect), UPDATE (undo = restore old values)
- `appendRecord()`: Returns global offset used as `undo_ptr` in TupleHeader
- `readRecord()`: Fetch record at offset
- `gc()`: Advance watermark past oldest active transaction
- `compact()`: Reclaim memory by shifting live data forward

---

## Parser Layer

### Lexer (`lexer.zig`)

SQL tokenizer producing ~85 token types.

- Keywords: `SELECT`, `FROM`, `WHERE`, `JOIN`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `BEGIN`, `COMMIT`, `ROLLBACK`, `EXPLAIN`, etc.
- Data types: `INT`, `BIGINT`, `FLOAT`, `BOOLEAN`, `VARCHAR`, `TEXT`
- Operators: `=`, `!=`, `<`, `>`, `<=`, `>=`, `+`, `-`, `*`, `/`
- Parameters: positional (`$1`, `$2`) and named (`@name`)
- Case-insensitive keyword matching via 64-byte lowercase buffer
- `nextToken()` / `peek()`: Stream interface

### AST (`ast.zig`)

Abstract Syntax Tree node definitions.

**Statement** (tagged union):
- `create_table`, `create_index`, `insert`, `select`, `update`, `delete`
- `drop_table`, `drop_index`, `alter_table`, `explain`
- `begin_txn`, `commit_txn`, `rollback_txn`

**Expression** (tagged union):
- `column_ref`, `qualified_ref` (table.column), `literal`
- `comparison` (left op right), `and_expr`, `or_expr`, `not_expr`
- `between_expr`, `like_expr`, `in_list`, `is_null`, `is_not_null`
- `function_call`, `exists_subquery`, `in_subquery`

**SelectStatement**: columns, aliases, distinct, table_name, joins, where_clause, group_by, having_clause, order_by, limit

**JoinClause**: join_type (inner/left), table_name, on_condition

**AggregateFunc**: count, sum, avg, min, max

### Parser (`parser.zig`)

Recursive-descent SQL parser with operator precedence.

Expression precedence (low to high): OR < AND < NOT < comparison

Supported SQL:
- `SELECT` with `*`, named columns, qualified refs, aggregates, `DISTINCT`
- `FROM` with `JOIN` / `LEFT JOIN` ... `ON`
- `WHERE` with full expression support
- `GROUP BY` with `HAVING`
- `ORDER BY` column `ASC`/`DESC`
- `LIMIT` count
- `INSERT INTO` table `VALUES` (...)
- `UPDATE` table `SET` col = val `WHERE` ...
- `DELETE FROM` table `WHERE` ...
- `CREATE TABLE` with column definitions (type, nullable, primary key)
- `CREATE [UNIQUE] INDEX` name `ON` table (column)
- `DROP TABLE` / `DROP INDEX`
- `ALTER TABLE` ... `ADD [COLUMN]`
- `BEGIN` / `COMMIT` / `ROLLBACK`
- `EXPLAIN` query
- Prepared statement parameters: `$1` (positional) and `@name` (named)

---

## Planner Layer

### Plan Nodes (`plan.zig`)

**PlanNode** (tagged union):
- `seq_scan`: table_name, table_id, estimated_rows, estimated_cost
- `index_scan`: table_name, index_name, column_ordinal, scan_type, estimated_cost
- `filter`: child node + predicate + cost
- `sort`: child + column + descending flag
- `limit`: child + count

**ScanType** (for index scans):
- `point`: Exact key match
- `range`: [low, high] inclusive
- `range_from`: key >= value
- `range_to`: key <= value

Includes `formatPlan()` for EXPLAIN output.

### Planner (`planner.zig`)

Cost-based query optimizer.

**Algorithm** (`planSelect`):
1. Look up table in catalog, get row count
2. Calculate `seq_cost = row_count`
3. For each available index, analyze WHERE predicate for sargability
4. Estimate selectivity: point=1 row, range=25%, range_from/to=33%
5. Calculate `index_cost = 4.0 + estimated_matches * 1.5`
6. Pick cheapest (index or seq scan)
7. Wrap with Filter (if WHERE), Sort (if ORDER BY), Limit (if LIMIT)

**Sargable patterns detected**:
- `column = literal` and `literal = column` (reversed)
- `column >= literal`, `column > literal`, `column <= literal`, `column < literal`
- `column BETWEEN low AND high`
- AND expressions (tries left, then right)
- OR is NOT sargable (falls back to seq scan)

---

## Index Layer

### B+Tree Page (`btree_page.zig`)

On-disk node format for B+Tree.

- `Key` type: i32
- `NodeType`: internal or leaf
- `NodeHeader`: type, key_count, page_id, parent

**Leaf nodes**:
```
[NodeHeader][next_leaf: PageId][prev_leaf: PageId][LeafEntry * N]
```
- `LeafEntry`: key (i32) + TupleId (page_id + slot_id)
- `LEAF_MAX_ENTRIES`: ~680
- Doubly-linked leaf chain for range scans
- Binary search via `findKey()`

**Internal nodes**:
```
[NodeHeader][first_child: PageId][InternalEntry * N]
```
- `InternalEntry`: key + child PageId
- `INTERNAL_MAX_KEYS`: ~508 (originally ~770 in some calculations)
- `findChild()`: Binary search to locate correct child

Both support `splitInto()` for node splitting.

### B+Tree (`btree.zig`)

B+Tree index implementation.

- `create()`: Initialize new tree with empty root leaf
- `open()`: Open existing tree by root page ID
- `search()`: O(log n) key lookup returning TupleId
- `insert()`: Insert key-TupleId; splits leaf/internal nodes as needed
- `delete()`: Remove key from leaf (returns bool)
- `rangeScan()`: Returns `RangeScanIterator` over [start_key, end_key]

**Split mechanics**:
1. Leaf full -> split into two leaves, push median key up
2. Internal full -> split, push median up
3. Root splits -> create new root with two children

**RangeScanIterator**: Traverses leaf chain following next_leaf pointers, yielding entries in range.

---

## Executor Layer

### Executor (`executor.zig`)

The SQL execution engine (~4000 lines). Largest file in the codebase.

**Initialization**:
- `init()`: Without MVCC (legacy/simple mode)
- `initWithMvcc()`: With transaction manager + undo log

**Prepared Statements**:
- `prepare(sql)`: Parse once, return `PreparedStatement`
- `executePrepared(stmt, params)`: Execute with bound `$N` parameter values

**DDL**:
- CREATE TABLE / DROP TABLE / ALTER TABLE ADD COLUMN
- CREATE [UNIQUE] INDEX (with backfill of existing rows) / DROP INDEX
- EXPLAIN (formats plan tree)

**DML**:
- INSERT: Column count validation, type conversion, index maintenance
- SELECT: Column resolution, WHERE evaluation, ORDER BY (with top-N optimization for LIMIT), DISTINCT
- UPDATE: Delete + insert pattern with index maintenance
- DELETE: MVCC xmax marking or physical deletion

**Aggregates**: COUNT(*), SUM, AVG, MIN, MAX with GROUP BY support using HashMap-based accumulation

**JOINs** (3 strategies):
1. **Hash join**: For equi-joins. Builds hash table on smaller side, probes with larger side. O(n+m).
2. **Index nested-loop join**: When right table has a matching index on the join column.
3. **Nested-loop join**: Fallback for non-equi conditions. O(n*m).

Supports INNER JOIN and LEFT JOIN.

**Transactions**:
- `execBegin()` / `execCommit()` / `execRollback()`: Explicit transaction control
- Implicit auto-commit for standalone DML statements
- `rollbackFromUndoLog()`: Walks undo chain to reverse changes

**Expression Evaluation**:
- `evalWhere()` / `evalExprWithParams()`: Recursive evaluation
- `compareValues()`: Type-aware comparison with coercion
- `matchLike()`: SQL LIKE with `%` and `_` wildcards
- `resolveExprValue()`: Resolves column refs and literal values

**Index Maintenance**:
- `maintainIndexesInsert()` / `maintainIndexesDelete()`: Automatically updates all indexes on a table during DML

---

## Network Layer

### MsQuic Bindings (`msquic.zig`)

Hand-written Zig FFI bindings for the Microsoft QUIC C API v2.

- `QUIC_API_TABLE`: Function pointer table for all msquic operations
- Event structures for listeners, connections, and streams
- `QUIC_SETTINGS`: Connection settings (idle timeout, stream counts, MTU)
- Certificate configuration types (hash, file, PKCS12, context)
- `MsQuicOpenVersion()` / `MsQuicClose()`: Library entry/exit

### TLS (`tls.zig`)

OpenSSL FFI for certificate management.

- `generateSelfSigned()`: RSA 2048, 365-day self-signed cert
- `loadFromBytes()`: Load PKCS12 from DER bytes
- `fingerprintFromDer()` / `fingerprintFromX509()`: SHA-256 fingerprints
- `fingerprintFromPeerCert()`: Cross-platform (Windows Schannel / Linux OpenSSL)
- `CertBundle`: PKCS12 bytes + fingerprint + platform-specific handles

### GP Protocol (`protocol.zig`)

GrapheneDB Protocol wire format.

Frame: `[type:1B][length:4B BE][payload:NB]`

| Type | Code | Direction | Payload |
|------|------|-----------|---------|
| QUERY | 0x01 | Client->Server | SQL string (UTF-8) |
| DISCONNECT | 0x02 | Client->Server | empty |
| OK_MESSAGE | 0x10 | Server->Client | message (UTF-8) |
| OK_ROW_COUNT | 0x11 | Server->Client | u64 BE |
| RESULT_SET | 0x12 | Server->Client | col_count:u16 + col_names + row_count:u32 + values |
| ERROR | 0x1F | Server->Client | error message (UTF-8) |

`MAX_PAYLOAD_SIZE`: 16MB

### Server (`server.zig`)

QUIC server with per-connection state.

- Each connection gets its own `Executor` with independent transaction state
- Thread-safe query execution via mutex
- Auto-rollback on disconnect
- TLS: Supports certificate hash (Schannel), PEM files (OpenSSL), or PKCS12
- Message flow: Receive GP frame -> decode -> execute SQL -> encode response -> send

### Client (`client.zig`)

QUIC client with TOFU certificate pinning.

- Each query opens a new bidirectional QUIC stream
- `PinResult`: none, trusted, new_host, mismatch
- First connection: accepts server fingerprint (Trust On First Use)
- Subsequent connections: validates fingerprint matches saved value
- Synchronization via `std.Io.Event` for blocking operations

---

## Entry Points

### main.zig

CLI application with four modes:

1. **Init** (`--init`): Create new data directory
2. **Server** (`--server`): Start QUIC listener on configurable port (default 4567)
3. **Connect** (`--connect host:port`): Remote QUIC client with TOFU pinning
4. **REPL** (default): Local interactive SQL shell

Initializes the full stack: DataDir -> DiskManager -> BufferPool (1024 frames) -> AllocManager -> Catalog -> WAL -> TransactionManager -> UndoLog -> Executor

Meta-commands: `\dt` (list tables), `\q` (quit)

### engine.zig

Lightweight module that re-exports all subsystems except network (no msquic/OpenSSL dependency). Used by benchmarks.

---

## Benchmarks

### Zig Benchmarks (`bench/zig/`)

Direct executor benchmarks (no network overhead).

- `bench_main.zig`: CLI with `--scale`, `--suite`, `--output`. Fresh DB per suite.
- `bench_runner.zig`: Timing framework with warmup/measure iterations, percentile stats, CSV output. Supports prepared statements.
- `bench_crud.zig`: bulk_insert (prepared stmts), point/range queries (seq + index scan), update, delete
- `bench_analytical.zig`: GROUP BY + COUNT/SUM/AVG, JOIN, LEFT JOIN, ORDER BY + LIMIT, MIN/MAX
- `bench_index.zig`: CREATE INDEX, UNIQUE INDEX, index speedup comparison
- `bench_txn.zig`: Transaction commit throughput, rollback overhead, mixed read/write

### Python Benchmarks (`bench/python/`)

Network benchmarks over QUIC using `aioquic`.

- `gp_protocol.py`: GP protocol encoder/decoder (Python equivalent of protocol.zig)
- `gp_client.py`: Async QUIC client with ALPN "graphenedb"
- `bench_runner.py`: CLI harness with argparse, CSV output
- `bench_suites.py`: Same 4 suites as Zig (CRUD, analytical, index, txn)

### Performance Highlights (from benchmark results at scale=1)

| Benchmark | Ops/sec | Notes |
|-----------|---------|-------|
| range_query_indexscan | ~141 | Fastest query type |
| min_max | ~78 | Aggregate scan |
| range_query_seqscan | ~59 | Sequential range |
| full_scan (1000 rows) | ~19 | Full table |
| join | ~29 | Hash join |
| create_index | ~40 | B-tree build |
| point_query_indexscan | ~5.4 | Single row via index |
| point_query_seqscan | ~0.7 | Single row via seq scan |
| Index speedup | ~8x | Point queries |

---

## Build System

### build.zig

- Executable: "GrapheneDB" from `src/main.zig`
- Benchmark binary: "graphene-bench" from `bench/zig/bench_main.zig`
- Links: msquic (QUIC), OpenSSL (TLS), C library
- Architecture-aware library paths (x64, arm64, x86)
- Windows resource file (`src/resources.rc`) for app icon
- Build steps: `run`, `test`, `bench`

### build.zig.zon

- Package name: GrapheneDB
- Version: 0.0.0
- Min Zig: 0.16.0-dev.2535+b5bd49460
- No external Zig dependencies (msquic/OpenSSL linked as system libs)

---

## Key Design Decisions

1. **SQL Server-inspired storage**: GAM/PFS/SGAM/IAM extent-based allocation mirrors SQL Server internals
2. **8KB pages**: Standard page size matching SQL Server
3. **Slotted pages**: Header + slot directory + tuple data for variable-length rows
4. **MVCC via tuple headers**: xmin/xmax fields with undo log, not separate version store
5. **B+Tree indexes**: Integer keys (i32), leaf chain for range scans
6. **Cost-based optimizer**: Simple but effective seq-scan vs index-scan cost comparison
7. **QUIC transport**: Modern protocol with built-in TLS, multiplexed streams
8. **TOFU certificate pinning**: SSH-style trust model for client connections
9. **Hash joins for equi-joins**: O(n+m) performance for equality-based joins
10. **Prepared statements**: Parse-once, execute-many with positional/named parameters
