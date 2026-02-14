# Microsoft SQL Server Internals

Comprehensive reference on SQL Server's internal architecture, covering the storage engine,
query processor, memory management, concurrency control, and more.

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Storage Engine](#2-storage-engine)
3. [Buffer Pool & Memory Management](#3-buffer-pool--memory-management)
4. [Write-Ahead Logging (WAL) & Recovery](#4-write-ahead-logging-wal--recovery)
5. [Checkpoints, Lazy Writer & Eager Writer](#5-checkpoints-lazy-writer--eager-writer)
6. [Index Internals (B+Tree)](#6-index-internals-btree)
7. [Columnstore Indexes](#7-columnstore-indexes)
8. [Statistics & Cardinality Estimation](#8-statistics--cardinality-estimation)
9. [Query Processor & Optimizer](#9-query-processor--optimizer)
10. [Query Execution Engine](#10-query-execution-engine)
11. [Parallel Query Execution](#11-parallel-query-execution)
12. [Plan Cache & Compilation](#12-plan-cache--compilation)
13. [Lock Manager & Concurrency Control](#13-lock-manager--concurrency-control)
14. [MVCC & Snapshot Isolation](#14-mvcc--snapshot-isolation)
15. [SQLOS: Scheduling & Threading](#15-sqlos-scheduling--threading)
16. [TempDB Internals](#16-tempdb-internals)
17. [In-Memory OLTP (Hekaton)](#17-in-memory-oltp-hekaton)

---

## 1. High-Level Architecture

SQL Server is composed of four major components:

```
┌─────────────────────────────────────────────────┐
│                  Client / TDS                   │
├─────────────────────────────────────────────────┤
│              Protocol Layer (SNI)               │
│         (Shared Memory, TCP/IP, Named Pipes)    │
├─────────────────────────────────────────────────┤
│           Query Processor (Relational Engine)   │
│     ┌──────────┬───────────┬──────────────┐     │
│     │  Parser  │ Optimizer │  Executor    │     │
│     └──────────┴───────────┴──────────────┘     │
├─────────────────────────────────────────────────┤
│              Storage Engine                     │
│  ┌──────────┬──────────┬────────────────────┐   │
│  │ Access   │ Buffer   │  Transaction       │   │
│  │ Methods  │ Manager  │  Manager           │   │
│  └──────────┴──────────┴────────────────────┘   │
├─────────────────────────────────────────────────┤
│                    SQLOS                        │
│     (Scheduling, Memory, I/O, Resources)        │
├─────────────────────────────────────────────────┤
│              Windows OS / Hardware               │
└─────────────────────────────────────────────────┘
```

- **Protocol Layer**: Handles network communication via Tabular Data Stream (TDS) protocol over
  Shared Memory, TCP/IP, or Named Pipes through the Server Network Interface (SNI).
- **Query Processor (Relational Engine)**: Parses SQL text, binds names to objects, optimizes the
  query into a physical execution plan, and executes it.
- **Storage Engine**: Manages physical data storage, buffer management, transactions, locking,
  and logging. Returns raw data pages to the relational engine.
- **SQLOS**: SQL Server's own operating system layer sitting between the engine and Windows.
  Handles cooperative scheduling, memory management, I/O completion, and resource governance.

---

## 2. Storage Engine

### 2.1 Pages

The fundamental unit of data storage is the **page** (8 KB / 8192 bytes). All disk I/O is
performed at the page level.

```
┌─────────────────────────────────────┐
│         Page Header (96 bytes)      │
│  ┌─────────────────────────────┐    │
│  │ Page Number (4 bytes)       │    │
│  │ Page Type                   │    │
│  │ Object ID                   │    │
│  │ Index ID                    │    │
│  │ Previous / Next Page Ptrs   │    │
│  │ LSN (Log Sequence Number)   │    │
│  │ Torn Bits / Checksum        │    │
│  │ Free Count / Free Data      │    │
│  └─────────────────────────────┘    │
├─────────────────────────────────────┤
│         Row Data Area               │
│  (rows inserted top-down)           │
│                                     │
│                                     │
│         ← free space →              │
│                                     │
│  (row offset array grows bottom-up) │
├─────────────────────────────────────┤
│      Row Offset Array (Slot Array)  │
│  2 bytes per row, bottom-up         │
└─────────────────────────────────────┘
```

**Page types include:**
| Type ID | Name | Description |
|---------|------|-------------|
| 1 | Data | Data rows (heap or clustered index leaf) |
| 2 | Index | Non-leaf index pages |
| 3 | Text/Image | LOB data pages |
| 8 | GAM | Global Allocation Map — tracks which extents are allocated |
| 9 | SGAM | Shared GAM — tracks mixed extents |
| 10 | IAM | Index Allocation Map — maps extents for a specific allocation unit |
| 11 | PFS | Page Free Space — tracks free space and allocation status per page |

Maximum usable row data per page: **8060 bytes** (8192 - 96 header - 36 overhead).

### 2.2 Extents

An **extent** is a group of 8 physically contiguous pages = **64 KB**.

- **Uniform extent**: All 8 pages belong to a single object. Used once an object grows beyond 8 pages.
- **Mixed extent**: Pages can belong to up to 8 different objects. Used for small tables/indexes to avoid wasting space.

### 2.3 Allocation Maps

- **GAM (Global Allocation Map)**: 1 bit per extent covering ~4 GB of data. Bit = 1 means the
  extent is free; 0 means allocated.
- **SGAM (Shared Global Allocation Map)**: Same structure. Bit = 1 means the extent is a mixed
  extent with free pages; 0 means it's full or a uniform extent.
- **PFS (Page Free Space)**: 1 byte per page covering ~64 MB. Tracks % free space on each page
  and whether the page is allocated, has ghost records, or is an IAM page.
- **IAM (Index Allocation Map)**: One IAM chain per allocation unit. Each IAM page has a bitmap
  where each bit represents one extent in a ~4 GB range. If the bit is set, that extent belongs
  to the allocation unit.

### 2.4 Allocation Units

Each partition of a heap or index has up to three allocation units:

| Type | Stores |
|------|--------|
| IN_ROW_DATA | Regular row data (columns that fit within the 8060-byte row limit) |
| ROW_OVERFLOW_DATA | Variable-length column data that overflows the 8060-byte limit |
| LOB_DATA | Large object data (varchar(max), varbinary(max), xml, etc.) |

### 2.5 Heaps vs. Clustered Tables

- **Heap**: A table without a clustered index. Rows are stored in no particular order. New rows go
  wherever there's space (tracked by PFS). Row locators are RID (FileID:PageID:SlotNumber).
- **Clustered table**: Has a clustered index. Leaf-level pages ARE the data pages, sorted by the
  clustering key. Row locators used by nonclustered indexes are the clustered index key values.

---

## 3. Buffer Pool & Memory Management

### 3.1 Buffer Pool Overview

The **buffer pool** (also called buffer cache) is the largest consumer of SQL Server memory.
It caches data and index pages in memory to reduce physical disk I/O.

- Operates on **8 KB pages** exclusively — nothing larger than 8 KB lives in the buffer pool.
- Controlled by `max server memory` and `min server memory` (sp_configure).
- Pages are committed on demand. The buffer pool calculates a **target** memory size based on
  internal requirements and external memory pressure.

### 3.2 Physical vs. Logical I/O

- **Physical I/O**: Reading a page from disk into the buffer pool.
- **Logical I/O**: Reading a page that's already in the buffer pool (memory).
- Goal: maximize the **buffer cache hit ratio** (logical reads / total reads).

### 3.3 Page Lifecycle in Buffer Pool

```
Disk → [Physical Read] → Buffer Pool (clean page)
                              ↓
                         Page modified
                              ↓
                     Page marked DIRTY
                              ↓
              ┌───────────────┼───────────────┐
              ↓               ↓               ↓
         Checkpoint      Lazy Writer     Eager Writer
         (periodic)      (memory         (bulk/minimally
                          pressure)       logged ops)
              ↓               ↓               ↓
              └───────────────┼───────────────┘
                              ↓
                    Written back to disk
                    Page marked CLEAN
```

### 3.4 Memory Clerks

Memory clerks sit between memory nodes and SQL Server components. Each component (buffer pool,
plan cache, lock manager, etc.) has its own memory clerk that interfaces with memory nodes to
allocate memory. Clerks enable fine-grained tracking of memory consumption via
`sys.dm_os_memory_clerks`.

### 3.5 Buffer Pool Extension (BPE)

Extends the buffer pool to SSD storage as a secondary cache tier. Clean pages evicted from
the buffer pool can be stored on SSD rather than requiring a full disk read on next access.

---

## 4. Write-Ahead Logging (WAL) & Recovery

### 4.1 WAL Protocol

SQL Server guarantees **no data modification is written to disk before its corresponding log
record is written to disk first**. This is the foundation of ACID durability.

```
Transaction modifies page in memory
         ↓
Log record written to log buffer
         ↓
Log buffer flushed to disk (transaction log file)
         ↓
Only THEN can the dirty data page be written to disk
```

### 4.2 Transaction Log Structure

- **Log Sequence Number (LSN)**: Every log record has a unique, monotonically increasing LSN.
- **Active log**: The portion from the **MinLSN** (oldest active transaction or oldest
  unreplicated record) to the last written log record. Required for crash recovery.
- **Virtual Log Files (VLFs)**: The transaction log is internally divided into VLFs. These are
  the units of log truncation/reuse.

### 4.3 Log Records

The transaction log records:
- Start and end of each transaction (BEGIN TRAN, COMMIT, ROLLBACK)
- Every data modification (INSERT, UPDATE, DELETE) — both before and after images
- Extent and page allocations/deallocations
- DDL operations
- System stored procedure modifications

### 4.4 Recovery Process (ARIES-based)

SQL Server uses an ARIES-style recovery algorithm with three phases:

1. **Analysis**: Scans forward from the last checkpoint to determine which transactions were
   active at crash time and which pages might be dirty.
2. **Redo (Roll Forward)**: Replays all logged operations from the last checkpoint forward,
   bringing the database to the exact state at the moment of the crash (including uncommitted
   transaction modifications).
3. **Undo (Roll Back)**: Rolls back all transactions that were not committed at crash time,
   restoring the database to a consistent state.

---

## 5. Checkpoints, Lazy Writer & Eager Writer

### 5.1 Checkpoint

- Periodically scans the buffer cache and writes **all dirty pages** for a database to disk.
- Creates a known recovery point — reduces the time needed for crash recovery (less redo work).
- Types: automatic (based on recovery interval), manual (CHECKPOINT command), internal (e.g.,
  during backup, database shutdown), indirect (SQL Server 2016+ default, targets a specific
  recovery time).
- Writes only dirty data pages; does NOT truncate the transaction log (that's a separate process).

### 5.2 Lazy Writer

- Background thread that monitors buffer pool memory pressure.
- Uses an **LRU (Least Recently Used)** algorithm to find cold, clean pages and free their
  buffer frames.
- If it encounters a **dirty** page that needs to be evicted, it writes it to disk first.
- Goal: maintain a pool of free buffer frames so incoming page requests don't stall.
- High lazy writes/sec indicates memory pressure.

### 5.3 Eager Writer

- Used specifically for **minimally-logged bulk operations** (BULK INSERT, SELECT INTO, index
  rebuilds).
- Writes dirty pages to disk **in parallel** with the bulk operation, allowing the operation to
  proceed without waiting for all pages to be flushed.
- Prevents bulk operations from overwhelming the buffer pool with dirty pages.

### 5.4 All Three Use Asynchronous I/O

Checkpoint, lazy writer, and eager writer all use asynchronous (overlapped) I/O — they issue
the write and continue processing, checking for completion later.

---

## 6. Index Internals (B+Tree)

### 6.1 B+Tree Structure

SQL Server rowstore indexes are organized as **B+ trees**:

```
                    ┌──────────┐
                    │   Root   │ (single page)
                    └────┬─────┘
              ┌──────────┼──────────┐
              ↓          ↓          ↓
         ┌────────┐ ┌────────┐ ┌────────┐
         │ Inter- │ │ Inter- │ │ Inter- │  Intermediate
         │ mediate│ │ mediate│ │ mediate│  Level(s)
         └───┬────┘ └───┬────┘ └───┬────┘
             ↓          ↓          ↓
     ┌──────────────────────────────────┐
     │  Leaf Level (doubly linked list) │
     │  ← Page ↔ Page ↔ Page ↔ Page →  │
     └──────────────────────────────────┘
```

- **Root node**: Top of the tree. Single page.
- **Intermediate levels**: Direct searches from root to leaf. Contain key values and child page
  pointers.
- **Leaf level**: Doubly-linked list of pages for range scans.

### 6.2 Clustered Index

- The leaf level **IS** the data — the actual table rows, sorted by the clustering key.
- Only one per table (since data can only be physically sorted one way).
- Non-leaf (intermediate) pages contain the clustering key + child page pointer.
- Lookups: Root → Intermediate(s) → Leaf (data page) = O(log n) I/Os.

### 6.3 Nonclustered Index

- Leaf level contains the **index key columns + row locator**:
  - On a clustered table: the row locator is the **clustered index key** (not a physical RID).
  - On a heap: the row locator is the **RID** (FileID:PageID:SlotNumber).
- May include additional **INCLUDE** columns stored only at the leaf level (covering index).
- **Key lookup / RID lookup**: When the nonclustered index doesn't cover all needed columns,
  SQL Server must do a lookup back to the base table using the row locator.
- In a non-unique nonclustered index on a clustered table, the clustered key is added to both
  leaf and non-leaf levels to uniquify entries.

### 6.4 Page Splits

When a page is full and a new row must be inserted into it:
1. A new page is allocated.
2. Approximately half the rows are moved to the new page.
3. The parent intermediate page is updated to point to both pages.
4. This can cascade upward if intermediate pages are full.

Page splits are expensive (logged, cause fragmentation) and are a key reason for monitoring
index fill factor.

---

## 7. Columnstore Indexes

### 7.1 Architecture

Columnstore indexes store data **column-by-column** rather than row-by-row:

```
Rowgroup (up to 1,048,576 rows)
┌─────────┬─────────┬─────────┬─────────┐
│ Col A   │ Col B   │ Col C   │ Col D   │
│ Segment │ Segment │ Segment │ Segment │
│ (comp.) │ (comp.) │ (comp.) │ (comp.) │
└─────────┴─────────┴─────────┴─────────┘
```

- **Rowgroup**: A group of up to **1,048,576 rows** (2^20). The unit of compression and I/O.
- **Column segment**: One column's data within a rowgroup. Each segment is independently
  compressed using techniques like dictionary encoding, run-length encoding, and bit-packing.
- **Delta store**: A rowstore B-tree that holds new rows before they're compressed into
  columnstore format.

### 7.2 Rowgroup Lifecycle

```
INSERT → Delta Store (OPEN)
              ↓ (reaches 1M rows)
         Delta Store (CLOSED)
              ↓ (tuple-mover background process)
         Compressed Rowgroup (COMPRESSED)
```

### 7.3 Batch Mode Processing

- Traditional row mode processes one row at a time through each operator.
- **Batch mode** processes rows in batches of **64 to 900 rows** at a time (64 KB structure).
- Achieves **2-4x performance improvement** for analytical queries through:
  - Better CPU cache utilization
  - SIMD-friendly memory layout
  - Reduced per-row overhead
- Since SQL Server 2019, batch mode can be used on **rowstore** tables too (Batch Mode on
  Rowstore), without requiring a columnstore index.

### 7.4 Segment Elimination

When a query has a filter predicate, the optimizer can skip entire segments whose min/max
metadata indicates they contain no matching rows — avoiding decompression of irrelevant data.

---

## 8. Statistics & Cardinality Estimation

### 8.1 Statistics Object Structure

A statistics object has three components:

1. **Header**: Metadata — name, columns, number of rows sampled, last update time, etc.
2. **Histogram**: Distribution of values for the **first key column** only. Up to **200 steps**
   (buckets). Built using a maximum-difference algorithm.
3. **Density Vector**: Measures cross-column correlation for multi-column statistics. One density
   value per column prefix. Density = 1 / (number of distinct values).

### 8.2 Histogram Structure

Each histogram step contains:
| Field | Meaning |
|-------|---------|
| RANGE_HI_KEY | Upper bound value of the step |
| RANGE_ROWS | Estimated rows in the range (exclusive of HI_KEY) |
| EQ_ROWS | Estimated rows exactly equal to RANGE_HI_KEY |
| DISTINCT_RANGE_ROWS | Estimated distinct values in the range |
| AVG_RANGE_ROWS | Average rows per distinct value = RANGE_ROWS / DISTINCT_RANGE_ROWS |

### 8.3 Auto-Update Mechanism

Statistics are automatically updated when the **modification counter** exceeds a threshold:
- Pre-SQL Server 2016: 20% + 500 rows of the table size.
- SQL Server 2016+ with trace flag 2371 (default): Dynamic threshold that decreases as table
  size increases (e.g., ~sqrt(1000 * table_rows)).
- Updates can be **synchronous** (query waits for stats update) or **asynchronous** (query
  uses stale stats while update happens in background).

### 8.4 Cardinality Estimator (CE)

The CE predicts how many rows each operator in the plan will produce. Two versions:
- **Legacy CE** (pre-2014): More conservative, assumes correlation between predicates.
- **New CE** (2014+): Uses exponential backoff for multi-predicate selectivity estimation,
  assumes more independence between columns.

Cardinality estimation is used in two key optimizer steps:
1. Early in simplification to evaluate rule applicability.
2. During cost-based optimization to estimate operator costs.

---

## 9. Query Processor & Optimizer

### 9.1 Query Processing Pipeline

```
SQL Text
  ↓
Parser → Parse Tree (syntax check)
  ↓
Algebrizer/Binder → Resolved Query Tree (name resolution, type checking)
  ↓
Simplification → Simplified Tree (remove redundancies, constant folding)
  ↓
Trivial Plan Check → If trivially optimal, skip full optimization
  ↓
Cost-Based Optimizer → Optimal Execution Plan
  ↓
Execution Engine → Results
```

### 9.2 Parsing

- Converts SQL text into an internal parse tree.
- Validates syntax only (not object existence or column names).
- Produces error for syntax violations.

### 9.3 Algebrizer / Binding

- Resolves object names to actual database objects.
- Resolves column names, performs type derivation and checking.
- Resolves aggregates, GROUP BY semantics, subquery structure.
- Outputs a **resolved query tree** (algebrized tree).

### 9.4 Simplification

- **Predicate pushdown**: Moves filters closer to the data source.
- **Join simplification**: Removes redundant joins.
- **Constant folding**: Evaluates constant expressions at compile time.
- **Contradiction detection**: Identifies impossible predicates (e.g., WHERE 1 = 0).
- **Subquery unnesting**: Converts correlated subqueries to joins where possible.

### 9.5 Trivial Plan

For very simple queries (single table, obvious access path), the optimizer skips full
cost-based optimization and produces a plan using heuristics. This avoids the overhead of
exploring many alternatives for queries where the best plan is obvious.

### 9.6 Cost-Based Optimization Phases

If no trivial plan is found, the optimizer enters cost-based search in phases:

| Phase | Name | Description |
|-------|------|-------------|
| 0 | Transaction Processing | Limited join orders for simple OLTP queries (≤3 tables). Cost threshold: 0.2. |
| 1 | Quick Plan | More exploration with restricted transformation rules. Handles most queries. |
| 2 | Full Optimization | Exhaustive search with all transformation rules. For complex queries. |

The optimizer uses **transformation rules** to generate candidate plans and **heuristics** to
prune the search space. It stops when:
- A "good enough" plan is found (cost below threshold for the phase).
- The search budget is exhausted.
- The full search space has been explored.

### 9.7 Cost Model

The optimizer estimates cost in internal **cost units** (not directly seconds or I/O counts)
based on:
- **I/O cost**: Sequential and random read costs, considering whether pages are likely cached.
- **CPU cost**: Per-row processing costs for each operator.
- **Memory cost**: Memory grant requirements (for sorts, hashes).
- **Network cost**: For distributed queries.

---

## 10. Query Execution Engine

### 10.1 Iterator (Volcano) Model

SQL Server uses the **Volcano/Iterator** model for query execution:

```
          ┌──────────┐
          │  SELECT  │ ← GetRow()
          └────┬─────┘
               ↓ GetRow()
          ┌──────────┐
          │   JOIN   │ ← GetRow() from both children
          └──┬────┬──┘
             ↓    ↓
        ┌──────┐ ┌──────┐
        │ SCAN │ │ SCAN │ ← GetRow() from storage
        └──────┘ └──────┘
```

Each operator implements three methods:
- **Open()**: Initialize the operator and its children.
- **GetRow() / Next()**: Request the next row. Pulls data up from children.
- **Close()**: Release resources.

This is a **demand-driven (pull)** model — the root operator pulls rows up through the tree one
at a time.

### 10.2 Physical Join Operators

| Operator | Best For | Complexity | Requires Sorted Input? |
|----------|----------|------------|----------------------|
| **Nested Loops** | Small outer input, indexed inner | O(n * m) worst case | No |
| **Merge Join** | Pre-sorted equi-joins | O(n + m) | Yes (both inputs) |
| **Hash Match** | Large unsorted equi-joins | O(n + m) average | No |
| **Adaptive Join** (2017+) | Unknown cardinality | Switches at runtime | No |

#### Nested Loops Join
- For each row in the outer (top) input, scans the inner (bottom) input for matches.
- Efficient when the outer input is small and there's an index on the inner input's join column.
- Can handle non-equi-join predicates (theta joins).

#### Merge Join
- Simultaneously scans two sorted inputs, merging matching rows.
- Requires both inputs sorted on the join key (or provides a Sort operator).
- Very efficient for large sorted datasets. Theoretically the fastest join type.

#### Hash Match Join
- **Build phase**: Reads the smaller (build) input and creates an in-memory hash table on the
  join key.
- **Probe phase**: Reads the larger (probe) input and looks up each row in the hash table.
- If the hash table doesn't fit in memory, it **spills to tempdb** (grace hash join / recursive
  hash join).
- Only supports equi-joins.

### 10.3 Other Key Operators

- **Sort**: Sorts rows. Can spill to tempdb if memory grant is insufficient.
- **Stream Aggregate**: Computes aggregates on pre-sorted input.
- **Hash Aggregate**: Builds a hash table of groups — doesn't need sorted input.
- **Filter**: Evaluates a predicate on each row.
- **Compute Scalar**: Evaluates expressions.
- **Spool (Table/Index)**: Stores intermediate results for repeated access.
- **Top**: Returns only the first N rows (enables top-N sort optimizations).
- **Concatenation**: Combines results from multiple inputs (UNION ALL).

---

## 11. Parallel Query Execution

### 11.1 When Queries Go Parallel

SQL Server considers parallel execution when:
- The estimated plan cost exceeds the **cost threshold for parallelism** (default: 5).
- The server has multiple available schedulers.
- **MAXDOP** (max degree of parallelism) setting allows it.

### 11.2 Exchange Operators (Parallelism)

Exchange operators manage data flow between serial and parallel zones:

```
Serial Zone          Parallel Zone           Serial Zone
              ┌──→ Worker 1 ──┐
    Distribute│──→ Worker 2 ──│Gather
     Streams  │──→ Worker 3 ──│Streams
              └──→ Worker 4 ──┘
```

| Exchange Type | Description |
|--------------|-------------|
| **Distribute Streams** | Takes serial input → distributes to parallel threads |
| **Repartition Streams** | Redistributes rows between parallel threads (e.g., for hash join on different key) |
| **Gather Streams** | Merges parallel threads back to a single serial stream |

### 11.3 Distribution Methods

- **Round Robin**: Distributes rows evenly across threads.
- **Hash**: Routes rows to threads based on hash of distribution column(s) — ensures rows with
  same key go to same thread (needed for joins, aggregates).
- **Broadcast**: Sends all rows to all threads (for small tables in joins).
- **Demand**: Workers pull rows on demand.

### 11.4 Parallel Scan

- Multiple threads cooperatively scan a table/index using a **parallel page supplier**.
- Each thread requests pages from the supplier; no two threads scan the same page.
- Achieves near-linear speedup for I/O-bound scans.

---

## 12. Plan Cache & Compilation

### 12.1 Plan Cache

SQL Server caches compiled execution plans in the **plan cache** (part of buffer pool) to
avoid repeated compilation of identical queries.

Cache stores include:
- **SQL Plans**: Ad-hoc and prepared SQL statements.
- **Object Plans**: Stored procedures, functions, triggers.
- **Bound Trees**: Views, constraints, defaults.

### 12.2 Plan Matching

Plans are matched by:
- Exact text match for ad-hoc queries (case-sensitive, whitespace-sensitive).
- **Simple parameterization**: SQL Server auto-parameterizes trivially safe queries.
- **Forced parameterization**: Database option that parameterizes all queries (with exceptions).
- sp_executesql and prepared statements get parameterized plans.

### 12.3 Compilation vs. Recompilation

- **Compilation**: No matching plan in cache → full optimization → new plan added to cache.
  Applies to the entire batch.
- **Recompilation**: Existing plan is invalidated → single statement re-optimized → plan
  replaced. Triggered by:
  - Statistics changes
  - Schema changes (DDL on referenced objects)
  - SET option changes
  - sp_recompile
  - Large modification counter changes on referenced tables

### 12.4 Parameter Sniffing

When a stored procedure is first compiled, the **actual parameter values** from that first
execution are used by the optimizer to generate the plan. This plan is then cached and reused
for all subsequent executions.

- **Good**: Most executions have similar data distributions → the sniffed plan works well.
- **Bad**: Data distribution varies significantly → the sniffed plan may be terrible for some
  parameter values (parameter sensitivity problem).

Mitigations:
- OPTION (RECOMPILE) — recompile every time
- OPTION (OPTIMIZE FOR ...) — optimize for specific or unknown values
- Plan guides
- Parameter Sensitive Plan (PSP) optimization (SQL Server 2022) — multiple plan variants

### 12.5 Plan Cache Eviction

Plans are evicted under memory pressure using a **cost-based** algorithm:
- Each plan has an original cost (based on compilation cost).
- Each time a plan is reused, its current cost is incremented.
- Under memory pressure, a background process decrements costs; plans reaching zero are evicted.
- The `DBCC FREEPROCCACHE` command manually clears the cache.

---

## 13. Lock Manager & Concurrency Control

### 13.1 Lock Architecture

The **lock manager** is an internal engine component that manages all locks. When a query
requires access to a resource, it requests the appropriate lock from the lock manager, which
grants it if no conflicting locks exist.

### 13.2 Lock Granularity

SQL Server supports **multigranular locking** — locks at different levels of the resource
hierarchy:

```
Database
  └── Table
       └── Partition (Hobt)
            └── Page
                 └── Row (RID / Key)
```

| Resource | Lock Target |
|----------|-------------|
| RID | Single row in a heap |
| KEY | Single row in an index (protects key range in serializable) |
| PAGE | 8 KB data or index page |
| EXTENT | 8 contiguous pages |
| HOBT | Heap or B-tree partition |
| TABLE | Entire table |
| DATABASE | Entire database |

### 13.3 Lock Modes

| Mode | Abbreviation | Description |
|------|-------------|-------------|
| Shared | S | Read access. Multiple S locks can coexist. |
| Update | U | Read with intent to modify. Prevents deadlock in read-then-write patterns. |
| Exclusive | X | Write access. No other locks allowed. |
| Intent Shared | IS | Intent to acquire S locks on child resources. |
| Intent Exclusive | IX | Intent to acquire X locks on child resources. |
| Shared Intent Exclusive | SIX | Table has S lock + intent to X lock some rows. |
| Schema Stability | Sch-S | Prevents DDL while query executes. |
| Schema Modification | Sch-M | Exclusive DDL lock. |
| Bulk Update | BU | For bulk copy operations. |
| Key-Range | RangeS-S, etc. | Protects ranges in serializable isolation. |

### 13.4 Lock Compatibility Matrix (simplified)

| Requested → | S | U | X |
|-------------|---|---|---|
| **S held** | Yes | Yes | No |
| **U held** | Yes | No | No |
| **X held** | No | No | No |

### 13.5 Lock Escalation

When a single transaction acquires more than **~5000 locks** on a single level (table/partition),
SQL Server **escalates** those fine-grained locks to a single table-level lock.

- Reduces memory overhead of maintaining many locks.
- Can hurt concurrency on large tables.
- Controllable with `ALTER TABLE ... SET (LOCK_ESCALATION = {TABLE | AUTO | DISABLE})`.

### 13.6 Optimized Locking (SQL Server 2022+)

Two improvements:
1. **TID (Transaction ID) Locking**: Instead of acquiring per-row locks for writes, the
   transaction marks rows with its transaction ID. Other transactions check the TID status.
2. **Lock After Qualification (LAQ)**: Evaluates query predicates against the latest committed
   row version **without acquiring a lock**. Only acquires locks on rows that actually qualify.
   Greatly reduces lock memory and contention.

### 13.7 Deadlock Detection

- SQL Server runs a background **deadlock monitor** thread.
- Detects cycles in the lock wait graph.
- Chooses a **victim** (usually the transaction with least log written) and rolls it back with
  error 1205.
- Detection interval starts at 5 seconds, reduces to 100ms when deadlocks are frequent.

---

## 14. MVCC & Snapshot Isolation

### 14.1 Row Versioning

When snapshot-based isolation is enabled, every row modification creates a **version** of the
previous committed row image in **tempdb's version store**.

```
Current Row (in data page)
  → pointer to Previous Version (in tempdb)
     → pointer to Even Older Version (in tempdb)
        → ... (version chain)
```

### 14.2 Isolation Levels

SQL Server supports two row-versioning-based isolation levels:

| Level | Enabled By | Behavior |
|-------|-----------|----------|
| **Read Committed Snapshot (RCSI)** | `ALTER DATABASE SET READ_COMMITTED_SNAPSHOT ON` | Each **statement** sees a snapshot as of its start. Default READ COMMITTED behavior changes from locking to versioning. |
| **Snapshot Isolation** | `ALTER DATABASE SET ALLOW_SNAPSHOT_ISOLATION ON` + `SET TRANSACTION ISOLATION LEVEL SNAPSHOT` | Each **transaction** sees a consistent snapshot as of the transaction's start. |

Traditional (pessimistic) isolation levels for comparison:
| Level | Behavior |
|-------|----------|
| Read Uncommitted | No shared locks; reads dirty data |
| Read Committed (default) | S locks held only during read; released immediately |
| Repeatable Read | S locks held until transaction ends |
| Serializable | Key-range locks prevent phantom reads |

### 14.3 RCSI vs. Snapshot

| Aspect | RCSI | Snapshot |
|--------|------|----------|
| Snapshot scope | Statement | Transaction |
| Write conflicts | No (last writer wins) | Yes (error 3960 on conflict) |
| Overhead | Lower | Higher |
| Common use | Most OLTP workloads | Long-running analytical reads |

### 14.4 Version Store Management

- Versions are stored in **tempdb** regardless of which database uses them.
- A background **version cleaner** periodically removes versions no longer needed by any active
  transaction.
- Long-running transactions can cause the version store to grow very large (tempdb bloat).
- Monitoring: `sys.dm_tran_version_store`, `sys.dm_tran_active_snapshot_database_transactions`.

---

## 15. SQLOS: Scheduling & Threading

### 15.1 SQLOS Overview

SQLOS is SQL Server's internal operating system layer. It provides:
- Cooperative (non-preemptive) scheduling
- Memory management (memory nodes, memory clerks)
- I/O completion
- Resource governance
- Exception handling

### 15.2 Scheduler Architecture

```
┌─────────────────────────────────────────┐
│               Scheduler                  │
│          (1 per logical CPU)            │
│                                         │
│  ┌──────────┐  ┌──────────────────┐     │
│  │ Processor │  │  Runnable Queue  │     │
│  │ (running  │  │  (FIFO - threads │     │
│  │  worker)  │  │   ready to run)  │     │
│  └──────────┘  └──────────────────┘     │
│                                         │
│  ┌──────────────────────────────────┐   │
│  │         Waiter List              │   │
│  │  (threads waiting for resources) │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

### 15.3 Key Concepts

| Concept | Description |
|---------|-------------|
| **Scheduler** | Mapped 1:1 to a logical CPU. Manages work assignment. |
| **Worker** | A logical thread (1:1 mapped to an OS thread or fiber). Does actual work. |
| **Task** | A unit of work (a request may spawn multiple tasks, e.g., for parallelism). |
| **Quantum** | Maximum time a worker can run before yielding: **4 ms**. |

### 15.4 Cooperative Scheduling

Unlike preemptive OS scheduling, SQL Server uses **cooperative (non-preemptive) scheduling**:
- Workers voluntarily **yield** the CPU at well-defined points.
- Workers yield when their 4ms quantum expires, or when they start an I/O wait, or when they
  encounter a lock wait.
- A worker that doesn't yield is called a **non-yielding scheduler** — a serious condition that
  generates a memory dump.

### 15.5 Worker States

```
                    RUNNING
                   (on CPU)
                  /         \
           yield /           \ resource wait
                /             \
         RUNNABLE ←────── SUSPENDED
         (in queue,        (waiting for
          ready to run)     I/O, lock, etc.)
```

1. **RUNNING**: Actively executing on the CPU (only one per scheduler).
2. **RUNNABLE**: Has everything it needs, waiting in the runnable queue for CPU time.
   High RUNNABLE time = **CPU pressure** (SOS_SCHEDULER_YIELD waits).
3. **SUSPENDED**: Waiting for a resource (I/O, lock, memory, network). Moves to RUNNABLE when
   the resource becomes available.

### 15.6 Worker Thread Pool

- Default max worker threads: auto-calculated based on CPU count and architecture.
  - 32-bit: 256 + ((logical CPUs - 4) * 8) for CPUs > 4
  - 64-bit: 512 + ((logical CPUs - 4) * 16) for CPUs > 4
- When all workers are busy and new requests arrive, they queue up. If the pool is exhausted,
  connections may time out.
- Monitor: `sys.dm_os_workers`, `sys.dm_os_schedulers`, `sys.dm_os_tasks`.

### 15.7 Large Deficit First (LDF) Scheduling

Introduced in SQL Server 2016. Monitors quantum usage patterns across workers on a scheduler
and ensures no single worker monopolizes the CPU, preventing starvation scenarios.

---

## 16. TempDB Internals

### 16.1 What Uses TempDB

- **User objects**: Temporary tables (#temp, ##temp), table variables (@table), cursors.
- **Internal objects**: Work tables for spools, sorts, hash joins, cursors. Intermediate
  results for GROUP BY, ORDER BY, UNION.
- **Version store**: Row versions for RCSI, snapshot isolation, online index rebuilds,
  AFTER triggers.

### 16.2 Allocation Contention

The concurrent creation of many temporary objects causes contention on **PFS, GAM, and SGAM**
allocation pages (visible as PAGELATCH_EX and PAGELATCH_UP waits on tempdb pages 2:1:1,
2:1:2, 2:1:3, etc.).

Mitigations:
- Multiple tempdb data files (1 per logical CPU up to 8, then add in multiples of 4).
- Trace flag 1118 (pre-2016): Forces uniform extent allocation, reducing SGAM contention.
- SQL Server 2016+: Uniform extent allocation is the default.
- SQL Server 2022: **Memory-optimized tempdb metadata** — moves system catalog tables to
  latch-free, in-memory structures.

### 16.3 Spills to TempDB

When a query operator's **memory grant** is insufficient for the actual data volume:
- **Sort spills**: Sort operator writes runs to tempdb, then merge-sorts them.
- **Hash spills**: Hash join/aggregate partitions overflow data to tempdb. Can have multiple
  spill levels (grace hash → recursive hash).
- Caused by inaccurate cardinality estimates leading to insufficient memory grants.
- Visible in execution plans as warnings and in `sys.dm_exec_query_stats` (spill columns).

### 16.4 TempDB Configuration Best Practices

- Place tempdb on fast storage (SSD preferred).
- Pre-size data files to avoid autogrowth events.
- All data files should be the **same size** (proportional fill ensures even distribution).
- Enable instant file initialization for data files.
- Use memory-optimized tempdb metadata on SQL Server 2022+ for high-concurrency workloads.

---

## 17. In-Memory OLTP (Hekaton)

### 17.1 Overview

In-Memory OLTP (codename **Hekaton**) was introduced in SQL Server 2014. Memory-optimized
tables reside entirely in memory with a fundamentally different engine architecture:

- **No pages** — rows are stored as linked lists of versioned row structures.
- **No locks or latches** — uses optimistic MVCC with lock-free data structures.
- **No buffer pool** — data lives directly in memory, not as cached pages.

### 17.2 Index Types

Memory-optimized tables support two index types:

#### Hash Index
- Lock-free hash table with a fixed number of buckets.
- O(1) point lookups.
- Bucket count must be configured at creation (should be 1-2x the expected distinct key values).
- Not suitable for range scans.

#### Bw-Tree (Range Index)
- A **lock-free, latch-free B-tree** variant.
- Uses **page mapping table** indirection — instead of modifying pages in place, a delta record
  is prepended. Periodically consolidated.
- Supports both point lookups and range scans.
- Good general-purpose index for memory-optimized tables.

### 17.3 Row Structure

```
┌──────────────────────────────────────────┐
│           Row Header                     │
│  Begin Timestamp | End Timestamp         │
│  (creating txn)  | (deleting txn / ∞)   │
│  Index Pointers (one per index)          │
├──────────────────────────────────────────┤
│           Row Payload (column data)      │
└──────────────────────────────────────────┘
```

- Each row has a validity interval: [BeginTs, EndTs).
- Updates create a new version and set EndTs on the old version.
- Garbage collection removes old versions no longer visible to any active transaction.

### 17.4 Natively Compiled Modules

Stored procedures, triggers, and scalar UDFs can be **natively compiled** to machine code (DLL)
at creation time:
- Eliminates interpretation overhead of T-SQL.
- Up to **30-40x performance improvement** for OLTP operations.
- Restrictions: subset of T-SQL supported, no interop with disk-based tables in some contexts.

### 17.5 Durability Options

- **SCHEMA_AND_DATA** (default): Both schema and data survive restart. Uses checkpoint files
  (data/delta file pairs) for persistence.
- **SCHEMA_ONLY**: Only schema survives. Data is lost on restart. Useful for session state,
  caching, or staging tables (replaces tempdb usage).

### 17.6 Checkpoint Files

For durable memory-optimized tables:
- **Data files**: Append-only files containing inserted rows.
- **Delta files**: Track which rows in the corresponding data file have been deleted.
- Periodically merged by a background process.
- On restart, SQL Server replays these files to reconstruct the in-memory data.

---

## Key System DMVs for Internals Investigation

| DMV | Purpose |
|-----|---------|
| `sys.dm_os_buffer_descriptors` | Pages in buffer pool |
| `sys.dm_os_memory_clerks` | Memory allocation by component |
| `sys.dm_os_schedulers` | Scheduler state and utilization |
| `sys.dm_os_workers` | Worker thread state |
| `sys.dm_os_tasks` | Task state and assignment |
| `sys.dm_os_wait_stats` | Cumulative wait statistics |
| `sys.dm_exec_query_stats` | Query performance statistics |
| `sys.dm_exec_cached_plans` | Plan cache contents |
| `sys.dm_exec_requests` | Currently executing requests |
| `sys.dm_tran_locks` | Current lock state |
| `sys.dm_tran_version_store` | Version store contents |
| `sys.dm_db_index_physical_stats` | Index fragmentation |
| `sys.dm_db_stats_properties` | Statistics metadata |
| `sys.dm_db_xtp_*` | In-Memory OLTP DMVs |
