# DragonFlyDB Internals Reference

Comprehensive technical reference on DragonFlyDB's architecture, data structures, transaction model, persistence, replication, and codebase organization.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Memory Management & Data Structures](#2-memory-management--data-structures)
3. [Transaction Model & Command Execution](#3-transaction-model--command-execution)
4. [Persistence & Snapshotting](#4-persistence--snapshotting)
5. [Replication](#5-replication)
6. [Cluster Mode](#6-cluster-mode)
7. [Codebase Structure](#7-codebase-structure)

---

## 1. Architecture Overview

### 1.1 Shared-Nothing, Thread-Per-Core Model

DragonFlyDB runs as a **single process with multiple threads**, where the in-memory keyspace is partitioned (sharded) into **N parts** (N <= number of CPU logical cores). Each shard is owned and managed exclusively by a single thread. No other thread may directly access the data structures of a shard it does not own. This eliminates the need for mutexes, spinlocks, or any traditional synchronization primitives on the hot data path.

Each thread is, in effect, a mini single-threaded database engine operating on its own slice of data.

**How partitioning works:**

- Keys are assigned to shards via hashing: `shard_id = hash(key) % N_shards`.
- Each shard contains its own independent hash table (a DashTable).
- A single thread owns one or more shards and handles both I/O duties (managing TCP client connections) and data duties (executing commands against the shard's data).
- Threads 0..N-1 each run their own event loop. A thread's CPU time is shared among multiple fibers (lightweight cooperative tasks) running within that thread.

### 1.2 Inter-Thread Communication: Message Bus

When a command arrives on a connection owned by thread A, but the target key lives in a shard owned by thread B, thread A does **not** directly touch thread B's data. Instead:

1. Thread A sends a message via a **lock-free message bus** (inter-thread queue) to thread B.
2. Thread B picks up the message in its event loop, executes the operation against its shard, and posts the result back.
3. Thread A's fiber resumes and returns the result to the client.

This message-passing design avoids lock contention entirely. At high throughput, the system achieves **6.43 million ops/sec on a 64-core AWS Graviton3 instance** with P50 latency of 0.3ms and P99 of 1.1ms.

### 1.3 The Helio Framework: io_uring and Fiber-Based I/O

**Helio** is the custom I/O and concurrency framework that powers DragonFlyDB (repo: [github.com/romange/helio](https://github.com/romange/helio)). Written by Roman Gershman as a successor to his earlier GAIA library, inspired by the Seastar framework (used by ScyllaDB) but simplified.

**io_uring integration:**

- A single polling loop per thread handles both networking I/O and disk I/O events via io_uring's unified submission/completion queue model.
- io_uring enables true asynchronous I/O for disk operations (e.g., RDB snapshotting), so persistence tasks do not block the event loop.
- The io_uring submission queue (SQ) and completion queue (CQ) are per-thread, aligning with the shared-nothing model.
- Fallback to **epoll** on Linux < 5.11, **kqueue** on FreeBSD.

**Fiber model (stackful coroutines):**

- Each fiber has its own stack (typically 8-64KB).
- Fibers yield cooperatively -- when a fiber needs to wait for I/O, it yields control back to the scheduler.
- The scheduler picks the next runnable fiber and switches to it. Context switches are extremely cheap (just a stack pointer swap).
- Originally used Boost.Fiber internally until May 2023, after which forked and integrated directly into Helio.
- Key improvements: custom `DispatchPolicy`, lock-free inter-thread notification queue, tighter io_uring integration.

**ProactorPool and Proactor pattern:**

- Each **Proactor** owns one OS thread and runs an event loop that: polls io_uring/epoll, checks the inter-thread message queue, schedules runnable fibers.
- `ProactorPool::GetNextProactor()` returns the next proactor in round-robin order for distributing new connections.
- Inter-thread communication uses `Proactor::DispatchOnce()` / `Proactor::Await()` / `Proactor::AsyncCall()`.

### 1.4 Connection Handling

**Connection acceptance:**
- `AcceptServer` listens on the configured TCP port.
- New connections are assigned to a proactor/thread via round-robin.
- A new fiber is spawned on the assigned thread to manage the connection for its entire lifetime.

**Per-connection fiber architecture:**
- Each client connection has a dedicated fiber that reads commands (RESP protocol parsing), dispatches them to the appropriate shard thread(s), and writes responses back.
- The fiber yields during I/O waits, allowing other fibers on the same thread to make progress.

**Pipeline squashing:**
- When the number of queued pipelined commands exceeds a configurable threshold (`pipeline_squash` flag), DragonFlyDB batches multiple single-shard commands destined for the same shard into a single dispatch, dramatically reducing inter-thread message overhead. This can double throughput for large pipelines.

---

## 2. Memory Management & Data Structures

### 2.1 The Dash Hash Table (DashTable)

DashTable is DragonFlyDB's core hash table, evolved from extendible hashing (1979) and based on the paper "Dash: Scalable Hashing on Persistent Memory" (adapted for DRAM).

**Three-level hierarchy:**

```
Directory (array of segment pointers)
  |
  v
Segment 0    Segment 1    Segment 2 ...
  |
  v
56 regular buckets + 4 stash buckets
Each bucket: 14 slots (key-value pairs)
Total capacity per segment: ~840 entries
```

**Directory (top level):**
- Array of pointers to segments. For N items, requires ~`N/840` entries (~9,600 bytes for 1M items -- negligible overhead).
- Grows by pointer-doubling (extendible hashing), but only a subset of pointers updated per split.

**Segments (middle level):**
- Self-contained mini-hash-table of constant size.
- 56 regular buckets + 4 stash buckets (60 total).
- Each bucket has 14 slots, max capacity ~840 records per segment.
- Open-addressing collision scheme with probing -- no linked-list nodes.

**Insertion algorithm:**
1. Hash key -> determine segment ID.
2. Within segment, hash determines a home bucket (one of 56 regular).
3. Algorithm predefines 2 candidate buckets where item can reside.
4. Place in any free slot within those two candidates.
5. If both full, place in one of 4 stash buckets (overflow).
6. If stash buckets also full, segment must split.

**Splitting (growth):**
- Segment contents split across two segments; one new segment allocated per split.
- No wholesale rehashing of entire table (unlike Redis).
- During split, expired items are opportunistically garbage-collected first (O(1) amortized).

**Metadata overhead:**
- Average ~20 bits per entry (vs 64 bits for Redis `dictEntry.next`).
- Per-entry memory footprint: 22-32 bytes (6-16 bytes overhead), vs Redis's significantly higher overhead.

**Cache eviction (2Q / LFRU):**
- When `cache_mode=true`, stash buckets serve as a probationary FIFO queue.
- Regular bucket slots have ranked positions (left = highest rank).
- Eviction operates at O(1) with **zero additional memory overhead per item** (no LRU pointers needed).

| Feature | Redis `dict` | DashTable |
|---|---|---|
| Collision handling | Separate chaining (linked list) | Open addressing with probing |
| Per-entry metadata | 64 bits (next pointer) | ~20 bits (fingerprints + control) |
| Resize strategy | Allocate 2x table, incremental rehash | Add one segment per split |
| Cache behavior | Poor (pointer chasing) | Excellent (1-3 cache lines per lookup) |

### 2.2 Memory Allocation: mimalloc and Per-Thread Heaps

DragonFlyDB uses [mimalloc](https://github.com/microsoft/mimalloc) (Microsoft's compact allocator) because of:
- Friendliness with shared-nothing architecture via `mi_heap_t` thread-local API.
- Support for first-class heaps: creating multiple independent heaps per thread, with ability to destroy a heap and free all objects at once.

Each shard has its own mimalloc heap (`mi_heap_t`). All objects belong to their shard's heap. Commands like `RENAME` that move keys between shards require inter-thread message passing -- the key is serialized out of one shard's heap and deserialized into the destination shard's heap.

`MiMemoryResource` wraps mimalloc in a C++ `pmr::memory_resource` interface. `CompactObj` uses it for all allocations.

### 2.3 CompactObject Encoding

`CompactObj` (defined in `src/core/compact_object.h`) replaces Redis's `robj` (redisObject). Total size: **16 bytes** (verified by `static_assert`).

**Tag system (`taglen_` field):**

| Tag Value | Meaning |
|---|---|
| 0-16 | Inline string (stored directly in the 16-byte union) |
| 17 (INT_TAG) | int64_t value stored inline |
| 18 (SMALL_TAG) | SmallString (heap-allocated small string) |
| 19 (ROBJ_TAG) | RobjWrapper (complex types: list, set, zset, hash, stream) |
| 20 (EXTERNAL_TAG) | ExternalPtr (tiered/SSD storage) |
| 21 (JSON_TAG) | JSON object |
| 22 (SBF_TAG) | Scalable Bloom Filter |

**String encodings:**
- Inline: strings up to 16 bytes stored directly in `CompactObj` (zero-allocation).
- `SMALL_TAG`: compact representation for slightly larger small strings.
- `ASCII1_ENC` / `ASCII2_ENC`: ASCII compression.
- `HUFFMAN_ENC`: Huffman compression (added in v1.21.0, applied when it saves >= 2 bytes).

**RobjWrapper** (for complex values): packed struct with `inner_obj_` (void*), `sz_` (56-bit), `type_` (4-bit), `encoding_` (4-bit).

### 2.4 Redis Data Type Implementations

**Strings:**
- Small integers: stored inline with `INT_TAG`.
- Short strings (up to 16 bytes): stored inline.
- Longer strings: `RobjWrapper` with optional Huffman compression.

**Lists:**
- Small: listpack encoding.
- Large: QList (custom reimplementation of Redis quicklist) -- doubly-linked list of listpack nodes.

**Sets:**
- Small integer sets: intset (same as Redis).
- Small string sets: listpack encoding.
- Large: **DenseSet** -- custom hash table with pointer tagging.
  - Exploits top 12 bits of x86-64 userspace addresses for tag bits.
  - At 100% utilization: ~12 bytes per record (vs Redis's ~32 bytes).
  - SIMD optimization for set operations.

**Hashes:**
- Small: listpack encoding.
- Large: **StringMap** -- variant built on DenseSet/DashTable infrastructure. 30-60% less memory than Redis hash tables, 2-3x faster lookups.

**Sorted Sets:**
- Small (up to 128 entries): listpack encoding.
- Large: **B+ tree** (replacing Redis's skiplist, stable since v1.11, only impl since v1.15).
  - 256-byte node arrays, up to 15 (member, score) pairs per node.
  - ~2-3 bytes overhead per entry (vs Redis skiplist's ~37 bytes).
  - ~40% memory reduction overall.

**Streams:**
- Reuses Redis's stream implementation largely unchanged.
- Radix tree indexed structure with listpack-encoded macro-nodes.

### 2.5 Expiry Mechanism

**Dual DashTable architecture:**
1. Primary DashTable: all key-value entries.
2. Expire DashTable: only keys with TTL set, mapping key -> expiration timestamp.

**TTL precision:** millisecond precision, but rounded to nearest second for deadlines > 2^28 ms (~3.1 days).

**Expiration strategies:**
- **Lazy**: checked on access, deleted if expired.
- **Active**: background heartbeat task (default `hz = 100`) samples segments from expire table and deletes expired entries.
- **Segment-split opportunistic GC**: when primary DashTable segment needs to split, expired items are garbage-collected first.

### 2.6 Memory Defragmentation

An active defragmentation task (`defrag_task`) traverses DashTable segments incrementally:
1. Checks if mimalloc page hosting an entry is underutilized (ratio below threshold).
2. If underutilized, reallocates entry to a new, more densely-packed page.
3. Runs in ~200-300 microsecond increments to avoid blocking.
4. Also defragments sub-structures (DenseSet nodes, QList nodes, etc.).

---

## 3. Transaction Model & Command Execution

### 3.1 VLL (Very Lightweight Locking) Framework

Based on the paper "VLL: A Lock Manager Redesign for Main Memory Database Systems" (VLDB 2013). Key ideas:

- **Eliminates the traditional lock manager.** Each data item gets a lightweight metadata field with two counters (shared-mode holders, exclusive-mode holders).
- **Transaction IDs (txid)**: assigned by incrementing a single global atomic counter, giving every transaction a unique, monotonically increasing ID.
- **Per-shard transaction queue (TxQueue)**: each shard maintains a queue of pending operations. A transaction spanning multiple shards appears in each relevant shard's queue with the same txid.
- **Intent locks**: lightweight records where a transaction declares its future intention to access certain keys. Stored in a compact table per-thread.
- **Out-of-order execution**: a transaction that doesn't conflict with anything currently locked can execute before reaching the front of the queue. In practice, **most commands execute out-of-order**.

### 3.2 The "Hop" Model

A **hop** is a single dispatch of callbacks to shard threads:

- **Single-hop**: coordinator sends one message to the target shard thread, operation executes, result returns. Most multi-key commands (MSET, MGET) are single-hop.
- **Multi-hop**: iterative rounds of dispatching work to shard threads. More expensive (holds locks longer). Examples: Lua scripts, MULTI/EXEC blocks that read before deciding what to write.
- **Quick-run optimization**: for simple single-shard commands, bypasses the transactional framework entirely -- no work submission, no lock acquisition.

### 3.3 Transaction Scheduling Phases

1. **InitByArgs**: analyze command arguments to determine which keys and shards are involved.
2. **Schedule/Arm**: enter each relevant shard's TxQueue and acquire intent locks. Transaction becomes "armed."
3. **Execute (RunInShard)**: operation callback runs on shard thread. Single-shard may use `RunQuickie`.
4. **Finalize/Unlock**: intent locks released, transaction removed from shard queues.

### 3.4 MULTI/EXEC Implementation

- Commands between MULTI and EXEC are buffered in the connection's command queue.
- At EXEC time, the framework analyzes all queued commands to determine involved shards.
- The entire batch executes atomically.

**Multi-Exec Squashing (`MultiCommandSquasher`):**
- When multiple commands within MULTI/EXEC target the same shard, they are squashed and executed in a single hop.
- Controlled by `multi_exec_squash` flag.
- Critical for workloads like BullMQ where many commands target the same logical queue.

### 3.5 Lua Scripting

- Embeds **Lua 5.4.4** (not 5.1 like Redis).
- **By default, all keys accessed by a script must be declared upfront** via KEYS argument. DragonFlyDB locks all declared keys before execution and prohibits accessing undeclared keys.
- **Global transaction mode** (`--default_lua_flags=allow-undeclared-keys`): locks the entire datastore; dramatically reduces parallelism.
- **Disable-atomicity mode** (`--default_lua_flags=disable-atomicity`): allows interleaving with regular commands, better parallelism.
- Supports **connection migration**: system can migrate a connection to the thread owning the target shard (at most once per connection).
- Multiple script execution units can run concurrently on different threads.

### 3.6 Command Registry and Dispatch

Each command family registers commands with:
- Command name, handler function, flags (`CO::WRITE`, `CO::READONLY`, `CO::FAST`, etc.).
- Key specification (which arguments are keys) for shard mapping.

**Dispatch flow:**
1. `Connection::ParseRedis()` -- parse incoming RESP command.
2. `Service::DispatchCommand()` -- look up command in registry.
3. `Transaction::InitByArgs()` -- determine involved keys/shards.
4. Scheduling: single-shard fast path or full VLL scheduling.
5. `Service::InvokeCmd()` -- call command handler.
6. Response sent back through connection fiber.

### 3.7 Pub/Sub

- Subscribers are connected to specific threads (their connection's thread).
- On PUBLISH, the publishing thread dispatches messages to all threads that have subscriber connections for that channel via inter-thread message passing.
- Supports shard channels (`SPUBLISH`/`SSUBSCRIBE` from Redis 7.0 cluster spec).

### 3.8 Blocking Commands (BLPOP, BRPOP, etc.)

- `BlockingController` manages blocked connections.
- When BLPOP finds no data, the connection fiber blocks (yields, not the thread). `BlockingController` registers the connection as waiting.
- When LPUSH/RPUSH occurs on a watched key, `AwakeWatched` resumes the blocked fiber.
- **Multi-key blocking across shards**: cannot guarantee the same unblock order as Redis. BLPOP may return from any listed key that became non-empty.

### 3.9 Performance Optimizations

| Optimization | Description |
|---|---|
| Single-shard fast path | `ScheduleSingleHop` / `RunQuickie` bypasses multi-shard machinery |
| Multi-Exec squashing | Groups MULTI/EXEC commands by shard, executes in one hop |
| Pipeline squashing | Groups pipelined commands by shard, dispatches in parallel |
| Connection migration | Moves connection fiber to target shard's thread for scripts |
| Hashtag-level locking | Locks at `{hashtag}` level instead of key level |
| Out-of-order execution | Most transactions skip the queue (no key conflicts) |
| Key prefetching | Keys prefetched into CPU cache during scheduling |

### 3.10 The Heartbeat Mechanism

Not primarily about transaction ordering (handled by global atomic txid counter). Instead serves:
- Background maintenance tasks (memory eviction scanning, dashtable segment scanning, statistics).
- Memory pressure eviction (`max_eviction_per_heartbeat` controls segments scanned).
- Configurable via `hz` flag (default 100).

---

## 4. Persistence & Snapshotting

### 4.1 Fork-less Snapshotting Algorithm

Redis uses `fork()` + copy-on-write, which can double memory under write-heavy workloads. DragonFlyDB eliminates `fork()` entirely with a **version-based point-in-time cut** algorithm.

**Core mechanism:**

1. **Global monotonic version counter**: each shard maintains a monotonically increasing counter. Each dictionary entry stores its `last_update_version`.

2. **Snapshot epoch**: when a snapshot begins, `SliceSnapshot` captures the current epoch as `cut.version`. Invariant: all entries with `version <= cut.version` have not yet been serialized.

3. **Main traversal loop**: iterates over DashTable buckets, serializing entries whose `version <= cut.version`. After serialization, entry's version is bumped past the cut.

4. **Pre-update hook for concurrent writes**:
   ```
   // Triggered on each entry mutation during snapshotting
   if (entry.version <= cut.version) {
       SendToSerializationSink(entry);  // serialize BEFORE mutation
   }
   entry = new_entry;
   entry.version = shard.epoch++;  // guaranteed > cut.version
   ```

This guarantees every entry existing at snapshot-start is serialized exactly once -- either by traversal or by the pre-update hook, whichever reaches it first. Entries added after snapshot start are never serialized.

**Key properties:**
- **Constant memory overhead** (unlike Redis's fork/COW which can 2-3x memory).
- **Natural back-pressure**: if serialization is slow, writes triggering the hook slow down proportionally.
- **No OS dependency**: does not rely on OS-level COW memory management.

### 4.2 Conservative vs. Relaxed Snapshotting

**Conservative (for RDB/DFS persistence to disk):**
- Strict point-in-time semantics. Hook pushes the previous (old) value before mutation.
- Snapshot reflects exact state at time `t` when snapshot began.

**Relaxed (for replication full-sync):**
- Snapshot that includes all data up to when snapshotting finishes is acceptable.
- Hook sends new entry value if `entry.version <= cut.version`.
- Avoids keeping a changelog of mutations during snapshot creation.

### 4.3 RDB Save Architecture

```
[Shard 0: SliceSnapshot] --blob--> |                    |
[Shard 1: SliceSnapshot] --blob--> | Blocking Channel   | --> SaveBody fiber --> AlignedBuffer --> io_uring --> disk
[Shard N: SliceSnapshot] --blob--> |                    |
```

Components:
- **`RdbSave`**: coordinator, creates a blocking channel and per-shard `SliceSnapshot` instances.
- **`SliceSnapshot`**: per-shard, uses its own `RdbSerializer` to serialize K/V entries into Redis RDB format.
- **`RdbSerializer`**: serializes entries per Redis format spec.
- **I/O**: uses io_uring with direct I/O; `AlignedBuffer` ensures page-aligned writes.

### 4.4 DFS vs. RDB Format

**DFS (Dragonfly Snapshot) format** (default):
- Creates multiple files (one per shard) in parallel.
- File extension `.dfs`.
- Optimized for DragonFlyDB's multi-threaded architecture.

**RDB format** (`--df_snapshot_format=false`):
- Single file, Redis-compatible.
- Useful for migration to/from Redis.

### 4.5 No AOF/WAL

DragonFlyDB does **not** support AOF/WAL persistence. The persistence model is purely snapshot-based. The internal journal mechanism is used exclusively for replication (streaming changes to replicas), not for on-disk durability.

For durability, configure frequent snapshots via `--snapshot_cron` (e.g., `"*/5 * * * *"` for every 5 minutes).

---

## 5. Replication

### 5.1 Architecture

DragonFlyDB's replication is internally different from Redis but keeps a compatible external API (`REPLICAOF` / `ROLE` commands, Redis OSS replication protocol up to v6.2).

Key difference: Redis replicates through a single thread, while DragonFlyDB replicates **in parallel across all shard threads** with one TCP connection per shard.

### 5.2 Replication Phases

**Phase 1 -- Handshake:**
- Replica opens connections to master (one per shard thread).
- Negotiation of replication parameters.

**Phase 2 -- Full Synchronization:**
- Each shard initiates a relaxed snapshot.
- Snapshot data streamed directly to replica over per-shard connections.
- Mutations during full sync are sent in parallel with snapshot data (no replication buffer needed).

**Phase 3 -- Streaming State (transition):**
- When individual shard finishes full sync, it transitions to streaming state.
- Different shards may finish at different times.

**Phase 4 -- Stable Synchronization:**
- Every mutation streamed asynchronously via the journal.
- `JournalStreamer` maintains logical sequence numbers (LSNs) for ordered delivery.

Full sync is approximately **5.5x faster** than Redis replication.

### 5.3 DragonFlyDB vs. Redis Replication

| Aspect | Redis | DragonFlyDB |
|---|---|---|
| Sync connections | Single thread, single connection | One connection per shard thread |
| Full sync mechanism | `fork()` + RDB transfer + backlog buffer | Relaxed snapshot + parallel journal streaming |
| Memory during sync | Spikes due to COW | Constant overhead |
| Partial resync (PSYNC) | Supported via backlog | Currently disabled (memory leak in ring buffer) |
| Protocol compatibility | Native | Compatible with Redis OSS up to v6.2 |

---

## 6. Cluster Mode

### 6.1 Emulated Cluster Mode (`--cluster_mode=emulated`)

- Single-node Dragonfly that responds to CLUSTER commands.
- Fully compatible with standalone mode (supports SELECT, multi-key operations).
- No actual horizontal scaling.
- Ideal for development, testing, and migration.

### 6.2 Multi-Node Cluster Mode (`--cluster_mode=yes`)

- True horizontal scaling via sharding across multiple nodes.
- Uses same **16,384 hash slot** model as Redis Cluster.
- Configuration pushed via `DFLYCLUSTER CONFIG` command (JSON-encoded).
- **Nodes do not communicate with each other** to sync config -- same config must be pushed externally.

**Slot migration:**
- Uses internal `DFLYMIGRATE` commands.
- Multiple parallel TCP connections, significantly faster than Redis Cluster's single-threaded approach.

**Control plane separation:**
- Data plane: Dragonfly server (open source).
- Control plane: health monitoring, failover, migration orchestration -- not part of the server.
- Self-managed: `cluster_mgr.py` script for basic management.
- Managed: **Dragonfly Swarm** (cloud offering, targets 100+ TB memory, 100M+ RPS).

---

## 7. Codebase Structure

### 7.1 Repository Layout

```
dragonfly/
  src/
    common/               # Common utilities
    core/                 # Core data structures (DashTable, CompactObject, etc.)
    facade/               # Network layer, protocol parsers, connection handling
    huff/                 # Huffman coding
    redis/                # Borrowed Redis C data structures (SDS, listpack, intset)
    server/               # Core server implementation
  helio/                  # Git submodule - async I/O and fiber framework
  tests/
    dragonfly/            # Pytest-based system tests
    fakeredis/            # Fake Redis compatibility tests
    integration/          # Docker-based integration tests (Node-Redis, ioredis, Jedis)
  docs/                   # Architecture documentation
  CMakeLists.txt
```

### 7.2 Build System

- **CMake** (minimum 3.15) with **Ninja** as build generator. **C++20** standard.
- Build: `./helio/blaze.sh && cd build-dbg && ninja dragonfly` (debug) or `./helio/blaze.sh -release && cd build-opt && ninja dragonfly` (release).

**Key dependencies:**

| Library | Purpose |
|---------|---------|
| mimalloc v2.2.4 | Memory allocator (with custom patches) |
| Lua Dragonfly-5.4.6a | Scripting engine |
| lz4 v1.10.0 | Fast compression |
| jsoncons | JSON processing |
| flatbuffers v23.5.26 | Serialization |
| hnswlib | Approximate nearest neighbor search (vector search) |
| SimSIMD v6.5.3 | SIMD vector similarity |

### 7.3 `src/facade/` -- Network and Protocol Layer

| File | Purpose |
|------|---------|
| `dragonfly_connection.cc/h` | Core connection handler, lifecycle management |
| `dragonfly_listener.cc/h` | TCP listener, creates connections |
| `redis_parser.cc/h` | RESP protocol parser |
| `memcache_parser.cc/h` | Memcached protocol parser |
| `reply_builder.cc/h` | Constructs RESP/Memcached replies |
| `conn_context.h` | Base ConnectionContext class |
| `service_interface.h` | Abstract interface for Service |
| `cmd_arg_parser.cc/h` | Argument parsing utilities |
| `op_status.cc/h` | OpStatus enum for error handling |

### 7.4 `src/core/` -- Core Data Structures

**Hash tables:**
- `dash.h` -- DashTable template class (the central hash table).
- `dash_internal.h` -- internal segment/bucket implementation.

**Object encoding:**
- `compact_object.cc/h` -- CompactObject (universal value representation).

**Data structures:**
- `dense_set.cc/h` -- memory-efficient set.
- `string_map.cc/h` -- string-keyed map (for Redis hashes).
- `string_set.cc/h` -- string set.
- `sorted_map.cc/h` -- sorted map (for sorted sets).
- `qlist.cc/h` -- quicklist (for lists).
- `bptree_set.h` -- B+ tree set.
- `bloom.cc/h` -- Bloom filter.

**Memory management:**
- `mi_memory_resource.cc/h` -- mimalloc-based memory resource.
- `segment_allocator.cc/h` -- segment-based allocation.
- `small_string.cc/h` -- small string optimization.

**Transaction infrastructure:**
- `tx_queue.cc/h` -- per-shard transaction queue (circular doubly-linked list).
- `intent_lock.h` -- SHARED/EXCLUSIVE intent lock with O(1) operations.

**Utilities:**
- `interpreter.cc/h` -- Lua interpreter integration.
- `huff_coder.cc/h` -- Huffman encoder/decoder.

### 7.5 `src/server/` -- Core Server Implementation

**Server infrastructure:**

| Class | File | Role |
|-------|------|------|
| `Service` | `main_service.cc/h` | Central command dispatcher. Owns CommandRegistry, ServerFamily, ClusterFamily, AclFamily, ProactorPool. |
| `EngineShard` | `engine_shard.cc/h` | Thread-local shard owner. Owns TxQueue, memory resources, task queues, defrag state, journal. |
| `EngineShardSet` | `engine_shard_set.cc/h` | Manages all shards. Provides `RunBriefInParallel` and `RunBlockingInParallel`. |
| `DbSlice` | `db_slice.cc/h` | Per-shard database manager. Find/Add/Delete, expiration, memory budgeting, auto-laundering iterators. |
| `DbTable` | `table.cc/h` | Single database (SELECT N). Contains PrimeTable, ExpireTable, LockTable, stats. |
| `Transaction` | `transaction.cc/h` | Multi-shard coordinator. Hop-based execution, VLL scheduling, blocking support. |
| `ConnectionContext` | `conn_context.cc/h` | Per-connection state: transaction, ACL, namespace, MULTI/EXEC, subscriptions. |

**Command families (one per Redis data type):**

| File | Commands |
|------|----------|
| `string_family.cc` | GET, SET, APPEND, INCR, etc. |
| `list_family.cc` | LPUSH, RPUSH, LPOP, LRANGE, etc. |
| `set_family.cc` | SADD, SMEMBERS, SINTER, etc. |
| `zset_family.cc` | ZADD, ZRANGE, ZRANGEBYSCORE, etc. |
| `hset_family.cc` | HSET, HGET, HGETALL, etc. |
| `stream_family.cc` | XADD, XREAD, XRANGE, etc. |
| `json_family.cc` | JSON.SET, JSON.GET, etc. |
| `generic_family.cc` | DEL, EXISTS, EXPIRE, TTL, KEYS, SCAN, etc. |
| `bitops_family.cc` | BITCOUNT, BITOP, GETBIT, etc. |
| `bloom_family.cc` | BF.ADD, BF.EXISTS, etc. |

**Persistence:**
- `rdb_save.cc/h`, `rdb_load.cc/h` -- RDB serialization/deserialization.
- `snapshot.cc/h` -- `SliceSnapshot`, point-in-time snapshot implementation.
- `server/detail/save_stages_controller.cc/h` -- save pipeline orchestration.

**Replication and journal:**
- `replica.cc/h` -- replica implementation.
- `dflycmd.cc/h` -- Dragonfly-specific replication commands.
- `server/journal/` -- journal subsystem: `journal.cc/h`, `journal_slice.cc/h`, `streamer.cc/h`, `cmd_serializer.cc/h`.

**Cluster:**
- `server/cluster/` -- `cluster_config.cc/h`, `cluster_family.cc/h`, `coordinator.cc/h`, slot migration files.

**Search:**
- `server/search/` -- `search_family.cc/h`, `doc_index.cc/h`, `aggregator.cc/h`, `global_hnsw_index.cc/h`.

**Tiered storage:**
- `server/tiering/` -- `disk_storage.cc/h`, `external_alloc.cc/h`, `op_manager.cc/h`, `small_bins.cc/h`.

### 7.6 Key Type Aliases

```cpp
using TxId = uint64_t;        // Transaction identifier
using TxClock = uint64_t;     // Transaction logical clock
using LSN = uint64_t;         // Log Sequence Number (journal)
using SlotId = uint16_t;      // Cluster slot identifier

// The core hash tables:
using PrimeTable = DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using ExpireTable = DashTable<PrimeKey, ExpirePeriod, detail::ExpireTablePolicy>;
```

### 7.7 Testing Infrastructure

**C++ unit tests:**
- Google Test + Google Mock.
- Base class `BaseFamilyTest` in `src/server/test_utils.h` creates a mocked server with `ProactorPool` and `Service`.
- `Run({"SET", "key", "value"})` dispatches RESP commands; `CheckedInt()`, `CheckedString()` verify results.
- Running: `cd build-dbg && ctest -V -L DFLY`.

**Python integration tests:**
- pytest with custom fixtures (`df_server`, `client`/`async_client`).
- Location: `tests/dragonfly/`.
- `@dfly_args` decorator for custom server flags.

**Client integration tests:**
- Docker-based, tests Node-Redis, ioredis, Jedis.
- Location: `tests/integration/`.

### 7.8 Coding Conventions

- Google C++ Style Guide 2020, 100-character line limit.
- `snake_case` for variables, `PascalCase` for functions/methods, `kPascalCase` for constants.
- Error handling via `OpStatus` enum returned via `OpResult<T>`.
- Command pattern: each command family registers handlers in `RegisterCommands()`.
- **Critical rule**: must use fiber-aware primitives (`util::fb2::Mutex`, `util::fb2::Fiber`), never `std::thread` or `std::mutex`.

---

## Sources

- [DragonFlyDB GitHub Repository](https://github.com/dragonflydb/dragonfly)
- [Helio Framework](https://github.com/romange/helio)
- [DashTable Documentation](https://github.com/dragonflydb/dragonfly/blob/main/docs/dashtable.md)
- [DenseSet Documentation](https://github.com/dragonflydb/dragonfly/blob/main/docs/dense_set.md)
- [RDB Save Documentation](https://github.com/dragonflydb/dragonfly/blob/main/docs/rdbsave.md)
- [Shared-Nothing Architecture Doc](https://github.com/dragonflydb/dragonfly/blob/main/docs/df-share-nothing.md)
- [Announcing Dragonfly](https://www.dragonflydb.io/blog/announcing-dragonfly)
- [Ensuring Atomicity: A Tale of Dragonfly Transactions](https://www.dragonflydb.io/blog/transactions-in-dragonfly)
- [Dragonfly Cache Design](https://www.dragonflydb.io/blog/dragonfly-cache-design)
- [Balanced vs Unbalanced (Snapshot Algorithm)](https://www.dragonflydb.io/blog/balanced-vs-unbalanced)
- [Dragonfly New Sorted Set (B+ Tree)](https://www.dragonflydb.io/blog/dragonfly-new-sorted-set)
- [Batch Operations in Dragonfly](https://www.dragonflydb.io/blog/batch-operations-in-dragonfly)
- [BullMQ Optimization Blog](https://www.dragonflydb.io/blog/running-bullmq-with-dragonfly-part-2-optimization)
- [Replication for High Availability](https://www.dragonflydb.io/blog/replication-for-high-availability)
- [A Preview of Dragonfly Cluster](https://www.dragonflydb.io/blog/a-preview-of-dragonfly-cluster)
- [Redis and Dragonfly Architecture Comparison](https://www.dragonflydb.io/guides/redis-and-dragonfly-architecture-comparison)
- [Dragonfly Achieves 6.43 Million RPS](https://www.dragonflydb.io/blog/dragonfly-achieves-6-million-rps-on-64-core-graviton3)
- [Lua Scripting Docs](https://www.dragonflydb.io/docs/managing-dragonfly/scripting)
- [Snapshotting Docs](https://www.dragonflydb.io/docs/managing-dragonfly/snapshotting)
- [Cluster Mode Docs](https://www.dragonflydb.io/docs/managing-dragonfly/cluster-mode)
- [VLL Paper (VLDB 2013)](http://cs.yale.edu/homes/thomson/publications/vll-vldb13.pdf)
