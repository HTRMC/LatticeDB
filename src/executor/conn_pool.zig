const std = @import("std");
const executor_mod = @import("executor.zig");
const catalog_mod = @import("../storage/catalog.zig");
const mvcc_mod = @import("../storage/mvcc.zig");
const undo_log_mod = @import("../storage/undo_log.zig");

const Executor = executor_mod.Executor;
const Catalog = catalog_mod.Catalog;
const TransactionManager = mvcc_mod.TransactionManager;
const UndoLog = undo_log_mod.UndoLog;

/// Connection pool that manages reusable Executor instances.
/// Each executor has its own transaction state but shares the catalog.
pub const ConnPool = struct {
    allocator: std.mem.Allocator,
    catalog: *Catalog,
    txn_manager: ?*TransactionManager,
    undo_log: ?*UndoLog,
    /// Available (idle) executors
    idle: std.ArrayListUnmanaged(*Executor),
    /// Maximum pool size
    max_size: usize,
    /// Total executors created (idle + active)
    total_created: usize,
    /// Cache size for statement cache per executor (0 = disabled)
    stmt_cache_size: usize,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        catalog: *Catalog,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
        max_size: usize,
    ) Self {
        return .{
            .allocator = allocator,
            .catalog = catalog,
            .txn_manager = txn_manager,
            .undo_log = undo_log,
            .idle = .empty,
            .max_size = max_size,
            .total_created = 0,
            .stmt_cache_size = 64, // default cache size
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.idle.items) |exec| {
            exec.deinit();
            self.allocator.destroy(exec);
        }
        self.idle.deinit(self.allocator);
    }

    /// Acquire an executor from the pool.
    /// Returns an idle executor if available, or creates a new one.
    pub fn acquire(self: *Self) !*Executor {
        // Try to reuse an idle executor
        if (self.idle.items.len > 0) {
            return self.idle.pop() orelse unreachable;
        }

        // Create a new executor
        if (self.total_created >= self.max_size) {
            return error.PoolExhausted;
        }

        const exec = try self.allocator.create(Executor);
        exec.* = Executor.initWithMvcc(
            self.allocator,
            self.catalog,
            self.txn_manager,
            self.undo_log,
        );
        if (self.stmt_cache_size > 0) {
            exec.enableStmtCache(self.stmt_cache_size);
        }
        self.total_created += 1;
        return exec;
    }

    /// Release an executor back to the pool.
    /// If the executor has an open transaction, it is aborted.
    pub fn release(self: *Self, exec: *Executor) void {
        // Abort any open transaction
        exec.abortCurrentTxn();

        // Return to idle pool
        self.idle.append(self.allocator, exec) catch {
            // If we can't store it, just destroy it
            exec.deinit();
            self.allocator.destroy(exec);
            self.total_created -= 1;
        };
    }

    /// Get pool statistics.
    pub fn stats(self: *const Self) struct { idle: usize, total: usize, max: usize } {
        return .{
            .idle = self.idle.items.len,
            .total = self.total_created,
            .max = self.max_size,
        };
    }
};

// ── Tests ────────────────────────────────────────────────────────

const disk_manager_mod = @import("../storage/disk_manager.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const alloc_map_mod = @import("../storage/alloc_map.zig");
const DiskManager = disk_manager_mod.DiskManager;
const BufferPool = buffer_pool_mod.BufferPool;
const AllocManager = alloc_map_mod.AllocManager;

test "connection pool acquire and release" {
    const test_file = "test_conn_pool.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var pool = ConnPool.init(std.testing.allocator, &catalog, null, null, 4);
    defer pool.deinit();

    // Acquire
    const exec1 = try pool.acquire();
    const exec2 = try pool.acquire();
    try std.testing.expectEqual(@as(usize, 2), pool.total_created);
    try std.testing.expectEqual(@as(usize, 0), pool.idle.items.len);

    // Release
    pool.release(exec1);
    try std.testing.expectEqual(@as(usize, 1), pool.idle.items.len);

    // Re-acquire — should reuse
    const exec3 = try pool.acquire();
    try std.testing.expectEqual(@as(usize, 0), pool.idle.items.len);
    try std.testing.expectEqual(@as(usize, 2), pool.total_created); // No new creation

    pool.release(exec2);
    pool.release(exec3);
}

test "connection pool max size" {
    const test_file = "test_conn_pool_max.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var pool = ConnPool.init(std.testing.allocator, &catalog, null, null, 2);
    defer pool.deinit();

    const exec1 = try pool.acquire();
    const exec2 = try pool.acquire();
    try std.testing.expectError(error.PoolExhausted, pool.acquire());

    pool.release(exec1);
    pool.release(exec2);
}

test "connection pool execute with cache" {
    const test_file = "test_conn_pool_cache.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var pool = ConnPool.init(std.testing.allocator, &catalog, null, null, 4);
    defer pool.deinit();

    const exec = try pool.acquire();

    // Setup
    exec.freeResult(try exec.execute("CREATE TABLE users (id INT, name VARCHAR(50))"));
    exec.freeResult(try exec.execute("INSERT INTO users VALUES (1, 'Alice')"));

    // Execute with cache
    const r1 = try exec.executeCached("SELECT * FROM users");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 1), r1.rows.rows.len);

    // Execute same query again (should hit cache)
    const r2 = try exec.executeCached("SELECT * FROM users");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);

    // Check cache stats
    try std.testing.expectEqual(@as(u64, 1), exec.stmt_cache.?.hits);

    pool.release(exec);
}
