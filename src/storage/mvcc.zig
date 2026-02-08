const std = @import("std");
const wal_mod = @import("wal.zig");

/// Transaction ID type
pub const TxnId = u32;

/// Special transaction IDs
pub const INVALID_TXN_ID: TxnId = 0;
pub const FIRST_TXN_ID: TxnId = 1;

/// No undo pointer sentinel
pub const NO_UNDO_PTR: u64 = std.math.maxInt(u64);

/// Tuple header — prepended to every heap tuple for MVCC.
/// 16 bytes, extern struct for stable layout.
pub const TupleHeader = extern struct {
    /// Transaction that created this version
    xmin: TxnId,
    /// Transaction that deleted/replaced this version (0 = live)
    xmax: TxnId,
    /// Offset in undo log to previous version (maxInt = none)
    undo_ptr: u64,

    pub const SIZE: usize = @sizeOf(TupleHeader);

    pub fn init(txn_id: TxnId) TupleHeader {
        return .{
            .xmin = txn_id,
            .xmax = 0,
            .undo_ptr = NO_UNDO_PTR,
        };
    }

    pub fn isLive(self: *const TupleHeader) bool {
        return self.xmax == 0;
    }
};

/// Transaction state
pub const TxnState = enum {
    active,
    committed,
    aborted,
};

/// Isolation level
pub const IsolationLevel = enum {
    snapshot_isolation,
    read_committed,
};

/// A point-in-time snapshot for snapshot isolation
pub const Snapshot = struct {
    /// All transactions below this are completed
    xmin: TxnId,
    /// First unassigned txn_id at snapshot time
    xmax: TxnId,
    /// Transactions in [xmin, xmax) that were still active at snapshot time
    active: []const TxnId,

    /// Check if a given txn_id is visible (committed) from this snapshot's perspective
    pub fn isCommitted(self: *const Snapshot, txn_id: TxnId, txn_manager: *const TransactionManager) bool {
        // Transaction 0 is never valid
        if (txn_id == INVALID_TXN_ID) return false;

        // If txn_id >= xmax, it started after our snapshot — invisible
        if (txn_id >= self.xmax) return false;

        // If txn_id < xmin, it completed before our snapshot — check commit status
        if (txn_id < self.xmin) return txn_manager.isCommitted(txn_id);

        // In the range [xmin, xmax): check if it was active at snapshot time
        for (self.active) |active_id| {
            if (active_id == txn_id) return false; // Was active → not committed from our view
        }

        // Not in active list — check if it's actually committed now
        return txn_manager.isCommitted(txn_id);
    }
};

/// Visibility check result
pub const Visibility = enum {
    visible,
    invisible,
};

/// Check if a tuple is visible to a given snapshot.
///
/// Rules:
/// 1. xmin must be committed and visible in snapshot → tuple exists
/// 2. xmax == 0 → tuple is live (visible)
/// 3. xmax committed and visible in snapshot → tuple is deleted (invisible)
/// 4. Otherwise → tuple is visible (deleter hasn't committed from our perspective)
pub fn isVisible(header: *const TupleHeader, snapshot: *const Snapshot, txn_manager: *const TransactionManager, own_txn_id: TxnId) Visibility {
    // Check if the creating transaction is visible
    const xmin_visible = if (header.xmin == own_txn_id)
        true // Our own transaction's inserts are always visible to us
    else
        snapshot.isCommitted(header.xmin, txn_manager);

    if (!xmin_visible) return .invisible;

    // Tuple exists from our perspective. Check if it's been deleted.
    if (header.xmax == 0) return .visible; // Not deleted

    // Check if the deleting transaction is visible
    const xmax_visible = if (header.xmax == own_txn_id)
        true // Our own deletes are visible to us
    else
        snapshot.isCommitted(header.xmax, txn_manager);

    if (xmax_visible) return .invisible; // Deleted and committed from our view

    return .visible; // Deleter not committed from our perspective
}

/// Transaction object
pub const Transaction = struct {
    txn_id: TxnId,
    state: TxnState,
    snapshot: Snapshot,
    /// Head of this transaction's undo chain (for rollback)
    undo_chain_head: u64,
    /// Isolation level for this transaction
    isolation_level: IsolationLevel,
    /// Last WAL LSN written by this transaction (for WAL chain)
    last_lsn: u64,

    pub fn isActive(self: *const Transaction) bool {
        return self.state == .active;
    }
};

/// Transaction manager — manages transaction lifecycle and snapshots
pub const TransactionManager = struct {
    allocator: std.mem.Allocator,
    /// Next transaction ID to assign
    next_txn_id: TxnId,
    /// Map of active transactions
    active_txns: std.AutoHashMap(TxnId, *Transaction),
    /// Set of committed transaction IDs (for visibility checks)
    committed_txns: std.AutoHashMap(TxnId, void),
    /// Optional WAL for durability (null = no WAL, for tests)
    wal: ?*wal_mod.Wal,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return initWithWal(allocator, null);
    }

    pub fn initWithWal(allocator: std.mem.Allocator, wal: ?*wal_mod.Wal) Self {
        return .{
            .allocator = allocator,
            .next_txn_id = FIRST_TXN_ID,
            .active_txns = std.AutoHashMap(TxnId, *Transaction).init(allocator),
            .committed_txns = std.AutoHashMap(TxnId, void).init(allocator),
            .wal = wal,
        };
    }

    pub fn deinit(self: *Self) void {
        // Free any remaining active transactions
        var it = self.active_txns.valueIterator();
        while (it.next()) |txn_ptr| {
            if (txn_ptr.*.snapshot.active.len > 0) {
                self.allocator.free(txn_ptr.*.snapshot.active);
            }
            self.allocator.destroy(txn_ptr.*);
        }
        self.active_txns.deinit();
        self.committed_txns.deinit();
    }

    /// Begin a new transaction
    pub fn begin(self: *Self) !*Transaction {
        return self.beginWithIsolation(.snapshot_isolation);
    }

    /// Begin a new transaction with a specific isolation level
    pub fn beginWithIsolation(self: *Self, isolation_level: IsolationLevel) !*Transaction {
        const txn_id = self.next_txn_id;
        self.next_txn_id += 1;

        // Take a snapshot of currently active transactions
        const snapshot = try self.takeSnapshot(txn_id);

        // Log BEGIN to WAL
        var begin_lsn: u64 = 0;
        if (self.wal) |w| {
            begin_lsn = w.logBegin(txn_id) catch 0;
        }

        const txn = try self.allocator.create(Transaction);
        txn.* = .{
            .txn_id = txn_id,
            .state = .active,
            .snapshot = snapshot,
            .undo_chain_head = NO_UNDO_PTR,
            .isolation_level = isolation_level,
            .last_lsn = begin_lsn,
        };

        try self.active_txns.put(txn_id, txn);
        return txn;
    }

    /// Refresh a transaction's snapshot (for READ COMMITTED isolation)
    pub fn refreshSnapshot(self: *Self, txn: *Transaction) !void {
        // Free old snapshot active list
        if (txn.snapshot.active.len > 0) {
            self.allocator.free(txn.snapshot.active);
        }
        // Take a fresh snapshot
        txn.snapshot = try self.takeSnapshot(txn.txn_id);
    }

    /// Commit a transaction
    pub fn commit(self: *Self, txn: *Transaction) !void {
        // Log COMMIT to WAL (must be durable before we acknowledge)
        if (self.wal) |w| {
            _ = w.logCommit(txn.txn_id, txn.last_lsn) catch {};
        }

        txn.state = .committed;
        try self.committed_txns.put(txn.txn_id, {});
        _ = self.active_txns.remove(txn.txn_id);
        if (txn.snapshot.active.len > 0) {
            self.allocator.free(txn.snapshot.active);
            txn.snapshot.active = &.{};
        }
        self.allocator.destroy(txn);
    }

    /// Abort a transaction (caller must handle undo log rollback separately)
    pub fn abort(self: *Self, txn: *Transaction) void {
        // Log ABORT to WAL
        if (self.wal) |w| {
            _ = w.logAbort(txn.txn_id, txn.last_lsn) catch {};
        }

        txn.state = .aborted;
        _ = self.active_txns.remove(txn.txn_id);
        if (txn.snapshot.active.len > 0) {
            self.allocator.free(txn.snapshot.active);
            txn.snapshot.active = &.{};
        }
        self.allocator.destroy(txn);
    }

    /// Check if a transaction is committed
    pub fn isCommitted(self: *const Self, txn_id: TxnId) bool {
        return self.committed_txns.contains(txn_id);
    }

    /// Take a snapshot of the current transaction state
    fn takeSnapshot(self: *Self, excluding_txn_id: TxnId) !Snapshot {
        // Find the minimum active txn_id (xmin)
        var xmin: TxnId = self.next_txn_id;
        var active_count: usize = 0;

        var it = self.active_txns.keyIterator();
        while (it.next()) |key_ptr| {
            const active_id = key_ptr.*;
            if (active_id == excluding_txn_id) continue;
            if (active_id < xmin) xmin = active_id;
            active_count += 1;
        }

        // If no active txns (besides ourselves), xmin = our txn_id
        if (active_count == 0) xmin = excluding_txn_id;

        // Collect active txn IDs
        const active = try self.allocator.alloc(TxnId, active_count);
        var idx: usize = 0;
        var it2 = self.active_txns.keyIterator();
        while (it2.next()) |key_ptr| {
            const active_id = key_ptr.*;
            if (active_id == excluding_txn_id) continue;
            active[idx] = active_id;
            idx += 1;
        }

        return .{
            .xmin = xmin,
            .xmax = self.next_txn_id,
            .active = active,
        };
    }

    /// Get the oldest active transaction ID (for GC)
    pub fn oldestActiveTxnId(self: *const Self) TxnId {
        var oldest: TxnId = self.next_txn_id;
        var it = self.active_txns.keyIterator();
        while (it.next()) |key_ptr| {
            if (key_ptr.* < oldest) oldest = key_ptr.*;
        }
        return oldest;
    }
};

// ============================================================
// Tests
// ============================================================

test "TupleHeader layout" {
    try std.testing.expectEqual(@as(usize, 16), TupleHeader.SIZE);
    const h = TupleHeader.init(42);
    try std.testing.expectEqual(@as(TxnId, 42), h.xmin);
    try std.testing.expectEqual(@as(TxnId, 0), h.xmax);
    try std.testing.expectEqual(NO_UNDO_PTR, h.undo_ptr);
    try std.testing.expect(h.isLive());
}

test "TransactionManager begin and commit" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn1 = try tm.begin();
    try std.testing.expectEqual(@as(TxnId, 1), txn1.txn_id);
    try std.testing.expectEqual(TxnState.active, txn1.state);

    const txn2 = try tm.begin();
    try std.testing.expectEqual(@as(TxnId, 2), txn2.txn_id);

    // txn1 should be in txn2's active list
    try std.testing.expectEqual(@as(usize, 1), txn2.snapshot.active.len);
    try std.testing.expectEqual(@as(TxnId, 1), txn2.snapshot.active[0]);

    try tm.commit(txn1);
    try std.testing.expect(tm.isCommitted(1));
    try std.testing.expect(!tm.isCommitted(2));

    try tm.commit(txn2);
    try std.testing.expect(tm.isCommitted(2));
}

test "TransactionManager abort" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn = try tm.begin();
    try std.testing.expectEqual(@as(TxnId, 1), txn.txn_id);
    tm.abort(txn);
    try std.testing.expect(!tm.isCommitted(1));
}

test "Snapshot visibility" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // txn1 inserts a row and commits
    const txn1 = try tm.begin();
    try tm.commit(txn1);

    // txn2 starts — should see txn1's changes
    const txn2 = try tm.begin();
    try std.testing.expect(txn2.snapshot.isCommitted(1, &tm));

    // txn3 starts concurrently with txn2
    const txn3 = try tm.begin();

    // txn3 should not be visible to txn2 (started after snapshot)
    try std.testing.expect(!txn2.snapshot.isCommitted(3, &tm));

    try tm.commit(txn2);
    try tm.commit(txn3);
}

test "isVisible — committed insert, no delete" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn1 = try tm.begin();
    const txn1_id = txn1.txn_id;
    try tm.commit(txn1);

    const txn2 = try tm.begin();

    const header = TupleHeader{ .xmin = txn1_id, .xmax = 0, .undo_ptr = NO_UNDO_PTR };
    try std.testing.expectEqual(Visibility.visible, isVisible(&header, &txn2.snapshot, &tm, txn2.txn_id));

    try tm.commit(txn2);
}

test "isVisible — committed delete" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // txn1 creates and commits
    const txn1 = try tm.begin();
    const txn1_id = txn1.txn_id;
    try tm.commit(txn1);

    // txn2 deletes and commits
    const txn2 = try tm.begin();
    const txn2_id = txn2.txn_id;
    try tm.commit(txn2);

    // txn3 should not see the deleted tuple
    const txn3 = try tm.begin();

    const header = TupleHeader{ .xmin = txn1_id, .xmax = txn2_id, .undo_ptr = NO_UNDO_PTR };
    try std.testing.expectEqual(Visibility.invisible, isVisible(&header, &txn3.snapshot, &tm, txn3.txn_id));

    try tm.commit(txn3);
}

test "isVisible — own transaction sees own inserts and deletes" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn = try tm.begin();
    const txn_id = txn.txn_id;

    // Own insert is visible
    const header1 = TupleHeader{ .xmin = txn_id, .xmax = 0, .undo_ptr = NO_UNDO_PTR };
    try std.testing.expectEqual(Visibility.visible, isVisible(&header1, &txn.snapshot, &tm, txn_id));

    // Own delete makes it invisible
    const header2 = TupleHeader{ .xmin = txn_id, .xmax = txn_id, .undo_ptr = NO_UNDO_PTR };
    try std.testing.expectEqual(Visibility.invisible, isVisible(&header2, &txn.snapshot, &tm, txn_id));

    try tm.commit(txn);
}

test "isVisible — uncommitted delete still visible to others" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // txn1 creates and commits
    const txn1 = try tm.begin();
    const txn1_id = txn1.txn_id;
    try tm.commit(txn1);

    // txn2 and txn3 start concurrently
    const txn2 = try tm.begin();
    const txn2_id = txn2.txn_id;
    const txn3 = try tm.begin();

    // txn2 deletes but hasn't committed yet
    const header = TupleHeader{ .xmin = txn1_id, .xmax = txn2_id, .undo_ptr = NO_UNDO_PTR };

    // txn3 should still see the tuple (txn2's delete is not committed)
    try std.testing.expectEqual(Visibility.visible, isVisible(&header, &txn3.snapshot, &tm, txn3.txn_id));

    try tm.commit(txn2);
    try tm.commit(txn3);
}

test "TransactionManager oldest active" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn1 = try tm.begin();
    _ = try tm.begin();
    _ = try tm.begin();

    try std.testing.expectEqual(@as(TxnId, 1), tm.oldestActiveTxnId());

    try tm.commit(txn1);
    try std.testing.expectEqual(@as(TxnId, 2), tm.oldestActiveTxnId());

    // Clean up remaining
    var it = tm.active_txns.valueIterator();
    var remaining: [2]*Transaction = undefined;
    var i: usize = 0;
    while (it.next()) |txn_ptr| {
        remaining[i] = txn_ptr.*;
        i += 1;
    }
    for (remaining[0..i]) |txn| {
        try tm.commit(txn);
    }
}

test "Snapshot excludes own transaction from active list" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn1 = try tm.begin();
    // txn1's snapshot should not include itself in active list
    for (txn1.snapshot.active) |active_id| {
        try std.testing.expect(active_id != txn1.txn_id);
    }

    try tm.commit(txn1);
}

test "refreshSnapshot sees new commits" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // txn1 starts and commits
    const txn1 = try tm.begin();
    try tm.commit(txn1);

    // txn2 starts — takes snapshot *before* txn3
    const txn2 = try tm.beginWithIsolation(.read_committed);

    // txn3 starts and commits
    const txn3 = try tm.begin();
    try tm.commit(txn3);

    // Before refresh: txn3 was NOT in txn2's original snapshot (started after)
    // but since txn3.txn_id >= txn2.snapshot.xmax, it's invisible
    try std.testing.expect(!txn2.snapshot.isCommitted(txn3.txn_id, &tm));

    // After refresh: txn2 should now see txn3's commit
    try tm.refreshSnapshot(txn2);
    try std.testing.expect(txn2.snapshot.isCommitted(txn3.txn_id, &tm));

    try tm.commit(txn2);
}

test "default isolation is snapshot" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn = try tm.begin();
    try std.testing.expectEqual(IsolationLevel.snapshot_isolation, txn.isolation_level);
    try tm.commit(txn);
}

test "WAL records written on commit" {
    const Wal = wal_mod.Wal;
    const test_dir = "test_mvcc_wal_commit_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    var tm = TransactionManager.initWithWal(std.testing.allocator, &wal);
    defer tm.deinit();

    const txn = try tm.begin();
    try tm.commit(txn);

    // Should have BEGIN + COMMIT records in WAL
    var iter = try wal.iterator();
    defer wal.freeIterator(&iter);
    var count: usize = 0;
    var types: [2]wal_mod.LogRecordType = undefined;
    while (try iter.next()) |record| {
        defer iter.freeData(record.data);
        if (count < 2) types[count] = record.header.record_type;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(wal_mod.LogRecordType.begin, types[0]);
    try std.testing.expectEqual(wal_mod.LogRecordType.commit, types[1]);
}

test "WAL abort record on rollback" {
    const Wal = wal_mod.Wal;
    const test_dir = "test_mvcc_wal_abort_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    var tm = TransactionManager.initWithWal(std.testing.allocator, &wal);
    defer tm.deinit();

    const txn = try tm.begin();
    tm.abort(txn);

    // Flush remaining buffered records
    try wal.flush();

    // Should have BEGIN + ABORT records in WAL
    var iter = try wal.iterator();
    defer wal.freeIterator(&iter);
    var count: usize = 0;
    var types: [2]wal_mod.LogRecordType = undefined;
    while (try iter.next()) |record| {
        defer iter.freeData(record.data);
        if (count < 2) types[count] = record.header.record_type;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(wal_mod.LogRecordType.begin, types[0]);
    try std.testing.expectEqual(wal_mod.LogRecordType.abort, types[1]);
}

test "isVisible — xmin == xmax same txn insert-delete" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn1 = try tm.begin();
    const txn1_id = txn1.txn_id;

    // Tuple where same txn created and deleted it
    const header = TupleHeader{ .xmin = txn1_id, .xmax = txn1_id, .undo_ptr = NO_UNDO_PTR };

    // From own txn's view: xmin visible (own insert), xmax visible (own delete) → invisible
    try std.testing.expectEqual(Visibility.invisible, isVisible(&header, &txn1.snapshot, &tm, txn1_id));

    try tm.commit(txn1);

    // From a new txn's view: both committed → invisible
    const txn2 = try tm.begin();
    try std.testing.expectEqual(Visibility.invisible, isVisible(&header, &txn2.snapshot, &tm, txn2.txn_id));
    try tm.commit(txn2);
}

test "refreshSnapshot idempotency" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn = try tm.beginWithIsolation(.read_committed);

    // Refresh multiple times — should not leak or crash
    try tm.refreshSnapshot(txn);
    try tm.refreshSnapshot(txn);
    try tm.refreshSnapshot(txn);

    // xmax should reflect the current next_txn_id each time
    try std.testing.expectEqual(tm.next_txn_id, txn.snapshot.xmax);

    try tm.commit(txn);
}

test "oldestActiveTxnId with no active txns" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // No active txns — should return next_txn_id
    try std.testing.expectEqual(tm.next_txn_id, tm.oldestActiveTxnId());

    // Begin and commit — still no active txns
    const txn = try tm.begin();
    try tm.commit(txn);
    try std.testing.expectEqual(tm.next_txn_id, tm.oldestActiveTxnId());
}

test "isCommitted on INVALID_TXN_ID" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // Txn 0 should never be committed
    try std.testing.expect(!tm.isCommitted(INVALID_TXN_ID));

    // A real txn that committed
    const txn = try tm.begin();
    try tm.commit(txn);
    try std.testing.expect(tm.isCommitted(1));

    // Non-existent txn ID
    try std.testing.expect(!tm.isCommitted(999));
}

test "snapshot with many concurrent active txns" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // Start 10 txns without committing
    var txns: [10]*Transaction = undefined;
    for (&txns, 0..) |*t, i| {
        t.* = try tm.begin();
        _ = i;
    }

    // txn 11 starts — its active list should contain all 10
    const txn11 = try tm.begin();
    try std.testing.expectEqual(@as(usize, 10), txn11.snapshot.active.len);
    try std.testing.expectEqual(@as(TxnId, 1), txn11.snapshot.xmin);
    try std.testing.expectEqual(@as(TxnId, 12), txn11.snapshot.xmax);

    // None of the 10 should be visible to txn11
    for (txns) |t| {
        try std.testing.expect(!txn11.snapshot.isCommitted(t.txn_id, &tm));
    }

    // Commit them all and clean up
    for (txns) |t| {
        try tm.commit(t);
    }
    try tm.commit(txn11);
}

test "isVisible — aborted xmin is invisible" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // txn1 inserts but then aborts
    const txn1 = try tm.begin();
    const txn1_id = txn1.txn_id;
    tm.abort(txn1);

    // txn2 checks visibility — aborted txn1's insert should be invisible
    const txn2 = try tm.begin();
    const header = TupleHeader{ .xmin = txn1_id, .xmax = 0, .undo_ptr = NO_UNDO_PTR };
    try std.testing.expectEqual(Visibility.invisible, isVisible(&header, &txn2.snapshot, &tm, txn2.txn_id));
    try tm.commit(txn2);
}

test "snapshot boundary — txn committed between begin and check" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // txn1 and txn2 start concurrently
    const txn1 = try tm.begin();
    const txn2 = try tm.begin();

    // txn1 commits after txn2's snapshot was taken
    try tm.commit(txn1);

    // txn2's snapshot should NOT see txn1 as committed (was active at snapshot time)
    // Because txn1 was in txn2's active list
    try std.testing.expect(!txn2.snapshot.isCommitted(txn1.txn_id, &tm));

    // But a NEW txn should see txn1
    const txn3 = try tm.begin();
    try std.testing.expect(txn3.snapshot.isCommitted(txn1.txn_id, &tm));

    try tm.commit(txn2);
    try tm.commit(txn3);
}

test "isVisible — insert then delete in same txn is invisible to self" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn = try tm.begin();

    // Tuple inserted then deleted by same txn (xmin == xmax == own)
    const header = TupleHeader{ .xmin = txn.txn_id, .xmax = txn.txn_id, .undo_ptr = NO_UNDO_PTR };
    // Own insert + own delete → invisible (deleted from our perspective)
    try std.testing.expectEqual(Visibility.invisible, isVisible(&header, &txn.snapshot, &tm, txn.txn_id));

    try tm.commit(txn);
}

test "isVisible — future xmin is invisible" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn1 = try tm.begin();

    // Simulate a tuple created by a future txn (txn_id > snapshot.xmax)
    const future_txn_id = tm.next_txn_id + 10;
    const header = TupleHeader{ .xmin = future_txn_id, .xmax = 0, .undo_ptr = NO_UNDO_PTR };
    try std.testing.expectEqual(Visibility.invisible, isVisible(&header, &txn1.snapshot, &tm, txn1.txn_id));

    try tm.commit(txn1);
}

test "snapshot empty active list" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    // Start and commit txn1 before txn2 starts
    const txn1 = try tm.begin();
    try tm.commit(txn1);

    // txn2's snapshot should have empty active list
    const txn2 = try tm.begin();
    try std.testing.expectEqual(@as(usize, 0), txn2.snapshot.active.len);

    // txn1 should be visible (committed before snapshot)
    try std.testing.expect(txn2.snapshot.isCommitted(txn1.txn_id, &tm));

    try tm.commit(txn2);
}

test "transaction ids are monotonically increasing" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn1 = try tm.begin();
    const txn2 = try tm.begin();
    const txn3 = try tm.begin();

    try std.testing.expect(txn2.txn_id > txn1.txn_id);
    try std.testing.expect(txn3.txn_id > txn2.txn_id);

    try tm.commit(txn1);
    try tm.commit(txn2);
    try tm.commit(txn3);
}

test "abort then begin reuses no ids" {
    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();

    const txn1 = try tm.begin();
    const id1 = txn1.txn_id;
    tm.abort(txn1);

    // Next txn should get a higher id, not reuse aborted one
    const txn2 = try tm.begin();
    try std.testing.expect(txn2.txn_id > id1);
    try tm.commit(txn2);
}
