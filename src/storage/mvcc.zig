const std = @import("std");

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

        // If txn_id < xmin, it completed before our snapshot
        if (txn_id < self.xmin) return true;

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

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
            .next_txn_id = FIRST_TXN_ID,
            .active_txns = std.AutoHashMap(TxnId, *Transaction).init(allocator),
            .committed_txns = std.AutoHashMap(TxnId, void).init(allocator),
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
        const txn_id = self.next_txn_id;
        self.next_txn_id += 1;

        // Take a snapshot of currently active transactions
        const snapshot = try self.takeSnapshot(txn_id);

        const txn = try self.allocator.create(Transaction);
        txn.* = .{
            .txn_id = txn_id,
            .state = .active,
            .snapshot = snapshot,
            .undo_chain_head = NO_UNDO_PTR,
        };

        try self.active_txns.put(txn_id, txn);
        return txn;
    }

    /// Commit a transaction
    pub fn commit(self: *Self, txn: *Transaction) !void {
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
