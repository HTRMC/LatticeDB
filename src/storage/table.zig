const std = @import("std");
const page_mod = @import("page.zig");
const buffer_pool_mod = @import("buffer_pool.zig");
const tuple_mod = @import("tuple.zig");
const mvcc_mod = @import("mvcc.zig");
const undo_log_mod = @import("undo_log.zig");

const Page = page_mod.Page;
const PageId = page_mod.PageId;
const SlotId = page_mod.SlotId;
const TupleId = page_mod.TupleId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const PAGE_SIZE = page_mod.PAGE_SIZE;
const BufferPool = buffer_pool_mod.BufferPool;
const Schema = tuple_mod.Schema;
const Tuple = tuple_mod.Tuple;
const Value = tuple_mod.Value;
const TupleHeader = mvcc_mod.TupleHeader;
const TxnId = mvcc_mod.TxnId;
const NO_UNDO_PTR = mvcc_mod.NO_UNDO_PTR;
const Transaction = mvcc_mod.Transaction;
const TransactionManager = mvcc_mod.TransactionManager;
const Snapshot = mvcc_mod.Snapshot;
const UndoLog = undo_log_mod.UndoLog;
const UndoRecordHeader = undo_log_mod.UndoRecordHeader;

pub const TableError = error{
    BufferPoolError,
    TupleTooBig,
    PageFull,
    TupleNotFound,
    SerializationError,
    OutOfMemory,
    UndoLogError,
    WriteConflict,
};

/// Table metadata header stored at the beginning of the first page
const TableMeta = extern struct {
    /// Number of pages allocated for this table
    page_count: u32,
    /// Page ID of the first page with free space
    first_free_page: PageId,
    /// Total number of tuples (including deleted)
    tuple_count: u64,

    pub const SIZE: usize = @sizeOf(TableMeta);
};

/// Heap-organized table with MVCC support.
/// All tuples are stored with a TupleHeader prefix for versioning.
pub const Table = struct {
    allocator: std.mem.Allocator,
    buffer_pool: *BufferPool,
    schema: *const Schema,
    /// Table ID (page ID of the first/meta page)
    table_id: PageId,
    /// Transaction manager for visibility checks (null = legacy mode, all visible)
    txn_manager: ?*TransactionManager,
    /// Undo log for version chain (null = legacy mode, no undo)
    undo_log: ?*UndoLog,

    const Self = @This();

    /// Maximum tuple size that can fit in a page (with overhead)
    pub const MAX_TUPLE_SIZE = PAGE_SIZE - page_mod.PageHeader.SIZE - page_mod.Slot.SIZE - TableMeta.SIZE - 64;

    /// Create a new table - allocates the first page
    pub fn create(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
    ) TableError!Self {
        return createWithMvcc(allocator, buffer_pool, schema, null, null);
    }

    /// Create a new table with MVCC support
    pub fn createWithMvcc(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
    ) TableError!Self {
        // Allocate the first page for this table
        const result = buffer_pool.newPage() catch {
            return TableError.BufferPoolError;
        };

        const table = Self{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .schema = schema,
            .table_id = result.page_id,
            .txn_manager = txn_manager,
            .undo_log = undo_log,
        };

        // Initialize meta in first page
        var pg = result.page;
        const meta = TableMeta{
            .page_count = 1,
            .first_free_page = result.page_id,
            .tuple_count = 0,
        };

        // Store meta as the first tuple on the meta page
        _ = pg.insertTuple(std.mem.asBytes(&meta));

        buffer_pool.unpinPage(result.page_id, true) catch {};

        return table;
    }

    /// Open an existing table by its table ID
    pub fn open(allocator: std.mem.Allocator, buffer_pool: *BufferPool, schema: *const Schema, table_id: PageId) Self {
        return openWithMvcc(allocator, buffer_pool, schema, table_id, null, null);
    }

    /// Open an existing table with MVCC support
    pub fn openWithMvcc(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        table_id: PageId,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
    ) Self {
        return .{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .schema = schema,
            .table_id = table_id,
            .txn_manager = txn_manager,
            .undo_log = undo_log,
        };
    }

    /// Insert a tuple into the table.
    /// If txn is provided, the tuple gets a TupleHeader with xmin=txn.txn_id.
    /// If txn is null, falls back to legacy mode (no header).
    pub fn insertTuple(self: *Self, txn: ?*Transaction, values: []const Value) TableError!TupleId {
        var buf: [PAGE_SIZE]u8 = undefined;
        var size: usize = undefined;

        if (txn) |t| {
            // MVCC mode: serialize with TupleHeader
            const header = TupleHeader.init(t.txn_id);
            size = Tuple.serializeWithHeader(header, self.schema, values, &buf) catch {
                return TableError.SerializationError;
            };
        } else {
            // Legacy mode: no header
            size = Tuple.serialize(self.schema, values, &buf) catch {
                return TableError.SerializationError;
            };
        }

        if (size > MAX_TUPLE_SIZE) {
            return TableError.TupleTooBig;
        }

        const tuple_data = buf[0..size];

        // Find a page with space
        const meta = try self.readMeta();
        var page_id = meta.first_free_page;

        while (page_id != INVALID_PAGE_ID) {
            var pg = self.buffer_pool.fetchPage(page_id) catch {
                return TableError.BufferPoolError;
            };

            const slot_id = pg.insertTuple(tuple_data);
            if (slot_id) |sid| {
                self.buffer_pool.unpinPage(page_id, true) catch {};
                try self.incrementTupleCount();

                const tid = TupleId{ .page_id = page_id, .slot_id = sid };

                // Write insert undo record
                if (txn != null and self.undo_log != null) {
                    try self.writeInsertUndo(txn.?, tid);
                }

                return tid;
            }

            self.buffer_pool.unpinPage(page_id, false) catch {};

            if (page_id == self.table_id) {
                if (meta.page_count > 1) {
                    page_id = self.table_id + 1;
                } else {
                    break;
                }
            } else if (page_id < self.table_id + meta.page_count - 1) {
                page_id += 1;
            } else {
                break;
            }
        }

        // No space found - allocate a new page
        const new_result = self.buffer_pool.newPage() catch {
            return TableError.BufferPoolError;
        };

        var new_pg = new_result.page;
        const slot_id = new_pg.insertTuple(tuple_data) orelse {
            self.buffer_pool.unpinPage(new_result.page_id, false) catch {};
            return TableError.TupleTooBig;
        };

        self.buffer_pool.unpinPage(new_result.page_id, true) catch {};

        // Update meta
        try self.updateMeta(.{
            .page_count = meta.page_count + 1,
            .first_free_page = new_result.page_id,
            .tuple_count = meta.tuple_count + 1,
        });

        const tid = TupleId{ .page_id = new_result.page_id, .slot_id = slot_id };

        // Write insert undo record
        if (txn != null and self.undo_log != null) {
            try self.writeInsertUndo(txn.?, tid);
        }

        return tid;
    }

    /// Get a tuple by its TupleId, with optional visibility check.
    /// Caller must free the returned values slice.
    pub fn getTuple(self: *Self, tid: TupleId, snapshot: ?*const Snapshot) TableError!?[]Value {
        var pg = self.buffer_pool.fetchPage(tid.page_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(tid.page_id, false) catch {};

        const raw = pg.getTuple(tid.slot_id) orelse return null;

        // MVCC visibility check
        if (snapshot != null and self.txn_manager != null) {
            if (raw.len < TupleHeader.SIZE) return TableError.SerializationError;
            const stripped = Tuple.stripHeader(raw) catch return TableError.SerializationError;
            const vis = mvcc_mod.isVisible(&stripped.header, snapshot.?, self.txn_manager.?, snapshot.?.xmax - 1);
            if (vis == .invisible) return null;

            return Tuple.deserialize(self.allocator, self.schema, stripped.user_data) catch {
                return TableError.SerializationError;
            };
        }

        // Legacy mode: no header
        return Tuple.deserialize(self.allocator, self.schema, raw) catch {
            return TableError.SerializationError;
        };
    }

    /// Get a tuple with visibility check using the transaction's own ID and snapshot
    pub fn getTupleTxn(self: *Self, tid: TupleId, txn: *const Transaction) TableError!?[]Value {
        var pg = self.buffer_pool.fetchPage(tid.page_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(tid.page_id, false) catch {};

        const raw = pg.getTuple(tid.slot_id) orelse return null;

        if (self.txn_manager) |tm| {
            if (raw.len < TupleHeader.SIZE) return TableError.SerializationError;
            const stripped = Tuple.stripHeader(raw) catch return TableError.SerializationError;
            const vis = mvcc_mod.isVisible(&stripped.header, &txn.snapshot, tm, txn.txn_id);
            if (vis == .invisible) return null;

            return Tuple.deserialize(self.allocator, self.schema, stripped.user_data) catch {
                return TableError.SerializationError;
            };
        }

        return Tuple.deserialize(self.allocator, self.schema, raw) catch {
            return TableError.SerializationError;
        };
    }

    /// Delete a tuple by setting xmax (MVCC) or zeroing the slot (legacy).
    pub fn deleteTuple(self: *Self, txn: ?*Transaction, tid: TupleId) TableError!bool {
        var pg = self.buffer_pool.fetchPage(tid.page_id) catch {
            return TableError.BufferPoolError;
        };

        if (txn != null and self.txn_manager != null) {
            // MVCC mode: set xmax in-place
            const raw = pg.getTuple(tid.slot_id) orelse {
                self.buffer_pool.unpinPage(tid.page_id, false) catch {};
                return false;
            };
            if (raw.len < TupleHeader.SIZE) {
                self.buffer_pool.unpinPage(tid.page_id, false) catch {};
                return false;
            }

            // Write-write conflict detection: check if another txn already set xmax
            const existing_header = std.mem.bytesToValue(TupleHeader, raw[0..TupleHeader.SIZE]);
            if (existing_header.xmax != 0 and existing_header.xmax != txn.?.txn_id) {
                self.buffer_pool.unpinPage(tid.page_id, false) catch {};
                return TableError.WriteConflict;
            }

            // Write delete undo record before modifying
            if (self.undo_log != null) {
                try self.writeDeleteUndo(txn.?, tid, raw[0..TupleHeader.SIZE]);
            }

            // Set xmax to the deleting transaction's ID
            const xmax_bytes = std.mem.asBytes(&txn.?.txn_id);
            // xmax is at offset 4 in TupleHeader (after xmin)
            const updated = pg.updateTupleData(tid.slot_id, 4, xmax_bytes);

            self.buffer_pool.unpinPage(tid.page_id, updated) catch {};
            return updated;
        } else {
            // Legacy mode: zero the slot
            const result = pg.deleteTuple(tid.slot_id);
            self.buffer_pool.unpinPage(tid.page_id, result) catch {};
            return result;
        }
    }

    /// Sequential scan with optional MVCC visibility filtering
    pub const ScanIterator = struct {
        table: *Self,
        current_page: PageId,
        current_slot: SlotId,
        page_count: u32,
        /// Transaction for visibility checks (null = all visible)
        txn: ?*const Transaction,

        /// Returns the next visible tuple ID and deserialized values.
        /// Caller must free the returned values slice.
        pub fn next(self: *ScanIterator) TableError!?struct { tid: TupleId, values: []Value } {
            while (self.current_page < self.table.table_id + self.page_count) {
                var pg = self.table.buffer_pool.fetchPage(self.current_page) catch {
                    return TableError.BufferPoolError;
                };

                const header = pg.getHeader();
                const start_slot: SlotId = if (self.current_page == self.table.table_id and self.current_slot == 0) 1 else self.current_slot;

                var slot = start_slot;
                while (slot < header.slot_count) : (slot += 1) {
                    if (pg.getTuple(slot)) |raw| {
                        const tid = TupleId{ .page_id = self.current_page, .slot_id = slot };

                        // MVCC visibility check
                        if (self.txn != null and self.table.txn_manager != null) {
                            if (raw.len < TupleHeader.SIZE) {
                                continue; // Skip malformed tuples
                            }
                            const stripped = Tuple.stripHeader(raw) catch continue;
                            const vis = mvcc_mod.isVisible(&stripped.header, &self.txn.?.snapshot, self.table.txn_manager.?, self.txn.?.txn_id);
                            if (vis == .invisible) continue;

                            self.current_slot = slot + 1;

                            const values = Tuple.deserialize(self.table.allocator, self.table.schema, stripped.user_data) catch {
                                self.table.buffer_pool.unpinPage(self.current_page, false) catch {};
                                return TableError.SerializationError;
                            };

                            self.table.buffer_pool.unpinPage(self.current_page, false) catch {};
                            return .{ .tid = tid, .values = values };
                        } else {
                            // Legacy mode: no header
                            self.current_slot = slot + 1;

                            const values = Tuple.deserialize(self.table.allocator, self.table.schema, raw) catch {
                                self.table.buffer_pool.unpinPage(self.current_page, false) catch {};
                                return TableError.SerializationError;
                            };

                            self.table.buffer_pool.unpinPage(self.current_page, false) catch {};
                            return .{ .tid = tid, .values = values };
                        }
                    }
                }

                self.table.buffer_pool.unpinPage(self.current_page, false) catch {};

                // Move to next page
                self.current_page += 1;
                self.current_slot = 0;
            }

            return null;
        }

        /// Free values returned by next()
        pub fn freeValues(self: *ScanIterator, values: []Value) void {
            self.table.allocator.free(values);
        }
    };

    /// Start a sequential scan of the table
    pub fn scan(self: *Self) TableError!ScanIterator {
        return self.scanWithTxn(null);
    }

    /// Start a sequential scan with a transaction for MVCC visibility
    pub fn scanWithTxn(self: *Self, txn: ?*const Transaction) TableError!ScanIterator {
        const meta = try self.readMeta();
        return .{
            .table = self,
            .current_page = self.table_id,
            .current_slot = 0,
            .page_count = meta.page_count,
            .txn = txn,
        };
    }

    // ── Undo record helpers ──────────────────────────────────────────

    fn writeInsertUndo(self: *Self, txn: *Transaction, tid: TupleId) TableError!void {
        const log = self.undo_log orelse return;
        const undo_header = UndoRecordHeader{
            .record_type = .insert,
            .slot_id = tid.slot_id,
            .txn_id = txn.txn_id,
            .prev_undo_ptr = NO_UNDO_PTR,
            .txn_prev_undo = txn.undo_chain_head,
            .table_page_id = tid.page_id,
            .data_len = 0,
        };

        const offset = log.appendRecord(undo_header, &.{}) catch {
            return TableError.UndoLogError;
        };
        txn.undo_chain_head = offset;
    }

    fn writeDeleteUndo(self: *Self, txn: *Transaction, tid: TupleId, old_header_bytes: []const u8) TableError!void {
        const log = self.undo_log orelse return;
        const undo_header = UndoRecordHeader{
            .record_type = .delete,
            .slot_id = tid.slot_id,
            .txn_id = txn.txn_id,
            .prev_undo_ptr = NO_UNDO_PTR,
            .txn_prev_undo = txn.undo_chain_head,
            .table_page_id = tid.page_id,
            .data_len = @intCast(old_header_bytes.len),
        };

        const offset = log.appendRecord(undo_header, old_header_bytes) catch {
            return TableError.UndoLogError;
        };
        txn.undo_chain_head = offset;
    }

    // ── Rollback support ─────────────────────────────────────────────

    /// Walk the transaction's undo chain and restore all changes.
    pub fn rollback(self: *Self, txn: *Transaction) TableError!void {
        const log = self.undo_log orelse return;
        var undo_ptr = txn.undo_chain_head;

        while (undo_ptr != NO_UNDO_PTR) {
            const rec = log.readRecord(undo_ptr) catch {
                return TableError.UndoLogError;
            };

            switch (rec.header.record_type) {
                .insert => {
                    // Undo insert = zero the slot
                    var pg = self.buffer_pool.fetchPage(rec.header.table_page_id) catch {
                        return TableError.BufferPoolError;
                    };
                    _ = pg.deleteTuple(rec.header.slot_id);
                    self.buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                },
                .delete => {
                    // Undo delete = restore xmax to 0
                    var pg = self.buffer_pool.fetchPage(rec.header.table_page_id) catch {
                        return TableError.BufferPoolError;
                    };
                    const zero_xmax = std.mem.asBytes(&@as(TxnId, 0));
                    _ = pg.updateTupleData(rec.header.slot_id, 4, zero_xmax);
                    self.buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                },
                .update => {
                    // Undo update = overwrite heap tuple with old data
                    var pg = self.buffer_pool.fetchPage(rec.header.table_page_id) catch {
                        return TableError.BufferPoolError;
                    };
                    _ = pg.updateTupleData(rec.header.slot_id, 0, rec.data);
                    self.buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                },
            }

            undo_ptr = rec.header.txn_prev_undo;
        }
    }

    // ── Internal helpers ─────────────────────────────────────────────

    /// Read the table metadata from the first page
    fn readMeta(self: *Self) TableError!TableMeta {
        var pg = self.buffer_pool.fetchPage(self.table_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(self.table_id, false) catch {};

        const raw = pg.getTuple(0) orelse {
            return TableError.TupleNotFound;
        };

        if (raw.len < TableMeta.SIZE) return TableError.SerializationError;
        return std.mem.bytesToValue(TableMeta, raw[0..TableMeta.SIZE]);
    }

    /// Update the table metadata
    fn updateMeta(self: *Self, meta: TableMeta) TableError!void {
        var pg = self.buffer_pool.fetchPage(self.table_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(self.table_id, true) catch {};

        // Overwrite the meta slot data directly
        const slot = pg.getSlot(0) orelse return TableError.TupleNotFound;
        const dest = pg.data[slot.offset..][0..TableMeta.SIZE];
        @memcpy(dest, std.mem.asBytes(&meta));
    }

    /// Increment the tuple count in metadata
    fn incrementTupleCount(self: *Self) TableError!void {
        var meta = try self.readMeta();
        meta.tuple_count += 1;
        try self.updateMeta(meta);
    }

    /// Get the total number of tuples
    pub fn tupleCount(self: *Self) TableError!u64 {
        const meta = try self.readMeta();
        return meta.tuple_count;
    }
};

// ============================================================
// Tests
// ============================================================
const disk_manager_mod = @import("disk_manager.zig");
const DiskManager = disk_manager_mod.DiskManager;

test "table create and insert (legacy)" {
    const test_file = "test_table_basic.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema);

    // Insert a row (legacy, no txn)
    const values = [_]Value{ .{ .integer = 1 }, .{ .bytes = "Alice" } };
    const tid = try table.insertTuple(null, &values);

    try std.testing.expectEqual(table.table_id, tid.page_id);
    try std.testing.expectEqual(@as(u64, 1), try table.tupleCount());
}

test "table insert and retrieve (legacy)" {
    const test_file = "test_table_get.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
            .{ .name = "active", .col_type = .boolean, .max_length = 0, .nullable = true },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema);

    const values = [_]Value{ .{ .integer = 42 }, .{ .bytes = "Bob" }, .{ .boolean = true } };
    const tid = try table.insertTuple(null, &values);

    // Retrieve (legacy, no snapshot)
    const result = try table.getTuple(tid, null) orelse unreachable;
    defer std.testing.allocator.free(result);

    try std.testing.expectEqual(@as(i32, 42), result[0].integer);
    try std.testing.expectEqualStrings("Bob", result[1].bytes);
    try std.testing.expectEqual(true, result[2].boolean);
}

test "table sequential scan (legacy)" {
    const test_file = "test_table_scan.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "value", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema);

    // Insert multiple rows
    var i: i32 = 0;
    while (i < 5) : (i += 1) {
        var name_buf: [32]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "row_{}", .{i}) catch unreachable;
        const vals = [_]Value{ .{ .integer = i }, .{ .bytes = name } };
        _ = try table.insertTuple(null, &vals);
    }

    try std.testing.expectEqual(@as(u64, 5), try table.tupleCount());

    // Scan all rows
    var iter = try table.scan();
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 5), count);
}

test "table delete tuple (legacy)" {
    const test_file = "test_table_delete.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema);

    const vals = [_]Value{.{ .integer = 1 }};
    const tid = try table.insertTuple(null, &vals);

    // Delete it (legacy)
    const deleted = try table.deleteTuple(null, tid);
    try std.testing.expect(deleted);

    // Should not be retrievable
    const result = try table.getTuple(tid, null);
    try std.testing.expect(result == null);
}

test "table multiple pages (legacy)" {
    const test_file = "test_table_multi_page.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "data", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema);

    // Insert rows with large data to force multiple pages
    var large_buf: [512]u8 = undefined;
    @memset(&large_buf, 'X');

    var i: i32 = 0;
    while (i < 20) : (i += 1) {
        const vals = [_]Value{ .{ .integer = i }, .{ .bytes = &large_buf } };
        _ = try table.insertTuple(null, &vals);
    }

    try std.testing.expectEqual(@as(u64, 20), try table.tupleCount());

    // Scan should return all 20 rows
    var iter = try table.scan();
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 20), count);
}

test "MVCC insert and visibility" {
    const test_file = "test_table_mvcc_insert.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // txn1 inserts a row
    const txn1 = try tm.begin();
    const vals = [_]Value{ .{ .integer = 1 }, .{ .bytes = "Alice" } };
    _ = try table.insertTuple(txn1, &vals);

    // txn1 can see its own insert
    var iter1 = try table.scanWithTxn(txn1);
    var count1: usize = 0;
    while (try iter1.next()) |row| {
        defer iter1.freeValues(row.values);
        count1 += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), count1);

    // txn2 starts — should NOT see txn1's uncommitted insert
    const txn2 = try tm.begin();
    var iter2 = try table.scanWithTxn(txn2);
    var count2: usize = 0;
    while (try iter2.next()) |row| {
        defer iter2.freeValues(row.values);
        count2 += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count2);

    // Commit txn1
    try tm.commit(txn1);

    // txn3 starts after commit — should see the row
    const txn3 = try tm.begin();
    var iter3 = try table.scanWithTxn(txn3);
    var count3: usize = 0;
    while (try iter3.next()) |row| {
        defer iter3.freeValues(row.values);
        count3 += 1;
        try std.testing.expectEqual(@as(i32, 1), row.values[0].integer);
    }
    try std.testing.expectEqual(@as(usize, 1), count3);

    try tm.commit(txn2);
    try tm.commit(txn3);
}

test "MVCC delete visibility" {
    const test_file = "test_table_mvcc_delete.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert and commit
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 42 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // Delete in txn2
    const txn2 = try tm.begin();
    _ = try table.deleteTuple(txn2, tid);

    // txn2 should NOT see the deleted row
    var iter2 = try table.scanWithTxn(txn2);
    var count2: usize = 0;
    while (try iter2.next()) |row| {
        defer iter2.freeValues(row.values);
        count2 += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count2);

    // txn3 (concurrent) should still see it (txn2 not committed)
    const txn3 = try tm.begin();
    var iter3 = try table.scanWithTxn(txn3);
    var count3: usize = 0;
    while (try iter3.next()) |row| {
        defer iter3.freeValues(row.values);
        count3 += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), count3);

    try tm.commit(txn2);
    try tm.commit(txn3);
}

test "MVCC rollback restores insert" {
    const test_file = "test_table_mvcc_rollback.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert in a transaction
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 99 }};
    _ = try table.insertTuple(txn1, &vals);

    // Rollback
    try table.rollback(txn1);
    tm.abort(txn1);

    // New transaction should see nothing
    const txn2 = try tm.begin();
    var iter = try table.scanWithTxn(txn2);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);

    try tm.commit(txn2);
}

test "MVCC rollback restores delete" {
    const test_file = "test_table_mvcc_rollback_del.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert and commit
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 42 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // Delete in txn2 then rollback
    const txn2 = try tm.begin();
    _ = try table.deleteTuple(txn2, tid);
    try table.rollback(txn2);
    tm.abort(txn2);

    // Row should still be visible
    const txn3 = try tm.begin();
    var iter = try table.scanWithTxn(txn3);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
        try std.testing.expectEqual(@as(i32, 42), row.values[0].integer);
    }
    try std.testing.expectEqual(@as(usize, 1), count);

    try tm.commit(txn3);
}

test "MVCC write-write conflict detection" {
    const test_file = "test_table_ww_conflict.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert and commit a row
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 1 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // txn2 deletes the row (sets xmax)
    const txn2 = try tm.begin();
    _ = try table.deleteTuple(txn2, tid);

    // txn3 tries to delete the same row — should get WriteConflict
    const txn3 = try tm.begin();
    const result = table.deleteTuple(txn3, tid);
    try std.testing.expectError(TableError.WriteConflict, result);

    try tm.commit(txn2);
    tm.abort(txn3);
}

test "MVCC same-txn double delete no conflict" {
    const test_file = "test_table_same_txn_delete.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert and commit a row
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 1 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // Same txn deletes twice — should not conflict
    const txn2 = try tm.begin();
    const del1 = try table.deleteTuple(txn2, tid);
    try std.testing.expect(del1);

    // Second delete on same tuple by same txn — xmax already set to our id, no conflict
    const del2 = try table.deleteTuple(txn2, tid);
    try std.testing.expect(del2);

    try tm.commit(txn2);
}

test "MVCC insert-delete same txn then rollback" {
    const test_file = "test_table_ins_del_rollback.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert a row, delete it, all in one txn, then rollback
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 77 }};
    const tid = try table.insertTuple(txn1, &vals);
    _ = try table.deleteTuple(txn1, tid);

    // Rollback: undo delete (restore xmax=0), undo insert (zero slot)
    try table.rollback(txn1);
    tm.abort(txn1);

    // New txn should see nothing — the row never existed
    const txn2 = try tm.begin();
    var iter = try table.scanWithTxn(txn2);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);
    try tm.commit(txn2);
}

test "MVCC insert-delete same txn commit makes row invisible" {
    const test_file = "test_table_ins_del_commit.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert + delete in same txn, then commit
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 88 }};
    const tid = try table.insertTuple(txn1, &vals);
    _ = try table.deleteTuple(txn1, tid);
    try tm.commit(txn1);

    // Next txn: xmin committed, xmax committed (same txn) → invisible
    const txn2 = try tm.begin();
    var iter = try table.scanWithTxn(txn2);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);
    try tm.commit(txn2);
}

test "MVCC scan empty table" {
    const test_file = "test_table_scan_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Scan with active txn on empty table — should return nothing, not crash
    const txn = try tm.begin();
    var iter = try table.scanWithTxn(txn);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);
    try tm.commit(txn);
}

test "MVCC large batch insert then rollback" {
    const test_file = "test_table_batch_rollback.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 40);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert 50 rows in one txn
    const txn1 = try tm.begin();
    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const vals = [_]Value{.{ .integer = i }};
        _ = try table.insertTuple(txn1, &vals);
    }

    // Verify they're visible within the txn
    var iter1 = try table.scanWithTxn(txn1);
    var count1: usize = 0;
    while (try iter1.next()) |row| {
        defer iter1.freeValues(row.values);
        count1 += 1;
    }
    try std.testing.expectEqual(@as(usize, 50), count1);

    // Rollback all 50
    try table.rollback(txn1);
    tm.abort(txn1);

    // New txn should see nothing
    const txn2 = try tm.begin();
    var iter2 = try table.scanWithTxn(txn2);
    var count2: usize = 0;
    while (try iter2.next()) |row| {
        defer iter2.freeValues(row.values);
        count2 += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count2);
    try tm.commit(txn2);
}

test "MVCC write conflict after first txn commits" {
    const test_file = "test_table_ww_after_commit.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo);

    // Insert and commit a row
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 1 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // txn2 deletes and commits — xmax is now set to a committed txn
    const txn2 = try tm.begin();
    _ = try table.deleteTuple(txn2, tid);
    try tm.commit(txn2);

    // txn3 tries to delete the same row — xmax is set to committed txn2
    // This is still a conflict because xmax != 0 and xmax != txn3.txn_id
    const txn3 = try tm.begin();
    const result = table.deleteTuple(txn3, tid);
    try std.testing.expectError(TableError.WriteConflict, result);
    tm.abort(txn3);
}
