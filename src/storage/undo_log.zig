const std = @import("std");
const mvcc = @import("mvcc.zig");
const page_mod = @import("page.zig");

const TxnId = mvcc.TxnId;
const NO_UNDO_PTR = mvcc.NO_UNDO_PTR;
const PageId = page_mod.PageId;
const SlotId = page_mod.SlotId;

const Io = std.Io;
const File = Io.File;
const Dir = Io.Dir;
const Threaded = Io.Threaded;
const process = std.process;

/// Undo record types
pub const UndoRecordType = enum(u8) {
    insert = 1, // Undo = remove the inserted tuple (zero the slot)
    delete = 2, // Undo = restore xmax to 0
    update = 3, // Undo = restore old tuple data
};

/// Undo record header — stored in the undo log file.
/// 32 bytes, extern struct for stable on-disk layout.
pub const UndoRecordHeader = extern struct {
    /// Type of undo operation
    record_type: UndoRecordType,
    /// Padding for alignment
    _pad1: u8 = 0,
    /// Slot ID of the affected tuple
    slot_id: SlotId,
    /// Transaction that wrote this undo record
    txn_id: TxnId,
    /// Previous undo record for the same tuple (version chain)
    prev_undo_ptr: u64,
    /// Previous undo record for the same transaction (rollback chain)
    txn_prev_undo: u64,
    /// Page ID of the affected tuple
    table_page_id: PageId,
    /// Length of the undo data that follows this header
    data_len: u16,
    /// Padding
    _pad2: u16 = 0,

    pub const SIZE: usize = @sizeOf(UndoRecordHeader);
};

/// In-memory undo log — stores undo records in a growable buffer.
/// For tests and when persistence is not yet needed.
pub const UndoLog = struct {
    allocator: std.mem.Allocator,
    /// In-memory buffer acting as the undo log
    buffer: std.ArrayListUnmanaged(u8),
    /// Current write position (= logical file size, relative to buffer start)
    write_pos: u64,
    /// GC watermark: records before this offset are logically dead (global offset)
    gc_watermark: u64,
    /// Bytes compacted out from the start of the buffer
    base_offset: u64 = 0,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
            .buffer = .empty,
            .write_pos = 0,
            .gc_watermark = 0,
            .base_offset = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit(self.allocator);
    }

    /// Append an undo record and return its global offset (= undo_ptr)
    pub fn appendRecord(self: *Self, header: UndoRecordHeader, data: []const u8) !u64 {
        const global_offset = self.base_offset + self.write_pos;
        const total_size = UndoRecordHeader.SIZE + data.len;

        // Ensure capacity
        const needed = @as(usize, @intCast(self.write_pos)) + total_size;
        if (needed > self.buffer.items.len) {
            try self.buffer.resize(self.allocator, needed);
        }

        const pos: usize = @intCast(self.write_pos);

        // Write header
        const header_bytes = std.mem.asBytes(&header);
        @memcpy(self.buffer.items[pos..][0..UndoRecordHeader.SIZE], header_bytes);

        // Write data
        if (data.len > 0) {
            @memcpy(self.buffer.items[pos + UndoRecordHeader.SIZE ..][0..data.len], data);
        }

        self.write_pos += @intCast(total_size);
        return global_offset;
    }

    /// Read an undo record at the given global offset
    pub fn readRecord(self: *const Self, offset: u64) !struct { header: UndoRecordHeader, data: []const u8 } {
        if (offset == NO_UNDO_PTR) return error.InvalidOffset;
        if (offset < self.base_offset) return error.InvalidOffset;

        const local: usize = @intCast(offset - self.base_offset);
        const buf_len = @as(usize, @intCast(self.write_pos));
        if (local + UndoRecordHeader.SIZE > buf_len) return error.InvalidOffset;

        const header = std.mem.bytesToValue(UndoRecordHeader, self.buffer.items[local..][0..UndoRecordHeader.SIZE]);
        const data_start = local + UndoRecordHeader.SIZE;
        const data_end = data_start + header.data_len;

        if (data_end > buf_len) return error.InvalidOffset;

        return .{
            .header = header,
            .data = self.buffer.items[data_start..data_end],
        };
    }

    /// Get the current global write position (for GC boundary calculations)
    pub fn currentOffset(self: *const Self) u64 {
        return self.base_offset + self.write_pos;
    }

    /// GC: Advance watermark past records whose txn_id < oldest_visible_txn_id.
    /// Records before the watermark are logically dead.
    pub fn gc(self: *Self, oldest_visible_txn_id: TxnId) void {
        var offset = self.gc_watermark;
        const global_end = self.base_offset + self.write_pos;

        while (offset < global_end) {
            const local: usize = @intCast(offset - self.base_offset);
            const buf_len = @as(usize, @intCast(self.write_pos));
            if (local + UndoRecordHeader.SIZE > buf_len) break;

            const header = std.mem.bytesToValue(UndoRecordHeader, self.buffer.items[local..][0..UndoRecordHeader.SIZE]);
            const record_size = UndoRecordHeader.SIZE + header.data_len;

            // Stop at the first record that might still be visible
            if (header.txn_id >= oldest_visible_txn_id) break;

            offset += @intCast(record_size);
        }

        self.gc_watermark = offset;
    }

    /// Compact: reclaim memory for GC'd records by shifting live data to the
    /// start of the buffer. External offsets remain valid via base_offset tracking.
    pub fn compact(self: *Self) void {
        const dead = self.gc_watermark - self.base_offset;
        if (dead == 0) return;

        const dead_bytes: usize = @intCast(dead);
        const live_bytes: usize = @intCast(self.write_pos - dead);

        // Move live data to the front
        if (live_bytes > 0) {
            std.mem.copyForwards(u8, self.buffer.items[0..live_bytes], self.buffer.items[dead_bytes..][0..live_bytes]);
        }

        self.base_offset = self.gc_watermark;
        self.write_pos = @intCast(live_bytes);

        // Shrink backing memory
        self.buffer.shrinkRetainingCapacity(live_bytes);
    }

    /// Returns the number of bytes that are logically dead (before watermark)
    pub fn reclaimableBytes(self: *const Self) u64 {
        return self.gc_watermark - self.base_offset;
    }
};

// ============================================================
// Tests
// ============================================================

test "UndoRecordHeader layout" {
    try std.testing.expectEqual(@as(usize, 32), UndoRecordHeader.SIZE);
}

test "UndoLog append and read insert record" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    const header = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 5,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 10,
        .data_len = 0,
    };

    const offset = try log.appendRecord(header, &.{});
    try std.testing.expectEqual(@as(u64, 0), offset);

    const result = try log.readRecord(offset);
    try std.testing.expectEqual(UndoRecordType.insert, result.header.record_type);
    try std.testing.expectEqual(@as(TxnId, 1), result.header.txn_id);
    try std.testing.expectEqual(@as(SlotId, 5), result.header.slot_id);
    try std.testing.expectEqual(@as(usize, 0), result.data.len);
}

test "UndoLog append and read delete record with data" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Simulate storing old TupleHeader bytes as undo data
    const old_header_bytes = [_]u8{ 1, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

    const header = UndoRecordHeader{
        .record_type = .delete,
        .slot_id = 3,
        .txn_id = 2,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 7,
        .data_len = @intCast(old_header_bytes.len),
    };

    const offset = try log.appendRecord(header, &old_header_bytes);
    const result = try log.readRecord(offset);

    try std.testing.expectEqual(UndoRecordType.delete, result.header.record_type);
    try std.testing.expectEqual(@as(usize, 16), result.data.len);
    try std.testing.expectEqualSlices(u8, &old_header_bytes, result.data);
}

test "UndoLog multiple records and chain traversal" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // First record: insert
    const h1 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 1,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    const offset1 = try log.appendRecord(h1, &.{});

    // Second record: delete (txn chain points back to first)
    const h2 = UndoRecordHeader{
        .record_type = .delete,
        .slot_id = 2,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = offset1,
        .table_page_id = 5,
        .data_len = 4,
    };
    const data2 = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    const offset2 = try log.appendRecord(h2, &data2);

    // Walk the txn chain backwards
    const rec2 = try log.readRecord(offset2);
    try std.testing.expectEqual(UndoRecordType.delete, rec2.header.record_type);
    try std.testing.expectEqual(offset1, rec2.header.txn_prev_undo);

    const rec1 = try log.readRecord(rec2.header.txn_prev_undo);
    try std.testing.expectEqual(UndoRecordType.insert, rec1.header.record_type);
    try std.testing.expectEqual(NO_UNDO_PTR, rec1.header.txn_prev_undo);
}

test "UndoLog update record with full tuple data" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Simulate storing a full old tuple (header + user data)
    var old_tuple: [64]u8 = undefined;
    for (&old_tuple, 0..) |*b, i| {
        b.* = @intCast(i);
    }

    const header = UndoRecordHeader{
        .record_type = .update,
        .slot_id = 0,
        .txn_id = 5,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 3,
        .data_len = 64,
    };

    const offset = try log.appendRecord(header, &old_tuple);
    const result = try log.readRecord(offset);

    try std.testing.expectEqual(UndoRecordType.update, result.header.record_type);
    try std.testing.expectEqual(@as(u16, 64), result.header.data_len);
    try std.testing.expectEqualSlices(u8, &old_tuple, result.data);
}

test "UndoLog invalid offset returns error" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Reading from NO_UNDO_PTR should error
    try std.testing.expectError(error.InvalidOffset, log.readRecord(NO_UNDO_PTR));

    // Reading from beyond buffer should error
    try std.testing.expectError(error.InvalidOffset, log.readRecord(9999));
}

test "UndoLog GC advances after all committed" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Txn 1: insert record
    const h1 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 1,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h1, &.{});

    // Txn 2: insert record
    const h2 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 2,
        .txn_id = 2,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h2, &.{});

    try std.testing.expectEqual(@as(u64, 0), log.reclaimableBytes());

    // GC with oldest_visible = 3 (both txns 1 and 2 are old)
    log.gc(3);
    try std.testing.expectEqual(log.write_pos, log.reclaimableBytes());
}

test "UndoLog GC stops at active txn boundary" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Txn 1: insert
    const h1 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 1,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    const offset1 = try log.appendRecord(h1, &.{});
    _ = offset1;

    // Txn 5: insert (still active)
    const h2 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 2,
        .txn_id = 5,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h2, &.{});

    // GC with oldest_visible = 3 — should advance past txn 1 but stop at txn 5
    log.gc(3);
    const expected_watermark = UndoRecordHeader.SIZE; // just past first record
    try std.testing.expectEqual(@as(u64, expected_watermark), log.reclaimableBytes());
}

test "UndoLog GC multiple txns progressive" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Txn 1, 2, 3 each write a record
    var i: u32 = 1;
    while (i <= 3) : (i += 1) {
        const h = UndoRecordHeader{
            .record_type = .insert,
            .slot_id = @intCast(i),
            .txn_id = i,
            .prev_undo_ptr = NO_UNDO_PTR,
            .txn_prev_undo = NO_UNDO_PTR,
            .table_page_id = 5,
            .data_len = 0,
        };
        _ = try log.appendRecord(h, &.{});
    }

    // GC with oldest_visible = 2 — should advance past txn 1 only
    log.gc(2);
    try std.testing.expectEqual(@as(u64, UndoRecordHeader.SIZE), log.reclaimableBytes());

    // GC with oldest_visible = 4 — should advance past all
    log.gc(4);
    try std.testing.expectEqual(log.write_pos, log.reclaimableBytes());
}

test "UndoLog GC on empty log" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // GC on empty log should be a no-op
    log.gc(100);
    try std.testing.expectEqual(@as(u64, 0), log.reclaimableBytes());
    try std.testing.expectEqual(@as(u64, 0), log.write_pos);
}

test "UndoLog GC idempotent double call" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    const h = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 1,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h, &.{});

    log.gc(2);
    const first = log.reclaimableBytes();
    // Second call with same boundary should not change anything
    log.gc(2);
    try std.testing.expectEqual(first, log.reclaimableBytes());
}

test "UndoLog GC with oldest_visible 0 reclaims nothing" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    const h = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 1,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h, &.{});

    // oldest_visible == 0 means nothing is old enough to reclaim
    log.gc(0);
    try std.testing.expectEqual(@as(u64, 0), log.reclaimableBytes());
}

test "UndoLog read record at watermark still works" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    const h1 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 1,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    const offset1 = try log.appendRecord(h1, &.{});

    const h2 = UndoRecordHeader{
        .record_type = .delete,
        .slot_id = 2,
        .txn_id = 2,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h2, &.{});

    // GC past first record
    log.gc(2);
    try std.testing.expectEqual(@as(u64, UndoRecordHeader.SIZE), log.reclaimableBytes());

    // Reading the GC'd record should still work (offsets remain stable)
    const rec = try log.readRecord(offset1);
    try std.testing.expectEqual(UndoRecordType.insert, rec.header.record_type);
    try std.testing.expectEqual(@as(TxnId, 1), rec.header.txn_id);
}

test "UndoLog mixed record types with data then GC" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Insert record (no data)
    const h1 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 1,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h1, &.{});

    // Update record (with 8 bytes data)
    const data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const h2 = UndoRecordHeader{
        .record_type = .update,
        .slot_id = 1,
        .txn_id = 2,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 8,
    };
    _ = try log.appendRecord(h2, &data);

    // Delete record (no data)
    const h3 = UndoRecordHeader{
        .record_type = .delete,
        .slot_id = 2,
        .txn_id = 3,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h3, &.{});

    // GC past txn 1 only — should skip 32 bytes (header only, no data)
    log.gc(2);
    try std.testing.expectEqual(@as(u64, UndoRecordHeader.SIZE), log.reclaimableBytes());

    // GC past txn 2 — should skip 32+8=40 more bytes (header + 8 bytes data)
    log.gc(3);
    const expected = UndoRecordHeader.SIZE + UndoRecordHeader.SIZE + 8;
    try std.testing.expectEqual(@as(u64, expected), log.reclaimableBytes());
}

test "UndoLog compact reclaims memory" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Txn 1: insert (32 bytes)
    const h1 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 1,
        .txn_id = 1,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    _ = try log.appendRecord(h1, &.{});

    // Txn 2: insert (32 bytes)
    const h2 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 2,
        .txn_id = 2,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    const offset2 = try log.appendRecord(h2, &.{});

    // GC txn 1
    log.gc(2);
    try std.testing.expectEqual(@as(u64, UndoRecordHeader.SIZE), log.reclaimableBytes());

    // Compact — memory should shrink
    log.compact();
    try std.testing.expectEqual(@as(u64, 0), log.reclaimableBytes());
    try std.testing.expectEqual(@as(u64, UndoRecordHeader.SIZE), log.write_pos);
    try std.testing.expectEqual(@as(u64, UndoRecordHeader.SIZE), log.base_offset);

    // Reading the compacted record should fail
    try std.testing.expectError(error.InvalidOffset, log.readRecord(0));

    // Reading the live record should still work
    const rec = try log.readRecord(offset2);
    try std.testing.expectEqual(UndoRecordType.insert, rec.header.record_type);
    try std.testing.expectEqual(@as(TxnId, 2), rec.header.txn_id);

    // New records should get correct global offsets
    const h3 = UndoRecordHeader{
        .record_type = .insert,
        .slot_id = 3,
        .txn_id = 3,
        .prev_undo_ptr = NO_UNDO_PTR,
        .txn_prev_undo = NO_UNDO_PTR,
        .table_page_id = 5,
        .data_len = 0,
    };
    const offset3 = try log.appendRecord(h3, &.{});
    try std.testing.expectEqual(@as(u64, 2 * UndoRecordHeader.SIZE), offset3);

    // Can read the new record
    const rec3 = try log.readRecord(offset3);
    try std.testing.expectEqual(@as(TxnId, 3), rec3.header.txn_id);
}

test "UndoLog compact on empty log is no-op" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    log.compact();
    try std.testing.expectEqual(@as(u64, 0), log.write_pos);
    try std.testing.expectEqual(@as(u64, 0), log.base_offset);
}

test "UndoLog gc then compact then gc again" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // Write 3 records
    var offsets: [3]u64 = undefined;
    for (0..3) |i| {
        const h = UndoRecordHeader{
            .record_type = .insert,
            .slot_id = @intCast(i),
            .txn_id = @as(u32, @intCast(i)) + 1,
            .prev_undo_ptr = NO_UNDO_PTR,
            .txn_prev_undo = NO_UNDO_PTR,
            .table_page_id = 5,
            .data_len = 0,
        };
        offsets[i] = try log.appendRecord(h, &.{});
    }

    // GC past txn 1, compact
    log.gc(2);
    log.compact();

    // GC past txn 2, compact again
    log.gc(3);
    log.compact();
    try std.testing.expectEqual(@as(u64, UndoRecordHeader.SIZE), log.write_pos);
    try std.testing.expectEqual(@as(u64, 2 * UndoRecordHeader.SIZE), log.base_offset);

    // Record 3 still readable
    const rec = try log.readRecord(offsets[2]);
    try std.testing.expectEqual(@as(TxnId, 3), rec.header.txn_id);
}
