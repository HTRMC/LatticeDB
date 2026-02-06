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
    /// Current write position (= logical file size)
    write_pos: u64,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
            .buffer = .empty,
            .write_pos = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit(self.allocator);
    }

    /// Append an undo record and return its offset (= undo_ptr)
    pub fn appendRecord(self: *Self, header: UndoRecordHeader, data: []const u8) !u64 {
        const offset = self.write_pos;
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
        return offset;
    }

    /// Read an undo record at the given offset
    pub fn readRecord(self: *const Self, offset: u64) !struct { header: UndoRecordHeader, data: []const u8 } {
        if (offset == NO_UNDO_PTR) return error.InvalidOffset;

        const pos: usize = @intCast(offset);
        if (pos + UndoRecordHeader.SIZE > self.buffer.items.len) return error.InvalidOffset;

        const header = std.mem.bytesToValue(UndoRecordHeader, self.buffer.items[pos..][0..UndoRecordHeader.SIZE]);
        const data_start = pos + UndoRecordHeader.SIZE;
        const data_end = data_start + header.data_len;

        if (data_end > self.buffer.items.len) return error.InvalidOffset;

        return .{
            .header = header,
            .data = self.buffer.items[data_start..data_end],
        };
    }

    /// Get the current write position (for GC boundary calculations)
    pub fn currentOffset(self: *const Self) u64 {
        return self.write_pos;
    }

    /// GC: Truncate undo entries before the safe offset.
    /// In this in-memory implementation, we just note the safe offset
    /// but don't actually reclaim memory (a real implementation would
    /// use a circular buffer or file truncation).
    pub fn gc(self: *Self, safe_offset: u64) void {
        _ = safe_offset;
        // In a production implementation, we'd reclaim space before safe_offset.
        // For now this is a no-op marker for the GC boundary.
        _ = self;
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
