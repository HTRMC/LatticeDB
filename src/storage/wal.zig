const std = @import("std");
const page_mod = @import("page.zig");

const PageId = page_mod.PageId;
const PAGE_SIZE = page_mod.PAGE_SIZE;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;

const Io = std.Io;
const File = Io.File;
const Dir = Io.Dir;
const Threaded = Io.Threaded;
const process = std.process;

pub const WalError = error{
    FileNotOpen,
    WriteError,
    ReadError,
    FlushError,
    CorruptedLog,
    OutOfMemory,
    InvalidRecord,
};

/// Log Sequence Number - uniquely identifies a log record
pub const Lsn = u64;
pub const INVALID_LSN: Lsn = 0;

/// Transaction ID
pub const TxnId = u32;
pub const INVALID_TXN_ID: TxnId = 0;

/// Log record types
pub const LogRecordType = enum(u8) {
    invalid = 0,
    begin = 1,
    commit = 2,
    abort = 3,
    update = 4,
    checkpoint = 5,
};

/// Log record header - fixed size portion
pub const LogRecordHeader = extern struct {
    /// Log sequence number
    lsn: Lsn,
    /// Transaction ID
    txn_id: TxnId,
    /// Previous LSN for this transaction (for rollback chain)
    prev_lsn: Lsn,
    /// Record type
    record_type: LogRecordType,
    /// Length of the data portion
    data_len: u32,
    /// Checksum of header + data
    checksum: u32,

    pub const SIZE: usize = @sizeOf(LogRecordHeader);

    pub fn computeChecksum(self: *const LogRecordHeader, data: []const u8) u32 {
        var hasher = std.hash.Crc32.init();
        // Hash header fields (excluding checksum itself)
        hasher.update(std.mem.asBytes(&self.lsn));
        hasher.update(std.mem.asBytes(&self.txn_id));
        hasher.update(std.mem.asBytes(&self.prev_lsn));
        hasher.update(std.mem.asBytes(&self.record_type));
        hasher.update(std.mem.asBytes(&self.data_len));
        // Hash data
        hasher.update(data);
        return hasher.final();
    }

    pub fn verify(self: *const LogRecordHeader, data: []const u8) bool {
        return self.checksum == self.computeChecksum(data);
    }
};

/// Update record data - contains page modification info
pub const UpdateRecordData = struct {
    page_id: PageId,
    offset: u16,
    length: u16,
    // Followed by: old_data (length bytes), new_data (length bytes)

    pub const HEADER_SIZE: usize = @sizeOf(PageId) + @sizeOf(u16) + @sizeOf(u16);

    pub fn serialize(self: UpdateRecordData, old_data: []const u8, new_data: []const u8, buffer: []u8) !usize {
        if (buffer.len < HEADER_SIZE + old_data.len + new_data.len) {
            return error.OutOfMemory;
        }

        var offset: usize = 0;

        // Write page_id
        @memcpy(buffer[offset..][0..@sizeOf(PageId)], std.mem.asBytes(&self.page_id));
        offset += @sizeOf(PageId);

        // Write offset
        @memcpy(buffer[offset..][0..@sizeOf(u16)], std.mem.asBytes(&self.offset));
        offset += @sizeOf(u16);

        // Write length
        @memcpy(buffer[offset..][0..@sizeOf(u16)], std.mem.asBytes(&self.length));
        offset += @sizeOf(u16);

        // Write old data
        @memcpy(buffer[offset..][0..old_data.len], old_data);
        offset += old_data.len;

        // Write new data
        @memcpy(buffer[offset..][0..new_data.len], new_data);
        offset += new_data.len;

        return offset;
    }

    pub fn deserialize(data: []const u8) !struct { header: UpdateRecordData, old_data: []const u8, new_data: []const u8 } {
        if (data.len < HEADER_SIZE) {
            return error.InvalidRecord;
        }

        var offset: usize = 0;

        const page_id = std.mem.bytesToValue(PageId, data[offset..][0..@sizeOf(PageId)]);
        offset += @sizeOf(PageId);

        const data_offset = std.mem.bytesToValue(u16, data[offset..][0..@sizeOf(u16)]);
        offset += @sizeOf(u16);

        const length = std.mem.bytesToValue(u16, data[offset..][0..@sizeOf(u16)]);
        offset += @sizeOf(u16);

        const data_size: usize = length;
        if (data.len < HEADER_SIZE + data_size * 2) {
            return error.InvalidRecord;
        }

        const old_data = data[offset..][0..data_size];
        offset += data_size;

        const new_data = data[offset..][0..data_size];

        return .{
            .header = .{
                .page_id = page_id,
                .offset = data_offset,
                .length = length,
            },
            .old_data = old_data,
            .new_data = new_data,
        };
    }
};

/// Write-Ahead Log Manager
pub const Wal = struct {
    allocator: std.mem.Allocator,
    file_path: []const u8,
    file: ?File,
    threaded: Threaded,

    /// Current LSN (next to be assigned)
    current_lsn: Lsn,
    /// LSN of last flushed record
    flushed_lsn: Lsn,
    /// Log buffer for batching writes
    buffer: std.ArrayList(u8),
    /// Offset in file where buffer will be written
    buffer_offset: u64,

    const Self = @This();
    const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer

    fn io(self: *Self) Io {
        return self.threaded.io();
    }

    /// Create a new WAL manager
    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) Self {
        return .{
            .allocator = allocator,
            .file_path = file_path,
            .file = null,
            .threaded = Threaded.init(allocator, .{ .environ = process.Environ.empty }),
            .current_lsn = 1, // LSN starts at 1 (0 is invalid)
            .flushed_lsn = 0,
            .buffer = .empty,
            .buffer_offset = 0,
        };
    }

    /// Open or create the WAL file
    pub fn open(self: *Self) WalError!void {
        // Try to open existing file
        self.file = Dir.openFile(.cwd(), self.io(), self.file_path, .{ .mode = .read_write }) catch |err| {
            if (err == error.FileNotFound) {
                // Create new file
                self.file = Dir.createFile(.cwd(), self.io(), self.file_path, .{
                    .read = true,
                    .truncate = false,
                }) catch {
                    return WalError.WriteError;
                };
                self.current_lsn = 1;
                self.flushed_lsn = 0;
                self.buffer_offset = 0;
                return;
            }
            return WalError.WriteError;
        };

        // Existing file - scan to find current LSN
        try self.scanLog();
    }

    /// Close the WAL file
    pub fn close(self: *Self) void {
        // Flush any remaining buffer
        self.flush() catch {};

        if (self.file) |f| {
            f.close(self.io());
            self.file = null;
        }
    }

    /// Deinitialize the WAL
    pub fn deinit(self: *Self) void {
        self.close();
        self.buffer.deinit(self.allocator);
        self.threaded.deinit();
    }

    /// Append a BEGIN record
    pub fn logBegin(self: *Self, txn_id: TxnId) WalError!Lsn {
        return self.appendRecord(txn_id, INVALID_LSN, .begin, &.{});
    }

    /// Append a COMMIT record
    pub fn logCommit(self: *Self, txn_id: TxnId, prev_lsn: Lsn) WalError!Lsn {
        const lsn = try self.appendRecord(txn_id, prev_lsn, .commit, &.{});
        // Commit must be durable - force flush
        try self.flush();
        return lsn;
    }

    /// Append an ABORT record
    pub fn logAbort(self: *Self, txn_id: TxnId, prev_lsn: Lsn) WalError!Lsn {
        return self.appendRecord(txn_id, prev_lsn, .abort, &.{});
    }

    /// Append an UPDATE record
    pub fn logUpdate(
        self: *Self,
        txn_id: TxnId,
        prev_lsn: Lsn,
        page_id: PageId,
        offset: u16,
        old_data: []const u8,
        new_data: []const u8,
    ) WalError!Lsn {
        // Serialize update data
        const data_len = UpdateRecordData.HEADER_SIZE + old_data.len + new_data.len;
        const data_buf = self.allocator.alloc(u8, data_len) catch {
            return WalError.OutOfMemory;
        };
        defer self.allocator.free(data_buf);

        const update = UpdateRecordData{
            .page_id = page_id,
            .offset = offset,
            .length = @intCast(old_data.len),
        };

        _ = update.serialize(old_data, new_data, data_buf) catch {
            return WalError.OutOfMemory;
        };

        return self.appendRecord(txn_id, prev_lsn, .update, data_buf);
    }

    /// Append a CHECKPOINT record
    pub fn logCheckpoint(self: *Self) WalError!Lsn {
        const lsn = try self.appendRecord(INVALID_TXN_ID, INVALID_LSN, .checkpoint, &.{});
        try self.flush();
        return lsn;
    }

    /// Append a record to the log
    fn appendRecord(self: *Self, txn_id: TxnId, prev_lsn: Lsn, record_type: LogRecordType, data: []const u8) WalError!Lsn {
        _ = self.file orelse return WalError.FileNotOpen;

        const lsn = self.current_lsn;
        self.current_lsn += 1;

        var header = LogRecordHeader{
            .lsn = lsn,
            .txn_id = txn_id,
            .prev_lsn = prev_lsn,
            .record_type = record_type,
            .data_len = @intCast(data.len),
            .checksum = 0,
        };
        header.checksum = header.computeChecksum(data);

        // Append header to buffer
        self.buffer.appendSlice(self.allocator, std.mem.asBytes(&header)) catch {
            return WalError.OutOfMemory;
        };

        // Append data to buffer
        if (data.len > 0) {
            self.buffer.appendSlice(self.allocator, data) catch {
                return WalError.OutOfMemory;
            };
        }

        // Flush if buffer is large enough
        if (self.buffer.items.len >= BUFFER_SIZE) {
            try self.flush();
        }

        return lsn;
    }

    /// Flush the log buffer to disk
    pub fn flush(self: *Self) WalError!void {
        const f = self.file orelse return WalError.FileNotOpen;

        if (self.buffer.items.len == 0) {
            return;
        }

        // Write buffer to file at current offset
        f.writePositionalAll(self.io(), self.buffer.items, self.buffer_offset) catch {
            return WalError.WriteError;
        };

        // Sync to disk
        f.sync(self.io()) catch {
            return WalError.FlushError;
        };

        // Update state
        self.buffer_offset += self.buffer.items.len;
        self.flushed_lsn = self.current_lsn - 1;
        self.buffer.clearRetainingCapacity();
    }

    /// Scan the log to find the current LSN (for recovery)
    fn scanLog(self: *Self) WalError!void {
        const f = self.file orelse return WalError.FileNotOpen;

        // Get file size
        const stat = f.stat(self.io()) catch {
            return WalError.ReadError;
        };

        if (stat.size == 0) {
            self.current_lsn = 1;
            self.flushed_lsn = 0;
            self.buffer_offset = 0;
            return;
        }

        // Read and validate records to find the last valid LSN
        var offset: u64 = 0;
        var last_valid_lsn: Lsn = 0;
        var header_buf: [LogRecordHeader.SIZE]u8 = undefined;

        while (offset + LogRecordHeader.SIZE <= stat.size) {
            // Read header
            const header_bytes = f.readPositionalAll(self.io(), &header_buf, offset) catch {
                break;
            };

            if (header_bytes != LogRecordHeader.SIZE) {
                break;
            }

            const header: *const LogRecordHeader = @ptrCast(@alignCast(&header_buf));

            // Validate record type
            if (@intFromEnum(header.record_type) == 0 or @intFromEnum(header.record_type) > 5) {
                break;
            }

            // Read data if present
            var data_buf: []u8 = &.{};
            if (header.data_len > 0) {
                data_buf = self.allocator.alloc(u8, header.data_len) catch {
                    break;
                };
                defer self.allocator.free(data_buf);

                const data_bytes = f.readPositionalAll(self.io(), data_buf, offset + LogRecordHeader.SIZE) catch {
                    break;
                };

                if (data_bytes != header.data_len) {
                    break;
                }
            }

            // Verify checksum
            if (!header.verify(data_buf)) {
                break;
            }

            // Valid record found
            last_valid_lsn = header.lsn;
            offset += LogRecordHeader.SIZE + header.data_len;
        }

        self.current_lsn = last_valid_lsn + 1;
        self.flushed_lsn = last_valid_lsn;
        self.buffer_offset = offset;
    }

    /// Get the current (next) LSN
    pub fn getCurrentLsn(self: *const Self) Lsn {
        return self.current_lsn;
    }

    /// Get the last flushed LSN
    pub fn getFlushedLsn(self: *const Self) Lsn {
        return self.flushed_lsn;
    }

    /// Iterator for reading log records
    pub const LogIterator = struct {
        wal: *Self,
        offset: u64,
        end_offset: u64,

        pub fn next(self: *LogIterator) WalError!?struct { header: LogRecordHeader, data: []const u8 } {
            if (self.offset >= self.end_offset) {
                return null;
            }

            const f = self.wal.file orelse return WalError.FileNotOpen;

            // Read header
            var header_buf: [LogRecordHeader.SIZE]u8 = undefined;
            const header_bytes = f.readPositionalAll(self.wal.io(), &header_buf, self.offset) catch {
                return WalError.ReadError;
            };

            if (header_bytes != LogRecordHeader.SIZE) {
                return null;
            }

            const header: LogRecordHeader = @bitCast(header_buf);

            // Read data
            var data: []u8 = &.{};
            if (header.data_len > 0) {
                data = self.wal.allocator.alloc(u8, header.data_len) catch {
                    return WalError.OutOfMemory;
                };

                const data_bytes = f.readPositionalAll(self.wal.io(), data, self.offset + LogRecordHeader.SIZE) catch {
                    self.wal.allocator.free(data);
                    return WalError.ReadError;
                };

                if (data_bytes != header.data_len) {
                    self.wal.allocator.free(data);
                    return WalError.CorruptedLog;
                }
            }

            // Verify checksum
            if (!header.verify(data)) {
                if (data.len > 0) self.wal.allocator.free(data);
                return WalError.CorruptedLog;
            }

            self.offset += LogRecordHeader.SIZE + header.data_len;
            return .{ .header = header, .data = data };
        }

        /// Free data returned by next()
        pub fn freeData(self: *LogIterator, data: []const u8) void {
            if (data.len > 0) {
                self.wal.allocator.free(@constCast(data));
            }
        }
    };

    /// Create an iterator to read log records
    pub fn iterator(self: *Self) WalError!LogIterator {
        // Flush buffer first to ensure all records are on disk
        try self.flush();

        return .{
            .wal = self,
            .offset = 0,
            .end_offset = self.buffer_offset,
        };
    }

    /// Delete the WAL file (for testing)
    pub fn deleteFile(self: *Self) void {
        self.close();
        Dir.deleteFile(.cwd(), self.io(), self.file_path) catch {};
    }
};

// Tests
test "wal basic operations" {
    const test_file = "test_wal_basic.log";

    var wal = Wal.init(std.testing.allocator, test_file);
    defer wal.deleteFile();
    defer wal.deinit();

    try wal.open();

    // Log a transaction
    const txn_id: TxnId = 1;
    const begin_lsn = try wal.logBegin(txn_id);
    try std.testing.expectEqual(@as(Lsn, 1), begin_lsn);

    const commit_lsn = try wal.logCommit(txn_id, begin_lsn);
    try std.testing.expectEqual(@as(Lsn, 2), commit_lsn);

    // Verify flushed
    try std.testing.expectEqual(@as(Lsn, 2), wal.getFlushedLsn());
}

test "wal update records" {
    const test_file = "test_wal_update.log";

    var wal = Wal.init(std.testing.allocator, test_file);
    defer wal.deleteFile();
    defer wal.deinit();

    try wal.open();

    const txn_id: TxnId = 1;
    const begin_lsn = try wal.logBegin(txn_id);

    // Log an update
    const old_data = "old value";
    const new_data = "new value";
    const update_lsn = try wal.logUpdate(txn_id, begin_lsn, 42, 100, old_data, new_data);

    try std.testing.expectEqual(@as(Lsn, 2), update_lsn);

    _ = try wal.logCommit(txn_id, update_lsn);
}

test "wal persistence and recovery" {
    const test_file = "test_wal_persist.log";

    // First session - write some records
    {
        var wal = Wal.init(std.testing.allocator, test_file);
        defer wal.deinit();

        try wal.open();

        _ = try wal.logBegin(1);
        _ = try wal.logCommit(1, 1);
        _ = try wal.logBegin(2);
        _ = try wal.logCommit(2, 3);

        try std.testing.expectEqual(@as(Lsn, 4), wal.getFlushedLsn());
    }

    // Second session - should recover state
    {
        var wal = Wal.init(std.testing.allocator, test_file);
        defer wal.deleteFile();
        defer wal.deinit();

        try wal.open();

        // Should continue from where we left off
        try std.testing.expectEqual(@as(Lsn, 5), wal.getCurrentLsn());
        try std.testing.expectEqual(@as(Lsn, 4), wal.getFlushedLsn());
    }
}

test "wal iterator" {
    const test_file = "test_wal_iterator.log";

    var wal = Wal.init(std.testing.allocator, test_file);
    defer wal.deleteFile();
    defer wal.deinit();

    try wal.open();

    // Write some records
    _ = try wal.logBegin(1);
    _ = try wal.logUpdate(1, 1, 10, 0, "old", "new");
    _ = try wal.logCommit(1, 2);

    // Read them back
    var iter = try wal.iterator();
    var count: usize = 0;
    var types: [3]LogRecordType = undefined;

    while (try iter.next()) |record| {
        defer iter.freeData(record.data);
        if (count < 3) {
            types[count] = record.header.record_type;
        }
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(LogRecordType.begin, types[0]);
    try std.testing.expectEqual(LogRecordType.update, types[1]);
    try std.testing.expectEqual(LogRecordType.commit, types[2]);
}

test "wal checkpoint" {
    const test_file = "test_wal_checkpoint.log";

    var wal = Wal.init(std.testing.allocator, test_file);
    defer wal.deleteFile();
    defer wal.deinit();

    try wal.open();

    _ = try wal.logBegin(1);
    _ = try wal.logCommit(1, 1);

    // Log checkpoint
    const cp_lsn = try wal.logCheckpoint();
    try std.testing.expectEqual(@as(Lsn, 3), cp_lsn);

    // Checkpoint should force flush
    try std.testing.expectEqual(@as(Lsn, 3), wal.getFlushedLsn());
}

test "wal multiple transactions" {
    const test_file = "test_wal_multi_txn.log";

    var wal = Wal.init(std.testing.allocator, test_file);
    defer wal.deleteFile();
    defer wal.deinit();

    try wal.open();

    // Interleaved transactions
    const t1_begin = try wal.logBegin(1);
    const t2_begin = try wal.logBegin(2);
    const t1_update = try wal.logUpdate(1, t1_begin, 1, 0, "a", "b");
    const t2_update = try wal.logUpdate(2, t2_begin, 2, 0, "c", "d");
    _ = try wal.logCommit(1, t1_update);
    _ = try wal.logAbort(2, t2_update);

    // Commit flushes up to LSN 5, abort (LSN 6) is still in buffer
    try std.testing.expectEqual(@as(Lsn, 5), wal.getFlushedLsn());
    try wal.flush();
    try std.testing.expectEqual(@as(Lsn, 6), wal.getFlushedLsn());

    // Verify via iterator
    var iter = try wal.iterator();
    var count: usize = 0;
    while (try iter.next()) |record| {
        defer iter.freeData(record.data);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 6), count);
}
