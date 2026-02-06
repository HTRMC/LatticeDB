const std = @import("std");
const page_mod = @import("page.zig");
const mvcc_mod = @import("mvcc.zig");
const buffer_pool_mod = @import("buffer_pool.zig");

const PageId = page_mod.PageId;
const PAGE_SIZE = page_mod.PAGE_SIZE;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const BufferPool = buffer_pool_mod.BufferPool;

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

/// Transaction ID (imported from mvcc)
pub const TxnId = mvcc_mod.TxnId;
pub const INVALID_TXN_ID: TxnId = mvcc_mod.INVALID_TXN_ID;

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

    /// Recovery result
    pub const RecoveryResult = struct {
        committed_count: u32,
        aborted_count: u32,
    };

    /// Crash recovery: replay WAL to restore consistent state.
    ///
    /// 1. Analysis pass: scan forward, build committed_txns and active_txns sets
    /// 2. Redo pass: for committed txns' update records, apply new_data to page
    /// 3. Undo pass: for active (uncommitted) txns' update records, apply old_data
    pub fn recover(self: *Self, buffer_pool: *BufferPool) WalError!RecoveryResult {
        // Flush any buffered records to disk first
        try self.flush();

        // === Analysis pass ===
        var committed_txns = std.AutoHashMap(TxnId, void).init(self.allocator);
        defer committed_txns.deinit();
        var active_txns = std.AutoHashMap(TxnId, void).init(self.allocator);
        defer active_txns.deinit();

        var iter = try self.iterator();
        while (try iter.next()) |record| {
            defer iter.freeData(record.data);
            switch (record.header.record_type) {
                .begin => {
                    active_txns.put(record.header.txn_id, {}) catch return WalError.OutOfMemory;
                },
                .commit => {
                    _ = active_txns.remove(record.header.txn_id);
                    committed_txns.put(record.header.txn_id, {}) catch return WalError.OutOfMemory;
                },
                .abort => {
                    _ = active_txns.remove(record.header.txn_id);
                },
                else => {},
            }
        }

        // === Redo pass: apply committed txns' updates ===
        var redo_iter = try self.iterator();
        while (try redo_iter.next()) |record| {
            defer redo_iter.freeData(record.data);
            if (record.header.record_type == .update and committed_txns.contains(record.header.txn_id)) {
                const update_info = UpdateRecordData.deserialize(record.data) catch continue;
                var pg = buffer_pool.fetchPage(update_info.header.page_id) catch continue;
                const off: usize = update_info.header.offset;
                const len: usize = update_info.header.length;
                if (off + len <= PAGE_SIZE) {
                    @memcpy(pg.data[off..][0..len], update_info.new_data);
                }
                buffer_pool.unpinPage(update_info.header.page_id, true) catch {};
            }
        }

        // === Undo pass: reverse active (uncommitted) txns' updates ===
        // Collect update records for active txns, then apply in reverse
        const UndoEntry = struct { page_id: PageId, offset: u16, old_data: []const u8 };
        var undo_records: std.ArrayList(UndoEntry) = .empty;
        defer {
            for (undo_records.items) |rec| {
                self.allocator.free(@constCast(rec.old_data));
            }
            undo_records.deinit(self.allocator);
        }

        var undo_scan = try self.iterator();
        while (try undo_scan.next()) |record| {
            if (record.header.record_type == .update and active_txns.contains(record.header.txn_id)) {
                const update_info = UpdateRecordData.deserialize(record.data) catch {
                    undo_scan.freeData(record.data);
                    continue;
                };
                const old_data_copy = self.allocator.dupe(u8, update_info.old_data) catch {
                    undo_scan.freeData(record.data);
                    return WalError.OutOfMemory;
                };
                undo_records.append(self.allocator, .{
                    .page_id = update_info.header.page_id,
                    .offset = update_info.header.offset,
                    .old_data = old_data_copy,
                }) catch {
                    self.allocator.free(old_data_copy);
                    undo_scan.freeData(record.data);
                    return WalError.OutOfMemory;
                };
            }
            undo_scan.freeData(record.data);
        }

        // Apply undo records in reverse order
        var i: usize = undo_records.items.len;
        while (i > 0) {
            i -= 1;
            const rec = undo_records.items[i];
            var pg = buffer_pool.fetchPage(rec.page_id) catch continue;
            const off: usize = rec.offset;
            const len = rec.old_data.len;
            if (off + len <= PAGE_SIZE) {
                @memcpy(pg.data[off..][0..len], rec.old_data);
            }
            buffer_pool.unpinPage(rec.page_id, true) catch {};
        }

        return .{
            .committed_count = @intCast(committed_txns.count()),
            .aborted_count = @intCast(active_txns.count()),
        };
    }

    /// Delete the WAL file (for testing)
    pub fn deleteFile(self: *Self) void {
        self.close();
        Dir.deleteFile(.cwd(), self.io(), self.file_path) catch {};
    }
};

// Tests
const disk_manager_mod = @import("disk_manager.zig");
const DiskManager = disk_manager_mod.DiskManager;

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

test "recovery redo committed" {
    const db_file = "test_recovery_redo.db";
    const wal_file = "test_recovery_redo.log";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    // Allocate a page and write initial data
    const result = bp.newPage() catch unreachable;
    const page_id = result.page_id;

    // Write "AAAA" at offset 100 in the page
    const old_data = "AAAA";
    const new_data = "BBBB";
    @memcpy(result.page.data[100..104], old_data);
    bp.unpinPage(page_id, true) catch {};

    // Create WAL and log a committed update: old_data → new_data at offset 100
    var wal = Wal.init(std.testing.allocator, wal_file);
    defer wal.deleteFile();
    defer wal.deinit();
    try wal.open();

    const begin_lsn = try wal.logBegin(1);
    const update_lsn = try wal.logUpdate(1, begin_lsn, page_id, 100, old_data, new_data);
    _ = try wal.logCommit(1, update_lsn);

    // Simulate crash: reset the page data back to old value
    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[100..104], old_data);
    bp.unpinPage(page_id, true) catch {};

    // Run recovery — should redo the committed update
    const recovery_result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), recovery_result.committed_count);
    try std.testing.expectEqual(@as(u32, 0), recovery_result.aborted_count);

    // Verify the page has the new data
    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, new_data, pg2.data[100..104]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery undo active" {
    const db_file = "test_recovery_undo.db";
    const wal_file = "test_recovery_undo.log";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    // Allocate a page and write initial data
    const result = bp.newPage() catch unreachable;
    const page_id = result.page_id;

    const old_data = "XXXX";
    const new_data = "YYYY";
    @memcpy(result.page.data[200..204], old_data);
    bp.unpinPage(page_id, true) catch {};

    // Create WAL: BEGIN + UPDATE but NO COMMIT (active/uncommitted txn)
    var wal = Wal.init(std.testing.allocator, wal_file);
    defer wal.deleteFile();
    defer wal.deinit();
    try wal.open();

    const begin_lsn = try wal.logBegin(1);
    _ = try wal.logUpdate(1, begin_lsn, page_id, 200, old_data, new_data);
    // No commit! Txn 1 is still active.

    // Simulate the uncommitted change being applied to the page
    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[200..204], new_data);
    bp.unpinPage(page_id, true) catch {};

    // Run recovery — should undo the active txn's update
    const recovery_result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 0), recovery_result.committed_count);
    try std.testing.expectEqual(@as(u32, 1), recovery_result.aborted_count);

    // Verify the page has been restored to old data
    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, old_data, pg2.data[200..204]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery mixed committed and active" {
    const db_file = "test_recovery_mixed.db";
    const wal_file = "test_recovery_mixed.log";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const result = bp.newPage() catch unreachable;
    const page_id = result.page_id;

    // Initial state: "1111" at offset 100, "aaaa" at offset 200
    @memcpy(result.page.data[100..104], "1111");
    @memcpy(result.page.data[200..204], "aaaa");
    bp.unpinPage(page_id, true) catch {};

    var wal = Wal.init(std.testing.allocator, wal_file);
    defer wal.deleteFile();
    defer wal.deinit();
    try wal.open();

    // Txn 1: committed — updates offset 100 from "1111" to "2222"
    const t1_begin = try wal.logBegin(1);
    const t1_update = try wal.logUpdate(1, t1_begin, page_id, 100, "1111", "2222");
    _ = try wal.logCommit(1, t1_update);

    // Txn 2: active (no commit) — updates offset 200 from "aaaa" to "bbbb"
    const t2_begin = try wal.logBegin(2);
    _ = try wal.logUpdate(2, t2_begin, page_id, 200, "aaaa", "bbbb");
    // No commit for txn 2

    // Simulate crash: page has old values (committed change lost, uncommitted applied)
    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[100..104], "1111"); // committed change lost
    @memcpy(pg.data[200..204], "bbbb"); // uncommitted change applied
    bp.unpinPage(page_id, true) catch {};

    // Run recovery
    const recovery_result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), recovery_result.committed_count);
    try std.testing.expectEqual(@as(u32, 1), recovery_result.aborted_count);

    // Verify: offset 100 should be "2222" (redone), offset 200 should be "aaaa" (undone)
    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "2222", pg2.data[100..104]);
    try std.testing.expectEqualSlices(u8, "aaaa", pg2.data[200..204]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery empty WAL" {
    const db_file = "test_recovery_empty.db";
    const wal_file = "test_recovery_empty.log";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var wal = Wal.init(std.testing.allocator, wal_file);
    defer wal.deleteFile();
    defer wal.deinit();
    try wal.open();

    // Recovery on empty WAL should succeed with zero counts
    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 0), result.committed_count);
    try std.testing.expectEqual(@as(u32, 0), result.aborted_count);
}

test "recovery idempotent run twice" {
    const db_file = "test_recovery_idempotent.db";
    const wal_file = "test_recovery_idempotent.log";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const new_page = bp.newPage() catch unreachable;
    const page_id = new_page.page_id;
    @memcpy(new_page.page.data[50..54], "AAAA");
    bp.unpinPage(page_id, true) catch {};

    var wal = Wal.init(std.testing.allocator, wal_file);
    defer wal.deleteFile();
    defer wal.deinit();
    try wal.open();

    const begin_lsn = try wal.logBegin(1);
    const update_lsn = try wal.logUpdate(1, begin_lsn, page_id, 50, "AAAA", "BBBB");
    _ = try wal.logCommit(1, update_lsn);

    // First recovery
    const r1 = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), r1.committed_count);

    // Verify data after first recovery
    var pg1 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "BBBB", pg1.data[50..54]);
    bp.unpinPage(page_id, false) catch {};

    // Second recovery — should be idempotent, data still "BBBB"
    const r2 = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), r2.committed_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "BBBB", pg2.data[50..54]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery all active no commits" {
    const db_file = "test_recovery_all_active.db";
    const wal_file = "test_recovery_all_active.log";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const new_page = bp.newPage() catch unreachable;
    const page_id = new_page.page_id;
    @memcpy(new_page.page.data[0..4], "ORIG");
    bp.unpinPage(page_id, true) catch {};

    var wal = Wal.init(std.testing.allocator, wal_file);
    defer wal.deleteFile();
    defer wal.deinit();
    try wal.open();

    // Two BEGINs with updates, no commits
    const b1 = try wal.logBegin(1);
    _ = try wal.logUpdate(1, b1, page_id, 0, "ORIG", "AAA1");
    const b2 = try wal.logBegin(2);
    _ = try wal.logUpdate(2, b2, page_id, 4, "ORIG", "BBB2");

    // Simulate the uncommitted writes being applied
    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[0..4], "AAA1");
    @memcpy(pg.data[4..8], "BBB2");
    bp.unpinPage(page_id, true) catch {};

    // Recovery should undo both
    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 0), result.committed_count);
    try std.testing.expectEqual(@as(u32, 2), result.aborted_count);

    // Verify both restored
    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "ORIG", pg2.data[0..4]);
    try std.testing.expectEqualSlices(u8, "ORIG", pg2.data[4..8]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery multiple updates same page" {
    const db_file = "test_recovery_multi_update.db";
    const wal_file = "test_recovery_multi_update.log";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const new_page = bp.newPage() catch unreachable;
    const page_id = new_page.page_id;
    @memcpy(new_page.page.data[100..104], "AAAA");
    bp.unpinPage(page_id, true) catch {};

    var wal = Wal.init(std.testing.allocator, wal_file);
    defer wal.deleteFile();
    defer wal.deinit();
    try wal.open();

    // Chain of updates: AAAA → BBBB → CCCC → DDDD (all committed)
    const b = try wal.logBegin(1);
    const upd1 = try wal.logUpdate(1, b, page_id, 100, "AAAA", "BBBB");
    const upd2 = try wal.logUpdate(1, upd1, page_id, 100, "BBBB", "CCCC");
    const upd3 = try wal.logUpdate(1, upd2, page_id, 100, "CCCC", "DDDD");
    _ = try wal.logCommit(1, upd3);

    // Simulate crash: page still has AAAA
    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[100..104], "AAAA");
    bp.unpinPage(page_id, true) catch {};

    // Recovery should replay all 3 updates, ending with DDDD
    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), result.committed_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "DDDD", pg2.data[100..104]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery abort with no updates" {
    const db_file = "test_recovery_abort_noop.db";
    const wal_file = "test_recovery_abort_noop.log";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var wal = Wal.init(std.testing.allocator, wal_file);
    defer wal.deleteFile();
    defer wal.deinit();
    try wal.open();

    // BEGIN with no updates, then abort
    _ = try wal.logBegin(1);
    // txn 1 is active (no commit, no abort logged before crash)

    // BEGIN + COMMIT with no updates
    _ = try wal.logBegin(2);
    _ = try wal.logCommit(2, 0);

    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), result.committed_count);
    try std.testing.expectEqual(@as(u32, 1), result.aborted_count);
}
