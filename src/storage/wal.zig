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

/// Default segment size: 64MB
pub const DEFAULT_SEGMENT_MAX_SIZE: u64 = 64 * 1024 * 1024;

/// Segment file name format: 12-digit zero-padded segment ID + .wal
/// e.g. "000000000001.wal"
const SEGMENT_NAME_LEN: usize = 16; // 12 digits + ".wal"

fn formatSegmentName(buf: *[SEGMENT_NAME_LEN]u8, segment_id: u64) []const u8 {
    _ = std.fmt.bufPrint(buf, "{d:0>12}.wal", .{segment_id}) catch unreachable;
    return buf[0..SEGMENT_NAME_LEN];
}

/// Write-Ahead Log Manager with segment-based storage.
///
/// WAL records are written to fixed-size segment files under a directory.
/// When the current segment exceeds `segment_max_size`, it is finalized
/// and a new segment is created. Completed segments can be archived
/// for replication shipping.
pub const Wal = struct {
    allocator: std.mem.Allocator,
    wal_dir: []const u8,
    threaded: Threaded,

    /// Current LSN (next to be assigned)
    current_lsn: Lsn,
    /// LSN of last flushed record
    flushed_lsn: Lsn,
    /// Log buffer for batching writes
    buffer: std.ArrayList(u8),

    /// Current segment state
    current_segment_id: u64,
    current_segment_file: ?File,
    current_segment_offset: u64,
    segment_max_size: u64,

    /// Next segment ID to create
    next_segment_id: u64,

    const Self = @This();
    const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer

    fn io(self: *Self) Io {
        return self.threaded.io();
    }

    /// Create a new WAL manager for a directory.
    pub fn init(allocator: std.mem.Allocator, wal_dir: []const u8) Self {
        return .{
            .allocator = allocator,
            .wal_dir = wal_dir,
            .threaded = Threaded.init(allocator, .{ .environ = process.Environ.empty }),
            .current_lsn = 1,
            .flushed_lsn = 0,
            .buffer = .empty,
            .current_segment_id = 1,
            .current_segment_file = null,
            .current_segment_offset = 0,
            .segment_max_size = DEFAULT_SEGMENT_MAX_SIZE,
            .next_segment_id = 2,
        };
    }

    /// Build the full path for a segment file.
    fn segmentPath(self: *Self, segment_id: u64) WalError![]const u8 {
        var name_buf: [SEGMENT_NAME_LEN]u8 = undefined;
        const name = formatSegmentName(&name_buf, segment_id);
        return std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.wal_dir, name }) catch return WalError.OutOfMemory;
    }

    /// Build the full path for an archived segment.
    fn archiveSegmentPath(self: *Self, segment_id: u64) WalError![]const u8 {
        var name_buf: [SEGMENT_NAME_LEN]u8 = undefined;
        const name = formatSegmentName(&name_buf, segment_id);
        return std.fmt.allocPrint(self.allocator, "{s}/archive/{s}", .{ self.wal_dir, name }) catch return WalError.OutOfMemory;
    }

    /// Open the WAL directory. Scans for existing segments and resumes
    /// from the latest one (for recovery). Creates segment 1 if none exist.
    pub fn open(self: *Self) WalError!void {
        // Ensure the directory exists
        Dir.createDir(.cwd(), self.io(), self.wal_dir, .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return WalError.WriteError,
        };

        // Scan for existing segments
        const segments = try self.listSegments();
        defer self.allocator.free(segments);

        if (segments.len == 0) {
            // No segments — create the first one
            try self.openSegment(1);
            self.current_segment_id = 1;
            self.next_segment_id = 2;
            self.current_lsn = 1;
            self.flushed_lsn = 0;
            return;
        }

        // Find the highest segment ID
        var max_seg: u64 = 0;
        for (segments) |seg_id| {
            if (seg_id > max_seg) max_seg = seg_id;
        }

        // Scan all segments to find current LSN
        try self.scanAllSegments(segments);

        // Open the last segment for appending
        try self.openSegment(max_seg);
        self.current_segment_id = max_seg;
        self.next_segment_id = max_seg + 1;

        // Get the size of the current segment to set offset
        if (self.current_segment_file) |f| {
            const stat = f.stat(self.io()) catch return WalError.ReadError;
            self.current_segment_offset = stat.size;
        }
    }

    /// Open (or create) a segment file for writing.
    fn openSegment(self: *Self, segment_id: u64) WalError!void {
        const path = try self.segmentPath(segment_id);
        defer self.allocator.free(path);

        self.current_segment_file = Dir.openFile(.cwd(), self.io(), path, .{ .mode = .read_write }) catch |err| blk: {
            if (err == error.FileNotFound) {
                break :blk Dir.createFile(.cwd(), self.io(), path, .{
                    .read = true,
                    .truncate = false,
                }) catch return WalError.WriteError;
            }
            return WalError.WriteError;
        };
        self.current_segment_offset = 0;
    }

    /// Close the current segment file.
    fn closeCurrentSegment(self: *Self) void {
        if (self.current_segment_file) |f| {
            f.close(self.io());
            self.current_segment_file = null;
        }
    }

    /// Close the WAL (flush + close current segment).
    pub fn close(self: *Self) void {
        self.flush() catch {};
        self.closeCurrentSegment();
    }

    /// Deinitialize the WAL.
    pub fn deinit(self: *Self) void {
        self.close();
        self.buffer.deinit(self.allocator);
        self.threaded.deinit();
    }

    /// Rotate to a new segment — finalize the current one and create the next.
    fn rotateSegment(self: *Self) WalError!void {
        // Flush remaining buffer to current segment
        try self.flush();

        // Close current segment
        self.closeCurrentSegment();

        // Create next segment
        const new_id = self.next_segment_id;
        self.next_segment_id += 1;
        try self.openSegment(new_id);
        self.current_segment_id = new_id;
        self.current_segment_offset = 0;
    }

    /// Append a BEGIN record
    pub fn logBegin(self: *Self, txn_id: TxnId) WalError!Lsn {
        return self.appendRecord(txn_id, INVALID_LSN, .begin, &.{});
    }

    /// Append a COMMIT record
    pub fn logCommit(self: *Self, txn_id: TxnId, prev_lsn: Lsn) WalError!Lsn {
        return self.appendRecord(txn_id, prev_lsn, .commit, &.{});
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
        const data_len = UpdateRecordData.HEADER_SIZE + old_data.len + new_data.len;
        const data_buf = self.allocator.alloc(u8, data_len) catch return WalError.OutOfMemory;
        defer self.allocator.free(data_buf);

        const update = UpdateRecordData{
            .page_id = page_id,
            .offset = offset,
            .length = @intCast(old_data.len),
        };

        _ = update.serialize(old_data, new_data, data_buf) catch return WalError.OutOfMemory;

        return self.appendRecord(txn_id, prev_lsn, .update, data_buf);
    }

    /// Append a CHECKPOINT record
    pub fn logCheckpoint(self: *Self) WalError!Lsn {
        const lsn = try self.appendRecord(INVALID_TXN_ID, INVALID_LSN, .checkpoint, &.{});
        try self.flush();
        return lsn;
    }

    /// Append a record to the log.
    fn appendRecord(self: *Self, txn_id: TxnId, prev_lsn: Lsn, record_type: LogRecordType, data: []const u8) WalError!Lsn {
        _ = self.current_segment_file orelse return WalError.FileNotOpen;

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

        self.buffer.appendSlice(self.allocator, std.mem.asBytes(&header)) catch return WalError.OutOfMemory;

        if (data.len > 0) {
            self.buffer.appendSlice(self.allocator, data) catch return WalError.OutOfMemory;
        }

        // Flush if buffer is large enough
        if (self.buffer.items.len >= BUFFER_SIZE) {
            try self.flush();
        }

        // Check if current segment is full — rotate after flush
        if (self.current_segment_offset + self.buffer.items.len >= self.segment_max_size) {
            try self.rotateSegment();
        }

        return lsn;
    }

    /// Flush the log buffer to disk (current segment).
    pub fn flush(self: *Self) WalError!void {
        const f = self.current_segment_file orelse return WalError.FileNotOpen;

        if (self.buffer.items.len == 0) return;

        f.writePositionalAll(self.io(), self.buffer.items, self.current_segment_offset) catch return WalError.WriteError;
        f.sync(self.io()) catch return WalError.FlushError;

        self.current_segment_offset += self.buffer.items.len;
        self.flushed_lsn = self.current_lsn - 1;
        self.buffer.clearRetainingCapacity();
    }

    /// List all segment IDs in the WAL directory (sorted ascending).
    fn listSegments(self: *Self) WalError![]u64 {
        var result: std.ArrayList(u64) = .empty;
        errdefer result.deinit(self.allocator);

        // Open the WAL directory and iterate entries
        var dir = Dir.openDir(.cwd(), self.io(), self.wal_dir, .{ .iterate = true }) catch return WalError.ReadError;
        defer dir.close(self.io());

        var iter = dir.iterateAssumeFirstIteration();
        while (iter.next(self.io()) catch null) |entry| {
            const name = entry.name;
            // Match pattern: 12 digits + ".wal"
            if (name.len == SEGMENT_NAME_LEN and std.mem.endsWith(u8, name, ".wal")) {
                const digit_part = name[0..12];
                const seg_id = std.fmt.parseInt(u64, digit_part, 10) catch continue;
                if (seg_id > 0) {
                    result.append(self.allocator, seg_id) catch return WalError.OutOfMemory;
                }
            }
        }

        // Sort ascending
        const items = result.items;
        std.mem.sort(u64, items, {}, std.sort.asc(u64));

        // Return the owned slice — caller frees with allocator.free()
        // We need to return just the items, not the ArrayList
        const owned = self.allocator.dupe(u64, items) catch return WalError.OutOfMemory;
        result.deinit(self.allocator);
        return owned;
    }

    /// Scan all segments to find the current LSN.
    fn scanAllSegments(self: *Self, segment_ids: []const u64) WalError!void {
        var last_valid_lsn: Lsn = 0;

        for (segment_ids) |seg_id| {
            const path = try self.segmentPath(seg_id);
            defer self.allocator.free(path);

            const f = Dir.openFile(.cwd(), self.io(), path, .{ .mode = .read_only }) catch continue;
            defer f.close(self.io());

            const stat = f.stat(self.io()) catch continue;
            if (stat.size == 0) continue;

            var offset: u64 = 0;
            var header_buf: [LogRecordHeader.SIZE]u8 = undefined;

            while (offset + LogRecordHeader.SIZE <= stat.size) {
                const header_bytes = f.readPositionalAll(self.io(), &header_buf, offset) catch break;
                if (header_bytes != LogRecordHeader.SIZE) break;

                const header: *const LogRecordHeader = @ptrCast(@alignCast(&header_buf));

                if (@intFromEnum(header.record_type) == 0 or @intFromEnum(header.record_type) > 5) break;

                var data_buf: []u8 = &.{};
                if (header.data_len > 0) {
                    data_buf = self.allocator.alloc(u8, header.data_len) catch break;
                    defer self.allocator.free(data_buf);

                    const data_bytes = f.readPositionalAll(self.io(), data_buf, offset + LogRecordHeader.SIZE) catch break;
                    if (data_bytes != header.data_len) break;
                }

                if (!header.verify(data_buf)) break;

                last_valid_lsn = header.lsn;
                offset += LogRecordHeader.SIZE + header.data_len;
            }
        }

        self.current_lsn = last_valid_lsn + 1;
        self.flushed_lsn = last_valid_lsn;
    }

    /// Get the current (next) LSN
    pub fn getCurrentLsn(self: *const Self) Lsn {
        return self.current_lsn;
    }

    /// Get the last flushed LSN
    pub fn getFlushedLsn(self: *const Self) Lsn {
        return self.flushed_lsn;
    }

    /// Get the current segment ID
    pub fn getCurrentSegmentId(self: *const Self) u64 {
        return self.current_segment_id;
    }

    /// Iterator for reading log records across all segments.
    pub const LogIterator = struct {
        allocator: std.mem.Allocator,
        wal_dir: []const u8,
        threaded: *Threaded,
        segment_ids: []const u64,
        current_seg_idx: usize,
        current_file: ?File,
        current_file_size: u64,
        offset: u64,

        fn iterIo(self: *LogIterator) Io {
            return self.threaded.io();
        }

        pub fn next(self: *LogIterator) WalError!?struct { header: LogRecordHeader, data: []const u8 } {
            while (true) {
                // Open current segment if needed
                if (self.current_file == null) {
                    if (self.current_seg_idx >= self.segment_ids.len) return null;
                    const seg_id = self.segment_ids[self.current_seg_idx];
                    var name_buf: [SEGMENT_NAME_LEN]u8 = undefined;
                    const name = formatSegmentName(&name_buf, seg_id);
                    const path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.wal_dir, name }) catch return WalError.OutOfMemory;
                    defer self.allocator.free(path);

                    self.current_file = Dir.openFile(.cwd(), self.iterIo(), path, .{ .mode = .read_only }) catch return WalError.ReadError;
                    const stat = self.current_file.?.stat(self.iterIo()) catch return WalError.ReadError;
                    self.current_file_size = stat.size;
                    self.offset = 0;
                }

                const f = self.current_file.?;

                // Check if we've exhausted this segment
                if (self.offset + LogRecordHeader.SIZE > self.current_file_size) {
                    f.close(self.iterIo());
                    self.current_file = null;
                    self.current_seg_idx += 1;
                    continue;
                }

                // Read header
                var header_buf: [LogRecordHeader.SIZE]u8 = undefined;
                const header_bytes = f.readPositionalAll(self.iterIo(), &header_buf, self.offset) catch return WalError.ReadError;
                if (header_bytes != LogRecordHeader.SIZE) {
                    // Partial header — move to next segment
                    f.close(self.iterIo());
                    self.current_file = null;
                    self.current_seg_idx += 1;
                    continue;
                }

                const header: LogRecordHeader = @bitCast(header_buf);

                // Validate record type
                if (@intFromEnum(header.record_type) == 0 or @intFromEnum(header.record_type) > 5) {
                    // Corrupt — stop this segment
                    f.close(self.iterIo());
                    self.current_file = null;
                    self.current_seg_idx += 1;
                    continue;
                }

                // Read data
                var data: []u8 = &.{};
                if (header.data_len > 0) {
                    data = self.allocator.alloc(u8, header.data_len) catch return WalError.OutOfMemory;
                    const data_bytes = f.readPositionalAll(self.iterIo(), data, self.offset + LogRecordHeader.SIZE) catch {
                        self.allocator.free(data);
                        return WalError.ReadError;
                    };
                    if (data_bytes != header.data_len) {
                        self.allocator.free(data);
                        // Incomplete record — move to next segment
                        f.close(self.iterIo());
                        self.current_file = null;
                        self.current_seg_idx += 1;
                        continue;
                    }
                }

                // Verify checksum
                if (!header.verify(data)) {
                    if (data.len > 0) self.allocator.free(data);
                    // Corrupt — move to next segment
                    f.close(self.iterIo());
                    self.current_file = null;
                    self.current_seg_idx += 1;
                    continue;
                }

                self.offset += LogRecordHeader.SIZE + header.data_len;
                return .{ .header = header, .data = data };
            }
        }

        /// Free data returned by next()
        pub fn freeData(self: *LogIterator, data: []const u8) void {
            if (data.len > 0) {
                self.allocator.free(@constCast(data));
            }
        }

        /// Close the iterator (release open file handle).
        pub fn close(self: *LogIterator) void {
            if (self.current_file) |f| {
                f.close(self.iterIo());
                self.current_file = null;
            }
        }
    };

    /// Create an iterator to read all log records across segments.
    pub fn iterator(self: *Self) WalError!LogIterator {
        // Flush buffer first
        try self.flush();

        const segments = try self.listSegments();

        return .{
            .allocator = self.allocator,
            .wal_dir = self.wal_dir,
            .threaded = &self.threaded,
            .segment_ids = segments,
            .current_seg_idx = 0,
            .current_file = null,
            .current_file_size = 0,
            .offset = 0,
        };
    }

    /// Free resources held by an iterator (segment list).
    pub fn freeIterator(self: *Self, iter: *LogIterator) void {
        iter.close();
        self.allocator.free(iter.segment_ids);
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
        try self.flush();

        // === Analysis pass ===
        var committed_txns = std.AutoHashMap(TxnId, void).init(self.allocator);
        defer committed_txns.deinit();
        var active_txns = std.AutoHashMap(TxnId, void).init(self.allocator);
        defer active_txns.deinit();

        var iter = try self.iterator();
        defer self.freeIterator(&iter);
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

        // === Redo pass ===
        var redo_iter = try self.iterator();
        defer self.freeIterator(&redo_iter);
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

        // === Undo pass ===
        const UndoEntry = struct { page_id: PageId, offset: u16, old_data: []const u8 };
        var undo_records: std.ArrayList(UndoEntry) = .empty;
        defer {
            for (undo_records.items) |rec| {
                self.allocator.free(@constCast(rec.old_data));
            }
            undo_records.deinit(self.allocator);
        }

        var undo_scan = try self.iterator();
        defer self.freeIterator(&undo_scan);
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

    /// Archive completed segments older than `keep_segment_id`.
    /// Moves segment files to the `archive/` subdirectory.
    pub fn archiveOldSegments(self: *Self, keep_segment_id: u64) WalError!u32 {
        // Ensure archive dir exists
        const archive_dir = std.fmt.allocPrint(self.allocator, "{s}/archive", .{self.wal_dir}) catch return WalError.OutOfMemory;
        defer self.allocator.free(archive_dir);
        Dir.createDir(.cwd(), self.io(), archive_dir, .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return WalError.WriteError,
        };

        const segments = try self.listSegments();
        defer self.allocator.free(segments);

        var archived: u32 = 0;
        for (segments) |seg_id| {
            if (seg_id >= keep_segment_id) continue;
            // Don't archive the current segment
            if (seg_id == self.current_segment_id) continue;

            const src_path = try self.segmentPath(seg_id);
            defer self.allocator.free(src_path);
            const dst_path = try self.archiveSegmentPath(seg_id);
            defer self.allocator.free(dst_path);

            // Rename src to dst (move to archive)
            Dir.rename(.cwd(), src_path, .cwd(), dst_path, self.io()) catch continue;
            archived += 1;
        }

        return archived;
    }

    /// Delete all WAL segment files and archive (for testing).
    pub fn deleteFiles(self: *Self) void {
        self.close();

        // Delete active segments
        const segments = self.listSegments() catch &.{};
        if (segments.len > 0) {
            for (segments) |seg_id| {
                const path = self.segmentPath(seg_id) catch continue;
                Dir.deleteFile(.cwd(), self.io(), path) catch {};
                self.allocator.free(path);
            }
            self.allocator.free(segments);
        }

        // Delete archive segments
        const archive_dir = std.fmt.allocPrint(self.allocator, "{s}/archive", .{self.wal_dir}) catch return;
        defer self.allocator.free(archive_dir);
        // Try to delete archive dir (may not exist)
        Dir.deleteDir(.cwd(), self.io(), archive_dir) catch {};

        // Delete the WAL dir itself
        Dir.deleteDir(.cwd(), self.io(), self.wal_dir) catch {};
    }

    /// Delete the WAL file (legacy compat name — calls deleteFiles)
    pub fn deleteFile(self: *Self) void {
        self.deleteFiles();
    }
};

// Tests
const disk_manager_mod = @import("disk_manager.zig");
const DiskManager = disk_manager_mod.DiskManager;

test "wal basic operations" {
    const test_dir = "test_wal_basic_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();

    try wal.open();

    const txn_id: TxnId = 1;
    const begin_lsn = try wal.logBegin(txn_id);
    try std.testing.expectEqual(@as(Lsn, 1), begin_lsn);

    const commit_lsn = try wal.logCommit(txn_id, begin_lsn);
    try std.testing.expectEqual(@as(Lsn, 2), commit_lsn);

    try wal.flush();
    try std.testing.expectEqual(@as(Lsn, 2), wal.getFlushedLsn());
}

test "wal update records" {
    const test_dir = "test_wal_update_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();

    try wal.open();

    const txn_id: TxnId = 1;
    const begin_lsn = try wal.logBegin(txn_id);

    const old_data = "old value";
    const new_data = "new value";
    const update_lsn = try wal.logUpdate(txn_id, begin_lsn, 42, 100, old_data, new_data);

    try std.testing.expectEqual(@as(Lsn, 2), update_lsn);

    _ = try wal.logCommit(txn_id, update_lsn);
}

test "wal persistence and recovery" {
    const test_dir = "test_wal_persist_dir";

    // First session
    {
        var wal = Wal.init(std.testing.allocator, test_dir);
        defer wal.deinit();

        try wal.open();

        _ = try wal.logBegin(1);
        _ = try wal.logCommit(1, 1);
        _ = try wal.logBegin(2);
        _ = try wal.logCommit(2, 3);

        try wal.flush();
        try std.testing.expectEqual(@as(Lsn, 4), wal.getFlushedLsn());
    }

    // Second session — should recover state
    {
        var wal = Wal.init(std.testing.allocator, test_dir);
        defer wal.deleteFiles();
        defer wal.deinit();

        try wal.open();

        try std.testing.expectEqual(@as(Lsn, 5), wal.getCurrentLsn());
        try std.testing.expectEqual(@as(Lsn, 4), wal.getFlushedLsn());
    }
}

test "wal iterator" {
    const test_dir = "test_wal_iterator_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();

    try wal.open();

    _ = try wal.logBegin(1);
    _ = try wal.logUpdate(1, 1, 10, 0, "old", "new");
    _ = try wal.logCommit(1, 2);

    var iter = try wal.iterator();
    defer wal.freeIterator(&iter);
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
    const test_dir = "test_wal_checkpoint_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();

    try wal.open();

    _ = try wal.logBegin(1);
    _ = try wal.logCommit(1, 1);

    const cp_lsn = try wal.logCheckpoint();
    try std.testing.expectEqual(@as(Lsn, 3), cp_lsn);

    try std.testing.expectEqual(@as(Lsn, 3), wal.getFlushedLsn());
}

test "wal multiple transactions" {
    const test_dir = "test_wal_multi_txn_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();

    try wal.open();

    const t1_begin = try wal.logBegin(1);
    const t2_begin = try wal.logBegin(2);
    const t1_update = try wal.logUpdate(1, t1_begin, 1, 0, "a", "b");
    const t2_update = try wal.logUpdate(2, t2_begin, 2, 0, "c", "d");
    _ = try wal.logCommit(1, t1_update);
    _ = try wal.logAbort(2, t2_update);

    try wal.flush();
    try std.testing.expectEqual(@as(Lsn, 6), wal.getFlushedLsn());

    var iter = try wal.iterator();
    defer wal.freeIterator(&iter);
    var count: usize = 0;
    while (try iter.next()) |record| {
        defer iter.freeData(record.data);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 6), count);
}

test "wal segment rotation" {
    const test_dir = "test_wal_rotation_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();

    // Use a tiny segment size to force rotation
    wal.segment_max_size = 128; // 128 bytes — a single record + header is ~40+ bytes

    try wal.open();

    try std.testing.expectEqual(@as(u64, 1), wal.getCurrentSegmentId());

    // Write enough records to trigger rotation
    _ = try wal.logBegin(1);
    _ = try wal.logUpdate(1, 1, 10, 0, "old_data_here!", "new_data_here!");
    _ = try wal.logUpdate(1, 2, 11, 0, "more_old_data!", "more_new_data!");
    _ = try wal.logCommit(1, 3);

    // Should have rotated to a new segment
    try std.testing.expect(wal.getCurrentSegmentId() > 1);

    // All records should still be readable via iterator
    var iter = try wal.iterator();
    defer wal.freeIterator(&iter);
    var count: usize = 0;
    while (try iter.next()) |record| {
        defer iter.freeData(record.data);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 4), count);
}

test "wal segment rotation persistence" {
    const test_dir = "test_wal_rot_persist_dir";

    // First session — write across multiple segments
    {
        var wal = Wal.init(std.testing.allocator, test_dir);
        defer wal.deinit();
        wal.segment_max_size = 128;
        try wal.open();

        _ = try wal.logBegin(1);
        _ = try wal.logUpdate(1, 1, 10, 0, "old_data_here!", "new_data_here!");
        _ = try wal.logUpdate(1, 2, 11, 0, "more_old_data!", "more_new_data!");
        _ = try wal.logCommit(1, 3);
    }

    // Second session — should recover all records across segments
    {
        var wal = Wal.init(std.testing.allocator, test_dir);
        defer wal.deleteFiles();
        defer wal.deinit();
        try wal.open();

        try std.testing.expectEqual(@as(Lsn, 5), wal.getCurrentLsn());

        var iter = try wal.iterator();
        defer wal.freeIterator(&iter);
        var count: usize = 0;
        while (try iter.next()) |record| {
            defer iter.freeData(record.data);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 4), count);
    }
}

test "wal archive old segments" {
    const test_dir = "test_wal_archive_dir";

    var wal = Wal.init(std.testing.allocator, test_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    wal.segment_max_size = 128;
    try wal.open();

    // Write enough to create multiple segments
    _ = try wal.logBegin(1);
    _ = try wal.logUpdate(1, 1, 10, 0, "old_data_here!", "new_data_here!");
    _ = try wal.logUpdate(1, 2, 11, 0, "more_old_data!", "more_new_data!");
    _ = try wal.logCommit(1, 3);
    _ = try wal.logBegin(2);
    _ = try wal.logUpdate(2, 5, 12, 0, "aaaa_old_data!", "aaaa_new_data!");
    _ = try wal.logCommit(2, 6);

    const current_seg = wal.getCurrentSegmentId();
    try std.testing.expect(current_seg > 1);

    // Archive all segments before the current one
    const archived = try wal.archiveOldSegments(current_seg);
    try std.testing.expect(archived > 0);

    // Only current segment should remain in active list
    const remaining = try wal.listSegments();
    defer std.testing.allocator.free(remaining);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqual(current_seg, remaining[0]);
}

test "recovery redo committed" {
    const db_file = "test_recovery_redo.db";
    const wal_dir = "test_recovery_redo_wal";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const result = bp.newPage() catch unreachable;
    const page_id = result.page_id;

    const old_data = "AAAA";
    const new_data = "BBBB";
    @memcpy(result.page.data[100..104], old_data);
    bp.unpinPage(page_id, true) catch {};

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    const begin_lsn = try wal.logBegin(1);
    const update_lsn = try wal.logUpdate(1, begin_lsn, page_id, 100, old_data, new_data);
    _ = try wal.logCommit(1, update_lsn);

    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[100..104], old_data);
    bp.unpinPage(page_id, true) catch {};

    const recovery_result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), recovery_result.committed_count);
    try std.testing.expectEqual(@as(u32, 0), recovery_result.aborted_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, new_data, pg2.data[100..104]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery undo active" {
    const db_file = "test_recovery_undo.db";
    const wal_dir = "test_recovery_undo_wal";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const result = bp.newPage() catch unreachable;
    const page_id = result.page_id;

    const old_data = "XXXX";
    const new_data = "YYYY";
    @memcpy(result.page.data[200..204], old_data);
    bp.unpinPage(page_id, true) catch {};

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    const begin_lsn = try wal.logBegin(1);
    _ = try wal.logUpdate(1, begin_lsn, page_id, 200, old_data, new_data);

    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[200..204], new_data);
    bp.unpinPage(page_id, true) catch {};

    const recovery_result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 0), recovery_result.committed_count);
    try std.testing.expectEqual(@as(u32, 1), recovery_result.aborted_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, old_data, pg2.data[200..204]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery mixed committed and active" {
    const db_file = "test_recovery_mixed.db";
    const wal_dir = "test_recovery_mixed_wal";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    const result = bp.newPage() catch unreachable;
    const page_id = result.page_id;

    @memcpy(result.page.data[100..104], "1111");
    @memcpy(result.page.data[200..204], "aaaa");
    bp.unpinPage(page_id, true) catch {};

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    const t1_begin = try wal.logBegin(1);
    const t1_update = try wal.logUpdate(1, t1_begin, page_id, 100, "1111", "2222");
    _ = try wal.logCommit(1, t1_update);

    const t2_begin = try wal.logBegin(2);
    _ = try wal.logUpdate(2, t2_begin, page_id, 200, "aaaa", "bbbb");

    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[100..104], "1111");
    @memcpy(pg.data[200..204], "bbbb");
    bp.unpinPage(page_id, true) catch {};

    const recovery_result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), recovery_result.committed_count);
    try std.testing.expectEqual(@as(u32, 1), recovery_result.aborted_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "2222", pg2.data[100..104]);
    try std.testing.expectEqualSlices(u8, "aaaa", pg2.data[200..204]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery empty WAL" {
    const db_file = "test_recovery_empty.db";
    const wal_dir = "test_recovery_empty_wal";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 0), result.committed_count);
    try std.testing.expectEqual(@as(u32, 0), result.aborted_count);
}

test "recovery idempotent run twice" {
    const db_file = "test_recovery_idempotent.db";
    const wal_dir = "test_recovery_idempotent_wal";

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

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    const begin_lsn = try wal.logBegin(1);
    const update_lsn = try wal.logUpdate(1, begin_lsn, page_id, 50, "AAAA", "BBBB");
    _ = try wal.logCommit(1, update_lsn);

    const r1 = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), r1.committed_count);

    var pg1 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "BBBB", pg1.data[50..54]);
    bp.unpinPage(page_id, false) catch {};

    const r2 = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), r2.committed_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "BBBB", pg2.data[50..54]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery all active no commits" {
    const db_file = "test_recovery_all_active.db";
    const wal_dir = "test_recovery_all_active_wal";

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

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    const b1 = try wal.logBegin(1);
    _ = try wal.logUpdate(1, b1, page_id, 0, "ORIG", "AAA1");
    const b2 = try wal.logBegin(2);
    _ = try wal.logUpdate(2, b2, page_id, 4, "ORIG", "BBB2");

    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[0..4], "AAA1");
    @memcpy(pg.data[4..8], "BBB2");
    bp.unpinPage(page_id, true) catch {};

    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 0), result.committed_count);
    try std.testing.expectEqual(@as(u32, 2), result.aborted_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "ORIG", pg2.data[0..4]);
    try std.testing.expectEqualSlices(u8, "ORIG", pg2.data[4..8]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery multiple updates same page" {
    const db_file = "test_recovery_multi_update.db";
    const wal_dir = "test_recovery_multi_update_wal";

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

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    const b = try wal.logBegin(1);
    const upd1 = try wal.logUpdate(1, b, page_id, 100, "AAAA", "BBBB");
    const upd2 = try wal.logUpdate(1, upd1, page_id, 100, "BBBB", "CCCC");
    const upd3 = try wal.logUpdate(1, upd2, page_id, 100, "CCCC", "DDDD");
    _ = try wal.logCommit(1, upd3);

    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[100..104], "AAAA");
    bp.unpinPage(page_id, true) catch {};

    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), result.committed_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "DDDD", pg2.data[100..104]);
    bp.unpinPage(page_id, false) catch {};
}

test "recovery abort with no updates" {
    const db_file = "test_recovery_abort_noop.db";
    const wal_dir = "test_recovery_abort_noop_wal";

    var dm = DiskManager.init(std.testing.allocator, db_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 20);
    defer bp.deinit();

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    try wal.open();

    _ = try wal.logBegin(1);

    _ = try wal.logBegin(2);
    _ = try wal.logCommit(2, 0);

    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), result.committed_count);
    try std.testing.expectEqual(@as(u32, 1), result.aborted_count);
}

test "recovery across multiple segments" {
    const db_file = "test_recovery_multi_seg.db";
    const wal_dir = "test_recovery_multi_seg_wal";

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

    var wal = Wal.init(std.testing.allocator, wal_dir);
    defer wal.deleteFiles();
    defer wal.deinit();
    wal.segment_max_size = 128; // Tiny segments
    try wal.open();

    // Write a committed txn — updates should span multiple segments
    const b = try wal.logBegin(1);
    const upd1 = try wal.logUpdate(1, b, page_id, 100, "AAAA", "BBBB");
    const upd2 = try wal.logUpdate(1, upd1, page_id, 100, "BBBB", "CCCC");
    _ = try wal.logCommit(1, upd2);

    // Should have rotated past segment 1
    try std.testing.expect(wal.getCurrentSegmentId() > 1);

    // Simulate crash
    var pg = bp.fetchPage(page_id) catch unreachable;
    @memcpy(pg.data[100..104], "AAAA");
    bp.unpinPage(page_id, true) catch {};

    // Recovery should replay from all segments
    const result = try wal.recover(&bp);
    try std.testing.expectEqual(@as(u32, 1), result.committed_count);

    var pg2 = bp.fetchPage(page_id) catch unreachable;
    try std.testing.expectEqualSlices(u8, "CCCC", pg2.data[100..104]);
    bp.unpinPage(page_id, false) catch {};
}
