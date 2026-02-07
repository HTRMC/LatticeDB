const std = @import("std");
const page = @import("page.zig");

const PageId = page.PageId;
const PAGE_SIZE = page.PAGE_SIZE;
const INVALID_PAGE_ID = page.INVALID_PAGE_ID;

const Io = std.Io;
const File = Io.File;
const Dir = Io.Dir;
const Threaded = Io.Threaded;
const process = std.process;

pub const DiskManagerError = error{
    FileNotOpen,
    InvalidPageId,
    ReadError,
    WriteError,
    SeekError,
    FlushError,
    FileCreateError,
    OutOfMemory,
    IoInitError,
};

/// Manages reading and writing pages to/from disk
pub const DiskManager = struct {
    file: ?File,
    file_path: []const u8,
    num_pages: PageId,
    allocator: std.mem.Allocator,
    threaded: Threaded,

    const Self = @This();

    /// Get the Io interface - must be called after struct is in final location
    fn io(self: *Self) Io {
        return self.threaded.io();
    }

    /// Create a new disk manager for the given file path
    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) Self {
        return .{
            .file = null,
            .file_path = file_path,
            .num_pages = 0,
            .allocator = allocator,
            .threaded = Threaded.init(allocator, .{ .environ = process.Environ.empty }),
        };
    }

    /// Open or create the database file
    pub fn open(self: *Self) DiskManagerError!void {
        // Try to open existing file first
        self.file = Dir.openFile(.cwd(), self.io(), self.file_path, .{ .mode = .read_write }) catch |err| {
            if (err == error.FileNotFound) {
                // Create new file with read access
                self.file = Dir.createFile(.cwd(), self.io(), self.file_path, .{
                    .read = true,
                    .truncate = false,
                }) catch {
                    return DiskManagerError.FileCreateError;
                };
                self.num_pages = 0;
                return;
            }
            return DiskManagerError.FileCreateError;
        };

        // Calculate number of pages from file size
        const stat = self.file.?.stat(self.io()) catch {
            return DiskManagerError.ReadError;
        };
        self.num_pages = @intCast(stat.size / PAGE_SIZE);
    }

    /// Close the database file
    pub fn close(self: *Self) void {
        if (self.file) |f| {
            f.close(self.io());
            self.file = null;
        }
    }

    /// Deinitialize the disk manager
    pub fn deinit(self: *Self) void {
        self.close();
        self.threaded.deinit();
    }

    /// Read a page from disk into the provided buffer
    pub fn readPage(self: *Self, page_id: PageId, buffer: *align(8) [PAGE_SIZE]u8) DiskManagerError!void {
        const f = self.file orelse return DiskManagerError.FileNotOpen;

        if (page_id >= self.num_pages) {
            return DiskManagerError.InvalidPageId;
        }

        const offset: u64 = @as(u64, page_id) * PAGE_SIZE;

        // Use readPositionalAll to read at specific offset
        const bytes_read = f.readPositionalAll(self.io(), buffer, offset) catch {
            return DiskManagerError.ReadError;
        };

        if (bytes_read != PAGE_SIZE) {
            return DiskManagerError.ReadError;
        }
    }

    /// Write a page to disk from the provided buffer
    pub fn writePage(self: *Self, page_id: PageId, buffer: *const [PAGE_SIZE]u8) DiskManagerError!void {
        const f = self.file orelse return DiskManagerError.FileNotOpen;

        if (page_id == INVALID_PAGE_ID) {
            return DiskManagerError.InvalidPageId;
        }

        const offset: u64 = @as(u64, page_id) * PAGE_SIZE;

        // Use writePositionalAll to write at specific offset
        f.writePositionalAll(self.io(), buffer, offset) catch {
            return DiskManagerError.WriteError;
        };

        // Update page count if we extended the file
        if (page_id >= self.num_pages) {
            self.num_pages = page_id + 1;
        }
    }

    /// Allocate a new page and return its ID
    pub fn allocatePage(self: *Self) DiskManagerError!PageId {
        _ = self.file orelse return DiskManagerError.FileNotOpen;

        const new_page_id = self.num_pages;
        self.num_pages += 1;

        // Initialize the new page with zeros
        var empty_page: [PAGE_SIZE]u8 align(8) = [_]u8{0} ** PAGE_SIZE;
        try self.writePage(new_page_id, &empty_page);

        return new_page_id;
    }

    /// Allocate a contiguous extent of pages and return the first page ID
    pub fn allocateExtent(self: *Self, count: u32) DiskManagerError!PageId {
        _ = self.file orelse return DiskManagerError.FileNotOpen;

        const first = self.num_pages;
        var empty: [PAGE_SIZE]u8 align(8) = [_]u8{0} ** PAGE_SIZE;
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            try self.writePage(first + i, &empty);
        }
        // writePage already updates num_pages incrementally, so no extra addition needed
        return first;
    }

    /// Flush all pending writes to disk
    pub fn flush(self: *Self) DiskManagerError!void {
        const f = self.file orelse return DiskManagerError.FileNotOpen;
        f.sync(self.io()) catch {
            return DiskManagerError.FlushError;
        };
    }

    /// Get the number of pages in the file
    pub fn getNumPages(self: *const Self) PageId {
        return self.num_pages;
    }

    /// Check if file is open
    pub fn isOpen(self: *const Self) bool {
        return self.file != null;
    }

    /// Delete the database file (for testing)
    pub fn deleteFile(self: *Self) void {
        self.close();
        Dir.deleteFile(.cwd(), self.io(), self.file_path) catch {};
    }
};

// Tests
test "disk manager create and open file" {
    const test_file = "test_disk_manager.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }

    try dm.open();

    try std.testing.expect(dm.isOpen());
    try std.testing.expectEqual(@as(PageId, 0), dm.getNumPages());
}

test "disk manager allocate and read page" {
    const test_file = "test_disk_manager_rw.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }

    try dm.open();

    // Allocate a page
    const page_id = try dm.allocatePage();
    try std.testing.expectEqual(@as(PageId, 0), page_id);
    try std.testing.expectEqual(@as(PageId, 1), dm.getNumPages());

    // Write data to the page
    var write_buffer: [PAGE_SIZE]u8 align(8) = undefined;
    var pg = page.Page.init(&write_buffer, page_id);
    const tuple_data = "Hello, Disk!";
    _ = pg.insertTuple(tuple_data);

    try dm.writePage(page_id, &write_buffer);
    try dm.flush();

    // Read it back
    var read_buffer: [PAGE_SIZE]u8 align(8) = undefined;
    try dm.readPage(page_id, &read_buffer);

    const read_page = page.Page.fromBytes(&read_buffer);
    const retrieved = read_page.getTuple(0) orelse unreachable;
    try std.testing.expectEqualStrings(tuple_data, retrieved);
}

test "disk manager persistence" {
    const test_file = "test_disk_manager_persist.db";

    // First session: create and write
    {
        var dm = DiskManager.init(std.testing.allocator, test_file);
        defer dm.deinit();

        try dm.open();

        const page_id = try dm.allocatePage();

        var buffer: [PAGE_SIZE]u8 align(8) = undefined;
        var pg = page.Page.init(&buffer, page_id);
        _ = pg.insertTuple("Persistent data");

        try dm.writePage(page_id, &buffer);
        try dm.flush();
    }

    // Second session: reopen and read
    {
        var dm = DiskManager.init(std.testing.allocator, test_file);
        defer {
            dm.deleteFile();
            dm.deinit();
        }

        try dm.open();

        try std.testing.expectEqual(@as(PageId, 1), dm.getNumPages());

        var buffer: [PAGE_SIZE]u8 align(8) = undefined;
        try dm.readPage(0, &buffer);

        const pg = page.Page.fromBytes(&buffer);
        const retrieved = pg.getTuple(0) orelse unreachable;
        try std.testing.expectEqualStrings("Persistent data", retrieved);
    }
}

test "disk manager multiple pages" {
    const test_file = "test_disk_manager_multi.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }

    try dm.open();

    // Allocate and write multiple pages
    var i: PageId = 0;
    while (i < 5) : (i += 1) {
        const page_id = try dm.allocatePage();

        var buffer: [PAGE_SIZE]u8 align(8) = undefined;
        var pg = page.Page.init(&buffer, page_id);

        var tuple_buf: [32]u8 = undefined;
        const tuple = std.fmt.bufPrint(&tuple_buf, "Page {}", .{page_id}) catch unreachable;
        _ = pg.insertTuple(tuple);

        try dm.writePage(page_id, &buffer);
    }

    try dm.flush();
    try std.testing.expectEqual(@as(PageId, 5), dm.getNumPages());

    // Verify all pages
    i = 0;
    while (i < 5) : (i += 1) {
        var buffer: [PAGE_SIZE]u8 align(8) = undefined;
        try dm.readPage(i, &buffer);

        const pg = page.Page.fromBytes(&buffer);
        const retrieved = pg.getTuple(0) orelse unreachable;

        var expected_buf: [32]u8 = undefined;
        const expected = std.fmt.bufPrint(&expected_buf, "Page {}", .{i}) catch unreachable;
        try std.testing.expectEqualStrings(expected, retrieved);
    }
}

test "disk manager read page beyond allocated" {
    const test_file = "test_disk_manager_beyond.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }

    try dm.open();

    // Allocate one page
    _ = try dm.allocatePage();
    try std.testing.expectEqual(@as(PageId, 1), dm.getNumPages());

    // Try to read page 999 (beyond allocated)
    var buffer: [PAGE_SIZE]u8 align(8) = undefined;
    try std.testing.expectError(DiskManagerError.InvalidPageId, dm.readPage(999, &buffer));
}

test "disk manager operations on unopened file" {
    const test_file = "test_disk_manager_closed.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deinit();

    // Do NOT call dm.open()

    // All operations should fail with FileNotOpen
    var buffer: [PAGE_SIZE]u8 align(8) = undefined;
    try std.testing.expectError(DiskManagerError.FileNotOpen, dm.readPage(0, &buffer));
    try std.testing.expectError(DiskManagerError.FileNotOpen, dm.writePage(0, &buffer));
    try std.testing.expectError(DiskManagerError.FileNotOpen, dm.allocatePage());
    try std.testing.expectError(DiskManagerError.FileNotOpen, dm.flush());
}

test "disk manager getNumPages initially zero" {
    const test_file = "test_disk_manager_init_zero.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }

    try dm.open();
    try std.testing.expectEqual(@as(PageId, 0), dm.getNumPages());
    try std.testing.expect(dm.isOpen());
}

test "disk manager write to INVALID_PAGE_ID fails" {
    const test_file = "test_disk_manager_invalid.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }

    try dm.open();

    var buffer: [PAGE_SIZE]u8 align(8) = [_]u8{0} ** PAGE_SIZE;
    try std.testing.expectError(DiskManagerError.InvalidPageId, dm.writePage(page.INVALID_PAGE_ID, &buffer));
}

test "disk manager allocateExtent" {
    const test_file = "test_disk_manager_extent.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }

    try dm.open();

    // Allocate a single page first
    const p0 = try dm.allocatePage();
    try std.testing.expectEqual(@as(PageId, 0), p0);
    try std.testing.expectEqual(@as(PageId, 1), dm.getNumPages());

    // Allocate an extent of 4 pages
    const ext_start = try dm.allocateExtent(4);
    try std.testing.expectEqual(@as(PageId, 1), ext_start);
    try std.testing.expectEqual(@as(PageId, 5), dm.getNumPages());

    // All pages in the extent should be readable (zero-initialized)
    var i: PageId = ext_start;
    while (i < ext_start + 4) : (i += 1) {
        var buffer: [PAGE_SIZE]u8 align(8) = undefined;
        try dm.readPage(i, &buffer);
        // Should be all zeros
        for (buffer) |b| {
            try std.testing.expectEqual(@as(u8, 0), b);
        }
    }

    // Allocate another extent — should be contiguous after the first
    const ext2_start = try dm.allocateExtent(2);
    try std.testing.expectEqual(@as(PageId, 5), ext2_start);
    try std.testing.expectEqual(@as(PageId, 7), dm.getNumPages());
}
