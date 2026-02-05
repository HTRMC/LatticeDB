const std = @import("std");
const page_mod = @import("page.zig");
const buffer_pool_mod = @import("buffer_pool.zig");
const tuple_mod = @import("tuple.zig");

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

pub const TableError = error{
    BufferPoolError,
    TupleTooBig,
    PageFull,
    TupleNotFound,
    SerializationError,
    OutOfMemory,
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

/// Heap-organized table
/// Data is stored in an unordered collection of pages
pub const Table = struct {
    allocator: std.mem.Allocator,
    buffer_pool: *BufferPool,
    schema: *const Schema,
    /// Table ID (page ID of the first/meta page)
    table_id: PageId,

    const Self = @This();

    /// Maximum tuple size that can fit in a page (with overhead)
    pub const MAX_TUPLE_SIZE = PAGE_SIZE - page_mod.PageHeader.SIZE - page_mod.Slot.SIZE - TableMeta.SIZE - 64;

    /// Create a new table - allocates the first page
    pub fn create(allocator: std.mem.Allocator, buffer_pool: *BufferPool, schema: *const Schema) TableError!Self {
        // Allocate the first page for this table
        const result = buffer_pool.newPage() catch {
            return TableError.BufferPoolError;
        };

        const table = Self{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .schema = schema,
            .table_id = result.page_id,
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
        return .{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .schema = schema,
            .table_id = table_id,
        };
    }

    /// Insert a tuple into the table
    /// Returns the TupleId where the tuple was stored
    pub fn insertTuple(self: *Self, values: []const Value) TableError!TupleId {
        // Serialize the tuple
        var buf: [PAGE_SIZE]u8 = undefined;
        const size = Tuple.serialize(self.schema, values, &buf) catch {
            return TableError.SerializationError;
        };

        if (size > MAX_TUPLE_SIZE) {
            return TableError.TupleTooBig;
        }

        const tuple_data = buf[0..size];

        // Try to find a page with space, starting from the meta hint
        const meta = try self.readMeta();
        var page_id = meta.first_free_page;

        // Try the hinted page first, then scan
        while (page_id != INVALID_PAGE_ID) {
            var pg = self.buffer_pool.fetchPage(page_id) catch {
                return TableError.BufferPoolError;
            };

            // Skip slot 0 on the meta page (it holds TableMeta)
            const slot_id = pg.insertTuple(tuple_data);
            if (slot_id) |sid| {
                self.buffer_pool.unpinPage(page_id, true) catch {};
                try self.incrementTupleCount();
                return .{ .page_id = page_id, .slot_id = sid };
            }

            self.buffer_pool.unpinPage(page_id, false) catch {};

            // This page is full, try the next one
            if (page_id == self.table_id) {
                // We were on the meta page, check if other pages exist
                if (meta.page_count > 1) {
                    // Simple scan: try pages after meta page
                    // In a real DB we'd maintain a free page list
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

        return .{ .page_id = new_result.page_id, .slot_id = slot_id };
    }

    /// Get a tuple by its TupleId
    /// Caller must free the returned values slice
    pub fn getTuple(self: *Self, tid: TupleId) TableError!?[]Value {
        var pg = self.buffer_pool.fetchPage(tid.page_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(tid.page_id, false) catch {};

        const raw = pg.getTuple(tid.slot_id) orelse return null;

        return Tuple.deserialize(self.allocator, self.schema, raw) catch {
            return TableError.SerializationError;
        };
    }

    /// Delete a tuple by its TupleId
    pub fn deleteTuple(self: *Self, tid: TupleId) TableError!bool {
        var pg = self.buffer_pool.fetchPage(tid.page_id) catch {
            return TableError.BufferPoolError;
        };

        const result = pg.deleteTuple(tid.slot_id);

        self.buffer_pool.unpinPage(tid.page_id, result) catch {};
        return result;
    }

    /// Sequential scan - iterates over all tuples in the table
    pub const ScanIterator = struct {
        table: *Self,
        current_page: PageId,
        current_slot: SlotId,
        page_count: u32,

        /// Returns the next tuple ID and deserialized values
        /// Caller must free the returned values slice
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
                        self.current_slot = slot + 1;

                        const values = Tuple.deserialize(self.table.allocator, self.table.schema, raw) catch {
                            self.table.buffer_pool.unpinPage(self.current_page, false) catch {};
                            return TableError.SerializationError;
                        };

                        self.table.buffer_pool.unpinPage(self.current_page, false) catch {};
                        return .{ .tid = tid, .values = values };
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
        const meta = try self.readMeta();
        return .{
            .table = self,
            .current_page = self.table_id,
            .current_slot = 0,
            .page_count = meta.page_count,
        };
    }

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

// Tests
const disk_manager_mod = @import("disk_manager.zig");
const DiskManager = disk_manager_mod.DiskManager;

test "table create and insert" {
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

    // Insert a row
    const values = [_]Value{ .{ .integer = 1 }, .{ .bytes = "Alice" } };
    const tid = try table.insertTuple(&values);

    try std.testing.expectEqual(table.table_id, tid.page_id);
    try std.testing.expectEqual(@as(u64, 1), try table.tupleCount());
}

test "table insert and retrieve" {
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
    const tid = try table.insertTuple(&values);

    // Retrieve
    const result = try table.getTuple(tid) orelse unreachable;
    defer std.testing.allocator.free(result);

    try std.testing.expectEqual(@as(i32, 42), result[0].integer);
    try std.testing.expectEqualStrings("Bob", result[1].bytes);
    try std.testing.expectEqual(true, result[2].boolean);
}

test "table sequential scan" {
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
        _ = try table.insertTuple(&vals);
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

test "table delete tuple" {
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
    const tid = try table.insertTuple(&vals);

    // Delete it
    const deleted = try table.deleteTuple(tid);
    try std.testing.expect(deleted);

    // Should not be retrievable
    const result = try table.getTuple(tid);
    try std.testing.expect(result == null);
}

test "table multiple pages" {
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
        _ = try table.insertTuple(&vals);
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
