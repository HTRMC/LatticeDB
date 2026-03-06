const std = @import("std");
const page_mod = @import("page.zig");
const buffer_pool_mod = @import("buffer_pool.zig");
const tuple_mod = @import("tuple.zig");
const alloc_map_mod = @import("alloc_map.zig");
const iam_mod = @import("iam.zig");

const PageId = page_mod.PageId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const PAGE_SIZE = page_mod.PAGE_SIZE;
const BufferPool = buffer_pool_mod.BufferPool;
const Schema = tuple_mod.Schema;
const Column = tuple_mod.Column;
const ColumnType = tuple_mod.ColumnType;
const Value = tuple_mod.Value;
const AllocManager = alloc_map_mod.AllocManager;
const EXTENT_SIZE = alloc_map_mod.EXTENT_SIZE;
const Iam = iam_mod.Iam;
const IamChainIterator = iam_mod.IamChainIterator;

pub const ColumnarError = error{
    BufferPoolError,
    OutOfMemory,
    PageFull,
    SerializationError,
};

/// Column page header — stored at the start of each column data page.
/// Layout: [ColPageHeader][null_bitmap][values...]
const ColPageHeader = extern struct {
    next_page: PageId, // linked list of column pages
    row_count: u16, // number of values stored
    col_type: u8, // ColumnType as u8
    _reserved: u8,

    const SIZE: usize = @sizeOf(ColPageHeader); // 8 bytes
};

/// Metadata page for a columnar table.
/// Stored in slot 0 of the first page (same pattern as Table).
/// Followed by column_count × PageId entries (first page per column).
pub const ColumnarMeta = extern struct {
    row_count: u64,
    column_count: u16,
    _reserved: [6]u8,

    const SIZE: usize = @sizeOf(ColumnarMeta); // 16 bytes
};

/// Maximum usable space per column page (after header)
const COL_PAGE_DATA: usize = PAGE_SIZE - ColPageHeader.SIZE;

/// Calculate how many rows of a fixed-width type fit in one column page.
fn fixedRowsPerPage(fixed_size: usize) usize {
    // null bitmap: ceil(rows/8) bytes, values: rows * fixed_size
    // We need: ceil(rows/8) + rows * fixed_size <= COL_PAGE_DATA
    // Approximate: rows * (fixed_size + 1/8) <= COL_PAGE_DATA
    // rows <= COL_PAGE_DATA / (fixed_size + 0.125)
    var rows: usize = COL_PAGE_DATA * 8 / (fixed_size * 8 + 1);
    // Verify the rows fit
    while (rows > 0) {
        const bitmap_bytes = (rows + 7) / 8;
        if (bitmap_bytes + rows * fixed_size <= COL_PAGE_DATA) break;
        rows -= 1;
    }
    return rows;
}

/// Columnar table — stores each column in separate page chains.
pub const ColumnarTable = struct {
    allocator: std.mem.Allocator,
    buffer_pool: *BufferPool,
    schema: *const Schema,
    alloc_manager: *AllocManager,
    /// First page ID (meta page)
    table_id: PageId,

    const Self = @This();

    /// Create a new columnar table.
    pub fn create(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        alloc_manager: *AllocManager,
    ) ColumnarError!Self {
        const col_count = schema.columns.len;

        // Allocate first extent for meta page
        const first_page = alloc_manager.allocateExtent() catch return ColumnarError.BufferPoolError;
        const table_id = first_page;

        // Initialize meta page
        {
            var pg = buffer_pool.fetchPage(table_id) catch return ColumnarError.BufferPoolError;
            pg = page_mod.Page.init(pg.data, table_id);

            // Meta + column page IDs
            const meta_size = ColumnarMeta.SIZE + col_count * @sizeOf(PageId);
            var meta_blob: [256]u8 = [_]u8{0} ** 256;
            if (meta_size > meta_blob.len) return ColumnarError.SerializationError;

            const meta = ColumnarMeta{
                .row_count = 0,
                .column_count = @intCast(col_count),
                ._reserved = .{0} ** 6,
            };
            @memcpy(meta_blob[0..ColumnarMeta.SIZE], std.mem.asBytes(&meta));

            // Allocate first page for each column
            for (0..col_count) |i| {
                const col_page = alloc_manager.allocateExtent() catch return ColumnarError.BufferPoolError;
                // Initialize column page
                {
                    var cpg = buffer_pool.fetchPage(col_page) catch return ColumnarError.BufferPoolError;
                    cpg = page_mod.Page.init(cpg.data, col_page);
                    const col_hdr = ColPageHeader{
                        .next_page = INVALID_PAGE_ID,
                        .row_count = 0,
                        .col_type = @intFromEnum(schema.columns[i].col_type),
                        ._reserved = 0,
                    };
                    @memcpy(cpg.data[0..ColPageHeader.SIZE], std.mem.asBytes(&col_hdr));
                    buffer_pool.unpinPage(col_page, true) catch {};
                }

                const offset = ColumnarMeta.SIZE + i * @sizeOf(PageId);
                @memcpy(meta_blob[offset..][0..@sizeOf(PageId)], std.mem.asBytes(&col_page));
            }

            _ = pg.insertTuple(meta_blob[0..meta_size]);
            buffer_pool.unpinPage(table_id, true) catch {};
        }

        return Self{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .schema = schema,
            .alloc_manager = alloc_manager,
            .table_id = table_id,
        };
    }

    /// Open an existing columnar table.
    pub fn open(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        table_id: PageId,
        alloc_manager: *AllocManager,
    ) Self {
        return Self{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .schema = schema,
            .alloc_manager = alloc_manager,
            .table_id = table_id,
        };
    }

    /// Read metadata from the meta page.
    fn readMeta(self: *Self) ColumnarError!ColumnarMeta {
        const pg = self.buffer_pool.fetchPage(self.table_id) catch return ColumnarError.BufferPoolError;
        defer self.buffer_pool.unpinPage(self.table_id, false) catch {};

        const slot = pg.getTuple(0) orelse return ColumnarError.SerializationError;
        if (slot.len < ColumnarMeta.SIZE) return ColumnarError.SerializationError;
        return std.mem.bytesToValue(ColumnarMeta, slot[0..ColumnarMeta.SIZE]);
    }

    /// Read the first page ID for a given column.
    fn getColumnPageId(self: *Self, col_idx: usize) ColumnarError!PageId {
        const pg = self.buffer_pool.fetchPage(self.table_id) catch return ColumnarError.BufferPoolError;
        defer self.buffer_pool.unpinPage(self.table_id, false) catch {};

        const slot = pg.getTuple(0) orelse return ColumnarError.SerializationError;
        const offset = ColumnarMeta.SIZE + col_idx * @sizeOf(PageId);
        if (slot.len < offset + @sizeOf(PageId)) return ColumnarError.SerializationError;
        return std.mem.bytesToValue(PageId, slot[offset..][0..@sizeOf(PageId)]);
    }

    /// Get total row count.
    pub fn rowCount(self: *Self) ColumnarError!u64 {
        const meta = try self.readMeta();
        return meta.row_count;
    }

    /// Insert a row (values for all columns).
    pub fn insertRow(self: *Self, values: []const Value) ColumnarError!void {
        const col_count = self.schema.columns.len;
        if (values.len != col_count) return ColumnarError.SerializationError;

        // For each column, append the value to the last page in the chain
        for (0..col_count) |ci| {
            const first_page_id = try self.getColumnPageId(ci);
            try self.appendToColumn(first_page_id, self.schema.columns[ci], values[ci]);
        }

        // Increment row count
        try self.updateRowCount(1);
    }

    /// Append a value to a column's page chain.
    fn appendToColumn(self: *Self, first_page_id: PageId, col: Column, value: Value) ColumnarError!void {
        // Walk to last page in chain
        var current_page_id = first_page_id;
        while (true) {
            const pg = self.buffer_pool.fetchPage(current_page_id) catch return ColumnarError.BufferPoolError;
            const hdr = std.mem.bytesToValue(ColPageHeader, pg.data[0..ColPageHeader.SIZE]);
            if (hdr.next_page == INVALID_PAGE_ID) {
                // This is the last page — try to append here
                if (self.tryAppendValue(pg.data, col, value)) {
                    self.buffer_pool.unpinPage(current_page_id, true) catch {};
                    return;
                }
                // Page full — allocate new page
                self.buffer_pool.unpinPage(current_page_id, false) catch {};
                const new_page_id = self.alloc_manager.allocateExtent() catch return ColumnarError.BufferPoolError;
                // Initialize new column page
                {
                    var npg = self.buffer_pool.fetchPage(new_page_id) catch return ColumnarError.BufferPoolError;
                    npg = page_mod.Page.init(npg.data, new_page_id);
                    const new_hdr = ColPageHeader{
                        .next_page = INVALID_PAGE_ID,
                        .row_count = 0,
                        .col_type = @intFromEnum(col.col_type),
                        ._reserved = 0,
                    };
                    @memcpy(npg.data[0..ColPageHeader.SIZE], std.mem.asBytes(&new_hdr));
                    // Append value to new page
                    if (!self.tryAppendValue(npg.data, col, value)) {
                        self.buffer_pool.unpinPage(new_page_id, true) catch {};
                        return ColumnarError.SerializationError;
                    }
                    self.buffer_pool.unpinPage(new_page_id, true) catch {};
                }
                // Link old last page to new page
                {
                    var opg = self.buffer_pool.fetchPage(current_page_id) catch return ColumnarError.BufferPoolError;
                    @memcpy(opg.data[0..@sizeOf(PageId)], std.mem.asBytes(&new_page_id));
                    self.buffer_pool.unpinPage(current_page_id, true) catch {};
                }
                return;
            }
            const next = hdr.next_page;
            self.buffer_pool.unpinPage(current_page_id, false) catch {};
            current_page_id = next;
        }
    }

    /// Try to append a value to a column page. Returns true on success.
    fn tryAppendValue(_: *Self, data: []u8, col: Column, value: Value) bool {
        return appendValueToPage(data, col, value);
    }

    fn appendValueToPage(data: []u8, col: Column, value: Value) bool {
        var hdr = std.mem.bytesToValue(ColPageHeader, data[0..ColPageHeader.SIZE]);
        const row_idx = hdr.row_count;

        if (col.col_type.fixedSize()) |fixed_size| {
            // Fixed-width: [header][null_bitmap (fixed size)][values]
            const max_rows = fixedRowsPerPage(fixed_size);
            if (@as(usize, row_idx) >= max_rows) return false;
            const bitmap_bytes = (max_rows + 7) / 8;
            const values_start = ColPageHeader.SIZE + bitmap_bytes;

            // Set/clear null bit
            const byte_idx = ColPageHeader.SIZE + @as(usize, row_idx) / 8;
            const bit_idx: u3 = @intCast(@as(usize, row_idx) % 8);
            if (value == .null_value) {
                data[byte_idx] |= @as(u8, 1) << bit_idx;
            } else {
                data[byte_idx] &= ~(@as(u8, 1) << bit_idx);
                const val_offset = values_start + @as(usize, row_idx) * fixed_size;
                writeFixedValue(data[val_offset..], col.col_type, value);
            }
        } else {
            return appendVarValueToPage(data, row_idx, value);
        }

        // Increment row count
        hdr.row_count = row_idx + 1;
        @memcpy(data[0..ColPageHeader.SIZE], std.mem.asBytes(&hdr));
        return true;
    }

    fn appendVarValueToPage(data: []u8, row_idx: u16, value: Value) bool {
        // Variable-length layout:
        // [ColPageHeader][var_used:u16][null_bitmap][entry0][entry1]...
        // Each entry: [len:u16][bytes...] or null is marked in bitmap
        const var_header_offset = ColPageHeader.SIZE;
        var var_used: u16 = if (row_idx == 0)
            0
        else
            std.mem.bytesToValue(u16, data[var_header_offset..][0..2]);

        const bitmap_start = var_header_offset + 2;
        const new_bitmap_bytes = (@as(usize, row_idx) + 1 + 7) / 8;
        const data_start = bitmap_start + new_bitmap_bytes;

        const byte_idx = bitmap_start + @as(usize, row_idx) / 8;
        const bit_idx: u3 = @intCast(@as(usize, row_idx) % 8);

        if (value == .null_value) {
            data[byte_idx] |= @as(u8, 1) << bit_idx;
            // Still need to account for the offset in var_used (no data needed)
        } else {
            data[byte_idx] &= ~(@as(u8, 1) << bit_idx);
            const bytes = switch (value) {
                .bytes => |b| b,
                .uuid => |u| u,
                else => return false,
            };
            const entry_size = 2 + bytes.len;
            if (data_start + var_used + entry_size > PAGE_SIZE) return false;

            const write_offset = data_start + var_used;
            const len: u16 = @intCast(bytes.len);
            @memcpy(data[write_offset..][0..2], std.mem.asBytes(&len));
            @memcpy(data[write_offset + 2 ..][0..bytes.len], bytes);
            var_used += @intCast(entry_size);
        }

        // Update var_used
        @memcpy(data[var_header_offset..][0..2], std.mem.asBytes(&var_used));

        // Update row count in header
        var hdr = std.mem.bytesToValue(ColPageHeader, data[0..ColPageHeader.SIZE]);
        hdr.row_count = row_idx + 1;
        @memcpy(data[0..ColPageHeader.SIZE], std.mem.asBytes(&hdr));
        return true;
    }

    fn writeFixedValue(dest: []u8, col_type: ColumnType, value: Value) void {
        switch (col_type) {
            .boolean => dest[0] = if (value.boolean) 1 else 0,
            .smallint => @memcpy(dest[0..2], std.mem.asBytes(&value.smallint)),
            .integer => @memcpy(dest[0..4], std.mem.asBytes(&value.integer)),
            .bigint => @memcpy(dest[0..8], std.mem.asBytes(&value.bigint)),
            .float => @memcpy(dest[0..8], std.mem.asBytes(&value.float)),
            .date => @memcpy(dest[0..8], std.mem.asBytes(&value.date)),
            .timestamp => @memcpy(dest[0..8], std.mem.asBytes(&value.timestamp)),
            .decimal => @memcpy(dest[0..8], std.mem.asBytes(&value.decimal)),
            .uuid => @memcpy(dest[0..16], value.uuid[0..16]),
            else => {},
        }
    }

    /// Update the row count in the meta page.
    fn updateRowCount(self: *Self, delta: i64) ColumnarError!void {
        var pg = self.buffer_pool.fetchPage(self.table_id) catch return ColumnarError.BufferPoolError;
        const slot = pg.getTuple(0) orelse {
            self.buffer_pool.unpinPage(self.table_id, false) catch {};
            return ColumnarError.SerializationError;
        };
        var meta = std.mem.bytesToValue(ColumnarMeta, slot[0..ColumnarMeta.SIZE]);
        if (delta >= 0) {
            meta.row_count += @intCast(@as(u64, @intCast(delta)));
        } else {
            const abs: u64 = @intCast(-delta);
            if (meta.row_count >= abs) meta.row_count -= abs else meta.row_count = 0;
        }
        // Write back — updateTupleData writes at offset within the tuple's data
        _ = pg.updateTupleData(0, 0, std.mem.asBytes(&meta));
        self.buffer_pool.unpinPage(self.table_id, true) catch {};
    }

    /// Scan a single column, returning values in order.
    pub fn scanColumn(self: *Self, col_idx: usize) ColumnarError!ColumnScanIterator {
        const first_page_id = try self.getColumnPageId(col_idx);
        const col = self.schema.columns[col_idx];
        return ColumnScanIterator{
            .buffer_pool = self.buffer_pool,
            .current_page_id = first_page_id,
            .current_row = 0,
            .col = col,
            .allocator = self.allocator,
        };
    }

    /// Scan all columns row-by-row, returning Value slices.
    pub fn scan(self: *Self) ColumnarError!RowScanIterator {
        const col_count = self.schema.columns.len;
        const col_iters = self.allocator.alloc(ColumnScanIterator, col_count) catch return ColumnarError.OutOfMemory;
        for (0..col_count) |ci| {
            col_iters[ci] = try self.scanColumn(ci);
        }
        return RowScanIterator{
            .allocator = self.allocator,
            .schema = self.schema,
            .col_iters = col_iters,
        };
    }
};

/// Iterator that reads values from a single column's page chain.
pub const ColumnScanIterator = struct {
    buffer_pool: *BufferPool,
    current_page_id: PageId,
    current_row: u16,
    col: Column,
    allocator: std.mem.Allocator,

    // Cached page data
    page_data: ?[]u8 = null,
    page_row_count: u16 = 0,

    pub fn next(self: *ColumnScanIterator) ColumnarError!?Value {
        while (true) {
            if (self.current_page_id == INVALID_PAGE_ID) return null;

            // Load page if needed
            if (self.page_data == null) {
                const pg = self.buffer_pool.fetchPage(self.current_page_id) catch return ColumnarError.BufferPoolError;
                self.page_data = pg.data;
                const hdr = std.mem.bytesToValue(ColPageHeader, pg.data[0..ColPageHeader.SIZE]);
                self.page_row_count = hdr.row_count;
            }

            if (self.current_row >= self.page_row_count) {
                // Move to next page
                const data = self.page_data.?;
                const hdr = std.mem.bytesToValue(ColPageHeader, data[0..ColPageHeader.SIZE]);
                self.buffer_pool.unpinPage(self.current_page_id, false) catch {};
                self.current_page_id = hdr.next_page;
                self.current_row = 0;
                self.page_data = null;
                continue;
            }

            const val = try self.readValue();
            self.current_row += 1;
            return val;
        }
    }

    fn readValue(self: *ColumnScanIterator) ColumnarError!Value {
        const data = self.page_data.?;
        const row = self.current_row;

        if (self.col.col_type.fixedSize()) |fixed_size| {
            const max_rows = fixedRowsPerPage(fixed_size);
            const bitmap_bytes = (max_rows + 7) / 8;
            const values_start = ColPageHeader.SIZE + bitmap_bytes;
            const byte_idx = ColPageHeader.SIZE + @as(usize, row) / 8;
            const bit_idx: u3 = @intCast(@as(usize, row) % 8);

            if ((data[byte_idx] & (@as(u8, 1) << bit_idx)) != 0) {
                return Value{ .null_value = {} };
            }

            const val_offset = values_start + @as(usize, row) * fixed_size;
            return readFixedValue(data[val_offset..], self.col.col_type);
        } else {
            return self.readVarValue(data, row);
        }
    }

    fn readVarValue(self: *ColumnScanIterator, data: []u8, target_row: u16) ColumnarError!Value {
        const var_header_offset = ColPageHeader.SIZE;
        const bitmap_start = var_header_offset + 2;
        const bitmap_bytes = (@as(usize, self.page_row_count) + 7) / 8;
        const data_start = bitmap_start + bitmap_bytes;

        // Check null
        const byte_idx = bitmap_start + @as(usize, target_row) / 8;
        const bit_idx: u3 = @intCast(@as(usize, target_row) % 8);
        if ((data[byte_idx] & (@as(u8, 1) << bit_idx)) != 0) {
            return Value{ .null_value = {} };
        }

        // Walk entries to find the target row (skip nulls)
        var offset = data_start;
        for (0..@as(usize, target_row)) |ri| {
            const b_idx = bitmap_start + ri / 8;
            const b_bit: u3 = @intCast(ri % 8);
            if ((data[b_idx] & (@as(u8, 1) << b_bit)) == 0) {
                // Not null — skip this entry
                const len = std.mem.bytesToValue(u16, data[offset..][0..2]);
                offset += 2 + len;
            }
        }

        // Read the entry at current offset
        const len = std.mem.bytesToValue(u16, data[offset..][0..2]);
        const bytes = data[offset + 2 ..][0..len];

        if (self.col.col_type == .uuid) {
            const copy = self.allocator.dupe(u8, bytes) catch return ColumnarError.OutOfMemory;
            return Value{ .uuid = copy };
        }
        // For varchar/text/json, dupe the string
        const copy = self.allocator.dupe(u8, bytes) catch return ColumnarError.OutOfMemory;
        return Value{ .bytes = copy };
    }

    pub fn deinit(self: *ColumnScanIterator) void {
        if (self.page_data != null) {
            self.buffer_pool.unpinPage(self.current_page_id, false) catch {};
            self.page_data = null;
        }
    }
};

/// Row-oriented scan iterator over a columnar table.
pub const RowScanIterator = struct {
    allocator: std.mem.Allocator,
    schema: *const Schema,
    col_iters: []ColumnScanIterator,

    pub const Row = struct {
        values: []Value,
    };

    pub fn next(self: *RowScanIterator) ColumnarError!?Row {
        const col_count = self.col_iters.len;
        const values = self.allocator.alloc(Value, col_count) catch return ColumnarError.OutOfMemory;

        for (0..col_count) |ci| {
            const val = try self.col_iters[ci].next();
            if (val == null) {
                self.allocator.free(values);
                return null;
            }
            values[ci] = val.?;
        }

        return Row{ .values = values };
    }

    pub fn freeValues(self: *RowScanIterator, values: []Value) void {
        for (values) |v| {
            switch (v) {
                .bytes => |b| self.allocator.free(b),
                .uuid => |u| self.allocator.free(u),
                else => {},
            }
        }
        self.allocator.free(values);
    }

    pub fn deinit(self: *RowScanIterator) void {
        for (self.col_iters) |*ci| ci.deinit();
        self.allocator.free(self.col_iters);
    }
};

fn readFixedValue(data: []const u8, col_type: ColumnType) Value {
    return switch (col_type) {
        .boolean => Value{ .boolean = data[0] != 0 },
        .smallint => Value{ .smallint = std.mem.bytesToValue(i16, data[0..2]) },
        .integer => Value{ .integer = std.mem.bytesToValue(i32, data[0..4]) },
        .bigint => Value{ .bigint = std.mem.bytesToValue(i64, data[0..8]) },
        .float => Value{ .float = std.mem.bytesToValue(f64, data[0..8]) },
        .date => Value{ .date = std.mem.bytesToValue(i64, data[0..8]) },
        .timestamp => Value{ .timestamp = std.mem.bytesToValue(i64, data[0..8]) },
        .decimal => Value{ .decimal = std.mem.bytesToValue(i64, data[0..8]) },
        .uuid => Value{ .uuid = data[0..16] },
        else => Value{ .null_value = {} },
    };
}

// ── Tests ────────────────────────────────────────────────────────────
const disk_manager_mod = @import("disk_manager.zig");
const DiskManager = disk_manager_mod.DiskManager;

test "columnar table create and insert" {
    const test_file = "test_columnar_basic.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    const columns = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "name", .col_type = .varchar, .max_length = 100, .nullable = true },
        .{ .name = "score", .col_type = .float, .max_length = 0, .nullable = true },
    };
    const schema = Schema{ .columns = &columns };

    var table = try ColumnarTable.create(std.testing.allocator, &bp, &schema, &am);

    // Insert rows
    try table.insertRow(&[_]Value{ .{ .integer = 1 }, .{ .bytes = "Alice" }, .{ .float = 95.5 } });
    try table.insertRow(&[_]Value{ .{ .integer = 2 }, .{ .bytes = "Bob" }, .{ .null_value = {} } });
    try table.insertRow(&[_]Value{ .{ .integer = 3 }, .{ .bytes = "Charlie" }, .{ .float = 88.0 } });

    try std.testing.expectEqual(@as(u64, 3), try table.rowCount());

    // Scan all rows
    var iter = try table.scan();
    defer iter.deinit();

    const row1 = (try iter.next()).?;
    defer iter.freeValues(row1.values);
    try std.testing.expectEqual(@as(i32, 1), row1.values[0].integer);
    try std.testing.expectEqualStrings("Alice", row1.values[1].bytes);
    try std.testing.expectApproxEqAbs(@as(f64, 95.5), row1.values[2].float, 0.01);

    const row2 = (try iter.next()).?;
    defer iter.freeValues(row2.values);
    try std.testing.expectEqual(@as(i32, 2), row2.values[0].integer);
    try std.testing.expectEqualStrings("Bob", row2.values[1].bytes);
    try std.testing.expect(row2.values[2] == .null_value);

    const row3 = (try iter.next()).?;
    defer iter.freeValues(row3.values);
    try std.testing.expectEqual(@as(i32, 3), row3.values[0].integer);
    try std.testing.expectEqualStrings("Charlie", row3.values[1].bytes);

    // No more rows
    try std.testing.expect((try iter.next()) == null);
}

test "columnar table single column scan" {
    const test_file = "test_columnar_col_scan.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    const columns = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "value", .col_type = .bigint, .max_length = 0, .nullable = false },
    };
    const schema = Schema{ .columns = &columns };

    var table = try ColumnarTable.create(std.testing.allocator, &bp, &schema, &am);

    for (0..100) |i| {
        try table.insertRow(&[_]Value{
            .{ .integer = @intCast(i) },
            .{ .bigint = @as(i64, @intCast(i)) * 10 },
        });
    }

    // Scan only the "value" column
    var col_iter = try table.scanColumn(1);
    defer col_iter.deinit();

    var count: usize = 0;
    while (try col_iter.next()) |val| {
        try std.testing.expectEqual(@as(i64, @intCast(count)) * 10, val.bigint);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}
