const std = @import("std");
const tuple_mod = @import("../storage/tuple.zig");

const ColumnType = tuple_mod.ColumnType;
const Value = tuple_mod.Value;

/// Number of rows processed per vectorized batch.
pub const VECTOR_SIZE: usize = 2048;

/// A selection vector tracks which rows in a DataChunk are "active" after filtering.
pub const SelectionVector = struct {
    indices: [VECTOR_SIZE]u16 = undefined,
    len: u16 = 0,
};

/// A columnar vector holding up to VECTOR_SIZE values of a single type.
/// Uses a tagged union so only one typed array is active, keeping memory ~18KB per vector.
pub const ColumnVector = struct {
    data: Data = .{ .integers = undefined },
    null_mask: [VECTOR_SIZE / 8]u8 = [_]u8{0} ** (VECTOR_SIZE / 8),
    col_type: ColumnType = .integer,

    pub const Data = union {
        booleans: [VECTOR_SIZE]bool,
        integers: [VECTOR_SIZE]i32,
        bigints: [VECTOR_SIZE]i64,
        floats: [VECTOR_SIZE]f64,
        bytes_ptrs: [VECTOR_SIZE][]const u8,
    };

    pub fn init(col_type: ColumnType) ColumnVector {
        var cv = ColumnVector{
            .col_type = col_type,
            .null_mask = [_]u8{0} ** (VECTOR_SIZE / 8),
        };
        switch (col_type) {
            .boolean => cv.data = .{ .booleans = undefined },
            .integer => cv.data = .{ .integers = undefined },
            .bigint => cv.data = .{ .bigints = undefined },
            .float => cv.data = .{ .floats = undefined },
            .varchar, .text => cv.data = .{ .bytes_ptrs = undefined },
        }
        return cv;
    }

    pub fn isNull(self: *const ColumnVector, row: u16) bool {
        return (self.null_mask[row / 8] & (@as(u8, 1) << @as(u3, @intCast(row % 8)))) != 0;
    }

    pub fn setNull(self: *ColumnVector, row: u16) void {
        self.null_mask[row / 8] |= @as(u8, 1) << @as(u3, @intCast(row % 8));
    }

    pub fn clearNull(self: *ColumnVector, row: u16) void {
        self.null_mask[row / 8] &= ~(@as(u8, 1) << @as(u3, @intCast(row % 8)));
    }

    /// Get the value at a given row as a storage Value.
    pub fn getValue(self: *const ColumnVector, row: u16) Value {
        if (self.isNull(row)) return .{ .null_value = {} };
        return switch (self.col_type) {
            .boolean => .{ .boolean = self.data.booleans[row] },
            .integer => .{ .integer = self.data.integers[row] },
            .bigint => .{ .bigint = self.data.bigints[row] },
            .float => .{ .float = self.data.floats[row] },
            .varchar, .text => .{ .bytes = self.data.bytes_ptrs[row] },
        };
    }

    /// Set a value at the given row from a storage Value.
    pub fn setValue(self: *ColumnVector, row: u16, val: Value) void {
        if (val == .null_value) {
            self.setNull(row);
            return;
        }
        self.clearNull(row);
        switch (self.col_type) {
            .boolean => self.data.booleans[row] = val.boolean,
            .integer => self.data.integers[row] = val.integer,
            .bigint => self.data.bigints[row] = val.bigint,
            .float => self.data.floats[row] = val.float,
            .varchar, .text => self.data.bytes_ptrs[row] = val.bytes,
        }
    }
};

/// A chunk of up to VECTOR_SIZE rows stored in columnar format.
/// Owns an arena allocator for string data copies (pages are unpinned after reading).
pub const DataChunk = struct {
    columns: []ColumnVector,
    count: u16 = 0,
    sel: ?SelectionVector = null,
    arena: std.heap.ArenaAllocator,
    /// Allocator that allocated the columns slice itself
    parent_allocator: std.mem.Allocator,

    pub fn initFromSchema(allocator: std.mem.Allocator, schema: *const tuple_mod.Schema) !DataChunk {
        const cols = try allocator.alloc(ColumnVector, schema.columns.len);
        for (schema.columns, 0..) |col, i| {
            cols[i] = ColumnVector.init(col.col_type);
        }
        return .{
            .columns = cols,
            .count = 0,
            .sel = null,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .parent_allocator = allocator,
        };
    }

    pub fn deinit(self: *DataChunk) void {
        self.arena.deinit();
        self.parent_allocator.free(self.columns);
    }

    /// Reset for reuse: clear row count, selection vector, and free all arena strings.
    pub fn reset(self: *DataChunk) void {
        self.count = 0;
        self.sel = null;
        // Reset null masks
        for (self.columns) |*col| {
            col.null_mask = [_]u8{0} ** (VECTOR_SIZE / 8);
        }
        // Free all arena-allocated string data at once
        _ = self.arena.reset(.retain_capacity);
    }

    /// Number of active rows (respecting selection vector).
    pub fn activeCount(self: *const DataChunk) u16 {
        if (self.sel) |sv| return sv.len;
        return self.count;
    }
};

// ── Tests ────────────────────────────────────────────────────────

test "ColumnVector init and getValue" {
    var cv = ColumnVector.init(.integer);
    cv.data.integers[0] = 42;
    cv.data.integers[1] = -7;
    cv.setNull(2);

    const v0 = cv.getValue(0);
    try std.testing.expectEqual(@as(i32, 42), v0.integer);

    const v1 = cv.getValue(1);
    try std.testing.expectEqual(@as(i32, -7), v1.integer);

    const v2 = cv.getValue(2);
    try std.testing.expect(v2 == .null_value);
}

test "ColumnVector null mask" {
    var cv = ColumnVector.init(.float);
    try std.testing.expect(!cv.isNull(0));
    try std.testing.expect(!cv.isNull(100));

    cv.setNull(5);
    try std.testing.expect(cv.isNull(5));
    try std.testing.expect(!cv.isNull(4));
    try std.testing.expect(!cv.isNull(6));

    cv.clearNull(5);
    try std.testing.expect(!cv.isNull(5));
}

test "ColumnVector setValue" {
    var cv = ColumnVector.init(.bigint);
    cv.setValue(0, .{ .bigint = 999 });
    cv.setValue(1, .{ .null_value = {} });

    try std.testing.expectEqual(@as(i64, 999), cv.getValue(0).bigint);
    try std.testing.expect(cv.getValue(1) == .null_value);
}

test "DataChunk initFromSchema and reset" {
    const schema = tuple_mod.Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .text, .max_length = 0, .nullable = true },
        },
    };

    var chunk = try DataChunk.initFromSchema(std.testing.allocator, &schema);
    defer chunk.deinit();

    try std.testing.expectEqual(@as(usize, 2), chunk.columns.len);
    try std.testing.expectEqual(ColumnType.integer, chunk.columns[0].col_type);
    try std.testing.expectEqual(ColumnType.text, chunk.columns[1].col_type);
    try std.testing.expectEqual(@as(u16, 0), chunk.count);

    // Simulate filling
    chunk.columns[0].data.integers[0] = 1;
    chunk.count = 1;

    chunk.reset();
    try std.testing.expectEqual(@as(u16, 0), chunk.count);
    try std.testing.expect(chunk.sel == null);
}

test "DataChunk activeCount with selection vector" {
    const schema = tuple_mod.Schema{
        .columns = &.{
            .{ .name = "x", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var chunk = try DataChunk.initFromSchema(std.testing.allocator, &schema);
    defer chunk.deinit();

    chunk.count = 100;
    try std.testing.expectEqual(@as(u16, 100), chunk.activeCount());

    chunk.sel = SelectionVector{ .len = 42 };
    try std.testing.expectEqual(@as(u16, 42), chunk.activeCount());
}
