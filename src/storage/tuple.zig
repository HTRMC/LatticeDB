const std = @import("std");
const mvcc = @import("mvcc.zig");

const TupleHeader = mvcc.TupleHeader;

/// Supported column data types
pub const ColumnType = enum(u8) {
    boolean = 1,
    integer = 2, // i32
    bigint = 3, // i64
    float = 4, // f64
    varchar = 5, // variable-length, max length stored in schema
    text = 6, // variable-length, no max

    /// Size of a fixed-length type, null for variable-length
    pub fn fixedSize(self: ColumnType) ?usize {
        return switch (self) {
            .boolean => 1,
            .integer => 4,
            .bigint => 8,
            .float => 8,
            .varchar, .text => null,
        };
    }

    pub fn isVariableLength(self: ColumnType) bool {
        return self.fixedSize() == null;
    }
};

/// Column definition in a schema
pub const Column = struct {
    name: []const u8,
    col_type: ColumnType,
    max_length: u16, // Only meaningful for varchar
    nullable: bool,
};

/// Schema - describes the structure of a table's rows
pub const Schema = struct {
    columns: []const Column,

    /// Number of columns
    pub fn columnCount(self: *const Schema) usize {
        return self.columns.len;
    }

    /// Get column by index
    pub fn getColumn(self: *const Schema, index: usize) ?Column {
        if (index >= self.columns.len) return null;
        return self.columns[index];
    }

    /// Find column index by name
    pub fn findColumn(self: *const Schema, name: []const u8) ?usize {
        for (self.columns, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, name)) return i;
        }
        return null;
    }

    /// Number of bytes for the null bitmap
    pub fn nullBitmapSize(self: *const Schema) usize {
        return (self.columns.len + 7) / 8;
    }
};

/// A value that can be stored in a column
pub const Value = union(enum) {
    null_value: void,
    boolean: bool,
    integer: i32,
    bigint: i64,
    float: f64,
    bytes: []const u8, // For varchar and text

    pub fn isNull(self: Value) bool {
        return self == .null_value;
    }

    /// Encode a value into a buffer, returns bytes written
    pub fn encode(self: Value, buf: []u8) !usize {
        switch (self) {
            .null_value => return 0,
            .boolean => |v| {
                if (buf.len < 1) return error.BufferTooSmall;
                buf[0] = if (v) 1 else 0;
                return 1;
            },
            .integer => |v| {
                if (buf.len < 4) return error.BufferTooSmall;
                @memcpy(buf[0..4], std.mem.asBytes(&v));
                return 4;
            },
            .bigint => |v| {
                if (buf.len < 8) return error.BufferTooSmall;
                @memcpy(buf[0..8], std.mem.asBytes(&v));
                return 8;
            },
            .float => |v| {
                if (buf.len < 8) return error.BufferTooSmall;
                @memcpy(buf[0..8], std.mem.asBytes(&v));
                return 8;
            },
            .bytes => |v| {
                // Length-prefixed: u16 length + data
                const total = 2 + v.len;
                if (buf.len < total) return error.BufferTooSmall;
                const len: u16 = @intCast(v.len);
                @memcpy(buf[0..2], std.mem.asBytes(&len));
                @memcpy(buf[2..][0..v.len], v);
                return total;
            },
        }
    }

    /// Decode a value from a buffer, returns the value and bytes consumed
    pub fn decode(col_type: ColumnType, buf: []const u8) !struct { value: Value, size: usize } {
        switch (col_type) {
            .boolean => {
                if (buf.len < 1) return error.BufferTooSmall;
                return .{ .value = .{ .boolean = buf[0] != 0 }, .size = 1 };
            },
            .integer => {
                if (buf.len < 4) return error.BufferTooSmall;
                return .{
                    .value = .{ .integer = std.mem.bytesToValue(i32, buf[0..4]) },
                    .size = 4,
                };
            },
            .bigint => {
                if (buf.len < 8) return error.BufferTooSmall;
                return .{
                    .value = .{ .bigint = std.mem.bytesToValue(i64, buf[0..8]) },
                    .size = 8,
                };
            },
            .float => {
                if (buf.len < 8) return error.BufferTooSmall;
                return .{
                    .value = .{ .float = std.mem.bytesToValue(f64, buf[0..8]) },
                    .size = 8,
                };
            },
            .varchar, .text => {
                if (buf.len < 2) return error.BufferTooSmall;
                const len = std.mem.bytesToValue(u16, buf[0..2]);
                if (buf.len < 2 + len) return error.BufferTooSmall;
                return .{
                    .value = .{ .bytes = buf[2..][0..len] },
                    .size = 2 + len,
                };
            },
        }
    }

    /// Format a value as a string for display
    pub fn format(self: Value, buf: []u8) ![]const u8 {
        switch (self) {
            .null_value => return "NULL",
            .boolean => |v| return if (v) "true" else "false",
            .integer => |v| {
                return std.fmt.bufPrint(buf, "{}", .{v}) catch return error.BufferTooSmall;
            },
            .bigint => |v| {
                return std.fmt.bufPrint(buf, "{}", .{v}) catch return error.BufferTooSmall;
            },
            .float => |v| {
                return std.fmt.bufPrint(buf, "{d:.6}", .{v}) catch return error.BufferTooSmall;
            },
            .bytes => |v| return v,
        }
    }
};

/// Tuple - a row of values
/// Serialized format: [null_bitmap][col1_data][col2_data]...
/// Variable-length columns are length-prefixed (u16 + data)
pub const Tuple = struct {
    /// Serialize a row of values into a byte buffer
    /// Returns the number of bytes written
    pub fn serialize(schema: *const Schema, values: []const Value, buf: []u8) !usize {
        if (values.len != schema.columns.len) return error.ColumnCountMismatch;

        const bitmap_size = schema.nullBitmapSize();
        if (buf.len < bitmap_size) return error.BufferTooSmall;

        // Write null bitmap
        @memset(buf[0..bitmap_size], 0);
        for (values, 0..) |val, i| {
            if (val == .null_value) {
                buf[i / 8] |= @as(u8, 1) << @intCast(i % 8);
            }
        }

        // Write column data
        var offset: usize = bitmap_size;
        for (values, 0..) |val, i| {
            if (val == .null_value) {
                if (!schema.columns[i].nullable) return error.NullNotAllowed;
                continue;
            }
            const written = val.encode(buf[offset..]) catch return error.BufferTooSmall;
            offset += written;
        }

        return offset;
    }

    /// Deserialize a byte buffer into a row of values
    /// Caller owns the returned slice
    pub fn deserialize(
        allocator: std.mem.Allocator,
        schema: *const Schema,
        data: []const u8,
    ) ![]Value {
        const col_count = schema.columns.len;
        const bitmap_size = schema.nullBitmapSize();

        if (data.len < bitmap_size) return error.BufferTooSmall;

        const values = try allocator.alloc(Value, col_count);
        errdefer allocator.free(values);

        // Read null bitmap
        var offset: usize = bitmap_size;
        for (schema.columns, 0..) |col, i| {
            const is_null = (data[i / 8] & (@as(u8, 1) << @intCast(i % 8))) != 0;
            if (is_null) {
                values[i] = .null_value;
            } else {
                const result = Value.decode(col.col_type, data[offset..]) catch return error.CorruptedTuple;
                values[i] = result.value;
                offset += result.size;
            }
        }

        return values;
    }

    /// Calculate the serialized size of a row
    pub fn serializedSize(schema: *const Schema, values: []const Value) !usize {
        if (values.len != schema.columns.len) return error.ColumnCountMismatch;

        var size: usize = schema.nullBitmapSize();

        for (values, 0..) |val, i| {
            if (val == .null_value) continue;
            switch (val) {
                .null_value => {},
                .boolean => size += 1,
                .integer => size += 4,
                .bigint, .float => size += 8,
                .bytes => |v| {
                    _ = i;
                    size += 2 + v.len;
                },
            }
        }

        return size;
    }

    /// Serialize a row with a TupleHeader prepended.
    /// On-disk format: [TupleHeader 16B][null_bitmap][col_data...]
    pub fn serializeWithHeader(header: TupleHeader, schema: *const Schema, values: []const Value, buf: []u8) !usize {
        if (buf.len < TupleHeader.SIZE) return error.BufferTooSmall;

        // Write TupleHeader
        @memcpy(buf[0..TupleHeader.SIZE], std.mem.asBytes(&header));

        // Serialize user data after the header
        const user_written = try serialize(schema, values, buf[TupleHeader.SIZE..]);

        return TupleHeader.SIZE + user_written;
    }

    /// Strip the TupleHeader from raw tuple data, returning the header and user data portion.
    pub fn stripHeader(data: []const u8) !struct { header: TupleHeader, user_data: []const u8 } {
        if (data.len < TupleHeader.SIZE) return error.BufferTooSmall;

        return .{
            .header = std.mem.bytesToValue(TupleHeader, data[0..TupleHeader.SIZE]),
            .user_data = data[TupleHeader.SIZE..],
        };
    }

    /// Calculate the serialized size including the TupleHeader.
    pub fn serializedSizeWithHeader(schema: *const Schema, values: []const Value) !usize {
        return TupleHeader.SIZE + try serializedSize(schema, values);
    }
};

// Tests
test "value encode and decode" {
    var buf: [256]u8 = undefined;

    // Boolean
    const bool_val = Value{ .boolean = true };
    const bool_len = try bool_val.encode(&buf);
    try std.testing.expectEqual(@as(usize, 1), bool_len);
    const bool_result = try Value.decode(.boolean, buf[0..bool_len]);
    try std.testing.expectEqual(true, bool_result.value.boolean);

    // Integer
    const int_val = Value{ .integer = 42 };
    const int_len = try int_val.encode(&buf);
    try std.testing.expectEqual(@as(usize, 4), int_len);
    const int_result = try Value.decode(.integer, buf[0..int_len]);
    try std.testing.expectEqual(@as(i32, 42), int_result.value.integer);

    // Bigint
    const big_val = Value{ .bigint = 9999999999 };
    const big_len = try big_val.encode(&buf);
    try std.testing.expectEqual(@as(usize, 8), big_len);
    const big_result = try Value.decode(.bigint, buf[0..big_len]);
    try std.testing.expectEqual(@as(i64, 9999999999), big_result.value.bigint);

    // Float
    const float_val = Value{ .float = 3.14 };
    const float_len = try float_val.encode(&buf);
    try std.testing.expectEqual(@as(usize, 8), float_len);
    const float_result = try Value.decode(.float, buf[0..float_len]);
    try std.testing.expectEqual(@as(f64, 3.14), float_result.value.float);

    // Bytes (varchar/text)
    const bytes_val = Value{ .bytes = "hello" };
    const bytes_len = try bytes_val.encode(&buf);
    try std.testing.expectEqual(@as(usize, 7), bytes_len); // 2 (len) + 5 (data)
    const bytes_result = try Value.decode(.varchar, buf[0..bytes_len]);
    try std.testing.expectEqualStrings("hello", bytes_result.value.bytes);
}

test "tuple serialize and deserialize" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
            .{ .name = "active", .col_type = .boolean, .max_length = 0, .nullable = true },
        },
    };

    const values = [_]Value{
        .{ .integer = 1 },
        .{ .bytes = "Alice" },
        .{ .boolean = true },
    };

    var buf: [256]u8 = undefined;
    const written = try Tuple.serialize(&schema, &values, &buf);
    try std.testing.expect(written > 0);

    // Deserialize
    const result = try Tuple.deserialize(std.testing.allocator, &schema, buf[0..written]);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqual(@as(i32, 1), result[0].integer);
    try std.testing.expectEqualStrings("Alice", result[1].bytes);
    try std.testing.expectEqual(true, result[2].boolean);
}

test "tuple with null values" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = true },
            .{ .name = "score", .col_type = .float, .max_length = 0, .nullable = true },
        },
    };

    const values = [_]Value{
        .{ .integer = 42 },
        .null_value,
        .{ .float = 98.6 },
    };

    var buf: [256]u8 = undefined;
    const written = try Tuple.serialize(&schema, &values, &buf);

    const result = try Tuple.deserialize(std.testing.allocator, &schema, buf[0..written]);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqual(@as(i32, 42), result[0].integer);
    try std.testing.expect(result[1] == .null_value);
    try std.testing.expectEqual(@as(f64, 98.6), result[2].float);
}

test "tuple serialized size" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "a", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "b", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    const values = [_]Value{
        .{ .integer = 1 },
        .{ .bytes = "test" },
    };

    const size = try Tuple.serializedSize(&schema, &values);
    // 1 byte null bitmap + 4 bytes int + 2 bytes len + 4 bytes text = 11
    try std.testing.expectEqual(@as(usize, 11), size);
}

test "schema find column" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
            .{ .name = "email", .col_type = .text, .max_length = 0, .nullable = true },
        },
    };

    try std.testing.expectEqual(@as(?usize, 0), schema.findColumn("id"));
    try std.testing.expectEqual(@as(?usize, 1), schema.findColumn("name"));
    try std.testing.expectEqual(@as(?usize, 2), schema.findColumn("email"));
    try std.testing.expectEqual(@as(?usize, null), schema.findColumn("missing"));
    try std.testing.expectEqual(@as(usize, 3), schema.columnCount());
}

test "serializeWithHeader and stripHeader" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
        },
    };

    const values = [_]Value{
        .{ .integer = 42 },
        .{ .bytes = "Alice" },
    };

    const header = TupleHeader.init(7);

    var buf: [256]u8 = undefined;
    const written = try Tuple.serializeWithHeader(header, &schema, &values, &buf);

    // Should be 16 (header) + 1 (bitmap) + 4 (int) + 2 (len) + 5 (data) = 28
    try std.testing.expectEqual(@as(usize, 28), written);

    // Strip and verify
    const stripped = try Tuple.stripHeader(buf[0..written]);
    try std.testing.expectEqual(@as(u32, 7), stripped.header.xmin);
    try std.testing.expectEqual(@as(u32, 0), stripped.header.xmax);
    try std.testing.expect(stripped.header.isLive());

    // Deserialize user data
    const result = try Tuple.deserialize(std.testing.allocator, &schema, stripped.user_data);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqual(@as(i32, 42), result[0].integer);
    try std.testing.expectEqualStrings("Alice", result[1].bytes);
}

test "serializedSizeWithHeader" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "a", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    const values = [_]Value{.{ .integer = 1 }};

    const size = try Tuple.serializedSizeWithHeader(&schema, &values);
    // 16 (header) + 1 (bitmap) + 4 (int) = 21
    try std.testing.expectEqual(@as(usize, 21), size);
}

test "stripHeader too small buffer" {
    const small = [_]u8{ 0, 1, 2 };
    try std.testing.expectError(error.BufferTooSmall, Tuple.stripHeader(&small));
}

test "tuple all null values" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "a", .col_type = .integer, .max_length = 0, .nullable = true },
            .{ .name = "b", .col_type = .varchar, .max_length = 255, .nullable = true },
            .{ .name = "c", .col_type = .float, .max_length = 0, .nullable = true },
        },
    };

    const values = [_]Value{ .null_value, .null_value, .null_value };

    var buf: [256]u8 = undefined;
    const written = try Tuple.serialize(&schema, &values, &buf);
    try std.testing.expect(written > 0);

    const result = try Tuple.deserialize(std.testing.allocator, &schema, buf[0..written]);
    defer std.testing.allocator.free(result);

    try std.testing.expect(result[0] == .null_value);
    try std.testing.expect(result[1] == .null_value);
    try std.testing.expect(result[2] == .null_value);
}

test "tuple single boolean column" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "flag", .col_type = .boolean, .max_length = 0, .nullable = false },
        },
    };

    const values_true = [_]Value{.{ .boolean = true }};
    const values_false = [_]Value{.{ .boolean = false }};

    var buf: [64]u8 = undefined;

    const written_t = try Tuple.serialize(&schema, &values_true, &buf);
    const res_t = try Tuple.deserialize(std.testing.allocator, &schema, buf[0..written_t]);
    defer std.testing.allocator.free(res_t);
    try std.testing.expectEqual(true, res_t[0].boolean);

    const written_f = try Tuple.serialize(&schema, &values_false, &buf);
    const res_f = try Tuple.deserialize(std.testing.allocator, &schema, buf[0..written_f]);
    defer std.testing.allocator.free(res_f);
    try std.testing.expectEqual(false, res_f[0].boolean);
}

test "tuple negative integer and bigint" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "small", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "big", .col_type = .bigint, .max_length = 0, .nullable = false },
        },
    };

    const values = [_]Value{
        .{ .integer = -2147483648 }, // i32 min
        .{ .bigint = -9223372036854775808 }, // i64 min
    };

    var buf: [256]u8 = undefined;
    const written = try Tuple.serialize(&schema, &values, &buf);
    const result = try Tuple.deserialize(std.testing.allocator, &schema, buf[0..written]);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqual(@as(i32, -2147483648), result[0].integer);
    try std.testing.expectEqual(@as(i64, -9223372036854775808), result[1].bigint);
}

test "tuple empty string varchar" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
        },
    };

    const values = [_]Value{.{ .bytes = "" }};

    var buf: [64]u8 = undefined;
    const written = try Tuple.serialize(&schema, &values, &buf);
    const result = try Tuple.deserialize(std.testing.allocator, &schema, buf[0..written]);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings("", result[0].bytes);
}

test "value encode null_value" {
    var buf: [16]u8 = undefined;
    const null_val = Value{ .null_value = {} };
    const len = try null_val.encode(&buf);
    try std.testing.expectEqual(@as(usize, 0), len);
}

test "value format" {
    var buf: [64]u8 = undefined;

    // null
    const null_str = try (Value{ .null_value = {} }).format(&buf);
    try std.testing.expectEqualStrings("NULL", null_str);

    // boolean
    const true_str = try (Value{ .boolean = true }).format(&buf);
    try std.testing.expectEqualStrings("true", true_str);
    const false_str = try (Value{ .boolean = false }).format(&buf);
    try std.testing.expectEqualStrings("false", false_str);

    // integer
    const int_str = try (Value{ .integer = -42 }).format(&buf);
    try std.testing.expectEqualStrings("-42", int_str);
    const zero_str = try (Value{ .integer = 0 }).format(&buf);
    try std.testing.expectEqualStrings("0", zero_str);

    // bigint
    const big_str = try (Value{ .bigint = 9999999999 }).format(&buf);
    try std.testing.expectEqualStrings("9999999999", big_str);

    // float
    const float_str = try (Value{ .float = 3.14 }).format(&buf);
    try std.testing.expectEqualStrings("3.140000", float_str);

    // bytes (passthrough)
    const bytes_str = try (Value{ .bytes = "hello" }).format(&buf);
    try std.testing.expectEqualStrings("hello", bytes_str);

    // bytes empty string
    const empty_str = try (Value{ .bytes = "" }).format(&buf);
    try std.testing.expectEqualStrings("", empty_str);
}
