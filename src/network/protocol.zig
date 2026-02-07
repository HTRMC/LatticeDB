const std = @import("std");
const mem = std.mem;

/// GP Protocol message types
pub const MessageType = enum(u8) {
    // Client → Server
    query = 0x01,
    disconnect = 0x02,

    // Server → Client
    ok_message = 0x10,
    ok_row_count = 0x11,
    result_set = 0x12,
    error_response = 0x1F,
};

fn toMessageType(byte: u8) ?MessageType {
    return switch (byte) {
        0x01 => .query,
        0x02 => .disconnect,
        0x10 => .ok_message,
        0x11 => .ok_row_count,
        0x12 => .result_set,
        0x1F => .error_response,
        else => null,
    };
}

/// A framed GP Protocol message
pub const Message = struct {
    msg_type: MessageType,
    payload: []const u8,
};

/// Maximum message payload size (16 MB)
pub const MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;

/// Header size: 1 byte type + 4 bytes length
pub const HEADER_SIZE = 5;

// ── Framing ──────────────────────────────────────────────────────────

/// Encode a message into a buffer: [type:1][length:4 BE][payload:N]
pub fn encode(msg: Message, allocator: mem.Allocator) ![]u8 {
    const total = HEADER_SIZE + msg.payload.len;
    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    buf[0] = @intFromEnum(msg.msg_type);
    mem.writeInt(u32, buf[1..5], @intCast(msg.payload.len), .big);
    if (msg.payload.len > 0) {
        @memcpy(buf[HEADER_SIZE..], msg.payload);
    }
    return buf;
}

/// Decode a message from a buffer. Returns the message and bytes consumed.
/// Buffer must contain at least HEADER_SIZE bytes.
pub fn decode(buf: []const u8) !struct { msg: Message, consumed: usize } {
    if (buf.len < HEADER_SIZE) return error.IncompleteHeader;

    const msg_type = toMessageType(buf[0]) orelse return error.InvalidMessageType;
    const payload_len: usize = mem.readInt(u32, buf[1..5], .big);

    if (payload_len > MAX_PAYLOAD_SIZE) return error.PayloadTooLarge;
    if (buf.len < HEADER_SIZE + payload_len) return error.IncompletePayload;

    return .{
        .msg = .{
            .msg_type = msg_type,
            .payload = buf[HEADER_SIZE .. HEADER_SIZE + payload_len],
        },
        .consumed = HEADER_SIZE + payload_len,
    };
}

// ── Payload builders ─────────────────────────────────────────────────

/// Build a QUERY message
pub fn makeQuery(sql: []const u8, allocator: mem.Allocator) ![]u8 {
    return encode(.{ .msg_type = .query, .payload = sql }, allocator);
}

/// Build a DISCONNECT message
pub fn makeDisconnect(allocator: mem.Allocator) ![]u8 {
    return encode(.{ .msg_type = .disconnect, .payload = &.{} }, allocator);
}

/// Build an OK_MESSAGE response
pub fn makeOkMessage(msg: []const u8, allocator: mem.Allocator) ![]u8 {
    return encode(.{ .msg_type = .ok_message, .payload = msg }, allocator);
}

/// Build an OK_ROW_COUNT response
pub fn makeOkRowCount(count: u64, allocator: mem.Allocator) ![]u8 {
    var payload: [8]u8 = undefined;
    mem.writeInt(u64, &payload, count, .big);
    return encode(.{ .msg_type = .ok_row_count, .payload = &payload }, allocator);
}

/// Build an ERROR response
pub fn makeError(msg: []const u8, allocator: mem.Allocator) ![]u8 {
    return encode(.{ .msg_type = .error_response, .payload = msg }, allocator);
}

/// Build a RESULT_SET response from columns and string rows
pub fn makeResultSet(
    columns: []const []const u8,
    rows: []const []const []const u8,
    allocator: mem.Allocator,
) ![]u8 {
    // Calculate total payload size
    var size: usize = 2; // column_count (u16)
    for (columns) |col| {
        size += 2 + col.len; // name_len (u16) + name
    }
    size += 4; // row_count (u32)
    for (rows) |row| {
        for (row) |val| {
            size += 2 + val.len; // value_len (u16) + value
        }
    }

    const payload = try allocator.alloc(u8, size);
    defer allocator.free(payload);

    var pos: usize = 0;

    // Column count
    mem.writeInt(u16, payload[pos..][0..2], @intCast(columns.len), .big);
    pos += 2;

    // Column names
    for (columns) |col| {
        mem.writeInt(u16, payload[pos..][0..2], @intCast(col.len), .big);
        pos += 2;
        @memcpy(payload[pos .. pos + col.len], col);
        pos += col.len;
    }

    // Row count
    mem.writeInt(u32, payload[pos..][0..4], @intCast(rows.len), .big);
    pos += 4;

    // Row values
    for (rows) |row| {
        for (row) |val| {
            mem.writeInt(u16, payload[pos..][0..2], @intCast(val.len), .big);
            pos += 2;
            @memcpy(payload[pos .. pos + val.len], val);
            pos += val.len;
        }
    }

    return encode(.{ .msg_type = .result_set, .payload = payload }, allocator);
}

/// Parsed result set from a RESULT_SET payload
pub const ResultSet = struct {
    columns: [][]const u8,
    rows: [][]const []const u8,

    pub fn deinit(self: *ResultSet, allocator: mem.Allocator) void {
        for (self.rows) |row| {
            allocator.free(row);
        }
        allocator.free(self.rows);
        allocator.free(self.columns);
    }
};

/// Parse a RESULT_SET payload into columns and rows.
/// The returned slices point into the original payload buffer.
pub fn parseResultSet(payload: []const u8, allocator: mem.Allocator) !ResultSet {
    var pos: usize = 0;

    if (payload.len < 2) return error.InvalidResultSet;
    const col_count: usize = mem.readInt(u16, payload[pos..][0..2], .big);
    pos += 2;

    const columns = try allocator.alloc([]const u8, col_count);
    errdefer allocator.free(columns);

    for (0..col_count) |i| {
        if (pos + 2 > payload.len) return error.InvalidResultSet;
        const name_len: usize = mem.readInt(u16, payload[pos..][0..2], .big);
        pos += 2;
        if (pos + name_len > payload.len) return error.InvalidResultSet;
        columns[i] = payload[pos .. pos + name_len];
        pos += name_len;
    }

    if (pos + 4 > payload.len) return error.InvalidResultSet;
    const row_count: usize = mem.readInt(u32, payload[pos..][0..4], .big);
    pos += 4;

    const rows = try allocator.alloc([]const []const u8, row_count);
    errdefer {
        for (rows, 0..) |_, i| {
            if (i < row_count) allocator.free(rows[i]);
        }
        allocator.free(rows);
    }

    for (0..row_count) |r| {
        const vals = try allocator.alloc([]const u8, col_count);
        rows[r] = vals;
        for (0..col_count) |c| {
            if (pos + 2 > payload.len) return error.InvalidResultSet;
            const val_len: usize = mem.readInt(u16, payload[pos..][0..2], .big);
            pos += 2;
            if (pos + val_len > payload.len) return error.InvalidResultSet;
            vals[c] = payload[pos .. pos + val_len];
            pos += val_len;
        }
    }

    return .{ .columns = columns, .rows = rows };
}

// ── Tests ────────────────────────────────────────────────────────────

test "encode and decode query message" {
    const allocator = std.testing.allocator;
    const sql = "SELECT * FROM users";
    const buf = try makeQuery(sql, allocator);
    defer allocator.free(buf);

    try std.testing.expectEqual(@as(usize, HEADER_SIZE + sql.len), buf.len);
    try std.testing.expectEqual(@as(u8, 0x01), buf[0]);

    const result = try decode(buf);
    try std.testing.expectEqual(MessageType.query, result.msg.msg_type);
    try std.testing.expectEqualStrings(sql, result.msg.payload);
    try std.testing.expectEqual(buf.len, result.consumed);
}

test "encode and decode disconnect message" {
    const allocator = std.testing.allocator;
    const buf = try makeDisconnect(allocator);
    defer allocator.free(buf);

    try std.testing.expectEqual(@as(usize, HEADER_SIZE), buf.len);

    const result = try decode(buf);
    try std.testing.expectEqual(MessageType.disconnect, result.msg.msg_type);
    try std.testing.expectEqual(@as(usize, 0), result.msg.payload.len);
}

test "encode and decode ok_message" {
    const allocator = std.testing.allocator;
    const msg = "TABLE CREATED";
    const buf = try makeOkMessage(msg, allocator);
    defer allocator.free(buf);

    const result = try decode(buf);
    try std.testing.expectEqual(MessageType.ok_message, result.msg.msg_type);
    try std.testing.expectEqualStrings(msg, result.msg.payload);
}

test "encode and decode ok_row_count" {
    const allocator = std.testing.allocator;
    const buf = try makeOkRowCount(42, allocator);
    defer allocator.free(buf);

    const result = try decode(buf);
    try std.testing.expectEqual(MessageType.ok_row_count, result.msg.msg_type);
    try std.testing.expectEqual(@as(usize, 8), result.msg.payload.len);

    const count = mem.readInt(u64, result.msg.payload[0..8], .big);
    try std.testing.expectEqual(@as(u64, 42), count);
}

test "encode and decode result_set" {
    const allocator = std.testing.allocator;

    const columns = &[_][]const u8{ "id", "name", "age" };
    const row1 = &[_][]const u8{ "1", "Alice", "30" };
    const row2 = &[_][]const u8{ "2", "Bob", "25" };
    const rows = &[_][]const []const u8{ row1, row2 };

    const buf = try makeResultSet(columns, rows, allocator);
    defer allocator.free(buf);

    const result = try decode(buf);
    try std.testing.expectEqual(MessageType.result_set, result.msg.msg_type);

    var rs = try parseResultSet(result.msg.payload, allocator);
    defer rs.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), rs.columns.len);
    try std.testing.expectEqualStrings("id", rs.columns[0]);
    try std.testing.expectEqualStrings("name", rs.columns[1]);
    try std.testing.expectEqualStrings("age", rs.columns[2]);

    try std.testing.expectEqual(@as(usize, 2), rs.rows.len);
    try std.testing.expectEqualStrings("1", rs.rows[0][0]);
    try std.testing.expectEqualStrings("Alice", rs.rows[0][1]);
    try std.testing.expectEqualStrings("30", rs.rows[0][2]);
    try std.testing.expectEqualStrings("2", rs.rows[1][0]);
    try std.testing.expectEqualStrings("Bob", rs.rows[1][1]);
    try std.testing.expectEqualStrings("25", rs.rows[1][2]);
}

test "decode incomplete header" {
    const buf = [_]u8{ 0x01, 0x00 }; // only 2 bytes, need 5
    try std.testing.expectError(error.IncompleteHeader, decode(&buf));
}

test "decode invalid message type" {
    const buf = [_]u8{ 0xFF, 0x00, 0x00, 0x00, 0x00 };
    try std.testing.expectError(error.InvalidMessageType, decode(&buf));
}

test "decode incomplete payload" {
    // Header says 10 bytes payload, but only 3 present
    const buf = [_]u8{ 0x01, 0x00, 0x00, 0x00, 0x0A, 0x41, 0x42, 0x43 };
    try std.testing.expectError(error.IncompletePayload, decode(&buf));
}

test "encode and decode error response" {
    const allocator = std.testing.allocator;
    const msg = "table not found: users";
    const buf = try makeError(msg, allocator);
    defer allocator.free(buf);

    const result = try decode(buf);
    try std.testing.expectEqual(MessageType.error_response, result.msg.msg_type);
    try std.testing.expectEqualStrings(msg, result.msg.payload);
}

test "empty result set" {
    const allocator = std.testing.allocator;

    const columns = &[_][]const u8{ "id", "name" };
    const rows = &[_][]const []const u8{};

    const buf = try makeResultSet(columns, rows, allocator);
    defer allocator.free(buf);

    const result = try decode(buf);
    var rs = try parseResultSet(result.msg.payload, allocator);
    defer rs.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), rs.columns.len);
    try std.testing.expectEqual(@as(usize, 0), rs.rows.len);
}

test "encode decode all message types" {
    const allocator = std.testing.allocator;

    // Test each message type round-trips correctly
    const types = [_]MessageType{ .query, .disconnect, .ok_message, .ok_row_count, .result_set, .error_response };
    for (types) |mt| {
        const payload = "test";
        const msg = Message{ .msg_type = mt, .payload = payload };
        const buf = try encode(msg, allocator);
        defer allocator.free(buf);

        const result = try decode(buf);
        try std.testing.expectEqual(mt, result.msg.msg_type);
        try std.testing.expectEqualStrings(payload, result.msg.payload);
    }
}

test "result set with empty string values" {
    const allocator = std.testing.allocator;

    const columns = &[_][]const u8{ "id", "name" };
    const rows = &[_][]const []const u8{
        &[_][]const u8{ "1", "" }, // empty string value
        &[_][]const u8{ "", "alice" }, // empty string in first col
    };

    const buf = try makeResultSet(columns, rows, allocator);
    defer allocator.free(buf);

    const result = try decode(buf);
    var rs = try parseResultSet(result.msg.payload, allocator);
    defer rs.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), rs.columns.len);
    try std.testing.expectEqual(@as(usize, 2), rs.rows.len);
    try std.testing.expectEqualStrings("1", rs.rows[0][0]);
    try std.testing.expectEqualStrings("", rs.rows[0][1]);
    try std.testing.expectEqualStrings("", rs.rows[1][0]);
    try std.testing.expectEqualStrings("alice", rs.rows[1][1]);
}

test "single column single row result set" {
    const allocator = std.testing.allocator;

    const columns = &[_][]const u8{"count"};
    const rows = &[_][]const []const u8{
        &[_][]const u8{"42"},
    };

    const buf = try makeResultSet(columns, rows, allocator);
    defer allocator.free(buf);

    const result = try decode(buf);
    var rs = try parseResultSet(result.msg.payload, allocator);
    defer rs.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), rs.columns.len);
    try std.testing.expectEqual(@as(usize, 1), rs.rows.len);
    try std.testing.expectEqualStrings("count", rs.columns[0]);
    try std.testing.expectEqualStrings("42", rs.rows[0][0]);
}
