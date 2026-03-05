const std = @import("std");
const ast = @import("../parser/ast.zig");
const tuple_mod = @import("../storage/tuple.zig");

const Value = tuple_mod.Value;
const Schema = tuple_mod.Schema;

/// Result of evaluating an expression — tracks whether the value's bytes are owned.
pub const ExprResult = struct {
    value: Value,
    owned: bool, // true if value.bytes was allocated and must be freed by caller

    pub fn borrowed(val: Value) ExprResult {
        return .{ .value = val, .owned = false };
    }

    pub fn owned_val(val: Value) ExprResult {
        return .{ .value = val, .owned = true };
    }

    pub fn deinit(self: ExprResult, allocator: std.mem.Allocator) void {
        if (self.owned) {
            switch (self.value) {
                .bytes => |b| allocator.free(b),
                else => {},
            }
        }
    }
};

/// SQL LIKE pattern matching: % = any chars, _ = single char
pub fn matchLike(text: []const u8, pattern: []const u8) bool {
    var ti: usize = 0;
    var pi: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len and pattern[pi] == '_') {
            ti += 1;
            pi += 1;
        } else if (pi < pattern.len and pattern[pi] == '%') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (pi < pattern.len and pattern[pi] == text[ti]) {
            ti += 1;
            pi += 1;
        } else if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while (pi < pattern.len and pattern[pi] == '%') {
        pi += 1;
    }
    return pi == pattern.len;
}

/// Compare two Values with a comparison operator. NULL comparisons always return false.
pub fn compareValues(left: Value, op: ast.CompOp, right: Value) bool {
    if (left == .null_value or right == .null_value) return false;

    switch (left) {
        .integer => |li| {
            const li_wide: i64 = li;
            const ri: i64 = switch (right) {
                .integer => |v| v,
                .bigint => |v| v,
                else => return false,
            };
            return switch (op) {
                .eq => li_wide == ri,
                .neq => li_wide != ri,
                .lt => li_wide < ri,
                .gt => li_wide > ri,
                .lte => li_wide <= ri,
                .gte => li_wide >= ri,
            };
        },
        .bigint => |li| {
            const ri: i64 = switch (right) {
                .integer => |v| v,
                .bigint => |v| v,
                else => return false,
            };
            return switch (op) {
                .eq => li == ri,
                .neq => li != ri,
                .lt => li < ri,
                .gt => li > ri,
                .lte => li <= ri,
                .gte => li >= ri,
            };
        },
        .float => |lf| {
            const rf: f64 = switch (right) {
                .float => |v| v,
                .integer => |v| @floatFromInt(v),
                else => return false,
            };
            return switch (op) {
                .eq => lf == rf,
                .neq => lf != rf,
                .lt => lf < rf,
                .gt => lf > rf,
                .lte => lf <= rf,
                .gte => lf >= rf,
            };
        },
        .bytes => |ls| {
            const rs = switch (right) {
                .bytes => |v| v,
                else => return false,
            };
            const cmp = std.mem.order(u8, ls, rs);
            return switch (op) {
                .eq => cmp == .eq,
                .neq => cmp != .eq,
                .lt => cmp == .lt,
                .gt => cmp == .gt,
                .lte => cmp != .gt,
                .gte => cmp != .lt,
            };
        },
        .boolean => |lb| {
            const rb = switch (right) {
                .boolean => |v| v,
                else => return false,
            };
            return switch (op) {
                .eq => lb == rb,
                .neq => lb != rb,
                else => false,
            };
        },
        .null_value => return false,
    }
}

/// Convert an AST literal to a storage Value. Does not allocate.
pub fn litToStorageValue(lit: ast.LiteralValue, params: ?[]const ast.LiteralValue) Value {
    const resolved = resolveParam(lit, params);
    return switch (resolved) {
        .integer => |i| blk: {
            if (i >= std.math.minInt(i32) and i <= std.math.maxInt(i32)) {
                break :blk .{ .integer = @intCast(i) };
            }
            break :blk .{ .bigint = i };
        },
        .float => |f| .{ .float = f },
        .string => |s| .{ .bytes = s },
        .boolean => |b| .{ .boolean = b },
        .null_value => .{ .null_value = {} },
        .parameter => .{ .null_value = {} },
    };
}

fn resolveParam(lit: ast.LiteralValue, params: ?[]const ast.LiteralValue) ast.LiteralValue {
    return switch (lit) {
        .parameter => |idx| if (params) |p| (if (idx < p.len) p[idx] else lit) else lit,
        else => lit,
    };
}

/// Resolve a simple expression (column_ref, qualified_ref, literal) to a Value.
/// Non-allocating — returns a borrowed reference into the row data.
pub fn resolveExprValue(expr: *const ast.Expression, schema: *const Schema, values: []const Value, params: ?[]const ast.LiteralValue) Value {
    switch (expr.*) {
        .column_ref => |name| {
            for (schema.columns, 0..) |col, i| {
                if (std.ascii.eqlIgnoreCase(col.name, name)) {
                    return values[i];
                }
            }
            return .{ .null_value = {} };
        },
        .qualified_ref => |qr| {
            for (schema.columns, 0..) |col, i| {
                if (std.ascii.eqlIgnoreCase(col.name, qr.column)) {
                    return values[i];
                }
            }
            return .{ .null_value = {} };
        },
        .literal => |lit| return litToStorageValue(lit, params),
        else => return .{ .null_value = {} },
    }
}

/// Evaluate any expression to a Value. May allocate for function results.
/// Caller must call result.deinit(allocator) to free owned values.
pub fn evalExprToValue(
    allocator: std.mem.Allocator,
    expr: *const ast.Expression,
    schema: *const Schema,
    values: []const Value,
    params: ?[]const ast.LiteralValue,
) ExprResult {
    switch (expr.*) {
        .column_ref, .qualified_ref, .literal => return ExprResult.borrowed(resolveExprValue(expr, schema, values, params)),
        .function_call => |fc| return evalFunctionCall(allocator, fc, schema, values, params),
        .case_expr => |ce| return evalCaseExpr(allocator, ce, schema, values, params),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    }
}

fn evalFunctionCall(
    allocator: std.mem.Allocator,
    fc: anytype,
    schema: *const Schema,
    values: []const Value,
    params: ?[]const ast.LiteralValue,
) ExprResult {
    switch (fc.func) {
        .lower => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            return strLower(allocator, arg.value);
        },
        .upper => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            return strUpper(allocator, arg.value);
        },
        .trim => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            return strTrim(allocator, arg.value);
        },
        .length => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            return strLength(arg.value);
        },
        .substring => {
            const str_arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer str_arg.deinit(allocator);
            const start_arg = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer start_arg.deinit(allocator);
            const len_arg = if (fc.args.len > 2) blk: {
                const r = evalExprToValue(allocator, fc.args[2], schema, values, params);
                break :blk r;
            } else ExprResult.borrowed(.{ .null_value = {} });
            defer len_arg.deinit(allocator);
            return strSubstring(allocator, str_arg.value, start_arg.value, len_arg.value);
        },
        .concat => {
            return strConcat(allocator, fc.args, schema, values, params);
        },
    }
}

fn evalCaseExpr(
    allocator: std.mem.Allocator,
    ce: anytype,
    schema: *const Schema,
    values: []const Value,
    params: ?[]const ast.LiteralValue,
) ExprResult {
    for (ce.when_clauses) |wc| {
        if (evalExprToBool(allocator, wc.condition, schema, values, params)) {
            return evalExprToValue(allocator, wc.result, schema, values, params);
        }
    }
    if (ce.else_result) |er| {
        return evalExprToValue(allocator, er, schema, values, params);
    }
    return ExprResult.borrowed(.{ .null_value = {} });
}

/// Evaluate an expression as a boolean (for CASE WHEN conditions).
fn evalExprToBool(
    allocator: std.mem.Allocator,
    expr: *const ast.Expression,
    schema: *const Schema,
    values: []const Value,
    params: ?[]const ast.LiteralValue,
) bool {
    switch (expr.*) {
        .comparison => |cmp| {
            const left_res = evalExprToValue(allocator, cmp.left, schema, values, params);
            defer left_res.deinit(allocator);
            const right_res = evalExprToValue(allocator, cmp.right, schema, values, params);
            defer right_res.deinit(allocator);
            return compareValues(left_res.value, cmp.op, right_res.value);
        },
        .and_expr => |a| {
            return evalExprToBool(allocator, a.left, schema, values, params) and
                evalExprToBool(allocator, a.right, schema, values, params);
        },
        .or_expr => |o| {
            return evalExprToBool(allocator, o.left, schema, values, params) or
                evalExprToBool(allocator, o.right, schema, values, params);
        },
        .not_expr => |n| {
            return !evalExprToBool(allocator, n.operand, schema, values, params);
        },
        .between_expr => |b| {
            const val = evalExprToValue(allocator, b.value, schema, values, params);
            defer val.deinit(allocator);
            const low = evalExprToValue(allocator, b.low, schema, values, params);
            defer low.deinit(allocator);
            const high = evalExprToValue(allocator, b.high, schema, values, params);
            defer high.deinit(allocator);
            return compareValues(val.value, .gte, low.value) and compareValues(val.value, .lte, high.value);
        },
        .like_expr => |l| {
            const val = evalExprToValue(allocator, l.value, schema, values, params);
            defer val.deinit(allocator);
            const pat = evalExprToValue(allocator, l.pattern, schema, values, params);
            defer pat.deinit(allocator);
            const text = switch (val.value) {
                .bytes => |s| s,
                else => return false,
            };
            const pattern = switch (pat.value) {
                .bytes => |s| s,
                else => return false,
            };
            return matchLike(text, pattern);
        },
        .literal => |lit| {
            const resolved = resolveParam(lit, params);
            return switch (resolved) {
                .boolean => |b| b,
                .null_value => false,
                .integer => |i| i != 0,
                else => true,
            };
        },
        .column_ref => return true,
        .qualified_ref => |qr| {
            for (schema.columns, 0..) |col, i| {
                if (std.ascii.eqlIgnoreCase(col.name, qr.column)) {
                    return switch (values[i]) {
                        .null_value => false,
                        .boolean => |b| b,
                        .integer => |i_val| i_val != 0,
                        else => true,
                    };
                }
            }
            return true;
        },
        .case_expr, .function_call => {
            const result = evalExprToValue(allocator, expr, schema, values, params);
            defer result.deinit(allocator);
            return switch (result.value) {
                .null_value => false,
                .boolean => |b| b,
                .integer => |i| i != 0,
                else => true,
            };
        },
        .in_list, .in_subquery, .exists_subquery => return true,
    }
}

// ── String functions ─────────────────────────────────────────────

fn strLower(allocator: std.mem.Allocator, val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const buf = allocator.alloc(u8, s.len) catch return ExprResult.borrowed(.{ .null_value = {} });
    for (s, 0..) |c, i| {
        buf[i] = std.ascii.toLower(c);
    }
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strUpper(allocator: std.mem.Allocator, val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const buf = allocator.alloc(u8, s.len) catch return ExprResult.borrowed(.{ .null_value = {} });
    for (s, 0..) |c, i| {
        buf[i] = std.ascii.toUpper(c);
    }
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strTrim(allocator: std.mem.Allocator, val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const trimmed = std.mem.trim(u8, s, " \t\n\r");
    const buf = allocator.dupe(u8, trimmed) catch return ExprResult.borrowed(.{ .null_value = {} });
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strLength(val: Value) ExprResult {
    return switch (val) {
        .bytes => |b| ExprResult.borrowed(.{ .integer = @intCast(b.len) }),
        .null_value => ExprResult.borrowed(.{ .null_value = {} }),
        else => ExprResult.borrowed(.{ .null_value = {} }),
    };
}

fn strSubstring(allocator: std.mem.Allocator, val: Value, start_val: Value, len_val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };

    // SQL SUBSTRING is 1-based
    const start_raw: i64 = switch (start_val) {
        .integer => |i| i,
        .bigint => |i| i,
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    if (start_raw < 1) return ExprResult.borrowed(.{ .null_value = {} });
    const start: usize = @intCast(start_raw - 1);
    if (start >= s.len) {
        const empty = allocator.dupe(u8, "") catch return ExprResult.borrowed(.{ .null_value = {} });
        return ExprResult.owned_val(.{ .bytes = empty });
    }

    const remaining = s.len - start;
    const take: usize = switch (len_val) {
        .integer => |i| if (i < 0) 0 else @min(@as(usize, @intCast(i)), remaining),
        .bigint => |i| if (i < 0) 0 else @min(@as(usize, @intCast(i)), remaining),
        .null_value => remaining, // no length arg = take rest
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };

    const buf = allocator.dupe(u8, s[start..][0..take]) catch return ExprResult.borrowed(.{ .null_value = {} });
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strConcat(
    allocator: std.mem.Allocator,
    args: []const *const ast.Expression,
    schema: *const Schema,
    values: []const Value,
    params: ?[]const ast.LiteralValue,
) ExprResult {
    // Collect all arg strings; NULL propagates
    var total_len: usize = 0;
    var parts: [16][]const u8 = undefined; // max 16 args
    var owned_flags: [16]bool = .{false} ** 16;
    const n = @min(args.len, 16);

    for (args[0..n], 0..) |arg, i| {
        const res = evalExprToValue(allocator, arg, schema, values, params);
        switch (res.value) {
            .null_value => {
                // Free any previously owned parts
                for (0..i) |j| {
                    if (owned_flags[j]) allocator.free(parts[j]);
                }
                return ExprResult.borrowed(.{ .null_value = {} });
            },
            .bytes => |b| {
                parts[i] = b;
                owned_flags[i] = res.owned;
                total_len += b.len;
            },
            else => {
                // Convert non-string to string
                const formatted = formatValueForConcat(allocator, res.value) catch {
                    if (res.owned) {
                        switch (res.value) {
                            .bytes => |b| allocator.free(b),
                            else => {},
                        }
                    }
                    for (0..i) |j| {
                        if (owned_flags[j]) allocator.free(parts[j]);
                    }
                    return ExprResult.borrowed(.{ .null_value = {} });
                };
                parts[i] = formatted;
                owned_flags[i] = true;
                total_len += formatted.len;
            },
        }
    }

    // Build concatenated result
    const buf = allocator.alloc(u8, total_len) catch {
        for (0..n) |j| {
            if (owned_flags[j]) allocator.free(parts[j]);
        }
        return ExprResult.borrowed(.{ .null_value = {} });
    };
    var pos: usize = 0;
    for (0..n) |i| {
        @memcpy(buf[pos..][0..parts[i].len], parts[i]);
        pos += parts[i].len;
    }

    // Free intermediate owned parts
    for (0..n) |j| {
        if (owned_flags[j]) allocator.free(parts[j]);
    }

    return ExprResult.owned_val(.{ .bytes = buf });
}

fn formatValueForConcat(allocator: std.mem.Allocator, val: Value) ![]const u8 {
    return switch (val) {
        .integer => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
        .bigint => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
        .float => |f| try std.fmt.allocPrint(allocator, "{d:.6}", .{f}),
        .boolean => |b| try allocator.dupe(u8, if (b) "true" else "false"),
        .null_value => try allocator.dupe(u8, "NULL"),
        .bytes => |s| try allocator.dupe(u8, s),
    };
}

// ── Tests ────────────────────────────────────────────────────────

test "matchLike basic patterns" {
    try std.testing.expect(matchLike("hello", "hello"));
    try std.testing.expect(matchLike("hello", "%"));
    try std.testing.expect(matchLike("hello", "h%"));
    try std.testing.expect(matchLike("hello", "%lo"));
    try std.testing.expect(matchLike("hello", "h_llo"));
    try std.testing.expect(!matchLike("hello", "world"));
    try std.testing.expect(!matchLike("hello", "h_lo"));
}

test "strLower" {
    const r = strLower(std.testing.allocator, .{ .bytes = "Hello World" });
    defer r.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("hello world", r.value.bytes);
    try std.testing.expect(r.owned);
}

test "strUpper" {
    const r = strUpper(std.testing.allocator, .{ .bytes = "Hello World" });
    defer r.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("HELLO WORLD", r.value.bytes);
}

test "strTrim" {
    const r = strTrim(std.testing.allocator, .{ .bytes = "  hello  " });
    defer r.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("hello", r.value.bytes);
}

test "strLength" {
    const r = strLength(.{ .bytes = "hello" });
    try std.testing.expectEqual(@as(i32, 5), r.value.integer);
    try std.testing.expect(!r.owned);
}

test "strSubstring basic" {
    const r = strSubstring(std.testing.allocator, .{ .bytes = "hello world" }, .{ .integer = 1 }, .{ .integer = 5 });
    defer r.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("hello", r.value.bytes);
}

test "strSubstring no length" {
    const r = strSubstring(std.testing.allocator, .{ .bytes = "hello" }, .{ .integer = 3 }, .{ .null_value = {} });
    defer r.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("llo", r.value.bytes);
}

test "null propagation in string functions" {
    const r1 = strLower(std.testing.allocator, .{ .null_value = {} });
    try std.testing.expectEqual(Value.null_value, std.meta.activeTag(r1.value));
    try std.testing.expect(!r1.owned);

    const r2 = strLength(.{ .null_value = {} });
    try std.testing.expectEqual(Value.null_value, std.meta.activeTag(r2.value));
}

test "compareValues integers" {
    try std.testing.expect(compareValues(.{ .integer = 5 }, .eq, .{ .integer = 5 }));
    try std.testing.expect(compareValues(.{ .integer = 3 }, .lt, .{ .integer = 5 }));
    try std.testing.expect(!compareValues(.{ .integer = 5 }, .lt, .{ .integer = 3 }));
}

test "compareValues null returns false" {
    try std.testing.expect(!compareValues(.{ .null_value = {} }, .eq, .{ .integer = 5 }));
    try std.testing.expect(!compareValues(.{ .integer = 5 }, .eq, .{ .null_value = {} }));
}

test "compareValues strings" {
    try std.testing.expect(compareValues(.{ .bytes = "abc" }, .eq, .{ .bytes = "abc" }));
    try std.testing.expect(compareValues(.{ .bytes = "abc" }, .lt, .{ .bytes = "def" }));
}
