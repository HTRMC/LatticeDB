const std = @import("std");
const ast = @import("../parser/ast.zig");
const tuple_mod = @import("../storage/tuple.zig");

const Value = tuple_mod.Value;
const Schema = tuple_mod.Schema;

/// Describes how to produce one output column: either a direct index or a computed expression.
pub const ProjectionColumn = union(enum) {
    /// Direct column from the row
    index: usize,
    /// Computed expression
    expression: *const ast.Expression,
};

/// Format a single projection column from row values to a string.
pub fn formatProjection(
    allocator: std.mem.Allocator,
    proj: ProjectionColumn,
    values: []const Value,
    schema: *const Schema,
    params: ?[]const ast.LiteralValue,
) ![]const u8 {
    switch (proj) {
        .index => |ci| return formatValue(allocator, values[ci]),
        .expression => |expr| {
            const result = evalExprToValue(allocator, expr, schema, values, params);
            defer result.deinit(allocator);
            return formatValue(allocator, result.value);
        },
    }
}

pub fn formatValue(allocator: std.mem.Allocator, val: Value) ![]const u8 {
    return switch (val) {
        .null_value => try allocator.dupe(u8, "NULL"),
        .boolean => |b| try allocator.dupe(u8, if (b) "true" else "false"),
        .integer => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
        .bigint => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
        .float => |f| try std.fmt.allocPrint(allocator, "{d:.6}", .{f}),
        .bytes => |s| try allocator.dupe(u8, s),
    };
}

/// Get the display name for a projection column.
pub fn projectionColumnName(proj: ProjectionColumn, schema: *const Schema, func_name_buf: *[32]u8) []const u8 {
    switch (proj) {
        .index => |ci| return schema.columns[ci].name,
        .expression => |expr| {
            switch (expr.*) {
                .function_call => |fc| {
                    const name = switch (fc.func) {
                        .lower => "lower",
                        .upper => "upper",
                        .trim => "trim",
                        .length => "length",
                        .substring => "substring",
                        .concat => "concat",
                        .coalesce => "coalesce",
                        .nullif => "nullif",
                        .abs => "abs",
                        .round => "round",
                        .ceil => "ceil",
                        .floor => "floor",
                        .mod => "mod",
                        .replace => "replace",
                        .position => "position",
                        .left => "left",
                        .right => "right",
                        .reverse => "reverse",
                        .lpad => "lpad",
                        .rpad => "rpad",
                    };
                    return name;
                },
                .case_expr => return "case",
                .cast_expr => return "cast",
                .arithmetic => return "?expr?",
                .unary_minus => return "?expr?",
                else => return "?expr?",
            }
        },
    }
    _ = func_name_buf;
}

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
        .arithmetic => |ar| return evalArithmetic(allocator, ar, schema, values, params),
        .unary_minus => |um| return evalUnaryMinus(allocator, um, schema, values, params),
        .is_null => |isn| {
            const operand = evalExprToValue(allocator, isn.operand, schema, values, params);
            defer operand.deinit(allocator);
            const result = operand.value == .null_value;
            return ExprResult.borrowed(.{ .boolean = if (isn.negated) !result else result });
        },
        .cast_expr => |ce| return evalCast(allocator, ce, schema, values, params),
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
        .coalesce => {
            for (fc.args) |arg| {
                const res = evalExprToValue(allocator, arg, schema, values, params);
                if (res.value != .null_value) return res;
                res.deinit(allocator);
            }
            return ExprResult.borrowed(.{ .null_value = {} });
        },
        .nullif => {
            const a = evalExprToValue(allocator, fc.args[0], schema, values, params);
            const b = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer b.deinit(allocator);
            if (compareValues(a.value, .eq, b.value)) {
                a.deinit(allocator);
                return ExprResult.borrowed(.{ .null_value = {} });
            }
            return a;
        },
        .abs => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            return numAbs(arg.value);
        },
        .round => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            const precision: i32 = if (fc.args.len > 1) blk: {
                const p = evalExprToValue(allocator, fc.args[1], schema, values, params);
                defer p.deinit(allocator);
                break :blk switch (p.value) {
                    .integer => |i| i,
                    .bigint => |i| @intCast(i),
                    else => 0,
                };
            } else 0;
            return numRound(arg.value, precision);
        },
        .ceil => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            return numCeil(arg.value);
        },
        .floor => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            return numFloor(arg.value);
        },
        .mod => {
            const a = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer a.deinit(allocator);
            const b = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer b.deinit(allocator);
            return numMod(a.value, b.value);
        },
        .replace => {
            const str_arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer str_arg.deinit(allocator);
            const from_arg = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer from_arg.deinit(allocator);
            const to_arg = evalExprToValue(allocator, fc.args[2], schema, values, params);
            defer to_arg.deinit(allocator);
            return strReplace(allocator, str_arg.value, from_arg.value, to_arg.value);
        },
        .position => {
            const substr_arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer substr_arg.deinit(allocator);
            const str_arg = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer str_arg.deinit(allocator);
            return strPosition(substr_arg.value, str_arg.value);
        },
        .left => {
            const str_arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer str_arg.deinit(allocator);
            const n_arg = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer n_arg.deinit(allocator);
            return strLeft(allocator, str_arg.value, n_arg.value);
        },
        .right => {
            const str_arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer str_arg.deinit(allocator);
            const n_arg = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer n_arg.deinit(allocator);
            return strRight(allocator, str_arg.value, n_arg.value);
        },
        .reverse => {
            const arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer arg.deinit(allocator);
            return strReverse(allocator, arg.value);
        },
        .lpad => {
            const str_arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer str_arg.deinit(allocator);
            const len_arg = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer len_arg.deinit(allocator);
            const pad_arg = if (fc.args.len > 2) blk: {
                break :blk evalExprToValue(allocator, fc.args[2], schema, values, params);
            } else ExprResult.borrowed(.{ .bytes = " " });
            defer pad_arg.deinit(allocator);
            return strLpad(allocator, str_arg.value, len_arg.value, pad_arg.value);
        },
        .rpad => {
            const str_arg = evalExprToValue(allocator, fc.args[0], schema, values, params);
            defer str_arg.deinit(allocator);
            const len_arg = evalExprToValue(allocator, fc.args[1], schema, values, params);
            defer len_arg.deinit(allocator);
            const pad_arg = if (fc.args.len > 2) blk: {
                break :blk evalExprToValue(allocator, fc.args[2], schema, values, params);
            } else ExprResult.borrowed(.{ .bytes = " " });
            defer pad_arg.deinit(allocator);
            return strRpad(allocator, str_arg.value, len_arg.value, pad_arg.value);
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
        .is_null => |isn| {
            const operand = evalExprToValue(allocator, isn.operand, schema, values, params);
            defer operand.deinit(allocator);
            const result = operand.value == .null_value;
            return if (isn.negated) !result else result;
        },
        .case_expr, .function_call, .arithmetic, .unary_minus, .cast_expr => {
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

// ── Arithmetic ──────────────────────────────────────────────────

fn evalArithmetic(
    allocator: std.mem.Allocator,
    ar: anytype,
    schema: *const Schema,
    values: []const Value,
    params: ?[]const ast.LiteralValue,
) ExprResult {
    const left_res = evalExprToValue(allocator, ar.left, schema, values, params);
    defer left_res.deinit(allocator);
    const right_res = evalExprToValue(allocator, ar.right, schema, values, params);
    defer right_res.deinit(allocator);

    const lv = left_res.value;
    const rv = right_res.value;

    // NULL propagation
    if (lv == .null_value or rv == .null_value) return ExprResult.borrowed(.{ .null_value = {} });

    // If either side is float, promote to float arithmetic
    const lf = toFloat(lv);
    const rf = toFloat(rv);
    if (lf != null or rf != null) {
        const l = lf orelse toFloatFromInt(lv) orelse return ExprResult.borrowed(.{ .null_value = {} });
        const r = rf orelse toFloatFromInt(rv) orelse return ExprResult.borrowed(.{ .null_value = {} });
        const result: f64 = switch (ar.op) {
            .add => l + r,
            .sub => l - r,
            .mul => l * r,
            .div => if (r == 0.0) return ExprResult.borrowed(.{ .null_value = {} }) else l / r,
        };
        return ExprResult.borrowed(.{ .float = result });
    }

    // Integer arithmetic
    const li = toInt(lv) orelse return ExprResult.borrowed(.{ .null_value = {} });
    const ri = toInt(rv) orelse return ExprResult.borrowed(.{ .null_value = {} });
    const result: i64 = switch (ar.op) {
        .add => li +% ri,
        .sub => li -% ri,
        .mul => li *% ri,
        .div => if (ri == 0) return ExprResult.borrowed(.{ .null_value = {} }) else @divTrunc(li, ri),
    };
    // Fit into i32 if possible
    if (result >= std.math.minInt(i32) and result <= std.math.maxInt(i32)) {
        return ExprResult.borrowed(.{ .integer = @intCast(result) });
    }
    return ExprResult.borrowed(.{ .bigint = result });
}

fn evalUnaryMinus(
    allocator: std.mem.Allocator,
    um: anytype,
    schema: *const Schema,
    values: []const Value,
    params: ?[]const ast.LiteralValue,
) ExprResult {
    const res = evalExprToValue(allocator, um.operand, schema, values, params);
    defer res.deinit(allocator);

    return switch (res.value) {
        .integer => |i| ExprResult.borrowed(.{ .integer = -%i }),
        .bigint => |i| ExprResult.borrowed(.{ .bigint = -%i }),
        .float => |f| ExprResult.borrowed(.{ .float = -f }),
        .null_value => ExprResult.borrowed(.{ .null_value = {} }),
        else => ExprResult.borrowed(.{ .null_value = {} }),
    };
}

fn evalCast(
    allocator: std.mem.Allocator,
    ce: anytype,
    schema: *const Schema,
    values: []const Value,
    params: ?[]const ast.LiteralValue,
) ExprResult {
    const res = evalExprToValue(allocator, ce.operand, schema, values, params);
    defer res.deinit(allocator);
    const val = res.value;

    if (val == .null_value) return ExprResult.borrowed(.{ .null_value = {} });

    switch (ce.target_type) {
        .int, .integer => {
            const i = valToInt(val, allocator) orelse return ExprResult.borrowed(.{ .null_value = {} });
            if (i >= std.math.minInt(i32) and i <= std.math.maxInt(i32)) {
                return ExprResult.borrowed(.{ .integer = @intCast(i) });
            }
            return ExprResult.borrowed(.{ .bigint = i });
        },
        .bigint => {
            const i = valToInt(val, allocator) orelse return ExprResult.borrowed(.{ .null_value = {} });
            return ExprResult.borrowed(.{ .bigint = i });
        },
        .float => {
            const f = valToFloat(val, allocator) orelse return ExprResult.borrowed(.{ .null_value = {} });
            return ExprResult.borrowed(.{ .float = f });
        },
        .boolean => {
            return switch (val) {
                .boolean => ExprResult.borrowed(val),
                .integer => |i| ExprResult.borrowed(.{ .boolean = i != 0 }),
                .bigint => |i| ExprResult.borrowed(.{ .boolean = i != 0 }),
                .bytes => |s| {
                    if (std.ascii.eqlIgnoreCase(s, "true") or std.mem.eql(u8, s, "1")) {
                        return ExprResult.borrowed(.{ .boolean = true });
                    }
                    if (std.ascii.eqlIgnoreCase(s, "false") or std.mem.eql(u8, s, "0")) {
                        return ExprResult.borrowed(.{ .boolean = false });
                    }
                    return ExprResult.borrowed(.{ .null_value = {} });
                },
                else => ExprResult.borrowed(.{ .null_value = {} }),
            };
        },
        .varchar, .text => {
            const str = formatValue(allocator, val) catch return ExprResult.borrowed(.{ .null_value = {} });
            return ExprResult.owned_val(.{ .bytes = str });
        },
    }
}

fn valToInt(val: Value, allocator: std.mem.Allocator) ?i64 {
    _ = allocator;
    return switch (val) {
        .integer => |i| i,
        .bigint => |i| i,
        .float => |f| blk: {
            const truncated = @as(i64, @intFromFloat(@trunc(f)));
            break :blk truncated;
        },
        .boolean => |b| if (b) @as(i64, 1) else @as(i64, 0),
        .bytes => |s| std.fmt.parseInt(i64, s, 10) catch null,
        .null_value => null,
    };
}

fn valToFloat(val: Value, allocator: std.mem.Allocator) ?f64 {
    _ = allocator;
    return switch (val) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        .bigint => |i| @floatFromInt(i),
        .boolean => |b| if (b) @as(f64, 1.0) else @as(f64, 0.0),
        .bytes => |s| std.fmt.parseFloat(f64, s) catch null,
        .null_value => null,
    };
}

fn toFloat(val: Value) ?f64 {
    return switch (val) {
        .float => |f| f,
        else => null,
    };
}

fn toFloatFromInt(val: Value) ?f64 {
    return switch (val) {
        .integer => |i| @floatFromInt(i),
        .bigint => |i| @floatFromInt(i),
        else => null,
    };
}

fn toInt(val: Value) ?i64 {
    return switch (val) {
        .integer => |i| i,
        .bigint => |i| i,
        else => null,
    };
}

// ── Numeric functions ────────────────────────────────────────────

fn numAbs(val: Value) ExprResult {
    return switch (val) {
        .integer => |i| ExprResult.borrowed(.{ .integer = if (i < 0) -%i else i }),
        .bigint => |i| ExprResult.borrowed(.{ .bigint = if (i < 0) -%i else i }),
        .float => |f| ExprResult.borrowed(.{ .float = @abs(f) }),
        .null_value => ExprResult.borrowed(.{ .null_value = {} }),
        else => ExprResult.borrowed(.{ .null_value = {} }),
    };
}

fn numRound(val: Value, precision: i32) ExprResult {
    switch (val) {
        .float => |f| {
            if (precision == 0) {
                return ExprResult.borrowed(.{ .float = @round(f) });
            }
            const p: f64 = std.math.pow(f64, 10.0, @floatFromInt(precision));
            return ExprResult.borrowed(.{ .float = @round(f * p) / p });
        },
        .integer, .bigint => return ExprResult.borrowed(val),
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    }
}

fn numCeil(val: Value) ExprResult {
    return switch (val) {
        .float => |f| ExprResult.borrowed(.{ .float = @ceil(f) }),
        .integer, .bigint => ExprResult.borrowed(val),
        .null_value => ExprResult.borrowed(.{ .null_value = {} }),
        else => ExprResult.borrowed(.{ .null_value = {} }),
    };
}

fn numFloor(val: Value) ExprResult {
    return switch (val) {
        .float => |f| ExprResult.borrowed(.{ .float = @floor(f) }),
        .integer, .bigint => ExprResult.borrowed(val),
        .null_value => ExprResult.borrowed(.{ .null_value = {} }),
        else => ExprResult.borrowed(.{ .null_value = {} }),
    };
}

fn numMod(a: Value, b: Value) ExprResult {
    if (a == .null_value or b == .null_value) return ExprResult.borrowed(.{ .null_value = {} });

    // Float mod
    const af = toFloat(a);
    const bf = toFloat(b);
    if (af != null or bf != null) {
        const lf = af orelse toFloatFromInt(a) orelse return ExprResult.borrowed(.{ .null_value = {} });
        const rf = bf orelse toFloatFromInt(b) orelse return ExprResult.borrowed(.{ .null_value = {} });
        if (rf == 0.0) return ExprResult.borrowed(.{ .null_value = {} });
        return ExprResult.borrowed(.{ .float = @mod(lf, rf) });
    }

    // Integer mod
    const li = toInt(a) orelse return ExprResult.borrowed(.{ .null_value = {} });
    const ri = toInt(b) orelse return ExprResult.borrowed(.{ .null_value = {} });
    if (ri == 0) return ExprResult.borrowed(.{ .null_value = {} });
    const result = @rem(li, ri);
    if (result >= std.math.minInt(i32) and result <= std.math.maxInt(i32)) {
        return ExprResult.borrowed(.{ .integer = @intCast(result) });
    }
    return ExprResult.borrowed(.{ .bigint = result });
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

fn strReplace(allocator: std.mem.Allocator, val: Value, from_val: Value, to_val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const from = switch (from_val) {
        .bytes => |b| b,
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const to = switch (to_val) {
        .bytes => |b| b,
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    if (from.len == 0) {
        const buf = allocator.dupe(u8, s) catch return ExprResult.borrowed(.{ .null_value = {} });
        return ExprResult.owned_val(.{ .bytes = buf });
    }
    // Count occurrences and build result
    var result: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < s.len) {
        if (i + from.len <= s.len and std.mem.eql(u8, s[i..][0..from.len], from)) {
            result.appendSlice(allocator, to) catch return ExprResult.borrowed(.{ .null_value = {} });
            i += from.len;
        } else {
            result.append(allocator, s[i]) catch return ExprResult.borrowed(.{ .null_value = {} });
            i += 1;
        }
    }
    const buf = result.toOwnedSlice(allocator) catch return ExprResult.borrowed(.{ .null_value = {} });
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strPosition(substr_val: Value, str_val: Value) ExprResult {
    const s = switch (str_val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const sub = switch (substr_val) {
        .bytes => |b| b,
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    if (sub.len == 0) return ExprResult.borrowed(.{ .integer = 1 });
    if (std.mem.indexOf(u8, s, sub)) |pos| {
        return ExprResult.borrowed(.{ .integer = @as(i32, @intCast(pos)) + 1 }); // 1-based
    }
    return ExprResult.borrowed(.{ .integer = 0 });
}

fn strLeft(allocator: std.mem.Allocator, val: Value, n_val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const n: usize = switch (n_val) {
        .integer => |i| if (i < 0) 0 else @intCast(i),
        .bigint => |i| if (i < 0) 0 else @intCast(i),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const take = @min(n, s.len);
    const buf = allocator.dupe(u8, s[0..take]) catch return ExprResult.borrowed(.{ .null_value = {} });
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strRight(allocator: std.mem.Allocator, val: Value, n_val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const n: usize = switch (n_val) {
        .integer => |i| if (i < 0) 0 else @intCast(i),
        .bigint => |i| if (i < 0) 0 else @intCast(i),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const take = @min(n, s.len);
    const buf = allocator.dupe(u8, s[s.len - take ..]) catch return ExprResult.borrowed(.{ .null_value = {} });
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strReverse(allocator: std.mem.Allocator, val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const buf = allocator.alloc(u8, s.len) catch return ExprResult.borrowed(.{ .null_value = {} });
    for (s, 0..) |c, i| {
        buf[s.len - 1 - i] = c;
    }
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strLpad(allocator: std.mem.Allocator, val: Value, len_val: Value, pad_val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const target_len: usize = switch (len_val) {
        .integer => |i| if (i < 0) 0 else @intCast(i),
        .bigint => |i| if (i < 0) 0 else @intCast(i),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const pad = switch (pad_val) {
        .bytes => |b| b,
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    if (s.len >= target_len) {
        const buf = allocator.dupe(u8, s[0..target_len]) catch return ExprResult.borrowed(.{ .null_value = {} });
        return ExprResult.owned_val(.{ .bytes = buf });
    }
    if (pad.len == 0) {
        const buf = allocator.dupe(u8, s) catch return ExprResult.borrowed(.{ .null_value = {} });
        return ExprResult.owned_val(.{ .bytes = buf });
    }
    const buf = allocator.alloc(u8, target_len) catch return ExprResult.borrowed(.{ .null_value = {} });
    const pad_needed = target_len - s.len;
    for (0..pad_needed) |i| {
        buf[i] = pad[i % pad.len];
    }
    @memcpy(buf[pad_needed..], s);
    return ExprResult.owned_val(.{ .bytes = buf });
}

fn strRpad(allocator: std.mem.Allocator, val: Value, len_val: Value, pad_val: Value) ExprResult {
    const s = switch (val) {
        .bytes => |b| b,
        .null_value => return ExprResult.borrowed(.{ .null_value = {} }),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const target_len: usize = switch (len_val) {
        .integer => |i| if (i < 0) 0 else @intCast(i),
        .bigint => |i| if (i < 0) 0 else @intCast(i),
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    const pad = switch (pad_val) {
        .bytes => |b| b,
        else => return ExprResult.borrowed(.{ .null_value = {} }),
    };
    if (s.len >= target_len) {
        const buf = allocator.dupe(u8, s[0..target_len]) catch return ExprResult.borrowed(.{ .null_value = {} });
        return ExprResult.owned_val(.{ .bytes = buf });
    }
    if (pad.len == 0) {
        const buf = allocator.dupe(u8, s) catch return ExprResult.borrowed(.{ .null_value = {} });
        return ExprResult.owned_val(.{ .bytes = buf });
    }
    const buf = allocator.alloc(u8, target_len) catch return ExprResult.borrowed(.{ .null_value = {} });
    @memcpy(buf[0..s.len], s);
    const pad_needed = target_len - s.len;
    for (0..pad_needed) |i| {
        buf[s.len + i] = pad[i % pad.len];
    }
    return ExprResult.owned_val(.{ .bytes = buf });
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

test "numAbs" {
    try std.testing.expectEqual(@as(i32, 5), numAbs(.{ .integer = -5 }).value.integer);
    try std.testing.expectEqual(@as(i32, 5), numAbs(.{ .integer = 5 }).value.integer);
    try std.testing.expectEqual(@as(f64, 3.14), numAbs(.{ .float = -3.14 }).value.float);
    try std.testing.expect(numAbs(.{ .null_value = {} }).value == .null_value);
}

test "numRound" {
    try std.testing.expectEqual(@as(f64, 3.0), numRound(.{ .float = 3.14 }, 0).value.float);
    try std.testing.expectEqual(@as(f64, 3.1), numRound(.{ .float = 3.14 }, 1).value.float);
    try std.testing.expectEqual(@as(i32, 5), numRound(.{ .integer = 5 }, 0).value.integer);
}

test "numCeil and numFloor" {
    try std.testing.expectEqual(@as(f64, 4.0), numCeil(.{ .float = 3.2 }).value.float);
    try std.testing.expectEqual(@as(f64, 3.0), numFloor(.{ .float = 3.8 }).value.float);
    try std.testing.expectEqual(@as(f64, -3.0), numCeil(.{ .float = -3.2 }).value.float);
    try std.testing.expectEqual(@as(f64, -4.0), numFloor(.{ .float = -3.2 }).value.float);
}

test "numMod" {
    try std.testing.expectEqual(@as(i32, 1), numMod(.{ .integer = 7 }, .{ .integer = 3 }).value.integer);
    try std.testing.expectEqual(@as(i32, 0), numMod(.{ .integer = 6 }, .{ .integer = 3 }).value.integer);
    try std.testing.expect(numMod(.{ .integer = 5 }, .{ .integer = 0 }).value == .null_value);
    try std.testing.expect(numMod(.{ .null_value = {} }, .{ .integer = 3 }).value == .null_value);
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

test "arithmetic integer add" {
    const empty_schema: Schema = .{ .columns = &.{} };
    const expr = ast.Expression{ .arithmetic = .{
        .left = &ast.Expression{ .literal = .{ .integer = 3 } },
        .op = .add,
        .right = &ast.Expression{ .literal = .{ .integer = 7 } },
    } };
    const r = evalExprToValue(std.testing.allocator, &expr, &empty_schema, &.{}, null);
    try std.testing.expectEqual(@as(i32, 10), r.value.integer);
}

test "arithmetic integer mul" {
    const empty_schema: Schema = .{ .columns = &.{} };
    const expr = ast.Expression{ .arithmetic = .{
        .left = &ast.Expression{ .literal = .{ .integer = 4 } },
        .op = .mul,
        .right = &ast.Expression{ .literal = .{ .integer = 5 } },
    } };
    const r = evalExprToValue(std.testing.allocator, &expr, &empty_schema, &.{}, null);
    try std.testing.expectEqual(@as(i32, 20), r.value.integer);
}

test "arithmetic float division" {
    const empty_schema: Schema = .{ .columns = &.{} };
    const expr = ast.Expression{ .arithmetic = .{
        .left = &ast.Expression{ .literal = .{ .float = 10.0 } },
        .op = .div,
        .right = &ast.Expression{ .literal = .{ .float = 4.0 } },
    } };
    const r = evalExprToValue(std.testing.allocator, &expr, &empty_schema, &.{}, null);
    try std.testing.expectEqual(@as(f64, 2.5), r.value.float);
}

test "arithmetic null propagation" {
    const empty_schema: Schema = .{ .columns = &.{} };
    const expr = ast.Expression{ .arithmetic = .{
        .left = &ast.Expression{ .literal = .{ .integer = 5 } },
        .op = .add,
        .right = &ast.Expression{ .literal = .{ .null_value = {} } },
    } };
    const r = evalExprToValue(std.testing.allocator, &expr, &empty_schema, &.{}, null);
    try std.testing.expect(r.value == .null_value);
}

test "arithmetic division by zero" {
    const empty_schema: Schema = .{ .columns = &.{} };
    const expr = ast.Expression{ .arithmetic = .{
        .left = &ast.Expression{ .literal = .{ .integer = 5 } },
        .op = .div,
        .right = &ast.Expression{ .literal = .{ .integer = 0 } },
    } };
    const r = evalExprToValue(std.testing.allocator, &expr, &empty_schema, &.{}, null);
    try std.testing.expect(r.value == .null_value);
}

test "arithmetic int-float promotion" {
    const empty_schema: Schema = .{ .columns = &.{} };
    const expr = ast.Expression{ .arithmetic = .{
        .left = &ast.Expression{ .literal = .{ .integer = 3 } },
        .op = .mul,
        .right = &ast.Expression{ .literal = .{ .float = 2.5 } },
    } };
    const r = evalExprToValue(std.testing.allocator, &expr, &empty_schema, &.{}, null);
    try std.testing.expectEqual(@as(f64, 7.5), r.value.float);
}

test "unary minus" {
    const empty_schema: Schema = .{ .columns = &.{} };
    const expr = ast.Expression{ .unary_minus = .{
        .operand = &ast.Expression{ .literal = .{ .integer = 42 } },
    } };
    const r = evalExprToValue(std.testing.allocator, &expr, &empty_schema, &.{}, null);
    try std.testing.expectEqual(@as(i32, -42), r.value.integer);
}
