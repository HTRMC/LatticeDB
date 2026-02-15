const std = @import("std");
const vector_mod = @import("vector.zig");
const tuple_mod = @import("../storage/tuple.zig");
const ast = @import("../parser/ast.zig");

const DataChunk = vector_mod.DataChunk;
const ColumnVector = vector_mod.ColumnVector;
const SelectionVector = vector_mod.SelectionVector;
const VECTOR_SIZE = vector_mod.VECTOR_SIZE;
const Value = tuple_mod.Value;
const Schema = tuple_mod.Schema;
const ColumnType = tuple_mod.ColumnType;

/// Describes a simple predicate that can be evaluated with SIMD: column <op> literal.
pub const SimplePredicate = struct {
    col_idx: usize,
    col_type: ColumnType,
    op: ast.CompOp,
    /// The literal value to compare against.
    literal: Value,
};

/// Try to detect a BETWEEN expression as two SIMD-eligible predicates: col >= low AND col <= high.
/// Returns null if the expression is not a BETWEEN on a numeric column with literal bounds.
pub fn detectBetweenPredicate(expr: *const ast.Expression, schema: *const Schema) ?[2]SimplePredicate {
    switch (expr.*) {
        .between_expr => |b| {
            // value must be a column ref
            const col_name = switch (b.value.*) {
                .column_ref => |name| name,
                else => return null,
            };
            const col_idx = findColumnIndex(schema, col_name) orelse return null;
            const col_type = schema.columns[col_idx].col_type;

            // Only numeric types for SIMD
            switch (col_type) {
                .integer, .bigint, .float => {},
                else => return null,
            }

            // low and high must be literals
            const low_lit = switch (b.low.*) {
                .literal => |l| l,
                else => return null,
            };
            const high_lit = switch (b.high.*) {
                .literal => |l| l,
                else => return null,
            };

            const low_val = literalToValue(low_lit, col_type) orelse return null;
            const high_val = literalToValue(high_lit, col_type) orelse return null;

            return .{
                .{ .col_idx = col_idx, .col_type = col_type, .op = .gte, .literal = low_val },
                .{ .col_idx = col_idx, .col_type = col_type, .op = .lte, .literal = high_val },
            };
        },
        else => return null,
    }
}

/// Try to detect a simple SIMD-eligible predicate from a WHERE expression.
/// Returns null if the expression is too complex for the SIMD path.
pub fn detectSimplePredicate(expr: *const ast.Expression, schema: *const Schema) ?SimplePredicate {
    switch (expr.*) {
        .comparison => |cmp| {
            // Pattern: column_ref <op> literal  or  literal <op> column_ref
            const col_info = resolveColumnAndLiteral(cmp.left, cmp.right, schema);
            if (col_info) |info| {
                // Only numeric types for SIMD
                switch (info.col_type) {
                    .integer, .bigint, .float => return .{
                        .col_idx = info.col_idx,
                        .col_type = info.col_type,
                        .op = if (info.reversed) flipOp(cmp.op) else cmp.op,
                        .literal = info.lit_val,
                    },
                    else => return null,
                }
            }
            return null;
        },
        else => return null,
    }
}

const ColumnLiteralInfo = struct {
    col_idx: usize,
    col_type: ColumnType,
    lit_val: Value,
    reversed: bool, // true if literal was on the left
};

fn resolveColumnAndLiteral(left: *const ast.Expression, right: *const ast.Expression, schema: *const Schema) ?ColumnLiteralInfo {
    // Try: column_ref <op> literal
    if (left.* == .column_ref and right.* == .literal) {
        const col_name = left.column_ref;
        if (findColumnIndex(schema, col_name)) |idx| {
            if (literalToValue(right.literal, schema.columns[idx].col_type)) |val| {
                return .{ .col_idx = idx, .col_type = schema.columns[idx].col_type, .lit_val = val, .reversed = false };
            }
        }
    }
    // Try: literal <op> column_ref (reversed)
    if (left.* == .literal and right.* == .column_ref) {
        const col_name = right.column_ref;
        if (findColumnIndex(schema, col_name)) |idx| {
            if (literalToValue(left.literal, schema.columns[idx].col_type)) |val| {
                return .{ .col_idx = idx, .col_type = schema.columns[idx].col_type, .lit_val = val, .reversed = true };
            }
        }
    }
    return null;
}

fn findColumnIndex(schema: *const Schema, name: []const u8) ?usize {
    for (schema.columns, 0..) |col, i| {
        if (std.ascii.eqlIgnoreCase(col.name, name)) return i;
    }
    return null;
}

fn literalToValue(lit: ast.LiteralValue, target_type: ColumnType) ?Value {
    switch (lit) {
        .integer => |v| {
            return switch (target_type) {
                .integer => .{ .integer = @as(i32, @intCast(v)) },
                .bigint => .{ .bigint = v },
                .float => .{ .float = @as(f64, @floatFromInt(v)) },
                else => null,
            };
        },
        .float => |v| {
            return switch (target_type) {
                .float => .{ .float = v },
                else => null,
            };
        },
        else => return null,
    }
}

fn flipOp(op: ast.CompOp) ast.CompOp {
    return switch (op) {
        .eq => .eq,
        .neq => .neq,
        .lt => .gt,
        .gt => .lt,
        .lte => .gte,
        .gte => .lte,
    };
}

// ── SIMD filter for i32 ─────────────────────────────────────────

const SimdWidth_i32 = 8;
const Vec8i32 = @Vector(SimdWidth_i32, i32);

fn simdCompare_i32(data: Vec8i32, splat: Vec8i32, op: ast.CompOp) @Vector(SimdWidth_i32, bool) {
    return switch (op) {
        .eq => data == splat,
        .neq => data != splat,
        .lt => data < splat,
        .gt => data > splat,
        .lte => data <= splat,
        .gte => data >= splat,
    };
}

// ── SIMD filter for i64 ─────────────────────────────────────────

const SimdWidth_i64 = 4;
const Vec4i64 = @Vector(SimdWidth_i64, i64);

fn simdCompare_i64(data: Vec4i64, splat: Vec4i64, op: ast.CompOp) @Vector(SimdWidth_i64, bool) {
    return switch (op) {
        .eq => data == splat,
        .neq => data != splat,
        .lt => data < splat,
        .gt => data > splat,
        .lte => data <= splat,
        .gte => data >= splat,
    };
}

// ── SIMD filter for f64 ─────────────────────────────────────────

const SimdWidth_f64 = 4;
const Vec4f64 = @Vector(SimdWidth_f64, f64);

fn simdCompare_f64(data: Vec4f64, splat: Vec4f64, op: ast.CompOp) @Vector(SimdWidth_f64, bool) {
    return switch (op) {
        .eq => data == splat,
        .neq => data != splat,
        .lt => data < splat,
        .gt => data > splat,
        .lte => data <= splat,
        .gte => data >= splat,
    };
}

/// Apply a SIMD filter that intersects with an existing selection vector.
/// Used for the second pass of BETWEEN (col >= low already applied, now col <= high).
pub fn filterSimdWithSel(chunk: *DataChunk, pred: SimplePredicate) SelectionVector {
    var result = SelectionVector{};
    const existing_sel = chunk.sel orelse return filterSimd(chunk, pred);

    // Apply pred only to rows that passed the first filter
    const col = &chunk.columns[pred.col_idx];
    for (existing_sel.indices[0..existing_sel.len]) |row_idx| {
        if (col.isNull(row_idx)) continue;
        const matches = switch (pred.col_type) {
            .integer => scalarCompare_i32(col.data.integers[row_idx], pred.literal.integer, pred.op),
            .bigint => scalarCompare_i64(col.data.bigints[row_idx], pred.literal.bigint, pred.op),
            .float => scalarCompare_f64(col.data.floats[row_idx], pred.literal.float, pred.op),
            else => false,
        };
        if (matches) {
            result.indices[result.len] = row_idx;
            result.len += 1;
        }
    }
    return result;
}

/// Apply a SIMD filter for a simple numeric predicate on a DataChunk.
/// Produces a SelectionVector with matching row indices.
pub fn filterSimd(chunk: *DataChunk, pred: SimplePredicate) SelectionVector {
    var sel = SelectionVector{};
    const count = chunk.count;
    const col = &chunk.columns[pred.col_idx];

    switch (pred.col_type) {
        .integer => {
            const lit_val = pred.literal.integer;
            const splat: Vec8i32 = @splat(lit_val);
            const data = &col.data.integers;

            // Process SIMD-width chunks
            var i: u16 = 0;
            while (i + SimdWidth_i32 <= count) {
                // Check nulls in this batch — if any null, fall back to scalar for this batch
                if (hasNullInRange(col, i, SimdWidth_i32)) {
                    // Scalar fallback for this batch
                    for (0..SimdWidth_i32) |j| {
                        const row = i + @as(u16, @intCast(j));
                        if (!col.isNull(row)) {
                            if (scalarCompare_i32(data[row], lit_val, pred.op)) {
                                sel.indices[sel.len] = row;
                                sel.len += 1;
                            }
                        }
                    }
                } else {
                    const vec: Vec8i32 = data[i..][0..SimdWidth_i32].*;
                    const mask: [SimdWidth_i32]bool = simdCompare_i32(vec, splat, pred.op);
                    for (0..SimdWidth_i32) |j| {
                        if (mask[j]) {
                            sel.indices[sel.len] = i + @as(u16, @intCast(j));
                            sel.len += 1;
                        }
                    }
                }
                i += SimdWidth_i32;
            }
            // Scalar tail
            while (i < count) : (i += 1) {
                if (!col.isNull(i) and scalarCompare_i32(data[i], lit_val, pred.op)) {
                    sel.indices[sel.len] = i;
                    sel.len += 1;
                }
            }
        },
        .bigint => {
            const lit_val = pred.literal.bigint;
            const splat: Vec4i64 = @splat(lit_val);
            const data = &col.data.bigints;

            var i: u16 = 0;
            while (i + SimdWidth_i64 <= count) {
                if (hasNullInRange(col, i, SimdWidth_i64)) {
                    for (0..SimdWidth_i64) |j| {
                        const row = i + @as(u16, @intCast(j));
                        if (!col.isNull(row)) {
                            if (scalarCompare_i64(data[row], lit_val, pred.op)) {
                                sel.indices[sel.len] = row;
                                sel.len += 1;
                            }
                        }
                    }
                } else {
                    const vec: Vec4i64 = data[i..][0..SimdWidth_i64].*;
                    const mask: [SimdWidth_i64]bool = simdCompare_i64(vec, splat, pred.op);
                    for (0..SimdWidth_i64) |j| {
                        if (mask[j]) {
                            sel.indices[sel.len] = i + @as(u16, @intCast(j));
                            sel.len += 1;
                        }
                    }
                }
                i += SimdWidth_i64;
            }
            while (i < count) : (i += 1) {
                if (!col.isNull(i) and scalarCompare_i64(data[i], lit_val, pred.op)) {
                    sel.indices[sel.len] = i;
                    sel.len += 1;
                }
            }
        },
        .float => {
            const lit_val = pred.literal.float;
            const splat: Vec4f64 = @splat(lit_val);
            const data = &col.data.floats;

            var i: u16 = 0;
            while (i + SimdWidth_f64 <= count) {
                if (hasNullInRange(col, i, SimdWidth_f64)) {
                    for (0..SimdWidth_f64) |j| {
                        const row = i + @as(u16, @intCast(j));
                        if (!col.isNull(row)) {
                            if (scalarCompare_f64(data[row], lit_val, pred.op)) {
                                sel.indices[sel.len] = row;
                                sel.len += 1;
                            }
                        }
                    }
                } else {
                    const vec: Vec4f64 = data[i..][0..SimdWidth_f64].*;
                    const mask: [SimdWidth_f64]bool = simdCompare_f64(vec, splat, pred.op);
                    for (0..SimdWidth_f64) |j| {
                        if (mask[j]) {
                            sel.indices[sel.len] = i + @as(u16, @intCast(j));
                            sel.len += 1;
                        }
                    }
                }
                i += SimdWidth_f64;
            }
            while (i < count) : (i += 1) {
                if (!col.isNull(i) and scalarCompare_f64(data[i], lit_val, pred.op)) {
                    sel.indices[sel.len] = i;
                    sel.len += 1;
                }
            }
        },
        else => {
            // Non-numeric: should not reach here (detectSimplePredicate guards this)
        },
    }

    return sel;
}

/// General (non-SIMD) filter: evaluate a full WHERE expression per active row.
/// Still benefits from columnar layout (cache-friendly reads).
pub fn filterGeneral(
    chunk: *DataChunk,
    expr: *const ast.Expression,
    schema: *const Schema,
) SelectionVector {
    var sel = SelectionVector{};
    const count = chunk.count;

    // If there's already a selection vector, iterate only those rows
    if (chunk.sel) |existing_sel| {
        for (existing_sel.indices[0..existing_sel.len]) |row| {
            if (evalExprOnChunk(expr, chunk, schema, row)) {
                sel.indices[sel.len] = row;
                sel.len += 1;
            }
        }
    } else {
        var i: u16 = 0;
        while (i < count) : (i += 1) {
            if (evalExprOnChunk(expr, chunk, schema, i)) {
                sel.indices[sel.len] = i;
                sel.len += 1;
            }
        }
    }

    return sel;
}

/// Evaluate an expression against a single row in a DataChunk.
fn evalExprOnChunk(expr: *const ast.Expression, chunk: *DataChunk, schema: *const Schema, row: u16) bool {
    switch (expr.*) {
        .comparison => |cmp| {
            const left_val = resolveExprValueFromChunk(cmp.left, chunk, schema, row);
            const right_val = resolveExprValueFromChunk(cmp.right, chunk, schema, row);
            return compareValues(left_val, cmp.op, right_val);
        },
        .and_expr => |a| {
            return evalExprOnChunk(a.left, chunk, schema, row) and evalExprOnChunk(a.right, chunk, schema, row);
        },
        .or_expr => |o| {
            return evalExprOnChunk(o.left, chunk, schema, row) or evalExprOnChunk(o.right, chunk, schema, row);
        },
        .not_expr => |n| {
            return !evalExprOnChunk(n.operand, chunk, schema, row);
        },
        .between_expr => |b| {
            const val = resolveExprValueFromChunk(b.value, chunk, schema, row);
            const low = resolveExprValueFromChunk(b.low, chunk, schema, row);
            const high = resolveExprValueFromChunk(b.high, chunk, schema, row);
            return compareValues(val, .gte, low) and compareValues(val, .lte, high);
        },
        .like_expr => |l| {
            const val = resolveExprValueFromChunk(l.value, chunk, schema, row);
            const pat = resolveExprValueFromChunk(l.pattern, chunk, schema, row);
            const text = switch (val) {
                .bytes => |s| s,
                else => return false,
            };
            const pattern = switch (pat) {
                .bytes => |s| s,
                else => return false,
            };
            return matchLike(text, pattern);
        },
        .in_list => |il| {
            const val = resolveExprValueFromChunk(il.value, chunk, schema, row);
            for (il.items) |item| {
                const item_val = resolveExprValueFromChunk(item, chunk, schema, row);
                if (compareValues(val, .eq, item_val)) return true;
            }
            return false;
        },
        .literal => |lit| {
            return switch (lit) {
                .boolean => |b| b,
                .null_value => false,
                .integer => |i| i != 0,
                else => true,
            };
        },
        .column_ref => return true,
        // Subqueries and exists not supported in vectorized path
        .in_subquery, .exists_subquery, .qualified_ref => return true,
    }
}

fn resolveExprValueFromChunk(expr: *const ast.Expression, chunk: *DataChunk, schema: *const Schema, row: u16) Value {
    switch (expr.*) {
        .column_ref => |name| {
            if (findColumnIndex(schema, name)) |idx| {
                return chunk.columns[idx].getValue(row);
            }
            return .{ .null_value = {} };
        },
        .qualified_ref => |qr| {
            if (findColumnIndex(schema, qr.column)) |idx| {
                return chunk.columns[idx].getValue(row);
            }
            return .{ .null_value = {} };
        },
        .literal => |lit| return litToStorageValue(lit),
        else => return .{ .null_value = {} },
    }
}

fn litToStorageValue(lit: ast.LiteralValue) Value {
    return switch (lit) {
        .integer => |v| blk: {
            if (v >= std.math.minInt(i32) and v <= std.math.maxInt(i32)) {
                break :blk .{ .integer = @intCast(v) };
            }
            break :blk .{ .bigint = v };
        },
        .float => |v| .{ .float = v },
        .string => |v| .{ .bytes = v },
        .boolean => |v| .{ .boolean = v },
        .null_value => .{ .null_value = {} },
        .parameter => .{ .null_value = {} },
    };
}

fn compareValues(left: Value, op: ast.CompOp, right: Value) bool {
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

// ── Scalar helpers ───────────────────────────────────────────────

fn scalarCompare_i32(a: i32, b: i32, op: ast.CompOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .gt => a > b,
        .lte => a <= b,
        .gte => a >= b,
    };
}

fn scalarCompare_i64(a: i64, b: i64, op: ast.CompOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .gt => a > b,
        .lte => a <= b,
        .gte => a >= b,
    };
}

fn scalarCompare_f64(a: f64, b: f64, op: ast.CompOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .gt => a > b,
        .lte => a <= b,
        .gte => a >= b,
    };
}

fn hasNullInRange(col: *const ColumnVector, start: u16, comptime width: u16) bool {
    // Fast path: check null mask bytes directly instead of bit-by-bit.
    // If the range is byte-aligned and fits in whole bytes, use a single word check.
    const start_byte = start / 8;
    const start_bit = @as(u3, @intCast(start % 8));

    if (start_bit == 0 and width == 8) {
        // Perfectly aligned 8-row batch: single byte check
        return col.null_mask[start_byte] != 0;
    } else if (start_bit == 0 and width == 4) {
        // Aligned 4-row batch: check low nibble
        return (col.null_mask[start_byte] & 0x0F) != 0;
    }

    // General case: bit-by-bit fallback
    var i: u16 = 0;
    while (i < width) : (i += 1) {
        if (col.isNull(start + i)) return true;
    }
    return false;
}

/// SQL LIKE pattern matching (same algorithm as executor.zig)
fn matchLike(text: []const u8, pattern: []const u8) bool {
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

// ── Tests ────────────────────────────────────────────────────────

test "detectSimplePredicate column > literal" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .text, .max_length = 0, .nullable = true },
        },
    };

    // Build AST: id > 100
    const col_expr = ast.Expression{ .column_ref = "id" };
    const lit_expr = ast.Expression{ .literal = .{ .integer = 100 } };
    const cmp_expr = ast.Expression{ .comparison = .{
        .left = &col_expr,
        .op = .gt,
        .right = &lit_expr,
    } };

    const pred = detectSimplePredicate(&cmp_expr, &schema);
    try std.testing.expect(pred != null);
    try std.testing.expectEqual(@as(usize, 0), pred.?.col_idx);
    try std.testing.expectEqual(ast.CompOp.gt, pred.?.op);
    try std.testing.expectEqual(@as(i32, 100), pred.?.literal.integer);
}

test "detectSimplePredicate reversed literal < column" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "val", .col_type = .float, .max_length = 0, .nullable = false },
        },
    };

    const lit_expr = ast.Expression{ .literal = .{ .float = 3.14 } };
    const col_expr = ast.Expression{ .column_ref = "val" };
    const cmp_expr = ast.Expression{ .comparison = .{
        .left = &lit_expr,
        .op = .lt,
        .right = &col_expr,
    } };

    const pred = detectSimplePredicate(&cmp_expr, &schema);
    try std.testing.expect(pred != null);
    // literal < column → column > literal
    try std.testing.expectEqual(ast.CompOp.gt, pred.?.op);
}

test "detectSimplePredicate rejects string column" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "name", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    const col_expr = ast.Expression{ .column_ref = "name" };
    const lit_expr = ast.Expression{ .literal = .{ .string = "foo" } };
    const cmp_expr = ast.Expression{ .comparison = .{
        .left = &col_expr,
        .op = .eq,
        .right = &lit_expr,
    } };

    const pred = detectSimplePredicate(&cmp_expr, &schema);
    try std.testing.expect(pred == null);
}

test "compareValues integer vs bigint out of i32 range" {
    // Bug: comparing .integer(42) < .bigint(3_000_000_000) panics
    // because @intCast truncates the bigint to i32.
    // After fix: should widen i32 → i64 and compare correctly.
    const left = Value{ .integer = 42 };
    const right = Value{ .bigint = 3_000_000_000 };

    // 42 < 3 billion → true
    try std.testing.expect(compareValues(left, .lt, right));
    // 42 == 3 billion → false
    try std.testing.expect(!compareValues(left, .eq, right));
    // 42 > 3 billion → false
    try std.testing.expect(!compareValues(left, .gt, right));

    // Reverse: bigint vs integer
    try std.testing.expect(compareValues(right, .gt, left));

    // Negative bigint outside i32 range
    const neg = Value{ .bigint = -3_000_000_000 };
    try std.testing.expect(compareValues(neg, .lt, left));
    try std.testing.expect(!compareValues(neg, .gt, left));
}

test "filterSimd i32 basic" {
    const schema = tuple_mod.Schema{
        .columns = &.{
            .{ .name = "x", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var chunk = try DataChunk.initFromSchema(std.testing.allocator, &schema);
    defer chunk.deinit();

    // Fill 16 rows: 0, 1, 2, ..., 15
    for (0..16) |i| {
        chunk.columns[0].data.integers[i] = @intCast(i);
    }
    chunk.count = 16;

    const pred = SimplePredicate{
        .col_idx = 0,
        .col_type = .integer,
        .op = .gt,
        .literal = .{ .integer = 10 },
    };

    const sel = filterSimd(&chunk, pred);
    // Should match 11, 12, 13, 14, 15 = 5 rows
    try std.testing.expectEqual(@as(u16, 5), sel.len);
    for (sel.indices[0..sel.len]) |idx| {
        try std.testing.expect(chunk.columns[0].data.integers[idx] > 10);
    }
}
