const std = @import("std");
const vector_mod = @import("vector.zig");
const vec_scan_mod = @import("vec_scan.zig");
const vec_filter_mod = @import("vec_filter.zig");
const vec_agg_mod = @import("vec_agg.zig");
const tuple_mod = @import("../storage/tuple.zig");
const table_mod = @import("../storage/table.zig");
const catalog_mod = @import("../storage/catalog.zig");
const mvcc_mod = @import("../storage/mvcc.zig");
const ast = @import("../parser/ast.zig");

const DataChunk = vector_mod.DataChunk;
const SelectionVector = vector_mod.SelectionVector;
const VECTOR_SIZE = vector_mod.VECTOR_SIZE;
const Value = tuple_mod.Value;
const Schema = tuple_mod.Schema;
const ColumnType = tuple_mod.ColumnType;
const Table = table_mod.Table;
const Catalog = catalog_mod.Catalog;
const Transaction = mvcc_mod.Transaction;
const TransactionManager = mvcc_mod.TransactionManager;

const ExecError = @import("executor.zig").ExecError;
const ExecResult = @import("executor.zig").ExecResult;
const ResultRow = @import("executor.zig").ResultRow;

/// Check if a SELECT query is eligible for the vectorized execution path.
/// Currently excludes: joins, DISTINCT, subqueries in WHERE, qualified column refs in WHERE.
pub fn canUseVectorized(sel: ast.Select) bool {
    // No joins
    if (sel.joins) |join_list| {
        if (join_list.len > 0) return false;
    }

    // No DISTINCT
    if (sel.distinct) return false;

    // Check WHERE for subqueries
    if (sel.where_clause) |where| {
        if (hasSubquery(where)) return false;
    }

    return true;
}

fn hasSubquery(expr: *const ast.Expression) bool {
    switch (expr.*) {
        .in_subquery, .exists_subquery => return true,
        .and_expr => |a| return hasSubquery(a.left) or hasSubquery(a.right),
        .or_expr => |o| return hasSubquery(o.left) or hasSubquery(o.right),
        .not_expr => |n| return hasSubquery(n.operand),
        else => return false,
    }
}

/// A row collected with native Value types (deferred materialization).
const CollectedRow = struct {
    values: []Value,
};

/// Execute a SELECT query using the vectorized engine.
pub fn execSelectVectorized(
    allocator: std.mem.Allocator,
    sel: ast.Select,
    table: *Table,
    schema: *const Schema,
    txn: ?*const Transaction,
) ExecError!ExecResult {
    // Check for aggregates
    var has_aggregates = false;
    for (sel.columns) |col| {
        if (col == .aggregate) {
            has_aggregates = true;
            break;
        }
    }

    if (has_aggregates or sel.group_by != null) {
        return execVectorizedAggregate(allocator, sel, table, schema, txn);
    }

    // Resolve column indices for projection
    const col_indices = resolveSelectColumns(allocator, sel.columns, schema) orelse return ExecError.ColumnNotFound;
    defer allocator.free(col_indices);

    // Build column name headers
    const col_names = allocator.alloc([]const u8, col_indices.len) catch return ExecError.OutOfMemory;
    for (col_indices, 0..) |ci, i| {
        const alias_name = if (sel.aliases) |aliases| (if (i < aliases.len) aliases[i] else null) else null;
        const name_src = alias_name orelse schema.columns[ci].name;
        col_names[i] = allocator.dupe(u8, name_src) catch {
            for (col_names[0..i]) |cn| allocator.free(cn);
            allocator.free(col_names);
            return ExecError.OutOfMemory;
        };
    }

    // Detect SIMD-eligible predicates
    const simd_pred = if (sel.where_clause) |where|
        vec_filter_mod.detectSimplePredicate(where, schema)
    else
        null;

    // Detect BETWEEN for dual-SIMD path
    const between_preds = if (sel.where_clause) |where|
        vec_filter_mod.detectBetweenPredicate(where, schema)
    else
        null;

    // Determine if general filter is still needed after SIMD
    const needs_general_filter = if (sel.where_clause) |where| blk: {
        if (simd_pred != null and where.* == .comparison) break :blk false;
        if (between_preds != null and where.* == .between_expr) break :blk false;
        break :blk true;
    } else false;

    // Scan and filter
    var scan = vec_scan_mod.VecSeqScan.init(allocator, table, txn) catch {
        for (col_names) |cn| allocator.free(cn);
        allocator.free(col_names);
        return ExecError.StorageError;
    };
    defer scan.deinit();

    const limit: ?usize = if (sel.limit) |l| @intCast(l) else null;

    // === ORDER BY path: deferred materialization with Value-based sorting ===
    if (sel.order_by) |order_clauses| {
        return execVectorizedWithOrderBy(
            allocator,
            &scan,
            sel,
            col_indices,
            col_names,
            schema,
            order_clauses,
            limit,
            simd_pred,
            between_preds,
            needs_general_filter,
        );
    }

    // === No ORDER BY: direct materialization from chunk to ResultRow ===
    var rows: std.ArrayList(ResultRow) = .empty;
    errdefer {
        for (rows.items) |row| {
            for (row.values) |v| allocator.free(v);
            allocator.free(row.values);
        }
        rows.deinit(allocator);
    }

    while (scan.next() catch {
        for (col_names) |cn| allocator.free(cn);
        allocator.free(col_names);
        return ExecError.StorageError;
    }) |chunk| {
        // Apply SIMD filter(s)
        if (simd_pred) |pred| {
            chunk.sel = vec_filter_mod.filterSimd(chunk, pred);
        }

        if (between_preds) |preds| {
            chunk.sel = vec_filter_mod.filterSimd(chunk, preds[0]);
            chunk.sel = vec_filter_mod.filterSimdWithSel(chunk, preds[1]);
        }

        if (needs_general_filter) {
            if (sel.where_clause) |where| {
                chunk.sel = vec_filter_mod.filterGeneral(chunk, where, schema);
            }
        }

        // Directly format matching rows to strings (no intermediate Value collection)
        try materializeChunkDirect(allocator, chunk, col_indices, &rows);

        // Early termination for LIMIT
        if (limit) |lim| {
            if (rows.items.len >= lim) break;
        }
    }

    // LIMIT
    if (limit) |lim| {
        const actual = @min(lim, rows.items.len);
        for (rows.items[actual..]) |row| {
            for (row.values) |v| allocator.free(v);
            allocator.free(row.values);
        }
        rows.shrinkRetainingCapacity(actual);
    }

    return .{ .rows = .{
        .columns = col_names,
        .rows = rows.toOwnedSlice(allocator) catch {
            for (col_names) |cn| allocator.free(cn);
            allocator.free(col_names);
            return ExecError.OutOfMemory;
        },
    } };
}

/// Direct materialization: format chunk rows straight to ResultRow strings.
fn materializeChunkDirect(
    allocator: std.mem.Allocator,
    chunk: *DataChunk,
    col_indices: []const usize,
    rows: *std.ArrayList(ResultRow),
) ExecError!void {
    if (chunk.sel) |sel| {
        for (sel.indices[0..sel.len]) |row_idx| {
            const formatted = allocator.alloc([]const u8, col_indices.len) catch return ExecError.OutOfMemory;
            for (col_indices, 0..) |ci, fi| {
                const val = chunk.columns[ci].getValue(row_idx);
                formatted[fi] = formatValue(allocator, val) catch {
                    for (formatted[0..fi]) |f| allocator.free(f);
                    allocator.free(formatted);
                    return ExecError.OutOfMemory;
                };
            }
            rows.append(allocator, .{ .values = formatted }) catch {
                for (formatted) |f| allocator.free(f);
                allocator.free(formatted);
                return ExecError.OutOfMemory;
            };
        }
    } else {
        var i: u16 = 0;
        while (i < chunk.count) : (i += 1) {
            const formatted = allocator.alloc([]const u8, col_indices.len) catch return ExecError.OutOfMemory;
            for (col_indices, 0..) |ci, fi| {
                const val = chunk.columns[ci].getValue(i);
                formatted[fi] = formatValue(allocator, val) catch {
                    for (formatted[0..fi]) |f| allocator.free(f);
                    allocator.free(formatted);
                    return ExecError.OutOfMemory;
                };
            }
            rows.append(allocator, .{ .values = formatted }) catch {
                for (formatted) |f| allocator.free(f);
                allocator.free(formatted);
                return ExecError.OutOfMemory;
            };
        }
    }
}

/// ORDER BY path with deferred materialization and Top-N sort.
fn execVectorizedWithOrderBy(
    allocator: std.mem.Allocator,
    scan: *vec_scan_mod.VecSeqScan,
    sel: ast.Select,
    col_indices: []const usize,
    col_names: [][]const u8,
    schema: *const Schema,
    order_clauses: []const ast.OrderByClause,
    limit: ?usize,
    simd_pred: ?vec_filter_mod.SimplePredicate,
    between_preds: ?[2]vec_filter_mod.SimplePredicate,
    needs_general_filter: bool,
) ExecError!ExecResult {
    var collected: std.ArrayList(CollectedRow) = .empty;
    errdefer {
        for (collected.items) |row| freeCollectedRow(allocator, row);
        collected.deinit(allocator);
    }

    while (scan.next() catch {
        for (col_names) |cn| allocator.free(cn);
        allocator.free(col_names);
        return ExecError.StorageError;
    }) |chunk| {
        if (simd_pred) |pred| {
            chunk.sel = vec_filter_mod.filterSimd(chunk, pred);
        }
        if (between_preds) |preds| {
            chunk.sel = vec_filter_mod.filterSimd(chunk, preds[0]);
            chunk.sel = vec_filter_mod.filterSimdWithSel(chunk, preds[1]);
        }
        if (needs_general_filter) {
            if (sel.where_clause) |where| {
                chunk.sel = vec_filter_mod.filterGeneral(chunk, where, schema);
            }
        }

        try collectChunkRows(allocator, chunk, col_indices, &collected);
    }

    // Resolve ORDER BY column indices
    const order_indices = allocator.alloc(usize, order_clauses.len) catch return ExecError.OutOfMemory;
    defer allocator.free(order_indices);

    for (order_clauses, 0..) |oc, oi| {
        var found = false;
        for (col_names, 0..) |cn, ci| {
            if (std.ascii.eqlIgnoreCase(cn, oc.column)) {
                order_indices[oi] = ci;
                found = true;
                break;
            }
        }
        if (!found) {
            for (col_names) |cn| allocator.free(cn);
            allocator.free(col_names);
            return ExecError.ColumnNotFound;
        }
    }

    // Top-N optimization: when ORDER BY + LIMIT, use bounded partial sort
    var order_by_applied = false;
    if (limit) |lim| {
        const n: usize = @min(lim, collected.items.len);
        if (n > 0 and n < collected.items.len) {
            const items = collected.items;
            {
                var si: usize = 1;
                while (si < n) : (si += 1) {
                    var j = si;
                    while (j > 0) {
                        if (compareCollectedRows(items[j - 1], items[j], order_clauses, order_indices) == .gt) {
                            const tmp = items[j];
                            items[j] = items[j - 1];
                            items[j - 1] = tmp;
                            j -= 1;
                        } else break;
                    }
                }
            }
            for (items[n..]) |*item| {
                if (compareCollectedRows(item.*, items[n - 1], order_clauses, order_indices) == .lt) {
                    const evicted = items[n - 1];
                    items[n - 1] = item.*;
                    item.* = evicted;
                    var j: usize = n - 1;
                    while (j > 0) {
                        if (compareCollectedRows(items[j - 1], items[j], order_clauses, order_indices) == .gt) {
                            const tmp = items[j];
                            items[j] = items[j - 1];
                            items[j - 1] = tmp;
                            j -= 1;
                        } else break;
                    }
                }
            }
            for (items[n..]) |row| freeCollectedRow(allocator, row);
            collected.shrinkRetainingCapacity(n);
            order_by_applied = true;
        }
    }

    if (!order_by_applied) {
        const items = collected.items;
        var si: usize = 1;
        while (si < items.len) : (si += 1) {
            var j = si;
            while (j > 0) {
                if (compareCollectedRows(items[j - 1], items[j], order_clauses, order_indices) == .gt) {
                    const tmp = items[j];
                    items[j] = items[j - 1];
                    items[j - 1] = tmp;
                    j -= 1;
                } else break;
            }
        }
    }

    // LIMIT
    if (limit) |lim| {
        const actual = @min(lim, collected.items.len);
        for (collected.items[actual..]) |row| freeCollectedRow(allocator, row);
        collected.shrinkRetainingCapacity(actual);
    }

    // Final materialization
    var rows: std.ArrayList(ResultRow) = .empty;
    errdefer {
        for (rows.items) |row| {
            for (row.values) |v| allocator.free(v);
            allocator.free(row.values);
        }
        rows.deinit(allocator);
    }

    for (collected.items) |crow| {
        const formatted = allocator.alloc([]const u8, crow.values.len) catch return ExecError.OutOfMemory;
        for (crow.values, 0..) |val, fi| {
            formatted[fi] = formatValue(allocator, val) catch {
                for (formatted[0..fi]) |f| allocator.free(f);
                allocator.free(formatted);
                return ExecError.OutOfMemory;
            };
        }
        rows.append(allocator, .{ .values = formatted }) catch {
            for (formatted) |f| allocator.free(f);
            allocator.free(formatted);
            return ExecError.OutOfMemory;
        };
    }

    for (collected.items) |row| freeCollectedRow(allocator, row);
    collected.deinit(allocator);

    return .{ .rows = .{
        .columns = col_names,
        .rows = rows.toOwnedSlice(allocator) catch {
            for (col_names) |cn| allocator.free(cn);
            allocator.free(col_names);
            return ExecError.OutOfMemory;
        },
    } };
}

/// Collect active rows from a DataChunk as native Value arrays.
fn collectChunkRows(
    allocator: std.mem.Allocator,
    chunk: *DataChunk,
    col_indices: []const usize,
    collected: *std.ArrayList(CollectedRow),
) ExecError!void {
    if (chunk.sel) |sel| {
        for (sel.indices[0..sel.len]) |row_idx| {
            try collectRow(allocator, chunk, col_indices, row_idx, collected);
        }
    } else {
        var i: u16 = 0;
        while (i < chunk.count) : (i += 1) {
            try collectRow(allocator, chunk, col_indices, i, collected);
        }
    }
}

fn collectRow(
    allocator: std.mem.Allocator,
    chunk: *DataChunk,
    col_indices: []const usize,
    row_idx: u16,
    collected: *std.ArrayList(CollectedRow),
) ExecError!void {
    const values = allocator.alloc(Value, col_indices.len) catch return ExecError.OutOfMemory;
    for (col_indices, 0..) |ci, i| {
        const val = chunk.columns[ci].getValue(row_idx);
        // Dupe string bytes since chunk buffer will be reused
        values[i] = if (val == .bytes)
            .{ .bytes = allocator.dupe(u8, val.bytes) catch {
                for (values[0..i]) |*v| {
                    if (v.* == .bytes) allocator.free(v.bytes);
                }
                allocator.free(values);
                return ExecError.OutOfMemory;
            } }
        else
            val;
    }
    collected.append(allocator, .{ .values = values }) catch {
        for (values) |*v| {
            if (v.* == .bytes) allocator.free(v.bytes);
        }
        allocator.free(values);
        return ExecError.OutOfMemory;
    };
}

fn freeCollectedRow(allocator: std.mem.Allocator, row: CollectedRow) void {
    for (row.values) |v| {
        if (v == .bytes) allocator.free(v.bytes);
    }
    allocator.free(row.values);
}

/// Compare two collected rows for ORDER BY using native Value comparison.
fn compareCollectedRows(
    a: CollectedRow,
    b: CollectedRow,
    clauses: []const ast.OrderByClause,
    indices: []const usize,
) std.math.Order {
    for (clauses, indices) |clause, ci| {
        const cmp = orderCompareValues(a.values[ci], b.values[ci]);
        if (cmp != .eq) {
            return if (clause.ascending) cmp else cmp.invert();
        }
    }
    return .eq;
}

/// Compare two Values for ordering purposes.
fn orderCompareValues(a: Value, b: Value) std.math.Order {
    // nulls sort last
    if (a == .null_value and b == .null_value) return .eq;
    if (a == .null_value) return .gt;
    if (b == .null_value) return .lt;

    switch (a) {
        .integer => |ai| {
            const bi: i64 = switch (b) {
                .integer => |v| v,
                .bigint => |v| return std.math.order(@as(i64, ai), v),
                .float => |v| return std.math.order(@as(f64, @floatFromInt(ai)), v),
                else => return .lt,
            };
            return std.math.order(@as(i64, ai), bi);
        },
        .bigint => |ai| {
            const bi: i64 = switch (b) {
                .integer => |v| v,
                .bigint => |v| v,
                .float => |v| return std.math.order(@as(f64, @floatFromInt(ai)), v),
                else => return .lt,
            };
            return std.math.order(ai, bi);
        },
        .float => |af| {
            const bf: f64 = switch (b) {
                .float => |v| v,
                .integer => |v| @floatFromInt(v),
                .bigint => |v| @floatFromInt(v),
                else => return .lt,
            };
            return std.math.order(af, bf);
        },
        .bytes => |as| {
            const bs = switch (b) {
                .bytes => |v| v,
                else => return .gt,
            };
            return std.mem.order(u8, as, bs);
        },
        .boolean => |ab| {
            const bb = switch (b) {
                .boolean => |v| v,
                else => return .gt,
            };
            const ai: u1 = @intFromBool(ab);
            const bi: u1 = @intFromBool(bb);
            return std.math.order(ai, bi);
        },
        .null_value => return .gt,
    }
}

fn formatValue(allocator: std.mem.Allocator, val: Value) ![]const u8 {
    return switch (val) {
        .null_value => try allocator.dupe(u8, "NULL"),
        .boolean => |b| try allocator.dupe(u8, if (b) "true" else "false"),
        .integer => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
        .bigint => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
        .float => |f| try std.fmt.allocPrint(allocator, "{d:.6}", .{f}),
        .bytes => |s| try allocator.dupe(u8, s),
    };
}

fn resolveSelectColumns(allocator: std.mem.Allocator, sel_cols: []const ast.SelectColumn, schema: *const Schema) ?[]usize {
    if (sel_cols.len == 1 and sel_cols[0] == .all_columns) {
        const indices = allocator.alloc(usize, schema.columns.len) catch return null;
        for (0..schema.columns.len) |i| indices[i] = i;
        return indices;
    }

    const indices = allocator.alloc(usize, sel_cols.len) catch return null;
    for (sel_cols, 0..) |sc, i| {
        switch (sc) {
            .named => |name| {
                if (resolveColumnIndex(schema, name)) |idx| {
                    indices[i] = idx;
                } else {
                    allocator.free(indices);
                    return null;
                }
            },
            .qualified => |q| {
                if (resolveColumnIndex(schema, q.column)) |idx| {
                    indices[i] = idx;
                } else {
                    allocator.free(indices);
                    return null;
                }
            },
            .all_columns => {
                allocator.free(indices);
                return null;
            },
            .aggregate => {
                allocator.free(indices);
                return null;
            },
        }
    }
    return indices;
}

fn resolveColumnIndex(schema: *const Schema, name: []const u8) ?usize {
    for (schema.columns, 0..) |col, i| {
        if (std.ascii.eqlIgnoreCase(col.name, name)) return i;
    }
    return null;
}


/// Vectorized aggregate execution.
fn execVectorizedAggregate(
    allocator: std.mem.Allocator,
    sel: ast.Select,
    table: *Table,
    schema: *const Schema,
    txn: ?*const Transaction,
) ExecError!ExecResult {
    var agg = vec_agg_mod.VecHashAggregate.init(allocator, sel, schema) catch return ExecError.OutOfMemory;
    defer agg.deinit();

    var scan = vec_scan_mod.VecSeqScan.init(allocator, table, txn) catch return ExecError.StorageError;
    defer scan.deinit();

    // Detect SIMD-eligible predicate for pre-filtering
    const simd_pred = if (sel.where_clause) |where|
        vec_filter_mod.detectSimplePredicate(where, schema)
    else
        null;

    const needs_general_filter = if (sel.where_clause) |where|
        (simd_pred == null or where.* != .comparison)
    else
        false;

    while (scan.next() catch return ExecError.StorageError) |chunk| {
        // Apply filter before aggregation
        if (simd_pred) |pred| {
            chunk.sel = vec_filter_mod.filterSimd(chunk, pred);
        }
        if (needs_general_filter) {
            if (sel.where_clause) |where| {
                chunk.sel = vec_filter_mod.filterGeneral(chunk, where, schema);
            }
        }

        agg.sink(chunk);
    }

    return agg.finalize();
}
