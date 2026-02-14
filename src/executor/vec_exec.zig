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

    // Detect SIMD-eligible predicate
    const simd_pred = if (sel.where_clause) |where|
        vec_filter_mod.detectSimplePredicate(where, schema)
    else
        null;

    // For SIMD fast path, check if there's additional WHERE complexity beyond the simple predicate
    // If the entire WHERE is just a simple comparison, SIMD handles it completely
    const needs_general_filter = if (sel.where_clause) |where|
        (simd_pred == null or where.* != .comparison)
    else
        false;

    // Scan and filter
    var scan = vec_scan_mod.VecSeqScan.init(allocator, table, txn) catch {
        for (col_names) |cn| allocator.free(cn);
        allocator.free(col_names);
        return ExecError.StorageError;
    };
    defer scan.deinit();

    var rows: std.ArrayList(ResultRow) = .empty;
    errdefer {
        for (rows.items) |row| {
            for (row.values) |v| allocator.free(v);
            allocator.free(row.values);
        }
        rows.deinit(allocator);
    }

    const limit: ?usize = if (sel.limit) |l| @intCast(l) else null;
    const has_order_by = sel.order_by != null;

    while (scan.next() catch {
        for (col_names) |cn| allocator.free(cn);
        allocator.free(col_names);
        return ExecError.StorageError;
    }) |chunk| {
        // Apply filter
        if (simd_pred) |pred| {
            chunk.sel = vec_filter_mod.filterSimd(chunk, pred);
        }

        if (needs_general_filter) {
            if (sel.where_clause) |where| {
                chunk.sel = vec_filter_mod.filterGeneral(chunk, where, schema);
            }
        }

        // Materialize matching rows
        try materializeChunkToResult(allocator, chunk, col_indices, &rows);

        // Early termination for LIMIT without ORDER BY
        if (!has_order_by) {
            if (limit) |lim| {
                if (rows.items.len >= lim) break;
            }
        }
    }

    // ORDER BY
    if (sel.order_by) |order_clauses| {
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

        // Insertion sort (matches executor.zig pattern)
        const items = rows.items;
        var si: usize = 1;
        while (si < items.len) : (si += 1) {
            var j = si;
            while (j > 0) {
                if (orderCompareRows(items[j - 1], items[j], order_clauses, order_indices) == .gt) {
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

/// Materialize active rows from a DataChunk into ResultRows.
fn materializeChunkToResult(
    allocator: std.mem.Allocator,
    chunk: *DataChunk,
    col_indices: []const usize,
    rows: *std.ArrayList(ResultRow),
) ExecError!void {
    if (chunk.sel) |sel| {
        for (sel.indices[0..sel.len]) |row_idx| {
            try materializeRow(allocator, chunk, col_indices, row_idx, rows);
        }
    } else {
        var i: u16 = 0;
        while (i < chunk.count) : (i += 1) {
            try materializeRow(allocator, chunk, col_indices, i, rows);
        }
    }
}

fn materializeRow(
    allocator: std.mem.Allocator,
    chunk: *DataChunk,
    col_indices: []const usize,
    row_idx: u16,
    rows: *std.ArrayList(ResultRow),
) ExecError!void {
    const formatted = allocator.alloc([]const u8, col_indices.len) catch return ExecError.OutOfMemory;
    for (col_indices, 0..) |ci, i| {
        const val = chunk.columns[ci].getValue(row_idx);
        formatted[i] = formatValue(allocator, val) catch {
            for (formatted[0..i]) |f| allocator.free(f);
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

fn orderCompareRows(
    a: ResultRow,
    b: ResultRow,
    clauses: []const ast.OrderByClause,
    indices: []const usize,
) std.math.Order {
    for (clauses, indices) |clause, ci| {
        const cmp = std.mem.order(u8, a.values[ci], b.values[ci]);
        if (cmp != .eq) {
            // Try numeric comparison first
            const a_num = std.fmt.parseFloat(f64, a.values[ci]) catch {
                return if (clause.ascending) cmp else cmp.invert();
            };
            const b_num = std.fmt.parseFloat(f64, b.values[ci]) catch {
                return if (clause.ascending) cmp else cmp.invert();
            };
            const num_cmp = std.math.order(a_num, b_num);
            if (num_cmp != .eq) {
                return if (clause.ascending) num_cmp else num_cmp.invert();
            }
            return if (clause.ascending) cmp else cmp.invert();
        }
    }
    return .eq;
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
