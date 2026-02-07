const std = @import("std");
const ast = @import("../parser/ast.zig");
const plan_mod = @import("plan.zig");
const catalog_mod = @import("../storage/catalog.zig");
const table_mod = @import("../storage/table.zig");
const page_mod = @import("../storage/page.zig");

const PlanNode = plan_mod.PlanNode;
const ScanType = plan_mod.ScanType;
const Catalog = catalog_mod.Catalog;
const Table = table_mod.Table;
const PageId = page_mod.PageId;
const Schema = @import("../storage/tuple.zig").Schema;

pub const PlanError = error{
    TableNotFound,
    OutOfMemory,
    StorageError,
};

/// Query planner — chooses between SeqScan and IndexScan based on cost
pub const Planner = struct {
    allocator: std.mem.Allocator,
    catalog: *Catalog,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, catalog: *Catalog) Self {
        return .{ .allocator = allocator, .catalog = catalog };
    }

    /// Plan a SELECT statement. Caller must free the returned plan tree with freePlan.
    pub fn planSelect(self: *Self, sel: ast.Select) PlanError!*PlanNode {
        // Look up table
        const table_id = (self.catalog.findTableId(sel.table_name) catch return PlanError.StorageError) orelse return PlanError.TableNotFound;

        // Get row count
        const table_result = self.catalog.openTable(sel.table_name) catch return PlanError.StorageError;
        if (table_result == null) return PlanError.TableNotFound;
        const schema = table_result.?.schema;
        defer self.catalog.freeSchema(schema);
        var table = table_result.?.table;
        const row_count = table.tupleCount() catch 0;

        // Get available indexes
        const indexes = self.catalog.getIndexesForTable(table_id) catch return PlanError.StorageError;
        defer self.catalog.freeIndexList(indexes);

        // SeqScan cost
        const seq_cost: f64 = @floatFromInt(row_count);

        // Try to find a sargable predicate in WHERE
        var best_index_cost: f64 = std.math.inf(f64);
        var best_scan_type: ?ScanType = null;
        var best_index_idx: ?usize = null;
        var best_estimated_rows: u64 = row_count;

        if (sel.where_clause) |where| {
            for (indexes, 0..) |idx, i| {
                if (analyzePredicate(where, idx.column_ordinal, schema)) |result| {
                    const estimated_matches: u64 = switch (result.scan_type) {
                        .point => 1,
                        .range => @intFromFloat(@max(1.0, @as(f64, @floatFromInt(row_count)) * 0.25)),
                        .range_from, .range_to => @intFromFloat(@max(1.0, @as(f64, @floatFromInt(row_count)) * 0.33)),
                    };
                    const cost: f64 = 4.0 + @as(f64, @floatFromInt(estimated_matches)) * 4.0;
                    if (cost < best_index_cost) {
                        best_index_cost = cost;
                        best_scan_type = result.scan_type;
                        best_index_idx = i;
                        best_estimated_rows = estimated_matches;
                    }
                }
            }
        }

        // Choose cheapest
        var base_node: *PlanNode = undefined;
        if (best_scan_type != null and best_index_cost < seq_cost) {
            const idx = indexes[best_index_idx.?];
            base_node = self.allocator.create(PlanNode) catch return PlanError.OutOfMemory;
            base_node.* = .{ .index_scan = .{
                .table_name = sel.table_name,
                .table_id = table_id,
                .index_name = idx.index_name,
                .index_id = idx.index_id,
                .column_ordinal = idx.column_ordinal,
                .scan_type = best_scan_type.?,
                .estimated_rows = best_estimated_rows,
                .estimated_cost = best_index_cost,
            } };
        } else {
            base_node = self.allocator.create(PlanNode) catch return PlanError.OutOfMemory;
            base_node.* = .{ .seq_scan = .{
                .table_name = sel.table_name,
                .table_id = table_id,
                .estimated_rows = row_count,
                .estimated_cost = seq_cost,
            } };
        }

        var current = base_node;

        // Wrap with Filter if there's a WHERE (and we're using seq scan, or residual filter needed)
        if (sel.where_clause) |where| {
            const filter_node = self.allocator.create(PlanNode) catch return PlanError.OutOfMemory;
            filter_node.* = .{ .filter = .{
                .child = current,
                .predicate = where,
                .estimated_cost = PlanNodeExt.getCost(current) + @as(f64, @floatFromInt(row_count)) * 0.1,
            } };
            current = filter_node;
        }

        // Wrap with Sort if ORDER BY
        if (sel.order_by) |order_clauses| {
            if (order_clauses.len > 0) {
                const sort_node = self.allocator.create(PlanNode) catch return PlanError.OutOfMemory;
                sort_node.* = .{ .sort = .{
                    .child = current,
                    .column = order_clauses[0].column,
                    .descending = !order_clauses[0].ascending,
                } };
                current = sort_node;
            }
        }

        // Wrap with Limit
        if (sel.limit) |limit_val| {
            const limit_node = self.allocator.create(PlanNode) catch return PlanError.OutOfMemory;
            limit_node.* = .{ .limit = .{
                .child = current,
                .count = limit_val,
            } };
            current = limit_node;
        }

        return current;
    }

    pub fn freePlan(self: *Self, node: *PlanNode) void {
        switch (node.*) {
            .filter => |f| self.freePlan(f.child),
            .limit => |l| self.freePlan(l.child),
            .sort => |s| self.freePlan(s.child),
            else => {},
        }
        self.allocator.destroy(node);
    }
};

/// Result of analyzing a predicate for index use
const AnalyzeResult = struct {
    scan_type: ScanType,
};

/// Check if an expression is sargable for the given column ordinal
fn analyzePredicate(expr: *const ast.Expression, col_ordinal: u16, schema: *const Schema) ?AnalyzeResult {
    switch (expr.*) {
        .comparison => |cmp| {
            // Check if left is our indexed column and right is a literal integer
            const col_idx = getColumnOrdinal(cmp.left, schema);
            const lit_val = getLiteralInt(cmp.right);

            if (col_idx != null and col_idx.? == col_ordinal and lit_val != null) {
                return switch (cmp.op) {
                    .eq => .{ .scan_type = .{ .point = @intCast(lit_val.?) } },
                    .gte => .{ .scan_type = .{ .range_from = @intCast(lit_val.?) } },
                    .gt => .{ .scan_type = .{ .range_from = @intCast(lit_val.? + 1) } },
                    .lte => .{ .scan_type = .{ .range_to = @intCast(lit_val.?) } },
                    .lt => .{ .scan_type = .{ .range_to = @intCast(lit_val.? - 1) } },
                    else => null,
                };
            }

            // Check reverse: literal op column
            const col_idx_r = getColumnOrdinal(cmp.right, schema);
            const lit_val_l = getLiteralInt(cmp.left);

            if (col_idx_r != null and col_idx_r.? == col_ordinal and lit_val_l != null) {
                return switch (cmp.op) {
                    .eq => .{ .scan_type = .{ .point = @intCast(lit_val_l.?) } },
                    // Reversed: literal >= col means col <= literal
                    .gte => .{ .scan_type = .{ .range_to = @intCast(lit_val_l.?) } },
                    .gt => .{ .scan_type = .{ .range_to = @intCast(lit_val_l.? - 1) } },
                    .lte => .{ .scan_type = .{ .range_from = @intCast(lit_val_l.?) } },
                    .lt => .{ .scan_type = .{ .range_from = @intCast(lit_val_l.? + 1) } },
                    else => null,
                };
            }
        },
        .between_expr => |b| {
            const col_idx = getColumnOrdinal(b.value, schema);
            const low_val = getLiteralInt(b.low);
            const high_val = getLiteralInt(b.high);

            if (col_idx != null and col_idx.? == col_ordinal and low_val != null and high_val != null) {
                return .{ .scan_type = .{ .range = .{ .low = @intCast(low_val.?), .high = @intCast(high_val.?) } } };
            }
        },
        .and_expr => |a| {
            // Try left first, then right
            if (analyzePredicate(a.left, col_ordinal, schema)) |result| return result;
            if (analyzePredicate(a.right, col_ordinal, schema)) |result| return result;
        },
        else => {},
    }
    return null;
}

fn getColumnOrdinal(expr: *const ast.Expression, schema: *const Schema) ?u16 {
    switch (expr.*) {
        .column_ref => |name| {
            for (schema.columns, 0..) |col, i| {
                if (std.ascii.eqlIgnoreCase(col.name, name)) return @intCast(i);
            }
        },
        else => {},
    }
    return null;
}

fn getLiteralInt(expr: *const ast.Expression) ?i64 {
    switch (expr.*) {
        .literal => |lit| switch (lit) {
            .integer => |v| return v,
            else => {},
        },
        else => {},
    }
    return null;
}

// Add getCost helper to PlanNode
const PlanNodeExt = struct {
    fn getCost(node: *const PlanNode) f64 {
        return switch (node.*) {
            .seq_scan => |ss| ss.estimated_cost,
            .index_scan => |is| is.estimated_cost,
            .filter => |f| f.estimated_cost,
            .limit => |l| PlanNodeExt.getCost(l.child),
            .sort => |s| PlanNodeExt.getCost(s.child),
        };
    }
};

// Tests

const disk_manager_mod = @import("../storage/disk_manager.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const DiskManager = disk_manager_mod.DiskManager;
const BufferPool = buffer_pool_mod.BufferPool;
const tuple_mod = @import("../storage/tuple.zig");
const Column = tuple_mod.Column;

test "planner seq scan when no indexes" {
    const test_file = "test_planner_seq.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const cols = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "name", .col_type = .text, .max_length = 0, .nullable = false },
    };
    _ = try catalog.createTable("users", &cols);

    var planner = Planner.init(std.testing.allocator, &catalog);

    const parser_mod = @import("../parser/parser.zig");
    var parser = parser_mod.Parser.init(std.testing.allocator, "SELECT * FROM users WHERE id = 1");
    defer parser.deinit();
    const stmt = try parser.parse();

    const plan = try planner.planSelect(stmt.select);
    defer planner.freePlan(plan);

    // With no indexes, top level should be filter -> seq_scan
    try std.testing.expect(plan.* == .filter);
    try std.testing.expect(plan.filter.child.* == .seq_scan);
}

test "planner index scan for equality" {
    const test_file = "test_planner_idx_eq.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const cols = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "name", .col_type = .text, .max_length = 0, .nullable = false },
    };
    _ = try catalog.createTable("users", &cols);

    // Insert enough rows so index scan is cheaper than seq scan
    const table_result = (try catalog.openTable("users")).?;
    defer catalog.freeSchema(table_result.schema);
    var table = table_result.table;
    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        _ = try table.insertTuple(null, &.{ .{ .integer = i }, .{ .bytes = "test" } });
    }

    _ = try catalog.createIndex("users", "idx_users_id", "id", false);

    var planner = Planner.init(std.testing.allocator, &catalog);

    const parser_mod = @import("../parser/parser.zig");
    var parser = parser_mod.Parser.init(std.testing.allocator, "SELECT * FROM users WHERE id = 50");
    defer parser.deinit();
    const stmt = try parser.parse();

    const plan = try planner.planSelect(stmt.select);
    defer planner.freePlan(plan);

    // Should choose index scan (wrapped in filter)
    try std.testing.expect(plan.* == .filter);
    try std.testing.expect(plan.filter.child.* == .index_scan);
    try std.testing.expectEqualStrings("idx_users_id", plan.filter.child.index_scan.index_name);
    try std.testing.expect(plan.filter.child.index_scan.scan_type == .point);
}

test "planner index scan for range" {
    const test_file = "test_planner_idx_range.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const cols = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
    };
    _ = try catalog.createTable("t", &cols);

    const table_result = (try catalog.openTable("t")).?;
    defer catalog.freeSchema(table_result.schema);
    var table = table_result.table;
    var i: i32 = 0;
    while (i < 200) : (i += 1) {
        _ = try table.insertTuple(null, &.{.{ .integer = i }});
    }

    _ = try catalog.createIndex("t", "idx_t_id", "id", false);

    var planner = Planner.init(std.testing.allocator, &catalog);

    const parser_mod = @import("../parser/parser.zig");
    var parser = parser_mod.Parser.init(std.testing.allocator, "SELECT * FROM t WHERE id >= 100");
    defer parser.deinit();
    const stmt = try parser.parse();

    const plan = try planner.planSelect(stmt.select);
    defer planner.freePlan(plan);

    try std.testing.expect(plan.* == .filter);
    try std.testing.expect(plan.filter.child.* == .index_scan);
    try std.testing.expect(plan.filter.child.index_scan.scan_type == .range_from);
}

test "planner prefers seq scan for tiny tables" {
    const test_file = "test_planner_tiny.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const cols = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
    };
    _ = try catalog.createTable("t", &cols);

    // Only 3 rows — seq scan cost (3) < index scan cost (4 + 1*4 = 8)
    const table_result = (try catalog.openTable("t")).?;
    defer catalog.freeSchema(table_result.schema);
    var table = table_result.table;
    _ = try table.insertTuple(null, &.{.{ .integer = 1 }});
    _ = try table.insertTuple(null, &.{.{ .integer = 2 }});
    _ = try table.insertTuple(null, &.{.{ .integer = 3 }});

    _ = try catalog.createIndex("t", "idx_t_id", "id", false);

    var planner = Planner.init(std.testing.allocator, &catalog);

    const parser_mod = @import("../parser/parser.zig");
    var parser = parser_mod.Parser.init(std.testing.allocator, "SELECT * FROM t WHERE id = 2");
    defer parser.deinit();
    const stmt = try parser.parse();

    const plan = try planner.planSelect(stmt.select);
    defer planner.freePlan(plan);

    // Should prefer seq scan for tiny table
    try std.testing.expect(plan.* == .filter);
    try std.testing.expect(plan.filter.child.* == .seq_scan);
}
