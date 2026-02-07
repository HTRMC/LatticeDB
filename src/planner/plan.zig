const std = @import("std");
const page_mod = @import("../storage/page.zig");
const ast = @import("../parser/ast.zig");

const PageId = page_mod.PageId;

/// How an index scan accesses data
pub const ScanType = union(enum) {
    /// Exact key lookup
    point: i32,
    /// Range [low, high]
    range: struct { low: i32, high: i32 },
    /// key >= value
    range_from: i32,
    /// key <= value
    range_to: i32,
};

/// Query plan node — describes how to execute a query
pub const PlanNode = union(enum) {
    seq_scan: SeqScan,
    index_scan: IndexScan,
    filter: Filter,
    limit: Limit,
    sort: Sort,

    pub const SeqScan = struct {
        table_name: []const u8,
        table_id: PageId,
        estimated_rows: u64,
        estimated_cost: f64,
    };

    pub const IndexScan = struct {
        table_name: []const u8,
        table_id: PageId,
        index_name: []const u8,
        index_id: PageId,
        column_ordinal: u16,
        scan_type: ScanType,
        estimated_rows: u64,
        estimated_cost: f64,
    };

    pub const Filter = struct {
        child: *PlanNode,
        predicate: *const ast.Expression,
        estimated_cost: f64,
    };

    pub const Limit = struct {
        child: *PlanNode,
        count: u64,
    };

    pub const Sort = struct {
        child: *PlanNode,
        column: []const u8,
        descending: bool,
    };
};

/// Format a plan node tree into EXPLAIN text output
pub fn formatPlan(allocator: std.mem.Allocator, node: *const PlanNode, indent: usize) ![]const u8 {
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);

    try formatPlanInner(allocator, &result, node, indent);

    return result.toOwnedSlice(allocator);
}

fn formatPlanInner(allocator: std.mem.Allocator, result: *std.ArrayList(u8), node: *const PlanNode, indent: usize) !void {
    // Add indentation
    for (0..indent) |_| {
        try result.appendSlice(allocator, "  ");
    }

    switch (node.*) {
        .seq_scan => |ss| {
            const line = try std.fmt.allocPrint(allocator, "Seq Scan on {s} (cost={d:.2} rows={d})\n", .{ ss.table_name, ss.estimated_cost, ss.estimated_rows });
            defer allocator.free(line);
            try result.appendSlice(allocator, line);
        },
        .index_scan => |is| {
            const scan_desc = try formatScanType(allocator, is.scan_type);
            defer allocator.free(scan_desc);
            const line = try std.fmt.allocPrint(allocator, "Index Scan on {s} using {s} ({s} cost={d:.2} rows={d})\n", .{ is.table_name, is.index_name, scan_desc, is.estimated_cost, is.estimated_rows });
            defer allocator.free(line);
            try result.appendSlice(allocator, line);
        },
        .filter => |f| {
            const line = try std.fmt.allocPrint(allocator, "Filter (cost={d:.2})\n", .{f.estimated_cost});
            defer allocator.free(line);
            try result.appendSlice(allocator, line);
            try formatPlanInner(allocator, result, f.child, indent + 1);
        },
        .limit => |l| {
            const line = try std.fmt.allocPrint(allocator, "Limit (count={d})\n", .{l.count});
            defer allocator.free(line);
            try result.appendSlice(allocator, line);
            try formatPlanInner(allocator, result, l.child, indent + 1);
        },
        .sort => |s| {
            const dir: []const u8 = if (s.descending) "DESC" else "ASC";
            const line = try std.fmt.allocPrint(allocator, "Sort (column={s} {s})\n", .{ s.column, dir });
            defer allocator.free(line);
            try result.appendSlice(allocator, line);
            try formatPlanInner(allocator, result, s.child, indent + 1);
        },
    }
}

fn formatScanType(allocator: std.mem.Allocator, st: ScanType) ![]const u8 {
    return switch (st) {
        .point => |k| try std.fmt.allocPrint(allocator, "point lookup key={d}", .{k}),
        .range => |r| try std.fmt.allocPrint(allocator, "range key={d}..{d}", .{ r.low, r.high }),
        .range_from => |k| try std.fmt.allocPrint(allocator, "range key>={d}", .{k}),
        .range_to => |k| try std.fmt.allocPrint(allocator, "range key<={d}", .{k}),
    };
}

// Tests

test "plan node creation" {
    const node = PlanNode{ .seq_scan = .{
        .table_name = "users",
        .table_id = 1,
        .estimated_rows = 100,
        .estimated_cost = 100.0,
    } };
    try std.testing.expectEqualStrings("users", node.seq_scan.table_name);
    try std.testing.expectEqual(@as(u64, 100), node.seq_scan.estimated_rows);
}

test "formatPlan output" {
    const allocator = std.testing.allocator;

    // Create a simple plan: Filter -> SeqScan
    var scan_node = PlanNode{ .seq_scan = .{
        .table_name = "users",
        .table_id = 1,
        .estimated_rows = 1000,
        .estimated_cost = 1000.0,
    } };
    var filter_node = PlanNode{ .filter = .{
        .child = &scan_node,
        .predicate = undefined,
        .estimated_cost = 1100.0,
    } };

    const text = try formatPlan(allocator, &filter_node, 0);
    defer allocator.free(text);

    // Should contain both nodes
    try std.testing.expect(std.mem.indexOf(u8, text, "Filter") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "Seq Scan on users") != null);

    // Index scan format
    var idx_node = PlanNode{ .index_scan = .{
        .table_name = "users",
        .table_id = 1,
        .index_name = "idx_users_id",
        .index_id = 2,
        .column_ordinal = 0,
        .scan_type = .{ .point = 42 },
        .estimated_rows = 1,
        .estimated_cost = 5.0,
    } };
    const idx_text = try formatPlan(allocator, &idx_node, 0);
    defer allocator.free(idx_text);
    try std.testing.expect(std.mem.indexOf(u8, idx_text, "Index Scan") != null);
    try std.testing.expect(std.mem.indexOf(u8, idx_text, "idx_users_id") != null);
    try std.testing.expect(std.mem.indexOf(u8, idx_text, "point lookup key=42") != null);
}
