const std = @import("std");
const ast = @import("../parser/ast.zig");
const plan_mod = @import("plan.zig");
const catalog_mod = @import("../storage/catalog.zig");
const page_mod = @import("../storage/page.zig");

const PlanNode = plan_mod.PlanNode;
const Catalog = catalog_mod.Catalog;
const PageId = page_mod.PageId;

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

    /// Plan a SELECT statement. Caller must free the returned plan tree.
    pub fn planSelect(self: *Self, sel: ast.Select) PlanError!*PlanNode {
        _ = sel;
        _ = self;
        return PlanError.TableNotFound; // stub — filled in step 5
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
