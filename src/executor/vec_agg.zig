const std = @import("std");
const vector_mod = @import("vector.zig");
const tuple_mod = @import("../storage/tuple.zig");
const ast = @import("../parser/ast.zig");

const DataChunk = vector_mod.DataChunk;
const VECTOR_SIZE = vector_mod.VECTOR_SIZE;
const Value = tuple_mod.Value;
const Schema = tuple_mod.Schema;
const ColumnType = tuple_mod.ColumnType;

const ExecError = @import("executor.zig").ExecError;
const ExecResult = @import("executor.zig").ExecResult;
const ResultRow = @import("executor.zig").ResultRow;

/// Per-group accumulator state.
const GroupState = struct {
    count: u64,
    sums: []f64,
    mins: []?f64,
    maxs: []?f64,
    str_mins: []?[]const u8,
    str_maxs: []?[]const u8,
    non_null_counts: []u64,
    group_values: [][]const u8,
};

/// Vectorized hash aggregate operator.
pub const VecHashAggregate = struct {
    allocator: std.mem.Allocator,
    sel: ast.Select,
    schema: *const Schema,
    num_cols: usize,
    agg_col_indices: []?usize,
    group_by_indices: []usize,
    groups: std.StringArrayHashMap(GroupState),

    pub fn init(allocator: std.mem.Allocator, sel: ast.Select, schema: *const Schema) !VecHashAggregate {
        const num_cols = sel.columns.len;

        // Resolve aggregate column indices
        const agg_col_indices = try allocator.alloc(?usize, num_cols);
        for (sel.columns, 0..) |col, i| {
            switch (col) {
                .aggregate => |agg| {
                    if (agg.column) |col_name| {
                        agg_col_indices[i] = resolveColumnIndex(schema, col_name);
                    } else {
                        agg_col_indices[i] = null; // COUNT(*)
                    }
                },
                .named => |name| {
                    agg_col_indices[i] = resolveColumnIndex(schema, name);
                },
                .qualified => |q| {
                    agg_col_indices[i] = resolveColumnIndex(schema, q.column);
                },
                .all_columns => {
                    agg_col_indices[i] = null;
                },
            }
        }

        // Resolve GROUP BY column indices
        var group_by_indices: []usize = &.{};
        if (sel.group_by) |gb_cols| {
            group_by_indices = try allocator.alloc(usize, gb_cols.len);
            for (gb_cols, 0..) |gb_name, gi| {
                group_by_indices[gi] = resolveColumnIndex(schema, gb_name) orelse {
                    allocator.free(group_by_indices);
                    allocator.free(agg_col_indices);
                    return error.OutOfMemory;
                };
            }
        }

        var agg = VecHashAggregate{
            .allocator = allocator,
            .sel = sel,
            .schema = schema,
            .num_cols = num_cols,
            .agg_col_indices = agg_col_indices,
            .group_by_indices = group_by_indices,
            .groups = std.StringArrayHashMap(GroupState).init(allocator),
        };

        // For non-GROUP BY aggregates, create a single group
        if (group_by_indices.len == 0) {
            const empty_vals = try allocator.alloc([]const u8, 0);
            const state = try initGroupState(allocator, num_cols, empty_vals);
            const key = try allocator.dupe(u8, "");
            try agg.groups.put(key, state);
        }

        return agg;
    }

    pub fn deinit(self: *VecHashAggregate) void {
        for (self.groups.values()) |*gs| self.freeGroupState(gs);
        for (self.groups.keys()) |k| self.allocator.free(k);
        self.groups.deinit();
        if (self.group_by_indices.len > 0) self.allocator.free(self.group_by_indices);
        self.allocator.free(self.agg_col_indices);
    }

    /// Consume a DataChunk, accumulating values into groups.
    pub fn sink(self: *VecHashAggregate, chunk: *DataChunk) void {
        const has_group_by = self.group_by_indices.len > 0;

        if (chunk.sel) |sel| {
            for (sel.indices[0..sel.len]) |row_idx| {
                self.processRow(chunk, row_idx, has_group_by);
            }
        } else {
            var i: u16 = 0;
            while (i < chunk.count) : (i += 1) {
                self.processRow(chunk, i, has_group_by);
            }
        }
    }

    fn processRow(self: *VecHashAggregate, chunk: *DataChunk, row_idx: u16, has_group_by: bool) void {
        if (has_group_by) {
            // Build group key
            var key_parts: std.ArrayList(u8) = .empty;
            for (self.group_by_indices, 0..) |gi, idx| {
                if (idx > 0) key_parts.append(self.allocator, 0) catch {
                    key_parts.deinit(self.allocator);
                    return;
                };
                const val = chunk.columns[gi].getValue(row_idx);
                const fv = formatValue(self.allocator, val) catch {
                    key_parts.deinit(self.allocator);
                    return;
                };
                defer self.allocator.free(fv);
                key_parts.appendSlice(self.allocator, fv) catch {
                    key_parts.deinit(self.allocator);
                    return;
                };
            }
            const group_key = key_parts.toOwnedSlice(self.allocator) catch {
                key_parts.deinit(self.allocator);
                return;
            };

            if (!self.groups.contains(group_key)) {
                // Create group values
                const gv = self.allocator.alloc([]const u8, self.group_by_indices.len) catch {
                    self.allocator.free(group_key);
                    return;
                };
                for (self.group_by_indices, 0..) |gi, idx| {
                    gv[idx] = formatValue(self.allocator, chunk.columns[gi].getValue(row_idx)) catch {
                        for (gv[0..idx]) |v| self.allocator.free(v);
                        self.allocator.free(gv);
                        self.allocator.free(group_key);
                        return;
                    };
                }
                const state = initGroupState(self.allocator, self.num_cols, gv) catch {
                    for (gv) |v| self.allocator.free(v);
                    self.allocator.free(gv);
                    self.allocator.free(group_key);
                    return;
                };
                self.groups.put(group_key, state) catch {
                    self.allocator.free(group_key);
                    return;
                };
            } else {
                self.allocator.free(group_key);
            }

            // Accumulate - rebuild key for lookup
            var key_parts2: std.ArrayList(u8) = .empty;
            for (self.group_by_indices, 0..) |gi, idx| {
                if (idx > 0) key_parts2.append(self.allocator, 0) catch {
                    key_parts2.deinit(self.allocator);
                    return;
                };
                const val = chunk.columns[gi].getValue(row_idx);
                const fv = formatValue(self.allocator, val) catch {
                    key_parts2.deinit(self.allocator);
                    return;
                };
                defer self.allocator.free(fv);
                key_parts2.appendSlice(self.allocator, fv) catch {
                    key_parts2.deinit(self.allocator);
                    return;
                };
            }
            const lookup_key = key_parts2.toOwnedSlice(self.allocator) catch {
                key_parts2.deinit(self.allocator);
                return;
            };
            defer self.allocator.free(lookup_key);

            if (self.groups.getPtr(lookup_key)) |state_ptr| {
                state_ptr.count += 1;
                for (0..self.num_cols) |i| {
                    const ci = self.agg_col_indices[i] orelse continue;
                    accumulateAgg(self.allocator, state_ptr, i, chunk.columns[ci].getValue(row_idx));
                }
            }
        } else {
            // Single group
            if (self.groups.getPtr("")) |state_ptr| {
                state_ptr.count += 1;
                for (0..self.num_cols) |i| {
                    const ci = self.agg_col_indices[i] orelse continue;
                    accumulateAgg(self.allocator, state_ptr, i, chunk.columns[ci].getValue(row_idx));
                }
            }
        }
    }

    /// Finalize aggregation and produce ExecResult.
    pub fn finalize(self: *VecHashAggregate) ExecError!ExecResult {
        const allocator = self.allocator;

        // Build column headers
        const col_names = allocator.alloc([]const u8, self.num_cols) catch return ExecError.OutOfMemory;
        errdefer {
            for (col_names) |cn| allocator.free(cn);
            allocator.free(col_names);
        }

        for (self.sel.columns, 0..) |col, i| {
            const alias_name = if (self.sel.aliases) |aliases| (if (i < aliases.len) aliases[i] else null) else null;
            col_names[i] = if (alias_name) |a|
                allocator.dupe(u8, a) catch return ExecError.OutOfMemory
            else switch (col) {
                .aggregate => |agg| blk: {
                    const func_name = switch (agg.func) {
                        .count => "count",
                        .sum => "sum",
                        .avg => "avg",
                        .min => "min",
                        .max => "max",
                    };
                    break :blk if (agg.column) |cn|
                        std.fmt.allocPrint(allocator, "{s}({s})", .{ func_name, cn }) catch return ExecError.OutOfMemory
                    else
                        std.fmt.allocPrint(allocator, "{s}(*)", .{func_name}) catch return ExecError.OutOfMemory;
                },
                .named => |name| allocator.dupe(u8, name) catch return ExecError.OutOfMemory,
                .qualified => |q| allocator.dupe(u8, q.column) catch return ExecError.OutOfMemory,
                .all_columns => unreachable,
            };
        }

        // Build result rows
        var result_rows: std.ArrayList(ResultRow) = .empty;
        errdefer {
            for (result_rows.items) |rr| {
                for (rr.values) |v| allocator.free(v);
                allocator.free(rr.values);
            }
            result_rows.deinit(allocator);
        }

        for (self.groups.values()) |*state| {
            const formatted = allocator.alloc([]const u8, self.num_cols) catch return ExecError.OutOfMemory;

            for (self.sel.columns, 0..) |col, i| {
                formatted[i] = switch (col) {
                    .aggregate => |agg| try formatAggValue(allocator, agg.func, agg.column != null, i, state),
                    .named => |name| blk: {
                        if (self.sel.group_by) |gb_cols| {
                            for (gb_cols, 0..) |gb_name, gi| {
                                if (std.ascii.eqlIgnoreCase(gb_name, name)) {
                                    break :blk allocator.dupe(u8, state.group_values[gi]) catch return ExecError.OutOfMemory;
                                }
                            }
                        }
                        break :blk allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
                    },
                    .qualified => |q| blk: {
                        if (self.sel.group_by) |gb_cols| {
                            for (gb_cols, 0..) |gb_name, gi| {
                                if (std.ascii.eqlIgnoreCase(gb_name, q.column)) {
                                    break :blk allocator.dupe(u8, state.group_values[gi]) catch return ExecError.OutOfMemory;
                                }
                            }
                        }
                        break :blk allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
                    },
                    .all_columns => unreachable,
                };
            }

            result_rows.append(allocator, .{ .values = formatted }) catch return ExecError.OutOfMemory;
        }

        return .{ .rows = .{
            .columns = col_names,
            .rows = result_rows.toOwnedSlice(allocator) catch return ExecError.OutOfMemory,
        } };
    }

    fn freeGroupState(self: *VecHashAggregate, state: *GroupState) void {
        self.allocator.free(state.sums);
        self.allocator.free(state.mins);
        self.allocator.free(state.maxs);
        for (state.str_mins) |s| if (s) |v| self.allocator.free(v);
        self.allocator.free(state.str_mins);
        for (state.str_maxs) |s| if (s) |v| self.allocator.free(v);
        self.allocator.free(state.str_maxs);
        self.allocator.free(state.non_null_counts);
        for (state.group_values) |gv| self.allocator.free(gv);
        self.allocator.free(state.group_values);
    }
};

fn initGroupState(allocator: std.mem.Allocator, num_cols: usize, group_vals: [][]const u8) !GroupState {
    var state: GroupState = .{
        .count = 0,
        .sums = try allocator.alloc(f64, num_cols),
        .mins = try allocator.alloc(?f64, num_cols),
        .maxs = try allocator.alloc(?f64, num_cols),
        .str_mins = try allocator.alloc(?[]const u8, num_cols),
        .str_maxs = try allocator.alloc(?[]const u8, num_cols),
        .non_null_counts = try allocator.alloc(u64, num_cols),
        .group_values = group_vals,
    };
    for (0..num_cols) |i| {
        state.sums[i] = 0;
        state.mins[i] = null;
        state.maxs[i] = null;
        state.str_mins[i] = null;
        state.str_maxs[i] = null;
        state.non_null_counts[i] = 0;
    }
    return state;
}

fn accumulateAgg(allocator: std.mem.Allocator, state: *GroupState, col_idx: usize, val: Value) void {
    if (val == .null_value) return;
    state.non_null_counts[col_idx] += 1;

    const num_val: ?f64 = switch (val) {
        .integer => |v| @floatFromInt(v),
        .bigint => |v| @floatFromInt(v),
        .float => |v| v,
        else => null,
    };

    if (num_val) |nv| {
        state.sums[col_idx] += nv;
        if (state.mins[col_idx] == null or nv < state.mins[col_idx].?) state.mins[col_idx] = nv;
        if (state.maxs[col_idx] == null or nv > state.maxs[col_idx].?) state.maxs[col_idx] = nv;
    } else {
        const sv = formatValue(allocator, val) catch return;
        if (state.str_mins[col_idx]) |cur| {
            if (std.mem.order(u8, sv, cur) == .lt) {
                allocator.free(cur);
                state.str_mins[col_idx] = sv;
            } else if (state.str_maxs[col_idx]) |cur_max| {
                if (std.mem.order(u8, sv, cur_max) == .gt) {
                    allocator.free(cur_max);
                    state.str_maxs[col_idx] = sv;
                } else {
                    allocator.free(sv);
                }
            } else {
                state.str_maxs[col_idx] = sv;
            }
        } else {
            state.str_mins[col_idx] = sv;
            state.str_maxs[col_idx] = allocator.dupe(u8, sv) catch return;
        }
    }
}

fn formatAggValue(allocator: std.mem.Allocator, func: ast.AggregateFunc, has_column: bool, col_idx: usize, state: *const GroupState) ExecError![]const u8 {
    return switch (func) {
        .count => blk: {
            if (has_column) {
                break :blk std.fmt.allocPrint(allocator, "{d}", .{state.non_null_counts[col_idx]}) catch return ExecError.OutOfMemory;
            }
            break :blk std.fmt.allocPrint(allocator, "{d}", .{state.count}) catch return ExecError.OutOfMemory;
        },
        .sum => std.fmt.allocPrint(allocator, "{d:.6}", .{state.sums[col_idx]}) catch return ExecError.OutOfMemory,
        .avg => blk: {
            if (state.non_null_counts[col_idx] == 0) {
                break :blk allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
            }
            break :blk std.fmt.allocPrint(allocator, "{d:.6}", .{state.sums[col_idx] / @as(f64, @floatFromInt(state.non_null_counts[col_idx]))}) catch return ExecError.OutOfMemory;
        },
        .min => blk: {
            if (state.mins[col_idx]) |m| {
                break :blk std.fmt.allocPrint(allocator, "{d:.6}", .{m}) catch return ExecError.OutOfMemory;
            }
            if (state.str_mins[col_idx]) |sm| {
                break :blk allocator.dupe(u8, sm) catch return ExecError.OutOfMemory;
            }
            break :blk allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
        },
        .max => blk: {
            if (state.maxs[col_idx]) |m| {
                break :blk std.fmt.allocPrint(allocator, "{d:.6}", .{m}) catch return ExecError.OutOfMemory;
            }
            if (state.str_maxs[col_idx]) |sm| {
                break :blk allocator.dupe(u8, sm) catch return ExecError.OutOfMemory;
            }
            break :blk allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
        },
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

fn resolveColumnIndex(schema: *const Schema, name: []const u8) ?usize {
    for (schema.columns, 0..) |col, i| {
        if (std.ascii.eqlIgnoreCase(col.name, name)) return i;
    }
    return null;
}
