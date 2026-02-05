const std = @import("std");
const ast = @import("../parser/ast.zig");
const parser_mod = @import("../parser/parser.zig");
const tuple_mod = @import("../storage/tuple.zig");
const catalog_mod = @import("../storage/catalog.zig");
const table_mod = @import("../storage/table.zig");
const page_mod = @import("../storage/page.zig");

const Value = tuple_mod.Value;
const Column = tuple_mod.Column;
const ColumnType = tuple_mod.ColumnType;
const Schema = tuple_mod.Schema;
const Catalog = catalog_mod.Catalog;
const Table = table_mod.Table;
const PageId = page_mod.PageId;
const Parser = parser_mod.Parser;

pub const ExecError = error{
    ParseError,
    TableNotFound,
    TableAlreadyExists,
    ColumnCountMismatch,
    TypeMismatch,
    ColumnNotFound,
    StorageError,
    OutOfMemory,
};

/// A result row — an array of string-formatted values
pub const ResultRow = struct {
    values: [][]const u8,
};

/// The result of executing a SQL statement
pub const ExecResult = union(enum) {
    /// DDL success message (CREATE TABLE, DROP TABLE)
    message: []const u8,
    /// Row count affected (INSERT, DELETE)
    row_count: u64,
    /// Query results (SELECT)
    rows: struct {
        columns: [][]const u8,
        rows: []ResultRow,
    },
};

pub const Executor = struct {
    allocator: std.mem.Allocator,
    catalog: *Catalog,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, catalog: *Catalog) Self {
        return .{
            .allocator = allocator,
            .catalog = catalog,
        };
    }

    /// Execute a SQL string. Caller must call freeResult on the result.
    pub fn execute(self: *Self, sql: []const u8) ExecError!ExecResult {
        var parser = Parser.init(self.allocator, sql);
        defer parser.deinit();

        const stmt = parser.parse() catch {
            return ExecError.ParseError;
        };

        return switch (stmt) {
            .create_table => |ct| self.execCreateTable(ct),
            .insert => |ins| self.execInsert(ins),
            .select => |sel| self.execSelect(sel),
            .delete => |del| self.execDelete(del),
            .drop_table => ExecError.StorageError, // TODO
        };
    }

    /// Free a result returned by execute
    pub fn freeResult(self: *Self, result: ExecResult) void {
        switch (result) {
            .message => |msg| self.allocator.free(msg),
            .row_count => {},
            .rows => |r| {
                for (r.rows) |row| {
                    for (row.values) |val| {
                        self.allocator.free(val);
                    }
                    self.allocator.free(row.values);
                }
                self.allocator.free(r.rows);
                for (r.columns) |col| {
                    self.allocator.free(col);
                }
                self.allocator.free(r.columns);
            },
        }
    }

    // ============================================================
    // CREATE TABLE
    // ============================================================
    fn execCreateTable(self: *Self, ct: ast.CreateTable) ExecError!ExecResult {
        // Map AST column defs to storage Column type
        const columns = self.allocator.alloc(Column, ct.columns.len) catch {
            return ExecError.OutOfMemory;
        };
        defer self.allocator.free(columns);

        for (ct.columns, 0..) |col_def, i| {
            columns[i] = .{
                .name = col_def.name,
                .col_type = mapDataType(col_def.data_type),
                .max_length = col_def.max_length,
                .nullable = col_def.nullable,
            };
        }

        _ = self.catalog.createTable(ct.table_name, columns) catch |err| {
            return switch (err) {
                catalog_mod.CatalogError.TableAlreadyExists => ExecError.TableAlreadyExists,
                else => ExecError.StorageError,
            };
        };

        const msg = std.fmt.allocPrint(self.allocator, "CREATE TABLE", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    // ============================================================
    // INSERT INTO
    // ============================================================
    fn execInsert(self: *Self, ins: ast.Insert) ExecError!ExecResult {
        const result = self.catalog.openTable(ins.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(result.schema);
        var table = result.table;

        const schema = result.schema;

        // Check column count matches
        if (ins.values.len != schema.columns.len) {
            return ExecError.ColumnCountMismatch;
        }

        // Convert AST literal values to storage Values
        const values = self.allocator.alloc(Value, ins.values.len) catch {
            return ExecError.OutOfMemory;
        };
        defer self.allocator.free(values);

        for (ins.values, schema.columns, 0..) |lit, col, i| {
            values[i] = litToValue(lit, col.col_type) catch {
                return ExecError.TypeMismatch;
            };
        }

        _ = table.insertTuple(values) catch {
            return ExecError.StorageError;
        };

        return .{ .row_count = 1 };
    }

    // ============================================================
    // SELECT
    // ============================================================
    fn execSelect(self: *Self, sel: ast.Select) ExecError!ExecResult {
        const result = self.catalog.openTable(sel.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(result.schema);
        var table = result.table;

        const schema = result.schema;

        // Determine which column indices to output
        const col_indices = try self.resolveSelectColumns(sel.columns, schema);
        defer self.allocator.free(col_indices);

        // Build column name headers
        const col_names = self.allocator.alloc([]const u8, col_indices.len) catch {
            return ExecError.OutOfMemory;
        };
        for (col_indices, 0..) |ci, i| {
            col_names[i] = self.allocator.dupe(u8, schema.columns[ci].name) catch {
                // Free already allocated
                for (col_names[0..i]) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            };
        }

        // Scan and filter
        var rows: std.ArrayList(ResultRow) = .empty;
        errdefer {
            for (rows.items) |row| {
                for (row.values) |v| self.allocator.free(v);
                self.allocator.free(row.values);
            }
            rows.deinit(self.allocator);
        }

        var iter = table.scan() catch {
            for (col_names) |cn| self.allocator.free(cn);
            self.allocator.free(col_names);
            return ExecError.StorageError;
        };

        while (iter.next() catch {
            for (col_names) |cn| self.allocator.free(cn);
            self.allocator.free(col_names);
            return ExecError.StorageError;
        }) |row| {
            defer iter.freeValues(row.values);

            // Apply WHERE filter
            if (sel.where_clause) |where| {
                if (!self.evalWhere(where, schema, row.values)) continue;
            }

            // Format selected columns
            const formatted = self.allocator.alloc([]const u8, col_indices.len) catch {
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            };
            for (col_indices, 0..) |ci, i| {
                formatted[i] = formatValue(self.allocator, row.values[ci]) catch {
                    for (formatted[0..i]) |f| self.allocator.free(f);
                    self.allocator.free(formatted);
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    return ExecError.OutOfMemory;
                };
            }

            rows.append(self.allocator, .{ .values = formatted }) catch {
                for (formatted) |f| self.allocator.free(f);
                self.allocator.free(formatted);
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            };
        }

        return .{ .rows = .{
            .columns = col_names,
            .rows = rows.toOwnedSlice(self.allocator) catch {
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            },
        } };
    }

    // ============================================================
    // DELETE
    // ============================================================
    fn execDelete(self: *Self, del: ast.Delete) ExecError!ExecResult {
        const result = self.catalog.openTable(del.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(result.schema);
        var table = result.table;

        const schema = result.schema;

        // Collect TIDs to delete (can't delete while scanning)
        var to_delete: std.ArrayList(page_mod.TupleId) = .empty;
        defer to_delete.deinit(self.allocator);

        var iter = table.scan() catch {
            return ExecError.StorageError;
        };

        while (iter.next() catch { return ExecError.StorageError; }) |row| {
            defer iter.freeValues(row.values);

            const should_delete = if (del.where_clause) |where|
                self.evalWhere(where, schema, row.values)
            else
                true;

            if (should_delete) {
                to_delete.append(self.allocator, row.tid) catch {
                    return ExecError.OutOfMemory;
                };
            }
        }

        // Now delete collected tuples
        var deleted: u64 = 0;
        for (to_delete.items) |tid| {
            if (table.deleteTuple(tid) catch { return ExecError.StorageError; }) {
                deleted += 1;
            }
        }

        return .{ .row_count = deleted };
    }

    // ============================================================
    // WHERE evaluation
    // ============================================================
    fn evalWhere(self: *Self, expr: *const ast.Expression, schema: *const Schema, values: []const Value) bool {
        _ = self;
        return evalExpr(expr, schema, values);
    }

    fn evalExpr(expr: *const ast.Expression, schema: *const Schema, values: []const Value) bool {
        switch (expr.*) {
            .comparison => |cmp| {
                const left_val = resolveExprValue(cmp.left, schema, values);
                const right_val = resolveExprValue(cmp.right, schema, values);
                return compareValues(left_val, cmp.op, right_val);
            },
            .and_expr => |a| {
                return evalExpr(a.left, schema, values) and evalExpr(a.right, schema, values);
            },
            .or_expr => |o| {
                return evalExpr(o.left, schema, values) or evalExpr(o.right, schema, values);
            },
            .not_expr => |n| {
                return !evalExpr(n.operand, schema, values);
            },
            .literal => |lit| {
                // Bare literal in WHERE - treat as truthy
                return switch (lit) {
                    .boolean => |b| b,
                    .null_value => false,
                    .integer => |i| i != 0,
                    else => true,
                };
            },
            .column_ref => {
                // Bare column ref - treat as truthy if not null/false/zero
                return true;
            },
        }
    }

    fn resolveExprValue(expr: *const ast.Expression, schema: *const Schema, values: []const Value) Value {
        switch (expr.*) {
            .column_ref => |name| {
                // Find column index
                for (schema.columns, 0..) |col, i| {
                    if (std.ascii.eqlIgnoreCase(col.name, name)) {
                        return values[i];
                    }
                }
                return .{ .null_value = {} };
            },
            .literal => |lit| return litToStorageValue(lit),
            else => return .{ .null_value = {} },
        }
    }

    fn litToStorageValue(lit: ast.LiteralValue) Value {
        return switch (lit) {
            .integer => |i| .{ .integer = @intCast(i) },
            .float => |f| .{ .float = f },
            .string => |s| .{ .bytes = s },
            .boolean => |b| .{ .boolean = b },
            .null_value => .{ .null_value = {} },
        };
    }

    fn compareValues(left: Value, op: ast.CompOp, right: Value) bool {
        // Handle null comparisons
        if (left == .null_value or right == .null_value) return false;

        switch (left) {
            .integer => |li| {
                const ri = switch (right) {
                    .integer => |v| v,
                    .bigint => |v| @as(i32, @intCast(v)),
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

    // ============================================================
    // Helpers
    // ============================================================

    fn resolveSelectColumns(self: *Self, sel_cols: []const ast.SelectColumn, schema: *const Schema) ExecError![]usize {
        if (sel_cols.len == 1 and sel_cols[0] == .all_columns) {
            // SELECT * — all columns
            const indices = self.allocator.alloc(usize, schema.columns.len) catch {
                return ExecError.OutOfMemory;
            };
            for (0..schema.columns.len) |i| {
                indices[i] = i;
            }
            return indices;
        }

        const indices = self.allocator.alloc(usize, sel_cols.len) catch {
            return ExecError.OutOfMemory;
        };

        for (sel_cols, 0..) |sc, i| {
            const name = sc.named;
            var found = false;
            for (schema.columns, 0..) |col, ci| {
                if (std.ascii.eqlIgnoreCase(col.name, name)) {
                    indices[i] = ci;
                    found = true;
                    break;
                }
            }
            if (!found) {
                self.allocator.free(indices);
                return ExecError.ColumnNotFound;
            }
        }

        return indices;
    }

    fn mapDataType(dt: ast.DataType) ColumnType {
        return switch (dt) {
            .int, .integer => .integer,
            .bigint => .bigint,
            .float => .float,
            .boolean => .boolean,
            .varchar => .varchar,
            .text => .text,
        };
    }

    fn litToValue(lit: ast.LiteralValue, col_type: ColumnType) !Value {
        return switch (lit) {
            .integer => |i| switch (col_type) {
                .integer => Value{ .integer = @intCast(i) },
                .bigint => Value{ .bigint = i },
                .float => Value{ .float = @floatFromInt(i) },
                else => error.TypeMismatch,
            },
            .float => |f| switch (col_type) {
                .float => Value{ .float = f },
                else => error.TypeMismatch,
            },
            .string => |s| switch (col_type) {
                .varchar, .text => Value{ .bytes = s },
                else => error.TypeMismatch,
            },
            .boolean => |b| switch (col_type) {
                .boolean => Value{ .boolean = b },
                else => error.TypeMismatch,
            },
            .null_value => Value{ .null_value = {} },
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
};

// ============================================================
// Tests
// ============================================================

const disk_manager_mod = @import("../storage/disk_manager.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const DiskManager = disk_manager_mod.DiskManager;
const BufferPool = buffer_pool_mod.BufferPool;

fn setupTestEnv(dm: *DiskManager, bp: *BufferPool, catalog: *Catalog) !void {
    _ = dm;
    _ = bp;
    _ = catalog;
}

test "executor create table and insert" {
    const test_file = "test_exec_basic.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    // CREATE TABLE
    const r1 = try exec.execute("CREATE TABLE users (id INT, name VARCHAR(255), email TEXT)");
    defer exec.freeResult(r1);
    try std.testing.expectEqualStrings("CREATE TABLE", r1.message);

    // INSERT
    const r2 = try exec.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com')");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(u64, 1), r2.row_count);
}

test "executor select all" {
    const test_file = "test_exec_select.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE users (id INT, name VARCHAR(255))");
    exec.freeResult(ct);

    const r1 = try exec.execute("INSERT INTO users VALUES (1, 'alice')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO users VALUES (2, 'bob')");
    exec.freeResult(r2);

    const r3 = try exec.execute("SELECT * FROM users");
    defer exec.freeResult(r3);

    const rows = r3.rows;
    try std.testing.expectEqual(@as(usize, 2), rows.columns.len);
    try std.testing.expectEqualStrings("id", rows.columns[0]);
    try std.testing.expectEqualStrings("name", rows.columns[1]);
    try std.testing.expectEqual(@as(usize, 2), rows.rows.len);
}

test "executor select with where" {
    const test_file = "test_exec_where.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE users (id INT, name VARCHAR(255))");
    exec.freeResult(ct);

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        const names = [_][]const u8{ "alice", "bob", "charlie" };
        const sql = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO users VALUES ({d}, '{s}')", .{ i + 1, names[i] }) catch unreachable;
        defer std.testing.allocator.free(sql);
        const r = try exec.execute(sql);
        exec.freeResult(r);
    }

    const r = try exec.execute("SELECT name FROM users WHERE id = 2");
    defer exec.freeResult(r);

    try std.testing.expectEqual(@as(usize, 1), r.rows.columns.len);
    try std.testing.expectEqualStrings("name", r.rows.columns[0]);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("bob", r.rows.rows[0].values[0]);
}

test "executor delete" {
    const test_file = "test_exec_delete.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE items (id INT, name TEXT)");
    exec.freeResult(ct);

    const r1 = try exec.execute("INSERT INTO items VALUES (1, 'apple')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO items VALUES (2, 'banana')");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO items VALUES (3, 'cherry')");
    exec.freeResult(r3);

    // Delete where id = 2
    const del = try exec.execute("DELETE FROM items WHERE id = 2");
    defer exec.freeResult(del);
    try std.testing.expectEqual(@as(u64, 1), del.row_count);

    // Should have 2 rows left
    const sel = try exec.execute("SELECT * FROM items");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);
}

test "executor table not found" {
    const test_file = "test_exec_notfound.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const result = exec.execute("SELECT * FROM nonexistent");
    try std.testing.expectError(ExecError.TableNotFound, result);
}
