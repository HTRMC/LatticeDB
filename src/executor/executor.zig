const std = @import("std");
const ast = @import("../parser/ast.zig");
const parser_mod = @import("../parser/parser.zig");
const tuple_mod = @import("../storage/tuple.zig");
const catalog_mod = @import("../storage/catalog.zig");
const table_mod = @import("../storage/table.zig");
const page_mod = @import("../storage/page.zig");
const mvcc_mod = @import("../storage/mvcc.zig");
const undo_log_mod = @import("../storage/undo_log.zig");

const Value = tuple_mod.Value;
const Column = tuple_mod.Column;
const ColumnType = tuple_mod.ColumnType;
const Schema = tuple_mod.Schema;
const Catalog = catalog_mod.Catalog;
const Table = table_mod.Table;
const PageId = page_mod.PageId;
const Parser = parser_mod.Parser;
const TransactionManager = mvcc_mod.TransactionManager;
const Transaction = mvcc_mod.Transaction;
const UndoLog = undo_log_mod.UndoLog;

pub const ExecError = error{
    ParseError,
    TableNotFound,
    TableAlreadyExists,
    ColumnCountMismatch,
    TypeMismatch,
    ColumnNotFound,
    StorageError,
    OutOfMemory,
    TransactionError,
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
    /// Transaction manager (null = legacy mode, no MVCC)
    txn_manager: ?*TransactionManager,
    /// Undo log (null = legacy mode)
    undo_log: ?*UndoLog,
    /// Current explicit transaction (null = auto-commit mode)
    current_txn: ?*Transaction,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, catalog: *Catalog) Self {
        return initWithMvcc(allocator, catalog, null, null);
    }

    pub fn initWithMvcc(
        allocator: std.mem.Allocator,
        catalog: *Catalog,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
    ) Self {
        return .{
            .allocator = allocator,
            .catalog = catalog,
            .txn_manager = txn_manager,
            .undo_log = undo_log,
            .current_txn = null,
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
            .update => |upd| self.execUpdate(upd),
            .delete => |del| self.execDelete(del),
            .drop_table => ExecError.StorageError, // TODO
            .begin_txn => self.execBegin(),
            .commit_txn => self.execCommit(),
            .rollback_txn => self.execRollback(),
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
    // Transaction control
    // ============================================================

    /// Abort the current transaction without returning a result.
    /// Used for connection cleanup (e.g., client disconnect with open txn).
    pub fn abortCurrentTxn(self: *Self) void {
        const txn = self.current_txn orelse return;
        if (self.undo_log) |undo| {
            self.rollbackFromUndoLog(txn, undo) catch {};
        }
        if (self.txn_manager) |tm| {
            tm.abort(txn);
        }
        self.current_txn = null;
    }

    fn execBegin(self: *Self) ExecError!ExecResult {
        if (self.current_txn != null) {
            return ExecError.TransactionError; // Already in a transaction
        }
        const tm = self.txn_manager orelse return ExecError.TransactionError;
        self.current_txn = tm.begin() catch return ExecError.TransactionError;
        const msg = std.fmt.allocPrint(self.allocator, "BEGIN", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    fn execCommit(self: *Self) ExecError!ExecResult {
        const txn = self.current_txn orelse return ExecError.TransactionError;
        const tm = self.txn_manager orelse return ExecError.TransactionError;
        tm.commit(txn) catch return ExecError.TransactionError;
        self.current_txn = null;
        const msg = std.fmt.allocPrint(self.allocator, "COMMIT", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    fn execRollback(self: *Self) ExecError!ExecResult {
        const txn = self.current_txn orelse return ExecError.TransactionError;
        const tm = self.txn_manager orelse return ExecError.TransactionError;

        // Walk undo chain and restore changes using the catalog's tables
        // For now, we need to rollback all tables this txn touched.
        // Since we don't track which tables were modified, we use a
        // general-purpose rollback through the undo log directly.
        if (self.undo_log) |undo| {
            self.rollbackFromUndoLog(txn, undo) catch {};
        }

        tm.abort(txn);
        self.current_txn = null;
        const msg = std.fmt.allocPrint(self.allocator, "ROLLBACK", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    /// Walk the undo chain and apply rollback operations directly
    fn rollbackFromUndoLog(self: *Self, txn: *Transaction, undo: *UndoLog) !void {
        var undo_ptr = txn.undo_chain_head;
        const buffer_pool = self.catalog.buffer_pool;

        while (undo_ptr != mvcc_mod.NO_UNDO_PTR) {
            const rec = undo.readRecord(undo_ptr) catch break;

            switch (rec.header.record_type) {
                .insert => {
                    // Undo insert = zero the slot
                    var pg = buffer_pool.fetchPage(rec.header.table_page_id) catch break;
                    _ = pg.deleteTuple(rec.header.slot_id);
                    buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                },
                .delete => {
                    // Undo delete = restore xmax to 0
                    var pg = buffer_pool.fetchPage(rec.header.table_page_id) catch break;
                    const zero_xmax = std.mem.asBytes(&@as(mvcc_mod.TxnId, 0));
                    _ = pg.updateTupleData(rec.header.slot_id, 4, zero_xmax);
                    buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                },
                .update => {
                    // Undo update = overwrite heap tuple with old data
                    var pg = buffer_pool.fetchPage(rec.header.table_page_id) catch break;
                    _ = pg.updateTupleData(rec.header.slot_id, 0, rec.data);
                    buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                },
            }

            undo_ptr = rec.header.txn_prev_undo;
        }
    }

    // ============================================================
    // Auto-commit helper: wraps DML in implicit transaction if needed
    // ============================================================

    fn beginImplicitTxn(self: *Self) ?*Transaction {
        if (self.current_txn != null) return self.current_txn;
        const tm = self.txn_manager orelse return null;
        return tm.begin() catch null;
    }

    fn commitImplicitTxn(self: *Self, txn: ?*Transaction) void {
        if (txn == null) return;
        if (self.current_txn != null) return; // Explicit transaction — don't auto-commit
        const tm = self.txn_manager orelse return;
        tm.commit(txn.?) catch {};
    }

    fn abortImplicitTxn(self: *Self, txn: ?*Transaction) void {
        if (txn == null) return;
        if (self.current_txn != null) return;
        const tm = self.txn_manager orelse return;
        tm.abort(txn.?);
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

        // Attach MVCC components
        table.txn_manager = self.txn_manager;
        table.undo_log = self.undo_log;

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

        // Use explicit txn or auto-commit
        const txn = self.current_txn orelse self.beginImplicitTxn();

        _ = table.insertTuple(txn, values) catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        self.commitImplicitTxn(txn);
        return .{ .row_count = 1 };
    }

    // ============================================================
    // SELECT
    // ============================================================
    fn execSelect(self: *Self, sel: ast.Select) ExecError!ExecResult {
        // Route to JOIN handler if joins are present
        if (sel.joins) |join_list| {
            if (join_list.len > 0) {
                return self.execSelectJoin(sel);
            }
        }

        const result = self.catalog.openTable(sel.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(result.schema);
        var table = result.table;

        // Attach MVCC components
        table.txn_manager = self.txn_manager;
        table.undo_log = self.undo_log;

        const schema = result.schema;

        // Check if this is an aggregate or GROUP BY query
        var has_aggregates = false;
        for (sel.columns) |col| {
            if (col == .aggregate) {
                has_aggregates = true;
                break;
            }
        }

        if (has_aggregates or sel.group_by != null) {
            return self.execSelectAggregate(sel, &table, schema);
        }

        // Determine which column indices to output
        const col_indices = try self.resolveSelectColumns(sel.columns, schema);
        defer self.allocator.free(col_indices);

        // Build column name headers
        const col_names = self.allocator.alloc([]const u8, col_indices.len) catch {
            return ExecError.OutOfMemory;
        };
        for (col_indices, 0..) |ci, i| {
            col_names[i] = self.allocator.dupe(u8, schema.columns[ci].name) catch {
                for (col_names[0..i]) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            };
        }

        // Use explicit txn or begin implicit for read
        const txn = self.current_txn orelse self.beginImplicitTxn();

        // Scan and filter
        var rows: std.ArrayList(ResultRow) = .empty;
        errdefer {
            for (rows.items) |row| {
                for (row.values) |v| self.allocator.free(v);
                self.allocator.free(row.values);
            }
            rows.deinit(self.allocator);
        }

        var iter = table.scanWithTxn(txn) catch {
            for (col_names) |cn| self.allocator.free(cn);
            self.allocator.free(col_names);
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        while (iter.next() catch {
            for (col_names) |cn| self.allocator.free(cn);
            self.allocator.free(col_names);
            self.abortImplicitTxn(txn);
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
                self.abortImplicitTxn(txn);
                return ExecError.OutOfMemory;
            };
            for (col_indices, 0..) |ci, i| {
                formatted[i] = formatValue(self.allocator, row.values[ci]) catch {
                    for (formatted[0..i]) |f| self.allocator.free(f);
                    self.allocator.free(formatted);
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
            }

            rows.append(self.allocator, .{ .values = formatted }) catch {
                for (formatted) |f| self.allocator.free(f);
                self.allocator.free(formatted);
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                self.abortImplicitTxn(txn);
                return ExecError.OutOfMemory;
            };
        }

        self.commitImplicitTxn(txn);

        // ORDER BY: sort result rows
        if (sel.order_by) |order_clauses| {
            // Resolve ORDER BY column indices within the selected columns
            const order_indices = self.allocator.alloc(usize, order_clauses.len) catch {
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            };
            defer self.allocator.free(order_indices);

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
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    return ExecError.ColumnNotFound;
                }
            }

            // Insertion sort on result rows
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

        // LIMIT: truncate
        if (sel.limit) |limit_val| {
            const limit: usize = @intCast(@min(limit_val, rows.items.len));
            // Free excess rows
            for (rows.items[limit..]) |row| {
                for (row.values) |v| self.allocator.free(v);
                self.allocator.free(row.values);
            }
            rows.shrinkRetainingCapacity(limit);
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
    // Aggregate SELECT (COUNT, SUM, AVG, MIN, MAX) with GROUP BY
    // ============================================================

    const GroupState = struct {
        count: u64,
        sums: []f64,
        mins: []?f64,
        maxs: []?f64,
        str_mins: []?[]const u8,
        str_maxs: []?[]const u8,
        non_null_counts: []u64,
        group_values: [][]const u8, // formatted GROUP BY column values
    };

    fn initGroupState(allocator: std.mem.Allocator, num_cols: usize, group_vals: [][]const u8) ExecError!GroupState {
        var state: GroupState = .{
            .count = 0,
            .sums = allocator.alloc(f64, num_cols) catch return ExecError.OutOfMemory,
            .mins = allocator.alloc(?f64, num_cols) catch return ExecError.OutOfMemory,
            .maxs = allocator.alloc(?f64, num_cols) catch return ExecError.OutOfMemory,
            .str_mins = allocator.alloc(?[]const u8, num_cols) catch return ExecError.OutOfMemory,
            .str_maxs = allocator.alloc(?[]const u8, num_cols) catch return ExecError.OutOfMemory,
            .non_null_counts = allocator.alloc(u64, num_cols) catch return ExecError.OutOfMemory,
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

    fn freeGroupState(self: *Self, state: *GroupState) void {
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

    fn accumulateAgg(self: *Self, state: *GroupState, col_idx: usize, val: Value) void {
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
            const sv = formatValue(self.allocator, val) catch return;
            if (state.str_mins[col_idx]) |cur| {
                if (std.mem.order(u8, sv, cur) == .lt) {
                    self.allocator.free(cur);
                    state.str_mins[col_idx] = sv;
                } else if (state.str_maxs[col_idx]) |cur_max| {
                    if (std.mem.order(u8, sv, cur_max) == .gt) {
                        self.allocator.free(cur_max);
                        state.str_maxs[col_idx] = sv;
                    } else {
                        self.allocator.free(sv);
                    }
                } else {
                    state.str_maxs[col_idx] = sv;
                }
            } else {
                state.str_mins[col_idx] = sv;
                state.str_maxs[col_idx] = self.allocator.dupe(u8, sv) catch return;
            }
        }
    }

    fn formatAggValue(self: *Self, func: ast.AggregateFunc, has_column: bool, col_idx: usize, state: *const GroupState) ExecError![]const u8 {
        return switch (func) {
            .count => blk: {
                if (has_column) {
                    break :blk std.fmt.allocPrint(self.allocator, "{d}", .{state.non_null_counts[col_idx]}) catch return ExecError.OutOfMemory;
                }
                break :blk std.fmt.allocPrint(self.allocator, "{d}", .{state.count}) catch return ExecError.OutOfMemory;
            },
            .sum => std.fmt.allocPrint(self.allocator, "{d:.6}", .{state.sums[col_idx]}) catch return ExecError.OutOfMemory,
            .avg => blk: {
                if (state.non_null_counts[col_idx] == 0) {
                    break :blk self.allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
                }
                break :blk std.fmt.allocPrint(self.allocator, "{d:.6}", .{state.sums[col_idx] / @as(f64, @floatFromInt(state.non_null_counts[col_idx]))}) catch return ExecError.OutOfMemory;
            },
            .min => blk: {
                if (state.mins[col_idx]) |m| {
                    break :blk std.fmt.allocPrint(self.allocator, "{d:.6}", .{m}) catch return ExecError.OutOfMemory;
                }
                if (state.str_mins[col_idx]) |sm| {
                    break :blk self.allocator.dupe(u8, sm) catch return ExecError.OutOfMemory;
                }
                break :blk self.allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
            },
            .max => blk: {
                if (state.maxs[col_idx]) |m| {
                    break :blk std.fmt.allocPrint(self.allocator, "{d:.6}", .{m}) catch return ExecError.OutOfMemory;
                }
                if (state.str_maxs[col_idx]) |sm| {
                    break :blk self.allocator.dupe(u8, sm) catch return ExecError.OutOfMemory;
                }
                break :blk self.allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
            },
        };
    }

    fn execSelectAggregate(
        self: *Self,
        sel: ast.Select,
        table: *Table,
        schema: *const Schema,
    ) ExecError!ExecResult {
        const num_cols = sel.columns.len;

        // Resolve column indices for each SELECT column
        const agg_col_indices = self.allocator.alloc(?usize, num_cols) catch return ExecError.OutOfMemory;
        defer self.allocator.free(agg_col_indices);

        for (sel.columns, 0..) |col, i| {
            switch (col) {
                .aggregate => |agg| {
                    if (agg.column) |col_name| {
                        agg_col_indices[i] = resolveColumnIndex(schema, col_name) orelse return ExecError.ColumnNotFound;
                    } else {
                        agg_col_indices[i] = null; // COUNT(*)
                    }
                },
                .named => |name| {
                    agg_col_indices[i] = resolveColumnIndex(schema, name) orelse return ExecError.ColumnNotFound;
                },
                .qualified => |q| {
                    agg_col_indices[i] = resolveColumnIndex(schema, q.column) orelse return ExecError.ColumnNotFound;
                },
                .all_columns => return ExecError.ColumnNotFound,
            }
        }

        // Resolve GROUP BY column indices
        var group_by_indices: []usize = &.{};
        if (sel.group_by) |gb_cols| {
            group_by_indices = self.allocator.alloc(usize, gb_cols.len) catch return ExecError.OutOfMemory;
            for (gb_cols, 0..) |gb_name, gi| {
                group_by_indices[gi] = resolveColumnIndex(schema, gb_name) orelse {
                    self.allocator.free(group_by_indices);
                    return ExecError.ColumnNotFound;
                };
            }
        }
        defer if (group_by_indices.len > 0) self.allocator.free(group_by_indices);

        const has_group_by = group_by_indices.len > 0;

        // Use explicit txn or begin implicit for read
        const txn = self.current_txn orelse self.beginImplicitTxn();

        // Groups: key = concatenated group values, value = GroupState
        var groups = std.StringArrayHashMap(GroupState).init(self.allocator);
        defer {
            for (groups.values()) |*gs| self.freeGroupState(gs);
            for (groups.keys()) |k| self.allocator.free(k);
            groups.deinit();
        }

        // For non-GROUP BY aggregates, use a single group with empty key
        if (!has_group_by) {
            const empty_vals = self.allocator.alloc([]const u8, 0) catch return ExecError.OutOfMemory;
            const state = initGroupState(self.allocator, num_cols, empty_vals) catch return ExecError.OutOfMemory;
            const key = self.allocator.dupe(u8, "") catch return ExecError.OutOfMemory;
            groups.put(key, state) catch return ExecError.OutOfMemory;
        }

        var iter = table.scanWithTxn(txn) catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        while (iter.next() catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        }) |row| {
            defer iter.freeValues(row.values);

            // Apply WHERE filter
            if (sel.where_clause) |where| {
                if (!self.evalWhere(where, schema, row.values)) continue;
            }

            // Build group key
            var group_key: []u8 = undefined;
            if (has_group_by) {
                var key_parts: std.ArrayList(u8) = .empty;
                for (group_by_indices, 0..) |gi, idx| {
                    if (idx > 0) key_parts.append(self.allocator, 0) catch {
                        key_parts.deinit(self.allocator);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                    const fv = formatValue(self.allocator, row.values[gi]) catch {
                        key_parts.deinit(self.allocator);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                    defer self.allocator.free(fv);
                    key_parts.appendSlice(self.allocator, fv) catch {
                        key_parts.deinit(self.allocator);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                }
                group_key = key_parts.toOwnedSlice(self.allocator) catch {
                    key_parts.deinit(self.allocator);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
            } else {
                group_key = self.allocator.dupe(u8, "") catch {
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
            }

            // Get or create group
            if (!groups.contains(group_key)) {
                // Create group values
                const gv = self.allocator.alloc([]const u8, group_by_indices.len) catch {
                    self.allocator.free(group_key);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                for (group_by_indices, 0..) |gi, idx| {
                    gv[idx] = formatValue(self.allocator, row.values[gi]) catch {
                        for (gv[0..idx]) |v| self.allocator.free(v);
                        self.allocator.free(gv);
                        self.allocator.free(group_key);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                }
                const state = initGroupState(self.allocator, num_cols, gv) catch {
                    for (gv) |v| self.allocator.free(v);
                    self.allocator.free(gv);
                    self.allocator.free(group_key);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                groups.put(group_key, state) catch {
                    self.allocator.free(group_key);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
            } else {
                self.allocator.free(group_key);
            }

            // Get group and accumulate (need to find the key again since we may have freed it)
            const state_ptr: *GroupState = if (has_group_by) blk: {
                // Rebuild key for lookup
                var key_parts2: std.ArrayList(u8) = .empty;
                for (group_by_indices, 0..) |gi, idx| {
                    if (idx > 0) key_parts2.append(self.allocator, 0) catch {
                        key_parts2.deinit(self.allocator);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                    const fv2 = formatValue(self.allocator, row.values[gi]) catch {
                        key_parts2.deinit(self.allocator);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                    defer self.allocator.free(fv2);
                    key_parts2.appendSlice(self.allocator, fv2) catch {
                        key_parts2.deinit(self.allocator);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                }
                const lookup_key = key_parts2.toOwnedSlice(self.allocator) catch {
                    key_parts2.deinit(self.allocator);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                defer self.allocator.free(lookup_key);
                break :blk groups.getPtr(lookup_key).?;
            } else groups.getPtr("").?;

            state_ptr.count += 1;

            for (0..num_cols) |i| {
                const ci = agg_col_indices[i] orelse continue;
                self.accumulateAgg(state_ptr, i, row.values[ci]);
            }
        }

        self.commitImplicitTxn(txn);

        // Build column headers
        const col_names = self.allocator.alloc([]const u8, num_cols) catch return ExecError.OutOfMemory;
        errdefer {
            for (col_names) |cn| self.allocator.free(cn);
            self.allocator.free(col_names);
        }

        for (sel.columns, 0..) |col, i| {
            col_names[i] = switch (col) {
                .aggregate => |agg| blk: {
                    const func_name = switch (agg.func) {
                        .count => "count",
                        .sum => "sum",
                        .avg => "avg",
                        .min => "min",
                        .max => "max",
                    };
                    break :blk if (agg.column) |cn|
                        std.fmt.allocPrint(self.allocator, "{s}({s})", .{ func_name, cn }) catch return ExecError.OutOfMemory
                    else
                        std.fmt.allocPrint(self.allocator, "{s}(*)", .{func_name}) catch return ExecError.OutOfMemory;
                },
                .named => |name| self.allocator.dupe(u8, name) catch return ExecError.OutOfMemory,
                .qualified => |q| self.allocator.dupe(u8, q.column) catch return ExecError.OutOfMemory,
                .all_columns => unreachable,
            };
        }

        // Build result rows — one per group
        var result_rows: std.ArrayList(ResultRow) = .empty;
        errdefer {
            for (result_rows.items) |rr| {
                for (rr.values) |v| self.allocator.free(v);
                self.allocator.free(rr.values);
            }
            result_rows.deinit(self.allocator);
        }

        for (groups.values()) |*state| {
            const formatted = self.allocator.alloc([]const u8, num_cols) catch return ExecError.OutOfMemory;

            for (sel.columns, 0..) |col, i| {
                formatted[i] = switch (col) {
                    .aggregate => |agg| try self.formatAggValue(agg.func, agg.column != null, i, state),
                    .named => |name| blk: {
                        // For GROUP BY columns, output the group value
                        if (sel.group_by) |gb_cols| {
                            for (gb_cols, 0..) |gb_name, gi| {
                                if (std.ascii.eqlIgnoreCase(gb_name, name)) {
                                    break :blk self.allocator.dupe(u8, state.group_values[gi]) catch return ExecError.OutOfMemory;
                                }
                            }
                        }
                        break :blk self.allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
                    },
                    .qualified => |q| blk: {
                        if (sel.group_by) |gb_cols| {
                            for (gb_cols, 0..) |gb_name, gi| {
                                if (std.ascii.eqlIgnoreCase(gb_name, q.column)) {
                                    break :blk self.allocator.dupe(u8, state.group_values[gi]) catch return ExecError.OutOfMemory;
                                }
                            }
                        }
                        break :blk self.allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
                    },
                    .all_columns => unreachable,
                };
            }

            result_rows.append(self.allocator, .{ .values = formatted }) catch return ExecError.OutOfMemory;
        }

        return .{ .rows = .{
            .columns = col_names,
            .rows = result_rows.toOwnedSlice(self.allocator) catch return ExecError.OutOfMemory,
        } };
    }

    fn resolveColumnIndex(schema: *const Schema, name: []const u8) ?usize {
        for (schema.columns, 0..) |col, i| {
            if (std.ascii.eqlIgnoreCase(col.name, name)) return i;
        }
        return null;
    }

    // ============================================================
    // JOIN execution (nested loop join)
    // ============================================================
    fn execSelectJoin(self: *Self, sel: ast.Select) ExecError!ExecResult {
        const join_list = sel.joins orelse return ExecError.StorageError;
        if (join_list.len == 0) return ExecError.StorageError;

        // Currently support single JOIN only
        const join = join_list[0];

        // Open left table
        const left_result = self.catalog.openTable(sel.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(left_result.schema);
        var left_table = left_result.table;
        left_table.txn_manager = self.txn_manager;
        left_table.undo_log = self.undo_log;
        const left_schema = left_result.schema;

        // Open right table
        const right_result = self.catalog.openTable(join.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(right_result.schema);
        var right_table = right_result.table;
        right_table.txn_manager = self.txn_manager;
        right_table.undo_log = self.undo_log;
        const right_schema = right_result.schema;

        // Build combined schema columns for column resolution
        const combined_count = left_schema.columns.len + right_schema.columns.len;
        const combined_cols = self.allocator.alloc(Column, combined_count) catch return ExecError.OutOfMemory;
        defer self.allocator.free(combined_cols);
        @memcpy(combined_cols[0..left_schema.columns.len], left_schema.columns);
        @memcpy(combined_cols[left_schema.columns.len..], right_schema.columns);

        const combined_schema_val = Schema{ .columns = combined_cols };
        const combined_schema: *const Schema = &combined_schema_val;

        // Resolve output columns (with table name context for qualified refs)
        const col_indices = try self.resolveSelectColumnsJoin(sel.columns, combined_schema, sel.table_name, join.table_name, left_schema.columns.len);
        defer self.allocator.free(col_indices);

        // Build column headers
        const col_names = self.allocator.alloc([]const u8, col_indices.len) catch return ExecError.OutOfMemory;
        for (col_indices, 0..) |ci, i| {
            col_names[i] = self.allocator.dupe(u8, combined_cols[ci].name) catch {
                for (col_names[0..i]) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            };
        }

        const txn = self.current_txn orelse self.beginImplicitTxn();

        var rows: std.ArrayList(ResultRow) = .empty;
        errdefer {
            for (rows.items) |row| {
                for (row.values) |v| self.allocator.free(v);
                self.allocator.free(row.values);
            }
            rows.deinit(self.allocator);
        }

        // Materialize right table rows for nested loop
        var right_rows: std.ArrayList([]Value) = .empty;
        defer {
            for (right_rows.items) |rv| {
                self.allocator.free(rv);
            }
            right_rows.deinit(self.allocator);
        }

        {
            var riter = right_table.scanWithTxn(txn) catch {
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            };
            while (riter.next() catch {
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            }) |row| {
                // Keep the values (don't free via iterator)
                right_rows.append(self.allocator, row.values) catch {
                    riter.freeValues(row.values);
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
            }
        }

        // Nested loop: for each left row, scan all right rows
        var liter = left_table.scanWithTxn(txn) catch {
            for (col_names) |cn| self.allocator.free(cn);
            self.allocator.free(col_names);
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        while (liter.next() catch {
            for (col_names) |cn| self.allocator.free(cn);
            self.allocator.free(col_names);
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        }) |left_row| {
            defer liter.freeValues(left_row.values);

            // Build combined values array
            const combined_values = self.allocator.alloc(Value, combined_count) catch {
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                self.abortImplicitTxn(txn);
                return ExecError.OutOfMemory;
            };
            @memcpy(combined_values[0..left_schema.columns.len], left_row.values);

            var matched = false;
            for (right_rows.items) |right_vals| {
                @memcpy(combined_values[left_schema.columns.len..], right_vals);

                // Evaluate ON condition with combined schema
                const on_match = self.evalJoinCondition(join.on_condition, sel.table_name, join.table_name, left_schema, right_schema, left_row.values, right_vals);
                if (!on_match) continue;

                // Apply WHERE filter on combined row
                if (sel.where_clause) |where| {
                    if (!evalExpr(where, combined_schema, combined_values)) continue;
                }

                matched = true;

                // Format output columns
                const formatted = self.allocator.alloc([]const u8, col_indices.len) catch {
                    self.allocator.free(combined_values);
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                for (col_indices, 0..) |ci, i| {
                    formatted[i] = formatValue(self.allocator, combined_values[ci]) catch {
                        for (formatted[0..i]) |f| self.allocator.free(f);
                        self.allocator.free(formatted);
                        self.allocator.free(combined_values);
                        for (col_names) |cn| self.allocator.free(cn);
                        self.allocator.free(col_names);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                }
                rows.append(self.allocator, .{ .values = formatted }) catch {
                    for (formatted) |f| self.allocator.free(f);
                    self.allocator.free(formatted);
                    self.allocator.free(combined_values);
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
            }

            // LEFT JOIN: emit row with NULLs for right side if no match
            if (join.join_type == .left and !matched) {
                // Fill right side with nulls
                for (combined_values[left_schema.columns.len..]) |*v| {
                    v.* = .{ .null_value = {} };
                }

                if (sel.where_clause) |where| {
                    if (!evalExpr(where, combined_schema, combined_values)) {
                        self.allocator.free(combined_values);
                        continue;
                    }
                }

                const formatted = self.allocator.alloc([]const u8, col_indices.len) catch {
                    self.allocator.free(combined_values);
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                for (col_indices, 0..) |ci, i| {
                    formatted[i] = formatValue(self.allocator, combined_values[ci]) catch {
                        for (formatted[0..i]) |f| self.allocator.free(f);
                        self.allocator.free(formatted);
                        self.allocator.free(combined_values);
                        for (col_names) |cn| self.allocator.free(cn);
                        self.allocator.free(col_names);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                }
                rows.append(self.allocator, .{ .values = formatted }) catch {
                    for (formatted) |f| self.allocator.free(f);
                    self.allocator.free(formatted);
                    self.allocator.free(combined_values);
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
            }

            self.allocator.free(combined_values);
        }

        self.commitImplicitTxn(txn);

        return .{ .rows = .{
            .columns = col_names,
            .rows = rows.toOwnedSlice(self.allocator) catch {
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            },
        } };
    }

    /// Evaluate a JOIN ON condition, resolving qualified refs to the correct table
    fn evalJoinCondition(
        self: *Self,
        expr: *const ast.Expression,
        left_table_name: []const u8,
        right_table_name: []const u8,
        left_schema: *const Schema,
        right_schema: *const Schema,
        left_values: []const Value,
        right_values: []const Value,
    ) bool {
        switch (expr.*) {
            .comparison => |cmp| {
                const left_val = resolveJoinExprValue(cmp.left, left_table_name, right_table_name, left_schema, right_schema, left_values, right_values);
                const right_val = resolveJoinExprValue(cmp.right, left_table_name, right_table_name, left_schema, right_schema, left_values, right_values);
                return compareValues(left_val, cmp.op, right_val);
            },
            .and_expr => |a| {
                return self.evalJoinCondition(a.left, left_table_name, right_table_name, left_schema, right_schema, left_values, right_values) and
                    self.evalJoinCondition(a.right, left_table_name, right_table_name, left_schema, right_schema, left_values, right_values);
            },
            .or_expr => |o| {
                return self.evalJoinCondition(o.left, left_table_name, right_table_name, left_schema, right_schema, left_values, right_values) or
                    self.evalJoinCondition(o.right, left_table_name, right_table_name, left_schema, right_schema, left_values, right_values);
            },
            else => return false,
        }
    }

    fn resolveJoinExprValue(
        expr: *const ast.Expression,
        left_table_name: []const u8,
        right_table_name: []const u8,
        left_schema: *const Schema,
        right_schema: *const Schema,
        left_values: []const Value,
        right_values: []const Value,
    ) Value {
        switch (expr.*) {
            .qualified_ref => |qr| {
                if (std.ascii.eqlIgnoreCase(qr.table, left_table_name)) {
                    if (resolveColumnIndex(left_schema, qr.column)) |idx| {
                        return left_values[idx];
                    }
                }
                if (std.ascii.eqlIgnoreCase(qr.table, right_table_name)) {
                    if (resolveColumnIndex(right_schema, qr.column)) |idx| {
                        return right_values[idx];
                    }
                }
                return .{ .null_value = {} };
            },
            .column_ref => |name| {
                // Try left table first, then right
                if (resolveColumnIndex(left_schema, name)) |idx| {
                    return left_values[idx];
                }
                if (resolveColumnIndex(right_schema, name)) |idx| {
                    return right_values[idx];
                }
                return .{ .null_value = {} };
            },
            .literal => |lit| return litToStorageValue(lit),
            else => return .{ .null_value = {} },
        }
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

    // ============================================================
    // DELETE
    // ============================================================
    fn execDelete(self: *Self, del: ast.Delete) ExecError!ExecResult {
        const result = self.catalog.openTable(del.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(result.schema);
        var table = result.table;

        // Attach MVCC components
        table.txn_manager = self.txn_manager;
        table.undo_log = self.undo_log;

        const schema = result.schema;

        // Use explicit txn or auto-commit
        const txn = self.current_txn orelse self.beginImplicitTxn();

        // Collect TIDs to delete (can't delete while scanning)
        var to_delete: std.ArrayList(page_mod.TupleId) = .empty;
        defer to_delete.deinit(self.allocator);

        var iter = table.scanWithTxn(txn) catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        while (iter.next() catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        }) |row| {
            defer iter.freeValues(row.values);

            const should_delete = if (del.where_clause) |where|
                self.evalWhere(where, schema, row.values)
            else
                true;

            if (should_delete) {
                to_delete.append(self.allocator, row.tid) catch {
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
            }
        }

        // Now delete collected tuples
        var deleted: u64 = 0;
        for (to_delete.items) |tid| {
            if (table.deleteTuple(txn, tid) catch {
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            }) {
                deleted += 1;
            }
        }

        self.commitImplicitTxn(txn);
        return .{ .row_count = deleted };
    }

    // ============================================================
    // UPDATE table SET col = val [, ...] [WHERE expr]
    // Implemented as delete + insert (reuses existing MVCC undo)
    // ============================================================
    fn execUpdate(self: *Self, upd: ast.Update) ExecError!ExecResult {
        const result = self.catalog.openTable(upd.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(result.schema);
        var table = result.table;

        // Attach MVCC components
        table.txn_manager = self.txn_manager;
        table.undo_log = self.undo_log;

        const schema = result.schema;

        // Resolve SET column indices
        const set_indices = self.allocator.alloc(usize, upd.assignments.len) catch {
            return ExecError.OutOfMemory;
        };
        defer self.allocator.free(set_indices);

        for (upd.assignments, 0..) |assign, i| {
            var found = false;
            for (schema.columns, 0..) |col, ci| {
                if (std.ascii.eqlIgnoreCase(col.name, assign.column)) {
                    set_indices[i] = ci;
                    found = true;
                    break;
                }
            }
            if (!found) return ExecError.ColumnNotFound;
        }

        // Use explicit txn or auto-commit
        const txn = self.current_txn orelse self.beginImplicitTxn();

        // Scan and collect rows to update (TID + current values)
        const UpdateEntry = struct { tid: page_mod.TupleId, values: []Value };
        var to_update: std.ArrayList(UpdateEntry) = .empty;
        defer {
            for (to_update.items) |entry| {
                self.allocator.free(entry.values);
            }
            to_update.deinit(self.allocator);
        }

        var iter = table.scanWithTxn(txn) catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        while (iter.next() catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        }) |row| {
            const matches = if (upd.where_clause) |where|
                self.evalWhere(where, schema, row.values)
            else
                true;

            if (matches) {
                to_update.append(self.allocator, .{ .tid = row.tid, .values = row.values }) catch {
                    iter.freeValues(row.values);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                // Don't free row.values — we're keeping them
            } else {
                iter.freeValues(row.values);
            }
        }

        // Apply updates: delete old tuple, insert new one
        var updated: u64 = 0;
        for (to_update.items) |entry| {
            // Build new values by copying old and applying SET
            const new_values = self.allocator.alloc(Value, schema.columns.len) catch {
                self.abortImplicitTxn(txn);
                return ExecError.OutOfMemory;
            };
            defer self.allocator.free(new_values);

            @memcpy(new_values, entry.values);

            // Apply SET assignments
            for (upd.assignments, set_indices) |assign, ci| {
                new_values[ci] = litToValue(assign.value, schema.columns[ci].col_type) catch {
                    self.abortImplicitTxn(txn);
                    return ExecError.TypeMismatch;
                };
            }

            // Delete old tuple (MVCC: sets xmax)
            _ = table.deleteTuple(txn, entry.tid) catch {
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            };

            // Insert new tuple (MVCC: new xmin)
            _ = table.insertTuple(txn, new_values) catch {
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            };

            updated += 1;
        }

        self.commitImplicitTxn(txn);
        return .{ .row_count = updated };
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
            .qualified_ref => |qr| {
                // Resolve as column_ref using just the column name (table prefix ignored in combined schema)
                if (resolveColumnIndex(schema, qr.column)) |idx| {
                    const val = values[idx];
                    return switch (val) {
                        .null_value => false,
                        .boolean => |b| b,
                        .integer => |i| i != 0,
                        else => true,
                    };
                }
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
            .qualified_ref => |qr| {
                // Resolve by column name (table prefix ignored — uses combined schema)
                for (schema.columns, 0..) |col, i| {
                    if (std.ascii.eqlIgnoreCase(col.name, qr.column)) {
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
        return self.resolveSelectColumnsJoin(sel_cols, schema, null, null, 0);
    }

    fn resolveSelectColumnsJoin(
        self: *Self,
        sel_cols: []const ast.SelectColumn,
        schema: *const Schema,
        left_table: ?[]const u8,
        right_table: ?[]const u8,
        left_col_count: usize,
    ) ExecError![]usize {
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
            switch (sc) {
                .named => |name| {
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
                },
                .qualified => |q| {
                    // Determine which table region to search
                    var search_start: usize = 0;
                    var search_end: usize = schema.columns.len;
                    if (left_table != null and right_table != null) {
                        if (std.ascii.eqlIgnoreCase(q.table, left_table.?)) {
                            search_end = left_col_count;
                        } else if (std.ascii.eqlIgnoreCase(q.table, right_table.?)) {
                            search_start = left_col_count;
                        }
                    }
                    var found = false;
                    for (schema.columns[search_start..search_end], search_start..) |col, ci| {
                        if (std.ascii.eqlIgnoreCase(col.name, q.column)) {
                            indices[i] = ci;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        self.allocator.free(indices);
                        return ExecError.ColumnNotFound;
                    }
                },
                else => {
                    self.allocator.free(indices);
                    return ExecError.ColumnNotFound;
                },
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

    const row_result = r3.rows;
    try std.testing.expectEqual(@as(usize, 2), row_result.columns.len);
    try std.testing.expectEqualStrings("id", row_result.columns[0]);
    try std.testing.expectEqualStrings("name", row_result.columns[1]);
    try std.testing.expectEqual(@as(usize, 2), row_result.rows.len);
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

test "executor MVCC begin commit rollback" {
    const test_file = "test_exec_mvcc.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    // Create table (auto-commit)
    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);

    // Begin explicit transaction
    const begin_r = try exec.execute("BEGIN");
    exec.freeResult(begin_r);
    try std.testing.expect(exec.current_txn != null);

    // Insert within transaction
    const ins = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(ins);

    // Should see the row within the same transaction
    const sel1 = try exec.execute("SELECT * FROM users");
    defer exec.freeResult(sel1);
    try std.testing.expectEqual(@as(usize, 1), sel1.rows.rows.len);

    // Rollback
    const rb = try exec.execute("ROLLBACK");
    exec.freeResult(rb);
    try std.testing.expect(exec.current_txn == null);

    // Row should be gone after rollback
    const sel2 = try exec.execute("SELECT * FROM users");
    defer exec.freeResult(sel2);
    try std.testing.expectEqual(@as(usize, 0), sel2.rows.rows.len);
}

test "executor MVCC auto-commit" {
    const test_file = "test_exec_autocommit.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    // Create table (auto-commit)
    const ct = try exec.execute("CREATE TABLE items (id INT)");
    exec.freeResult(ct);

    // Insert without explicit BEGIN — should auto-commit
    const ins = try exec.execute("INSERT INTO items VALUES (42)");
    exec.freeResult(ins);

    // Should be visible in next query (also auto-commit)
    const sel = try exec.execute("SELECT * FROM items");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
    try std.testing.expectEqualStrings("42", sel.rows.rows[0].values[0]);
}

test "executor MVCC commit persists" {
    const test_file = "test_exec_mvcc_commit.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    // BEGIN, INSERT, COMMIT
    const b = try exec.execute("BEGIN");
    exec.freeResult(b);
    const ins = try exec.execute("INSERT INTO t VALUES (1)");
    exec.freeResult(ins);
    const c = try exec.execute("COMMIT");
    exec.freeResult(c);

    // Row should be visible after commit
    const sel = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
}

test "executor UPDATE with WHERE" {
    const test_file = "test_exec_update.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);

    const ins1 = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO users VALUES (2, 'Bob')");
    exec.freeResult(ins2);

    // UPDATE one row
    const upd = try exec.execute("UPDATE users SET name = 'Alicia' WHERE id = 1");
    defer exec.freeResult(upd);
    try std.testing.expectEqual(@as(u64, 1), upd.row_count);

    // Verify: Alice → Alicia, Bob unchanged
    const sel = try exec.execute("SELECT name FROM users WHERE id = 1");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
    try std.testing.expectEqualStrings("Alicia", sel.rows.rows[0].values[0]);

    const sel2 = try exec.execute("SELECT name FROM users WHERE id = 2");
    defer exec.freeResult(sel2);
    try std.testing.expectEqual(@as(usize, 1), sel2.rows.rows.len);
    try std.testing.expectEqualStrings("Bob", sel2.rows.rows[0].values[0]);
}

test "executor UPDATE all rows" {
    const test_file = "test_exec_update_all.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE items (id INT, price INT)");
    exec.freeResult(ct);

    const ins1 = try exec.execute("INSERT INTO items VALUES (1, 10)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO items VALUES (2, 20)");
    exec.freeResult(ins2);

    // UPDATE all rows (no WHERE)
    const upd = try exec.execute("UPDATE items SET price = 99");
    defer exec.freeResult(upd);
    try std.testing.expectEqual(@as(u64, 2), upd.row_count);

    // All prices should be 99
    const sel = try exec.execute("SELECT * FROM items");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);
    for (sel.rows.rows) |row| {
        try std.testing.expectEqualStrings("99", row.values[1]);
    }
}

test "executor UPDATE rollback" {
    const test_file = "test_exec_update_rollback.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT, val TEXT)");
    exec.freeResult(ct);

    const ins = try exec.execute("INSERT INTO t VALUES (1, 'original')");
    exec.freeResult(ins);

    // BEGIN, UPDATE, ROLLBACK
    const b = try exec.execute("BEGIN");
    exec.freeResult(b);
    const upd = try exec.execute("UPDATE t SET val = 'changed' WHERE id = 1");
    exec.freeResult(upd);
    const rb = try exec.execute("ROLLBACK");
    exec.freeResult(rb);

    // Value should be back to 'original'
    const sel = try exec.execute("SELECT val FROM t WHERE id = 1");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
    try std.testing.expectEqualStrings("original", sel.rows.rows[0].values[0]);
}

test "executor ORDER BY" {
    const test_file = "test_exec_orderby.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    const r1 = try exec.execute("INSERT INTO t VALUES (3, 'Charlie')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO t VALUES (1, 'Alice')");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO t VALUES (2, 'Bob')");
    exec.freeResult(r3);

    // ORDER BY id ASC
    const sel = try exec.execute("SELECT * FROM t ORDER BY id");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 3), sel.rows.rows.len);
    try std.testing.expectEqualStrings("1", sel.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("2", sel.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("3", sel.rows.rows[2].values[0]);

    // ORDER BY id DESC
    const sel2 = try exec.execute("SELECT * FROM t ORDER BY id DESC");
    defer exec.freeResult(sel2);
    try std.testing.expectEqualStrings("3", sel2.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("2", sel2.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("1", sel2.rows.rows[2].values[0]);

    // ORDER BY name ASC
    const sel3 = try exec.execute("SELECT * FROM t ORDER BY name");
    defer exec.freeResult(sel3);
    try std.testing.expectEqualStrings("Alice", sel3.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("Bob", sel3.rows.rows[1].values[1]);
    try std.testing.expectEqualStrings("Charlie", sel3.rows.rows[2].values[1]);
}

test "executor LIMIT" {
    const test_file = "test_exec_limit.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var idx: usize = 0;
    while (idx < 5) : (idx += 1) {
        const sql = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{idx + 1}) catch unreachable;
        defer std.testing.allocator.free(sql);
        const r = try exec.execute(sql);
        exec.freeResult(r);
    }

    // LIMIT 3
    const sel = try exec.execute("SELECT * FROM t LIMIT 3");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 3), sel.rows.rows.len);

    // ORDER BY + LIMIT
    const sel2 = try exec.execute("SELECT * FROM t ORDER BY id DESC LIMIT 2");
    defer exec.freeResult(sel2);
    try std.testing.expectEqual(@as(usize, 2), sel2.rows.rows.len);
    try std.testing.expectEqualStrings("5", sel2.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("4", sel2.rows.rows[1].values[0]);
}

test "executor COUNT" {
    const test_file = "test_exec_count.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    const r1 = try exec.execute("INSERT INTO t VALUES (1, 'Alice')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO t VALUES (2, 'Bob')");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO t VALUES (3, 'Charlie')");
    exec.freeResult(r3);

    // COUNT(*)
    const sel = try exec.execute("SELECT COUNT(*) FROM t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
    try std.testing.expectEqualStrings("count(*)", sel.rows.columns[0]);
    try std.testing.expectEqualStrings("3", sel.rows.rows[0].values[0]);

    // COUNT(*) with WHERE
    const sel2 = try exec.execute("SELECT COUNT(*) FROM t WHERE id > 1");
    defer exec.freeResult(sel2);
    try std.testing.expectEqualStrings("2", sel2.rows.rows[0].values[0]);
}

test "executor SUM AVG" {
    const test_file = "test_exec_sum_avg.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE items (id INT, price INT)");
    exec.freeResult(ct);

    const r1 = try exec.execute("INSERT INTO items VALUES (1, 10)");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO items VALUES (2, 20)");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO items VALUES (3, 30)");
    exec.freeResult(r3);

    // SUM
    const sel = try exec.execute("SELECT SUM(price) FROM items");
    defer exec.freeResult(sel);
    try std.testing.expectEqualStrings("sum(price)", sel.rows.columns[0]);
    // SUM = 60, formatted as float
    const sum_val = std.fmt.parseFloat(f64, sel.rows.rows[0].values[0]) catch unreachable;
    try std.testing.expectEqual(@as(f64, 60.0), sum_val);

    // AVG
    const sel2 = try exec.execute("SELECT AVG(price) FROM items");
    defer exec.freeResult(sel2);
    const avg_val = std.fmt.parseFloat(f64, sel2.rows.rows[0].values[0]) catch unreachable;
    try std.testing.expectEqual(@as(f64, 20.0), avg_val);
}

test "executor MIN MAX" {
    const test_file = "test_exec_min_max.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    const r1 = try exec.execute("INSERT INTO t VALUES (5, 'Eve')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO t VALUES (1, 'Alice')");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO t VALUES (9, 'Zara')");
    exec.freeResult(r3);

    // MIN/MAX on numeric column
    const sel = try exec.execute("SELECT MIN(id), MAX(id) FROM t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.columns.len);
    try std.testing.expectEqualStrings("min(id)", sel.rows.columns[0]);
    try std.testing.expectEqualStrings("max(id)", sel.rows.columns[1]);
    const min_val = std.fmt.parseFloat(f64, sel.rows.rows[0].values[0]) catch unreachable;
    const max_val = std.fmt.parseFloat(f64, sel.rows.rows[0].values[1]) catch unreachable;
    try std.testing.expectEqual(@as(f64, 1.0), min_val);
    try std.testing.expectEqual(@as(f64, 9.0), max_val);

    // MIN/MAX on string column
    const sel2 = try exec.execute("SELECT MIN(name), MAX(name) FROM t");
    defer exec.freeResult(sel2);
    try std.testing.expectEqualStrings("Alice", sel2.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("Zara", sel2.rows.rows[0].values[1]);
}

test "executor GROUP BY" {
    const test_file = "test_exec_groupby.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE emp (id INT, dept TEXT, salary INT)");
    exec.freeResult(ct);

    const r1 = try exec.execute("INSERT INTO emp VALUES (1, 'Engineering', 100)");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO emp VALUES (2, 'Engineering', 120)");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO emp VALUES (3, 'Sales', 80)");
    exec.freeResult(r3);
    const r4 = try exec.execute("INSERT INTO emp VALUES (4, 'Sales', 90)");
    exec.freeResult(r4);
    const r5 = try exec.execute("INSERT INTO emp VALUES (5, 'HR', 95)");
    exec.freeResult(r5);

    // GROUP BY dept with COUNT
    const sel = try exec.execute("SELECT dept, COUNT(*) FROM emp GROUP BY dept");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.columns.len);
    try std.testing.expectEqualStrings("dept", sel.rows.columns[0]);
    try std.testing.expectEqualStrings("count(*)", sel.rows.columns[1]);
    try std.testing.expectEqual(@as(usize, 3), sel.rows.rows.len); // 3 departments

    // GROUP BY dept with SUM
    const sel2 = try exec.execute("SELECT dept, SUM(salary) FROM emp GROUP BY dept");
    defer exec.freeResult(sel2);
    try std.testing.expectEqual(@as(usize, 3), sel2.rows.rows.len);

    // Verify Engineering group: sum should be 220
    for (sel2.rows.rows) |row| {
        if (std.mem.eql(u8, row.values[0], "Engineering")) {
            const sum_val = std.fmt.parseFloat(f64, row.values[1]) catch unreachable;
            try std.testing.expectEqual(@as(f64, 220.0), sum_val);
        }
    }
}

test "executor GROUP BY with WHERE" {
    const test_file = "test_exec_groupby_where.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE orders (id INT, category TEXT, amount INT)");
    exec.freeResult(ct);

    const r1 = try exec.execute("INSERT INTO orders VALUES (1, 'Food', 50)");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO orders VALUES (2, 'Food', 30)");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO orders VALUES (3, 'Tech', 200)");
    exec.freeResult(r3);
    const r4 = try exec.execute("INSERT INTO orders VALUES (4, 'Tech', 10)");
    exec.freeResult(r4);

    // GROUP BY with WHERE filter (only amounts > 20)
    const sel = try exec.execute("SELECT category, COUNT(*), SUM(amount) FROM orders WHERE amount > 20 GROUP BY category");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len); // Food and Tech

    for (sel.rows.rows) |row| {
        if (std.mem.eql(u8, row.values[0], "Food")) {
            try std.testing.expectEqualStrings("2", row.values[1]); // 2 food items > 20
        }
        if (std.mem.eql(u8, row.values[0], "Tech")) {
            try std.testing.expectEqualStrings("1", row.values[1]); // only 1 tech item > 20
        }
    }
}

test "executor INNER JOIN" {
    const test_file = "test_exec_join.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct1 = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");
    exec.freeResult(ct2);

    const usr1 = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(usr1);
    const usr2 = try exec.execute("INSERT INTO users VALUES (2, 'Bob')");
    exec.freeResult(usr2);
    const usr3 = try exec.execute("INSERT INTO users VALUES (3, 'Charlie')");
    exec.freeResult(usr3);

    const o1 = try exec.execute("INSERT INTO orders VALUES (1, 1, 100)");
    exec.freeResult(o1);
    const o2 = try exec.execute("INSERT INTO orders VALUES (2, 1, 200)");
    exec.freeResult(o2);
    const o3 = try exec.execute("INSERT INTO orders VALUES (3, 2, 50)");
    exec.freeResult(o3);

    // INNER JOIN — should get 3 rows (Alice x2, Bob x1)
    const sel = try exec.execute("SELECT name, amount FROM users JOIN orders ON users.id = orders.user_id");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.columns.len);
    try std.testing.expectEqualStrings("name", sel.rows.columns[0]);
    try std.testing.expectEqualStrings("amount", sel.rows.columns[1]);
    try std.testing.expectEqual(@as(usize, 3), sel.rows.rows.len);

    // Check Alice has 2 orders
    var alice_count: usize = 0;
    for (sel.rows.rows) |row| {
        if (std.mem.eql(u8, row.values[0], "Alice")) alice_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), alice_count);
}

test "executor LEFT JOIN" {
    const test_file = "test_exec_leftjoin.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct1 = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE orders (id INT, user_id INT, amount INT)");
    exec.freeResult(ct2);

    const usr1 = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(usr1);
    const usr2 = try exec.execute("INSERT INTO users VALUES (2, 'Bob')");
    exec.freeResult(usr2);

    const o1 = try exec.execute("INSERT INTO orders VALUES (1, 1, 100)");
    exec.freeResult(o1);
    // Bob has no orders

    // LEFT JOIN — should get 2 rows (Alice with order, Bob with NULL)
    const sel = try exec.execute("SELECT name, amount FROM users LEFT JOIN orders ON users.id = orders.user_id");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);

    // Find Bob's row — amount should be NULL
    for (sel.rows.rows) |row| {
        if (std.mem.eql(u8, row.values[0], "Bob")) {
            try std.testing.expectEqualStrings("NULL", row.values[1]);
        }
        if (std.mem.eql(u8, row.values[0], "Alice")) {
            try std.testing.expectEqualStrings("100", row.values[1]);
        }
    }
}
