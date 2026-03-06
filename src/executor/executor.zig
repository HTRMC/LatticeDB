const std = @import("std");
const ast = @import("../parser/ast.zig");
const parser_mod = @import("../parser/parser.zig");
const tuple_mod = @import("../storage/tuple.zig");
const catalog_mod = @import("../storage/catalog.zig");
const table_mod = @import("../storage/table.zig");
const page_mod = @import("../storage/page.zig");
const mvcc_mod = @import("../storage/mvcc.zig");
const undo_log_mod = @import("../storage/undo_log.zig");
const btree_mod = @import("../index/btree.zig");
const planner_mod = @import("../planner/planner.zig");
const plan_mod = @import("../planner/plan.zig");
const vec_exec_mod = @import("vec_exec.zig");
const expr_eval = @import("expr_eval.zig");

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
    WriteConflict,
    IndexNotFound,
    IndexAlreadyExists,
    NotNullViolation,
    CheckViolation,
    ForeignKeyViolation,
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

pub const FkDef = struct {
    column: []const u8, // local column name
    ref_table: []const u8, // referenced table name
    ref_column: []const u8, // referenced column name
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
    /// Bound parameters for current prepared statement execution (null = no params)
    current_params: ?[]const ast.LiteralValue = null,
    /// In-memory CHECK constraints (table_name → check expressions SQL text)
    check_constraints: std.StringHashMapUnmanaged([]const []const u8) = .empty,
    /// In-memory view definitions (view_name → SELECT SQL text)
    views: std.StringHashMapUnmanaged([]const u8) = .empty,
    /// In-memory foreign key constraints (child_table → []FkDef)
    foreign_keys: std.StringHashMapUnmanaged([]const FkDef) = .empty,
    /// Auto-increment counters per table (table_name → next serial value)
    serial_counters: std.StringHashMapUnmanaged(i32) = .empty,
    /// Tracks which columns are SERIAL for each table (table_name → []column_index)
    serial_columns: std.StringHashMapUnmanaged([]const usize) = .empty,

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

    pub fn deinit(self: *Self) void {
        var it = self.check_constraints.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.*) |sql| self.allocator.free(sql);
            self.allocator.free(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.check_constraints.deinit(self.allocator);

        var vit = self.views.iterator();
        while (vit.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.views.deinit(self.allocator);

        var fkit = self.foreign_keys.iterator();
        while (fkit.next()) |entry| {
            for (entry.value_ptr.*) |fk| {
                self.allocator.free(fk.column);
                self.allocator.free(fk.ref_table);
                self.allocator.free(fk.ref_column);
            }
            self.allocator.free(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.foreign_keys.deinit(self.allocator);

        var sit = self.serial_counters.iterator();
        while (sit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.serial_counters.deinit(self.allocator);

        var scit = self.serial_columns.iterator();
        while (scit.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.serial_columns.deinit(self.allocator);
    }

    /// A prepared statement that can be executed multiple times with different parameters.
    pub const PreparedStatement = struct {
        arena_state: std.heap.ArenaAllocator,
        statement: ast.Statement,
        param_count: u32,

        pub fn deinit(self: *PreparedStatement) void {
            self.arena_state.deinit();
        }
    };

    /// Parse SQL into a prepared statement. Caller must call stmt.deinit() when done.
    pub fn prepare(self: *Self, sql: []const u8) ExecError!PreparedStatement {
        var parser = Parser.init(self.allocator, sql);
        // Do NOT call parser.deinit() — we transfer arena ownership to PreparedStatement
        const stmt = parser.parse() catch {
            parser.deinit();
            return ExecError.ParseError;
        };
        return .{
            .arena_state = parser.arena_state,
            .statement = stmt,
            .param_count = parser.param_count,
        };
    }

    /// Execute a prepared statement with bound parameter values.
    pub fn executePrepared(self: *Self, stmt: *const PreparedStatement, params: []const ast.LiteralValue) ExecError!ExecResult {
        if (params.len < stmt.param_count) return ExecError.ColumnCountMismatch;
        return switch (stmt.statement) {
            .create_table => |ct| self.execCreateTable(ct),
            .create_index => |ci| self.execCreateIndex(ci),
            .insert => |ins| self.execInsertWithParams(ins, params),
            .insert_select => |is| self.execInsertSelect(is),
            .select => |sel| self.execSelectWithParams(sel, params),
            .update => |upd| self.execUpdateWithParams(upd, params),
            .delete => |del| self.execDeleteWithParams(del, params),
            .drop_table => |dt| self.execDropTable(dt),
            .drop_index => |di| self.execDropIndex(di),
            .alter_table => |at| self.execAlterTable(at),
            .explain => |ex| self.execExplain(ex),
            .union_query => |uq| self.execUnion(uq),
            .create_view => |cv| self.execCreateView(cv),
            .drop_view => |name| self.execDropView(name),
            .cte_select => |cs| self.execCTESelect(cs),
            .begin_txn => self.execBegin(),
            .commit_txn => self.execCommit(),
            .rollback_txn => self.execRollback(),
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
            .create_index => |ci| self.execCreateIndex(ci),
            .insert => |ins| self.execInsert(ins),
            .insert_select => |is| self.execInsertSelect(is),
            .select => |sel| self.execSelect(sel),
            .update => |upd| self.execUpdate(upd),
            .delete => |del| self.execDelete(del),
            .drop_table => |dt| self.execDropTable(dt),
            .drop_index => |di| self.execDropIndex(di),
            .alter_table => |at| self.execAlterTable(at),
            .explain => |ex| self.execExplain(ex),
            .union_query => |uq| self.execUnion(uq),
            .create_view => |cv| self.execCreateView(cv),
            .drop_view => |name| self.execDropView(name),
            .cte_select => |cs| self.execCTESelect(cs),
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
    // Index maintenance helpers
    // ============================================================

    /// Insert key into all indexes for this table
    fn maintainIndexesInsert(self: *Self, table_id: PageId, schema: *const Schema, values: []const Value, tid: page_mod.TupleId) void {
        const indexes = self.catalog.getIndexesForTable(table_id) catch return;
        defer self.catalog.freeIndexList(indexes);

        for (indexes) |idx| {
            if (idx.column_ordinal >= schema.columns.len) continue;
            if (schema.columns[idx.column_ordinal].col_type != .integer) continue;
            const val = values[idx.column_ordinal];
            if (val != .integer) continue;

            var btree = btree_mod.BTree.open(self.catalog.buffer_pool, idx.index_id, self.catalog.alloc_manager);
            btree.insert(val.integer, tid) catch {};
        }
    }

    /// Delete key from all indexes for this table
    fn maintainIndexesDelete(self: *Self, table_id: PageId, schema: *const Schema, values: []const Value) void {
        const indexes = self.catalog.getIndexesForTable(table_id) catch return;
        defer self.catalog.freeIndexList(indexes);

        for (indexes) |idx| {
            if (idx.column_ordinal >= schema.columns.len) continue;
            if (schema.columns[idx.column_ordinal].col_type != .integer) continue;
            const val = values[idx.column_ordinal];
            if (val != .integer) continue;

            var btree = btree_mod.BTree.open(self.catalog.buffer_pool, idx.index_id, self.catalog.alloc_manager);
            _ = btree.delete(val.integer) catch {};
        }
    }

    // ============================================================
    // Shared index-scan row collection helper
    // ============================================================

    const CollectedRow = struct { tid: page_mod.TupleId, values: []Value };

    /// Use planner to decide seq scan vs index scan, collect (TupleId, []Value) pairs.
    /// Caller must free each entry's .values and deinit the list.
    fn collectMatchingRows(
        self: *Self,
        table_name: []const u8,
        table: *Table,
        schema: *const Schema,
        where_clause: ?*const ast.Expression,
        txn: ?*Transaction,
        out: *std.ArrayList(CollectedRow),
    ) ExecError!void {
        // Build minimal Select for planner (only table_name + where_clause matter)
        const empty_cols = [_]ast.SelectColumn{.all_columns};
        const sel = ast.Select{
            .columns = &empty_cols,
            .aliases = null,
            .distinct = false,
            .table_name = table_name,
            .joins = null,
            .where_clause = where_clause,
            .group_by = null,
            .having_clause = null,
            .order_by = null,
            .limit = null,
            .offset = null,
        };

        // Run planner
        var planner = planner_mod.Planner.init(self.allocator, self.catalog);
        const plan = planner.planSelect(sel) catch {
            // Planner failed, fall back to seq scan
            return self.collectViaSeqScan(table, schema, where_clause, txn, out);
        };
        defer planner.freePlan(plan);

        const base = getBasePlanNode(plan);
        if (base.* == .index_scan) {
            const is = base.index_scan;
            var btree = btree_mod.BTree.open(table.buffer_pool, is.index_id, self.catalog.alloc_manager);

            switch (is.scan_type) {
                .point => |key| {
                    const tid = btree.search(key) catch return ExecError.StorageError;
                    if (tid) |t| try self.fetchAndCollectRow(table, t, txn, schema, where_clause, out);
                },
                .range => |r| {
                    var range_iter = btree.rangeScan(r.low, r.high) catch return ExecError.StorageError;
                    while (range_iter.next() catch return ExecError.StorageError) |entry| {
                        try self.fetchAndCollectRow(table, entry.tid, txn, schema, where_clause, out);
                    }
                },
                .range_from => |key| {
                    var range_iter = btree.rangeScan(key, std.math.maxInt(i32)) catch return ExecError.StorageError;
                    while (range_iter.next() catch return ExecError.StorageError) |entry| {
                        try self.fetchAndCollectRow(table, entry.tid, txn, schema, where_clause, out);
                    }
                },
                .range_to => |key| {
                    var range_iter = btree.rangeScan(std.math.minInt(i32), key) catch return ExecError.StorageError;
                    while (range_iter.next() catch return ExecError.StorageError) |entry| {
                        try self.fetchAndCollectRow(table, entry.tid, txn, schema, where_clause, out);
                    }
                },
            }
        } else {
            return self.collectViaSeqScan(table, schema, where_clause, txn, out);
        }
    }

    /// Fetch a single tuple by TID, apply WHERE, and append to collected rows.
    fn fetchAndCollectRow(
        self: *Self,
        table: *Table,
        tid: page_mod.TupleId,
        txn: ?*Transaction,
        schema: *const Schema,
        where_clause: ?*const ast.Expression,
        out: *std.ArrayList(CollectedRow),
    ) ExecError!void {
        const values = if (txn) |t|
            (table.getTupleTxn(tid, t) catch return ExecError.StorageError)
        else
            (table.getTuple(tid, null) catch return ExecError.StorageError);

        if (values == null) return;
        const vals = values.?;

        // Apply residual WHERE filter
        if (where_clause) |where| {
            if (!self.evalWhere(where, schema, vals)) {
                self.allocator.free(vals);
                return;
            }
        }

        out.append(self.allocator, .{ .tid = tid, .values = vals }) catch {
            self.allocator.free(vals);
            return ExecError.OutOfMemory;
        };
    }

    /// Sequential scan fallback for collectMatchingRows.
    fn collectViaSeqScan(
        self: *Self,
        table: *Table,
        schema: *const Schema,
        where_clause: ?*const ast.Expression,
        txn: ?*Transaction,
        out: *std.ArrayList(CollectedRow),
    ) ExecError!void {
        var iter = table.scanWithTxn(txn) catch return ExecError.StorageError;

        while (iter.next() catch return ExecError.StorageError) |row| {
            const matches = if (where_clause) |where|
                self.evalWhere(where, schema, row.values)
            else
                true;

            if (matches) {
                out.append(self.allocator, .{ .tid = row.tid, .values = row.values }) catch {
                    iter.freeValues(row.values);
                    return ExecError.OutOfMemory;
                };
            } else {
                iter.freeValues(row.values);
            }
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

        // Opportunistic background checkpoint: flush a few dirty pages
        self.catalog.buffer_pool.checkpoint(8);

        // Opportunistic undo log GC + compaction
        if (self.undo_log) |undo| {
            undo.gc(tm.oldestActiveTxnId());
            if (undo.reclaimableBytes() >= 64 * 1024) {
                undo.compact();
            }
        }
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
                .default_value = if (col_def.default_value) |dv| litToValue(self.allocator, dv, mapDataType(col_def.data_type)) catch null else null,
                .scale = col_def.scale,
            };
        }

        _ = self.catalog.createTable(ct.table_name, columns) catch |err| {
            return switch (err) {
                catalog_mod.CatalogError.TableAlreadyExists => ExecError.TableAlreadyExists,
                else => ExecError.StorageError,
            };
        };

        // Store CHECK constraints in memory
        if (ct.checks.len > 0) {
            const exprs = self.allocator.alloc([]const u8, ct.checks.len) catch return ExecError.OutOfMemory;
            for (ct.checks, 0..) |chk, i| {
                // Serialize expression back to SQL text for storage
                exprs[i] = self.serializeExpr(chk.expr) catch return ExecError.OutOfMemory;
            }
            const name_copy = self.allocator.dupe(u8, ct.table_name) catch return ExecError.OutOfMemory;
            self.check_constraints.put(self.allocator, name_copy, exprs) catch return ExecError.OutOfMemory;
        }

        // Store FOREIGN KEY constraints in memory
        if (ct.foreign_keys.len > 0) {
            const fks = self.allocator.alloc(FkDef, ct.foreign_keys.len) catch return ExecError.OutOfMemory;
            for (ct.foreign_keys, 0..) |fk, i| {
                fks[i] = .{
                    .column = self.allocator.dupe(u8, fk.column) catch return ExecError.OutOfMemory,
                    .ref_table = self.allocator.dupe(u8, fk.ref_table) catch return ExecError.OutOfMemory,
                    .ref_column = self.allocator.dupe(u8, fk.ref_column) catch return ExecError.OutOfMemory,
                };
            }
            const name_copy = self.allocator.dupe(u8, ct.table_name) catch return ExecError.OutOfMemory;
            self.foreign_keys.put(self.allocator, name_copy, fks) catch return ExecError.OutOfMemory;
        }

        // Track SERIAL columns
        {
            var serial_list: std.ArrayList(usize) = .empty;
            for (ct.columns, 0..) |col_def, i| {
                if (col_def.data_type == .serial) {
                    serial_list.append(self.allocator, i) catch return ExecError.OutOfMemory;
                }
            }
            if (serial_list.items.len > 0) {
                const name_copy = self.allocator.dupe(u8, ct.table_name) catch return ExecError.OutOfMemory;
                self.serial_columns.put(self.allocator, name_copy, serial_list.toOwnedSlice(self.allocator) catch return ExecError.OutOfMemory) catch return ExecError.OutOfMemory;
                // Initialize counter (use separate key copy)
                const counter_name = self.allocator.dupe(u8, ct.table_name) catch return ExecError.OutOfMemory;
                self.serial_counters.put(self.allocator, counter_name, 1) catch return ExecError.OutOfMemory;
            } else {
                serial_list.deinit(self.allocator);
            }
        }

        const msg = std.fmt.allocPrint(self.allocator, "CREATE TABLE", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    fn serializeExpr(self: *Self, expr: *const ast.Expression) ![]const u8 {
        var buf: std.ArrayList(u8) = .empty;
        try self.writeExpr(&buf, expr);
        return buf.toOwnedSlice(self.allocator);
    }

    fn writeExpr(self: *Self, buf: *std.ArrayList(u8), expr: *const ast.Expression) !void {
        switch (expr.*) {
            .column_ref => |name| {
                try buf.appendSlice(self.allocator, name);
            },
            .literal => |lit| {
                switch (lit) {
                    .integer => |v| {
                        const s = try std.fmt.allocPrint(self.allocator, "{d}", .{v});
                        defer self.allocator.free(s);
                        try buf.appendSlice(self.allocator, s);
                    },
                    .float => |v| {
                        const s = try std.fmt.allocPrint(self.allocator, "{d:.6}", .{v});
                        defer self.allocator.free(s);
                        try buf.appendSlice(self.allocator, s);
                    },
                    .string => |s| {
                        try buf.append(self.allocator, '\'');
                        try buf.appendSlice(self.allocator, s);
                        try buf.append(self.allocator, '\'');
                    },
                    .boolean => |b| try buf.appendSlice(self.allocator, if (b) "TRUE" else "FALSE"),
                    .null_value => try buf.appendSlice(self.allocator, "NULL"),
                    .parameter => {},
                }
            },
            .comparison => |cmp| {
                try buf.append(self.allocator, '(');
                try self.writeExpr(buf, cmp.left);
                try buf.appendSlice(self.allocator, switch (cmp.op) {
                    .eq => " = ",
                    .neq => " != ",
                    .lt => " < ",
                    .gt => " > ",
                    .lte => " <= ",
                    .gte => " >= ",
                });
                try self.writeExpr(buf, cmp.right);
                try buf.append(self.allocator, ')');
            },
            .and_expr => |ae| {
                try buf.append(self.allocator, '(');
                try self.writeExpr(buf, ae.left);
                try buf.appendSlice(self.allocator, " AND ");
                try self.writeExpr(buf, ae.right);
                try buf.append(self.allocator, ')');
            },
            .or_expr => |oe| {
                try buf.append(self.allocator, '(');
                try self.writeExpr(buf, oe.left);
                try buf.appendSlice(self.allocator, " OR ");
                try self.writeExpr(buf, oe.right);
                try buf.append(self.allocator, ')');
            },
            .not_expr => |ne| {
                try buf.appendSlice(self.allocator, "NOT ");
                try self.writeExpr(buf, ne.operand);
            },
            else => try buf.appendSlice(self.allocator, "TRUE"), // fallback
        }
    }

    fn validateChecks(self: *Self, check_sqls: []const []const u8, schema: *const Schema, values: []const Value) bool {
        for (check_sqls) |sql| {
            // Wrap in a SELECT WHERE context to parse
            const full_sql = std.fmt.allocPrint(self.allocator, "SELECT * FROM _t WHERE {s}", .{sql}) catch return true;
            defer self.allocator.free(full_sql);
            var parser = Parser.init(self.allocator, full_sql);
            defer parser.deinit();
            const stmt = parser.parse() catch return true; // if can't parse, pass
            const sel = stmt.select;
            if (sel.where_clause) |where| {
                if (!expr_eval.evalExprToBool(self.allocator, where, schema, values, null)) {
                    return false;
                }
            }
        }
        return true;
    }

    fn validateForeignKeys(self: *Self, fks: []const FkDef, schema: *const Schema, values: []const Value) bool {
        for (fks) |fk| {
            // Find the local column index
            var local_idx: ?usize = null;
            for (schema.columns, 0..) |col, ci| {
                if (std.mem.eql(u8, col.name, fk.column)) {
                    local_idx = ci;
                    break;
                }
            }
            const idx = local_idx orelse continue;
            const val = values[idx];

            // NULL values pass FK checks
            if (val == .null_value) continue;

            // Open the referenced table and check if value exists
            const ref_result = self.catalog.openTable(fk.ref_table) catch continue;
            if (ref_result) |res| {
                defer self.catalog.freeSchema(res.schema);
                var ref_table = res.table;
                ref_table.txn_manager = self.txn_manager;
                ref_table.undo_log = self.undo_log;

                // Find referenced column index
                var ref_idx: ?usize = null;
                for (res.schema.columns, 0..) |col, ci| {
                    if (std.mem.eql(u8, col.name, fk.ref_column)) {
                        ref_idx = ci;
                        break;
                    }
                }
                const ri = ref_idx orelse continue;

                // Scan referenced table for matching value
                var found = false;
                var scan_iter = ref_table.scanWithTxn(self.current_txn) catch continue;
                while (scan_iter.next() catch null) |row| {
                    if (ri < row.values.len and valuesEqual(val, row.values[ri])) {
                        scan_iter.freeValues(row.values);
                        found = true;
                        break;
                    }
                    scan_iter.freeValues(row.values);
                }
                if (!found) return false;
            } else {
                return false; // referenced table doesn't exist
            }
        }
        return true;
    }

    fn validateForeignKeyDelete(self: *Self, parent_table: []const u8, parent_schema: *const Schema, deleted_values: []const Value) bool {
        // Check all FK definitions to find child tables referencing this parent
        var fk_it = self.foreign_keys.iterator();
        while (fk_it.next()) |entry| {
            const fks = entry.value_ptr.*;
            for (fks) |fk| {
                if (!std.mem.eql(u8, fk.ref_table, parent_table)) continue;

                // Find which column in parent is referenced
                var parent_idx: ?usize = null;
                for (parent_schema.columns, 0..) |col, ci| {
                    if (std.mem.eql(u8, col.name, fk.ref_column)) {
                        parent_idx = ci;
                        break;
                    }
                }
                const pi = parent_idx orelse continue;
                const deleted_val = deleted_values[pi];
                if (deleted_val == .null_value) continue;

                // Open the child table and check for references
                const child_name = entry.key_ptr.*;
                const child_result = self.catalog.openTable(child_name) catch continue;
                if (child_result) |res| {
                    defer self.catalog.freeSchema(res.schema);
                    var child_table = res.table;
                    child_table.txn_manager = self.txn_manager;
                    child_table.undo_log = self.undo_log;

                    var child_idx: ?usize = null;
                    for (res.schema.columns, 0..) |col, ci| {
                        if (std.mem.eql(u8, col.name, fk.column)) {
                            child_idx = ci;
                            break;
                        }
                    }
                    const ci = child_idx orelse continue;

                    var scan_iter = child_table.scanWithTxn(self.current_txn) catch continue;
                    while (scan_iter.next() catch null) |row| {
                        if (ci < row.values.len and valuesEqual(deleted_val, row.values[ci])) {
                            scan_iter.freeValues(row.values);
                            return false;
                        }
                        scan_iter.freeValues(row.values);
                    }
                }
            }
        }
        return true;
    }

    fn valuesEqual(a: Value, b: Value) bool {
        const tag_a = std.meta.activeTag(a);
        const tag_b = std.meta.activeTag(b);
        if (tag_a != tag_b) {
            // Cross-type integer comparison
            const int_a = switch (a) {
                .smallint => |v| @as(i64, v),
                .integer => |v| @as(i64, v),
                .bigint => |v| v,
                else => return false,
            };
            const int_b = switch (b) {
                .smallint => |v| @as(i64, v),
                .integer => |v| @as(i64, v),
                .bigint => |v| v,
                else => return false,
            };
            return int_a == int_b;
        }
        return switch (a) {
            .smallint => |v| v == b.smallint,
            .integer => |v| v == b.integer,
            .bigint => |v| v == b.bigint,
            .float => |v| v == b.float,
            .boolean => |v| v == b.boolean,
            .bytes => |v| std.mem.eql(u8, v, b.bytes),
            .null_value => true,
            .date => |v| v == b.date,
            .timestamp => |v| v == b.timestamp,
            .decimal => |v| v == b.decimal,
            .uuid => |v| std.mem.eql(u8, v[0..16], b.uuid[0..16]),
        };
    }

    // ============================================================
    // DROP TABLE
    // ============================================================
    fn execDropTable(self: *Self, dt: ast.DropTable) ExecError!ExecResult {
        self.catalog.dropTable(dt.table_name) catch |err| {
            return switch (err) {
                catalog_mod.CatalogError.TableNotFound => ExecError.TableNotFound,
                catalog_mod.CatalogError.SystemTableError => ExecError.StorageError,
                else => ExecError.StorageError,
            };
        };

        const msg = std.fmt.allocPrint(self.allocator, "DROP TABLE", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    // ============================================================
    // ALTER TABLE
    // ============================================================
    fn execAlterTable(self: *Self, at: ast.AlterTable) ExecError!ExecResult {
        switch (at.action) {
            .add_column => |col_def| {
                const col = Column{
                    .name = col_def.name,
                    .col_type = mapDataType(col_def.data_type),
                    .max_length = col_def.max_length,
                    .nullable = true,
                    .scale = col_def.scale,
                };
                self.catalog.addColumn(at.table_name, col) catch |err| {
                    return switch (err) {
                        catalog_mod.CatalogError.TableNotFound => ExecError.TableNotFound,
                        else => ExecError.StorageError,
                    };
                };
            },
            .rename_column => |rc| {
                self.catalog.renameColumn(at.table_name, rc.old_name, rc.new_name) catch |err| {
                    return switch (err) {
                        catalog_mod.CatalogError.TableNotFound => ExecError.TableNotFound,
                        else => ExecError.StorageError,
                    };
                };
            },
            .drop_column => |col_name| {
                _ = col_name;
                // DROP COLUMN requires tuple rewriting — not yet supported
                return ExecError.StorageError;
            },
        }
        const msg = std.fmt.allocPrint(self.allocator, "ALTER TABLE", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    // ============================================================
    // CREATE INDEX
    // ============================================================
    fn execCreateIndex(self: *Self, ci: ast.CreateIndex) ExecError!ExecResult {
        // For catalog storage, use first column name (composite index metadata stored in AST)
        const first_col = if (ci.columns.len > 0) ci.columns[0] else return ExecError.ColumnNotFound;
        const index_id = self.catalog.createIndex(ci.table_name, ci.index_name, first_col, ci.is_unique) catch |err| {
            return switch (err) {
                catalog_mod.CatalogError.IndexAlreadyExists => ExecError.IndexAlreadyExists,
                catalog_mod.CatalogError.TableNotFound => ExecError.TableNotFound,
                else => ExecError.StorageError,
            };
        };

        // Backfill: scan existing rows and insert into the new index
        const table_result = self.catalog.openTable(ci.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(table_result.schema);
        var table = table_result.table;
        table.txn_manager = self.txn_manager;
        table.undo_log = self.undo_log;

        const schema = table_result.schema;

        // Find column ordinal (use first column for btree index)
        var col_ord: ?usize = null;
        for (schema.columns, 0..) |col, i| {
            if (std.ascii.eqlIgnoreCase(col.name, first_col)) {
                col_ord = i;
                break;
            }
        }

        if (col_ord) |ordinal| {
            if (schema.columns[ordinal].col_type == .integer) {
                var btree = btree_mod.BTree.open(self.catalog.buffer_pool, index_id, self.catalog.alloc_manager);
                const txn = self.current_txn orelse self.beginImplicitTxn();
                var iter = table.scanWithTxn(txn) catch {
                    self.abortImplicitTxn(txn);
                    return ExecError.StorageError;
                };
                while (iter.next() catch {
                    self.abortImplicitTxn(txn);
                    return ExecError.StorageError;
                }) |row| {
                    defer iter.freeValues(row.values);
                    if (ordinal < row.values.len and row.values[ordinal] == .integer) {
                        btree.insert(row.values[ordinal].integer, row.tid) catch {};
                    }
                }
                self.commitImplicitTxn(txn);
            }
        }

        const msg = std.fmt.allocPrint(self.allocator, "CREATE INDEX", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    // ============================================================
    // DROP INDEX
    // ============================================================
    fn execDropIndex(self: *Self, di: ast.DropIndex) ExecError!ExecResult {
        self.catalog.dropIndex(di.index_name) catch |err| {
            return switch (err) {
                catalog_mod.CatalogError.IndexNotFound => ExecError.IndexNotFound,
                else => ExecError.StorageError,
            };
        };

        const msg = std.fmt.allocPrint(self.allocator, "DROP INDEX", .{}) catch {
            return ExecError.OutOfMemory;
        };
        return .{ .message = msg };
    }

    // ============================================================
    // EXPLAIN
    // ============================================================
    fn execExplain(self: *Self, ex: ast.Explain) ExecError!ExecResult {
        var planner = planner_mod.Planner.init(self.allocator, self.catalog);
        const plan = planner.planSelect(ex.select) catch |err| {
            return switch (err) {
                planner_mod.PlanError.TableNotFound => ExecError.TableNotFound,
                planner_mod.PlanError.OutOfMemory => ExecError.OutOfMemory,
                else => ExecError.StorageError,
            };
        };
        defer planner.freePlan(plan);

        const plan_text = plan_mod.formatPlan(self.allocator, plan, 0) catch {
            return ExecError.OutOfMemory;
        };

        // Return as single-row result with "QUERY PLAN" column
        const columns = self.allocator.alloc([]const u8, 1) catch {
            self.allocator.free(plan_text);
            return ExecError.OutOfMemory;
        };
        columns[0] = self.allocator.dupe(u8, "QUERY PLAN") catch {
            self.allocator.free(plan_text);
            self.allocator.free(columns);
            return ExecError.OutOfMemory;
        };

        const row_values = self.allocator.alloc([]const u8, 1) catch {
            self.allocator.free(columns[0]);
            self.allocator.free(columns);
            self.allocator.free(plan_text);
            return ExecError.OutOfMemory;
        };
        // Trim trailing newline from plan text
        const trimmed = std.mem.trimEnd(u8, plan_text, "\n");
        row_values[0] = self.allocator.dupe(u8, trimmed) catch {
            self.allocator.free(columns[0]);
            self.allocator.free(columns);
            self.allocator.free(plan_text);
            self.allocator.free(row_values);
            return ExecError.OutOfMemory;
        };
        self.allocator.free(plan_text);

        const rows = self.allocator.alloc(ResultRow, 1) catch {
            self.allocator.free(row_values[0]);
            self.allocator.free(row_values);
            self.allocator.free(columns[0]);
            self.allocator.free(columns);
            return ExecError.OutOfMemory;
        };
        rows[0] = .{ .values = row_values };

        return .{ .rows = .{ .columns = columns, .rows = rows } };
    }

    fn execCreateView(self: *Self, cv: ast.CreateView) ExecError!ExecResult {
        // Check if view already exists
        if (self.views.contains(cv.view_name)) {
            return ExecError.TableAlreadyExists;
        }
        // Also check if a real table with this name exists
        const tbl_result = self.catalog.openTable(cv.view_name) catch {
            return ExecError.StorageError;
        };
        if (tbl_result) |res| {
            self.catalog.freeSchema(res.schema);
            return ExecError.TableAlreadyExists;
        }

        // Serialize the SELECT query back to SQL text and store it
        var sql_buf: std.ArrayList(u8) = .empty;
        defer sql_buf.deinit(self.allocator);
        self.writeSelect(cv.query, &sql_buf) catch return ExecError.OutOfMemory;

        const sql_text = self.allocator.dupe(u8, sql_buf.items) catch return ExecError.OutOfMemory;
        errdefer self.allocator.free(sql_text);
        const name = self.allocator.dupe(u8, cv.view_name) catch return ExecError.OutOfMemory;
        errdefer self.allocator.free(name);

        self.views.put(self.allocator, name, sql_text) catch return ExecError.OutOfMemory;

        const msg = std.fmt.allocPrint(self.allocator, "VIEW {s} created.", .{cv.view_name}) catch return ExecError.OutOfMemory;
        return ExecResult{ .message = msg };
    }

    fn execDropView(self: *Self, name: []const u8) ExecError!ExecResult {
        const kv = self.views.fetchRemove(name) orelse return ExecError.TableNotFound;
        self.allocator.free(kv.value);
        self.allocator.free(kv.key);

        const msg = std.fmt.allocPrint(self.allocator, "VIEW {s} dropped.", .{name}) catch return ExecError.OutOfMemory;
        return ExecResult{ .message = msg };
    }

    fn execViewSelect(self: *Self, outer_sel: ast.Select, view_sql: []const u8) ExecError!ExecResult {
        // Parse and execute the view's stored query
        var parser = Parser.init(self.allocator, view_sql);
        defer parser.deinit();
        const stmt = parser.parse() catch return ExecError.ParseError;
        const view_sel = stmt.select;

        // Execute the view query to get the base result
        const view_result = try self.execSelect(view_sel);

        // If the outer query is just SELECT * with no WHERE/ORDER/LIMIT, return as-is
        const is_simple = outer_sel.columns.len == 1 and outer_sel.columns[0] == .all_columns and
            outer_sel.where_clause == null and outer_sel.order_by == null and
            outer_sel.limit == null and outer_sel.offset == null and !outer_sel.distinct;

        if (is_simple) return view_result;

        // Otherwise, apply outer projections via derived table execution
        defer self.freeResult(view_result);
        const sub_rows = view_result.rows;

        const out_col_count = outer_sel.columns.len;
        const col_indices = self.allocator.alloc(usize, out_col_count) catch return ExecError.OutOfMemory;
        defer self.allocator.free(col_indices);
        var all_star = false;

        for (outer_sel.columns, 0..) |col, ci| {
            switch (col) {
                .all_columns => {
                    all_star = true;
                    break;
                },
                .named => |name| {
                    var found = false;
                    for (sub_rows.columns, 0..) |sc, si| {
                        if (std.ascii.eqlIgnoreCase(name, sc)) {
                            col_indices[ci] = si;
                            found = true;
                            break;
                        }
                    }
                    if (!found) return ExecError.ColumnNotFound;
                },
                else => return ExecError.ColumnNotFound,
            }
        }

        if (all_star) {
            // Copy all columns
            const cols = self.allocator.alloc([]const u8, sub_rows.columns.len) catch return ExecError.OutOfMemory;
            for (sub_rows.columns, 0..) |c, i| {
                cols[i] = self.allocator.dupe(u8, c) catch return ExecError.OutOfMemory;
            }
            const rows = self.allocator.alloc(ResultRow, sub_rows.rows.len) catch return ExecError.OutOfMemory;
            for (sub_rows.rows, 0..) |row, ri| {
                const vals = self.allocator.alloc([]const u8, row.values.len) catch return ExecError.OutOfMemory;
                for (row.values, 0..) |v, vi| {
                    vals[vi] = self.allocator.dupe(u8, v) catch return ExecError.OutOfMemory;
                }
                rows[ri] = .{ .values = vals };
            }
            return ExecResult{ .rows = .{ .columns = cols, .rows = rows } };
        }

        // Project specific columns
        const col_names = self.allocator.alloc([]const u8, out_col_count) catch return ExecError.OutOfMemory;
        for (0..out_col_count) |ci| {
            const alias = if (outer_sel.aliases) |a| a[ci] else null;
            col_names[ci] = self.allocator.dupe(u8, alias orelse sub_rows.columns[col_indices[ci]]) catch return ExecError.OutOfMemory;
        }
        const rows = self.allocator.alloc(ResultRow, sub_rows.rows.len) catch return ExecError.OutOfMemory;
        for (sub_rows.rows, 0..) |row, ri| {
            const vals = self.allocator.alloc([]const u8, out_col_count) catch return ExecError.OutOfMemory;
            for (0..out_col_count) |ci| {
                vals[ci] = self.allocator.dupe(u8, row.values[col_indices[ci]]) catch return ExecError.OutOfMemory;
            }
            rows[ri] = .{ .values = vals };
        }
        return ExecResult{ .rows = .{ .columns = col_names, .rows = rows } };
    }

    fn writeSelect(self: *Self, sel: ast.Select, buf: *std.ArrayList(u8)) !void {
        try buf.appendSlice(self.allocator, "SELECT ");
        if (sel.distinct) try buf.appendSlice(self.allocator, "DISTINCT ");
        for (sel.columns, 0..) |col, ci| {
            if (ci > 0) try buf.appendSlice(self.allocator, ", ");
            switch (col) {
                .all_columns => try buf.appendSlice(self.allocator, "*"),
                .named => |n| try buf.appendSlice(self.allocator, n),
                .qualified => |q| {
                    try buf.appendSlice(self.allocator, q.table);
                    try buf.append(self.allocator, '.');
                    try buf.appendSlice(self.allocator, q.column);
                },
                .aggregate => |agg| {
                    const fname: []const u8 = switch (agg.func) {
                        .count => "COUNT",
                        .sum => "SUM",
                        .avg => "AVG",
                        .min => "MIN",
                        .max => "MAX",
                    };
                    try buf.appendSlice(self.allocator, fname);
                    try buf.append(self.allocator, '(');
                    if (agg.distinct) try buf.appendSlice(self.allocator, "DISTINCT ");
                    if (agg.column) |c| try buf.appendSlice(self.allocator, c) else try buf.append(self.allocator, '*');
                    try buf.append(self.allocator, ')');
                },
                .expression => |expr| try self.writeExpr(buf, expr),
                .window_function => |wf| {
                    const fname: []const u8 = switch (wf.func) {
                        .row_number => "ROW_NUMBER",
                        .rank => "RANK",
                        .dense_rank => "DENSE_RANK",
                    };
                    try buf.appendSlice(self.allocator, fname);
                    try buf.appendSlice(self.allocator, "() OVER(");
                    if (wf.spec.partition_by) |pb| {
                        try buf.appendSlice(self.allocator, "PARTITION BY ");
                        for (pb, 0..) |p, pi| {
                            if (pi > 0) try buf.appendSlice(self.allocator, ", ");
                            try buf.appendSlice(self.allocator, p);
                        }
                    }
                    if (wf.spec.order_by) |ob| {
                        if (wf.spec.partition_by != null) try buf.append(self.allocator, ' ');
                        try buf.appendSlice(self.allocator, "ORDER BY ");
                        for (ob, 0..) |o, oi| {
                            if (oi > 0) try buf.appendSlice(self.allocator, ", ");
                            try buf.appendSlice(self.allocator, o.column);
                            if (!o.ascending) try buf.appendSlice(self.allocator, " DESC");
                        }
                    }
                    try buf.append(self.allocator, ')');
                },
            }
            if (sel.aliases) |aliases| {
                if (aliases[ci]) |alias| {
                    try buf.appendSlice(self.allocator, " AS ");
                    try buf.appendSlice(self.allocator, alias);
                }
            }
        }
        try buf.appendSlice(self.allocator, " FROM ");
        try buf.appendSlice(self.allocator, sel.table_name);
        if (sel.where_clause) |wc| {
            try buf.appendSlice(self.allocator, " WHERE ");
            try self.writeExpr(buf, wc);
        }
        if (sel.group_by) |gb| {
            try buf.appendSlice(self.allocator, " GROUP BY ");
            for (gb, 0..) |g, gi| {
                if (gi > 0) try buf.appendSlice(self.allocator, ", ");
                try buf.appendSlice(self.allocator, g);
            }
        }
        if (sel.having_clause) |hc| {
            try buf.appendSlice(self.allocator, " HAVING ");
            try self.writeExpr(buf, hc);
        }
        if (sel.order_by) |ob| {
            try buf.appendSlice(self.allocator, " ORDER BY ");
            for (ob, 0..) |o, oi| {
                if (oi > 0) try buf.appendSlice(self.allocator, ", ");
                try buf.appendSlice(self.allocator, o.column);
                if (!o.ascending) try buf.appendSlice(self.allocator, " DESC");
            }
        }
        if (sel.limit) |l| {
            const s = try std.fmt.allocPrint(self.allocator, " LIMIT {d}", .{l});
            defer self.allocator.free(s);
            try buf.appendSlice(self.allocator, s);
        }
        if (sel.offset) |o| {
            const s = try std.fmt.allocPrint(self.allocator, " OFFSET {d}", .{o});
            defer self.allocator.free(s);
            try buf.appendSlice(self.allocator, s);
        }
    }

    // ============================================================
    // Index Scan execution
    // ============================================================

    /// Try to use an index scan for the given SELECT. Returns true if index scan was used
    /// and rows were populated, false if caller should fall back to seq scan.
    fn tryIndexScan(
        self: *Self,
        sel: ast.Select,
        table: *Table,
        schema: *const Schema,
        col_proj: []const ProjectionColumn,
        txn: ?*Transaction,
        rows: *std.ArrayList(ResultRow),
    ) bool {
        // Run planner using pre-opened table info to avoid redundant catalog lookups
        const row_count = table.tupleCount() catch return false;
        var planner = planner_mod.Planner.init(self.allocator, self.catalog);
        const plan = planner.planSelectWithTable(sel, table.table_id, schema, row_count) catch return false;
        defer planner.freePlan(plan);

        // Find the base node (unwrap filter/sort/limit wrappers)
        const base = getBasePlanNode(plan);

        // Only execute if planner chose an index scan
        if (base.* != .index_scan) return false;

        const is = base.index_scan;
        var btree = btree_mod.BTree.open(table.buffer_pool, is.index_id, self.catalog.alloc_manager);

        switch (is.scan_type) {
            .point => |key| {
                // Point lookup: single key
                const tid = btree.search(key) catch return false;
                if (tid) |t| {
                    self.fetchAndAppendRow(table, t, txn, sel, schema, col_proj, rows) catch return false;
                }
            },
            .range => |r| {
                // Range scan [low, high]
                var range_iter = btree.rangeScan(r.low, r.high) catch return false;
                while (range_iter.next() catch return false) |entry| {
                    self.fetchAndAppendRow(table, entry.tid, txn, sel, schema, col_proj, rows) catch return false;
                }
            },
            .range_from => |key| {
                // key >= value → scan from key to max
                var range_iter = btree.rangeScan(key, std.math.maxInt(i32)) catch return false;
                while (range_iter.next() catch return false) |entry| {
                    self.fetchAndAppendRow(table, entry.tid, txn, sel, schema, col_proj, rows) catch return false;
                }
            },
            .range_to => |key| {
                // key <= value → scan from min to key
                var range_iter = btree.rangeScan(std.math.minInt(i32), key) catch return false;
                while (range_iter.next() catch return false) |entry| {
                    self.fetchAndAppendRow(table, entry.tid, txn, sel, schema, col_proj, rows) catch return false;
                }
            },
        }

        return true;
    }

    /// Get the base (leaf) plan node by unwrapping filter/sort/limit wrappers
    fn getBasePlanNode(node: *const plan_mod.PlanNode) *const plan_mod.PlanNode {
        return switch (node.*) {
            .filter => |f| getBasePlanNode(f.child),
            .sort => |s| getBasePlanNode(s.child),
            .limit => |l| getBasePlanNode(l.child),
            else => node,
        };
    }

    /// Fetch a tuple by TupleId, apply WHERE filter, format, and append to rows
    fn fetchAndAppendRow(
        self: *Self,
        table: *Table,
        tid: page_mod.TupleId,
        txn: ?*Transaction,
        sel: ast.Select,
        schema: *const Schema,
        col_proj: []const ProjectionColumn,
        rows: *std.ArrayList(ResultRow),
    ) !void {
        // Fetch tuple with MVCC visibility
        const values = if (txn) |t|
            (try table.getTupleTxn(tid, t))
        else
            (try table.getTuple(tid, null));

        if (values == null) return; // deleted or invisible
        const vals = values.?;
        defer self.allocator.free(vals);

        // Apply residual WHERE filter
        if (sel.where_clause) |where| {
            if (!self.evalWhere(where, schema, vals)) return;
        }

        // Format selected columns
        const formatted = try self.allocator.alloc([]const u8, col_proj.len);
        errdefer self.allocator.free(formatted);
        for (col_proj, 0..) |cp, i| {
            formatted[i] = try expr_eval.formatProjection(self.allocator, cp, vals, schema, self.current_params);
        }

        try rows.append(self.allocator, .{ .values = formatted });
    }

    // ============================================================
    // INSERT INTO
    // ============================================================
    fn execInsertSelect(self: *Self, is: ast.InsertSelect) ExecError!ExecResult {
        // Execute the SELECT query
        const select_result = try self.execSelect(is.query);
        const sel_rows = select_result.rows;
        defer {
            for (sel_rows.rows) |row| {
                for (row.values) |v| self.allocator.free(v);
                self.allocator.free(row.values);
            }
            self.allocator.free(sel_rows.rows);
            for (sel_rows.columns) |col| self.allocator.free(col);
            self.allocator.free(sel_rows.columns);
        }

        // Open target table
        const result = self.catalog.openTable(is.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(result.schema);
        var table = result.table;
        table.txn_manager = self.txn_manager;
        table.undo_log = self.undo_log;
        const schema = result.schema;

        // Column count must match
        if (sel_rows.columns.len != schema.columns.len) {
            return ExecError.ColumnCountMismatch;
        }

        const values = self.allocator.alloc(Value, schema.columns.len) catch {
            return ExecError.OutOfMemory;
        };
        defer self.allocator.free(values);

        const txn = self.current_txn orelse self.beginImplicitTxn();

        var row_count: usize = 0;
        for (sel_rows.rows) |row| {
            for (row.values, schema.columns, 0..) |str_val, col, i| {
                values[i] = strToValue(str_val, col.col_type) catch {
                    self.abortImplicitTxn(txn);
                    return ExecError.TypeMismatch;
                };
            }

            const tid = table.insertTuple(txn, values) catch {
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            };

            self.maintainIndexesInsert(table.table_id, schema, values, tid);
            row_count += 1;
        }

        self.commitImplicitTxn(txn);
        return .{ .row_count = row_count };
    }

    fn strToValue(str: []const u8, col_type: ColumnType) !Value {
        if (std.mem.eql(u8, str, "NULL")) return .{ .null_value = {} };
        return switch (col_type) {
            .boolean => Value{ .boolean = std.mem.eql(u8, str, "true") },
            .smallint => Value{ .smallint = std.fmt.parseInt(i16, str, 10) catch return error.TypeMismatch },
            .integer => Value{ .integer = std.fmt.parseInt(i32, str, 10) catch return error.TypeMismatch },
            .bigint => Value{ .bigint = std.fmt.parseInt(i64, str, 10) catch return error.TypeMismatch },
            .float => Value{ .float = std.fmt.parseFloat(f64, str) catch return error.TypeMismatch },
            .varchar, .text, .json => Value{ .bytes = str },
            .date => Value{ .date = std.fmt.parseInt(i64, str, 10) catch return error.TypeMismatch },
            .timestamp => Value{ .timestamp = std.fmt.parseInt(i64, str, 10) catch return error.TypeMismatch },
            .decimal => Value{ .decimal = std.fmt.parseInt(i64, str, 10) catch return error.TypeMismatch },
            .uuid => Value{ .uuid = str }, // raw bytes or formatted string pointer
        };
    }

    fn execInsert(self: *Self, ins: ast.Insert) ExecError!ExecResult {
        return self.execInsertImpl(ins);
    }

    fn execInsertWithParams(self: *Self, ins: ast.Insert, params: []const ast.LiteralValue) ExecError!ExecResult {
        self.current_params = params;
        defer self.current_params = null;
        return self.execInsertImpl(ins);
    }

    fn execInsertImpl(self: *Self, ins: ast.Insert) ExecError!ExecResult {
        const result = self.catalog.openTable(ins.table_name) catch {
            return ExecError.StorageError;
        } orelse return ExecError.TableNotFound;
        defer self.catalog.freeSchema(result.schema);
        var table = result.table;

        // Attach MVCC components
        table.txn_manager = self.txn_manager;
        table.undo_log = self.undo_log;

        const schema = result.schema;

        // Convert AST literal values to storage Values
        const values = self.allocator.alloc(Value, schema.columns.len) catch {
            return ExecError.OutOfMemory;
        };
        defer self.allocator.free(values);

        // Resolve column mapping if column list is specified
        var col_map: ?[]const usize = null;
        defer if (col_map) |cm| self.allocator.free(cm);
        if (ins.columns) |col_names| {
            const map = self.allocator.alloc(usize, col_names.len) catch return ExecError.OutOfMemory;
            for (col_names, 0..) |name, i| {
                var found = false;
                for (schema.columns, 0..) |sc, si| {
                    if (std.mem.eql(u8, sc.name, name)) {
                        map[i] = si;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    self.allocator.free(map);
                    return ExecError.ColumnNotFound;
                }
            }
            col_map = map;
        }

        // Use explicit txn or auto-commit
        const txn = self.current_txn orelse self.beginImplicitTxn();

        var row_count: usize = 0;
        for (ins.rows) |row_vals| {
            if (col_map) |map| {
                // Column list: check value count matches column list length
                if (row_vals.len != map.len) {
                    self.abortImplicitTxn(txn);
                    return ExecError.ColumnCountMismatch;
                }
                // Fill all columns with defaults or NULL
                for (schema.columns, 0..) |col, i| {
                    values[i] = col.default_value orelse .{ .null_value = {} };
                }
                // Place values at mapped positions
                for (row_vals, map) |lit, target_idx| {
                    const resolved = resolveParam(lit, self.current_params);
                    values[target_idx] = litToValue(self.allocator, resolved, schema.columns[target_idx].col_type) catch {
                        self.abortImplicitTxn(txn);
                        return ExecError.TypeMismatch;
                    };
                }
            } else {
                // No column list: check value count matches schema
                if (row_vals.len != schema.columns.len) {
                    self.abortImplicitTxn(txn);
                    return ExecError.ColumnCountMismatch;
                }
                for (row_vals, schema.columns, 0..) |lit, col, i| {
                    const resolved = resolveParam(lit, self.current_params);
                    values[i] = litToValue(self.allocator, resolved, col.col_type) catch {
                        self.abortImplicitTxn(txn);
                        return ExecError.TypeMismatch;
                    };
                }
            }

            // Auto-fill SERIAL columns
            if (self.serial_columns.get(ins.table_name)) |serial_cols| {
                const counter_ptr = self.serial_counters.getPtr(ins.table_name).?;
                for (serial_cols) |col_idx| {
                    if (values[col_idx] == .null_value) {
                        values[col_idx] = .{ .integer = counter_ptr.* };
                        counter_ptr.* += 1;
                    } else if (values[col_idx] == .integer) {
                        // If user provides value, advance counter past it
                        if (values[col_idx].integer >= counter_ptr.*) {
                            counter_ptr.* = values[col_idx].integer + 1;
                        }
                    }
                }
            }

            // Enforce NOT NULL constraints
            for (schema.columns, values) |col, val| {
                if (!col.nullable and val == .null_value) {
                    self.abortImplicitTxn(txn);
                    return ExecError.NotNullViolation;
                }
            }

            // Enforce CHECK constraints
            if (self.check_constraints.get(ins.table_name)) |checks| {
                if (!self.validateChecks(checks, schema, values)) {
                    self.abortImplicitTxn(txn);
                    return ExecError.CheckViolation;
                }
            }

            // Enforce FOREIGN KEY constraints
            if (self.foreign_keys.get(ins.table_name)) |fks| {
                if (!self.validateForeignKeys(fks, schema, values)) {
                    self.abortImplicitTxn(txn);
                    return ExecError.ForeignKeyViolation;
                }
            }

            // Handle ON CONFLICT (upsert)
            if (ins.on_conflict) |oc| {
                // Find conflict column indices
                const conflict_indices = self.allocator.alloc(usize, oc.conflict_columns.len) catch return ExecError.OutOfMemory;
                defer self.allocator.free(conflict_indices);
                for (oc.conflict_columns, 0..) |name, ci| {
                    conflict_indices[ci] = resolveColumnIndex(schema, name) orelse return ExecError.ColumnNotFound;
                }

                // Scan for conflicting row
                const conflict_found = self.findConflictingRow(&table, schema, values, conflict_indices);
                if (conflict_found) {
                    switch (oc.action) {
                        .do_nothing => continue, // skip this row
                        .do_update => |assignments| {
                            // Build WHERE matching the conflict columns and execute UPDATE
                            const upd = ast.Update{
                                .table_name = ins.table_name,
                                .assignments = assignments,
                                .where_clause = self.buildConflictWhere(schema, values, conflict_indices),
                            };
                            _ = self.execUpdate(upd) catch {
                                self.abortImplicitTxn(txn);
                                return ExecError.StorageError;
                            };
                            row_count += 1;
                            continue;
                        },
                    }
                }
            }

            const tid = table.insertTuple(txn, values) catch {
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            };

            // Maintain indexes
            self.maintainIndexesInsert(table.table_id, schema, values, tid);
            row_count += 1;
        }

        self.commitImplicitTxn(txn);
        return .{ .row_count = row_count };
    }

    // ============================================================
    // SELECT
    // ============================================================
    fn execSelectWithParams(self: *Self, sel: ast.Select, params: []const ast.LiteralValue) ExecError!ExecResult {
        self.current_params = params;
        defer self.current_params = null;
        return self.execSelect(sel);
    }

    fn execUnion(self: *Self, uq: ast.UnionQuery) ExecError!ExecResult {
        // Execute both sides
        const left_result = try self.execSelect(uq.left);
        const right_result = try self.execSelect(uq.right);

        const left = left_result.rows;
        const right = right_result.rows;

        // Column count must match
        if (left.columns.len != right.columns.len) {
            self.freeResult(left_result);
            self.freeResult(right_result);
            return ExecError.ColumnCountMismatch;
        }

        // Use left's column names, free right's
        for (right.columns) |col| self.allocator.free(col);
        self.allocator.free(right.columns);

        if (uq.op == .@"union" and uq.all) {
            // UNION ALL: just concatenate
            const combined = self.allocator.alloc(ResultRow, left.rows.len + right.rows.len) catch return ExecError.OutOfMemory;
            @memcpy(combined[0..left.rows.len], left.rows);
            @memcpy(combined[left.rows.len..], right.rows);
            self.allocator.free(left.rows);
            self.allocator.free(right.rows);
            return ExecResult{ .rows = .{ .columns = left.columns, .rows = combined } };
        }

        // Build right-side hash set for EXCEPT/INTERSECT, or combined dedup for UNION
        var right_set: std.StringHashMapUnmanaged(void) = .empty;
        defer {
            var kit = right_set.keyIterator();
            while (kit.next()) |k| self.allocator.free(k.*);
            right_set.deinit(self.allocator);
        }

        // Build keys for right rows
        for (right.rows) |row| {
            const key = self.buildRowKey(row) catch return ExecError.OutOfMemory;
            const gop = right_set.getOrPut(self.allocator, key) catch return ExecError.OutOfMemory;
            if (gop.found_existing) self.allocator.free(key);
        }

        var result_rows: std.ArrayList(ResultRow) = .empty;

        switch (uq.op) {
            .@"union" => {
                // UNION (no ALL): deduplicate across both sides
                var seen: std.StringHashMapUnmanaged(void) = .empty;
                defer {
                    var kit = seen.keyIterator();
                    while (kit.next()) |k| self.allocator.free(k.*);
                    seen.deinit(self.allocator);
                }
                const all_rows = [_][]ResultRow{ left.rows, right.rows };
                for (&all_rows) |rows| {
                    for (rows) |row| {
                        const key = self.buildRowKey(row) catch return ExecError.OutOfMemory;
                        if (seen.contains(key)) {
                            self.allocator.free(key);
                            self.freeRow(row);
                        } else {
                            seen.put(self.allocator, key, {}) catch return ExecError.OutOfMemory;
                            result_rows.append(self.allocator, row) catch return ExecError.OutOfMemory;
                        }
                    }
                }
            },
            .except => {
                // EXCEPT: keep left rows not in right
                for (left.rows) |row| {
                    const key = self.buildRowKey(row) catch return ExecError.OutOfMemory;
                    defer self.allocator.free(key);
                    if (right_set.contains(key)) {
                        self.freeRow(row);
                    } else {
                        result_rows.append(self.allocator, row) catch return ExecError.OutOfMemory;
                    }
                }
                // Free all right rows
                for (right.rows) |row| self.freeRow(row);
            },
            .intersect => {
                // INTERSECT: keep left rows that are also in right
                for (left.rows) |row| {
                    const key = self.buildRowKey(row) catch return ExecError.OutOfMemory;
                    defer self.allocator.free(key);
                    if (right_set.contains(key)) {
                        result_rows.append(self.allocator, row) catch return ExecError.OutOfMemory;
                    } else {
                        self.freeRow(row);
                    }
                }
                // Free all right rows
                for (right.rows) |row| self.freeRow(row);
            },
        }

        self.allocator.free(left.rows);
        self.allocator.free(right.rows);

        const rows = result_rows.toOwnedSlice(self.allocator) catch return ExecError.OutOfMemory;
        return ExecResult{ .rows = .{ .columns = left.columns, .rows = rows } };
    }

    fn buildRowKey(self: *Self, row: ResultRow) ![]const u8 {
        var key_parts: std.ArrayList(u8) = .empty;
        for (row.values, 0..) |val, i| {
            if (i > 0) key_parts.append(self.allocator, 0) catch return ExecError.OutOfMemory;
            key_parts.appendSlice(self.allocator, val) catch return ExecError.OutOfMemory;
        }
        return key_parts.toOwnedSlice(self.allocator) catch return ExecError.OutOfMemory;
    }

    fn freeRow(self: *Self, row: ResultRow) void {
        for (row.values) |val| self.allocator.free(val);
        self.allocator.free(row.values);
    }

    fn execDerivedTable(self: *Self, outer_sel: ast.Select, sub: *const ast.Select) ExecError!ExecResult {
        // Execute the subquery
        const sub_result = try self.execSelect(sub.*);
        defer self.freeResult(sub_result);

        const sub_rows = sub_result.rows;

        // Resolve column indices in subquery result
        const out_col_count = outer_sel.columns.len;
        const col_indices = self.allocator.alloc(usize, out_col_count) catch return ExecError.OutOfMemory;
        defer self.allocator.free(col_indices);
        var all_star = false;

        for (outer_sel.columns, 0..) |col, i| {
            switch (col) {
                .all_columns => {
                    all_star = true;
                    break;
                },
                .named => |name| {
                    var found = false;
                    for (sub_rows.columns, 0..) |sc, si| {
                        if (std.ascii.eqlIgnoreCase(sc, name)) {
                            col_indices[i] = si;
                            found = true;
                            break;
                        }
                    }
                    if (!found) return ExecError.ColumnNotFound;
                },
                else => return ExecError.ColumnNotFound,
            }
        }

        // Build result
        const num_out_cols = if (all_star) sub_rows.columns.len else out_col_count;
        var result_rows: std.ArrayList(ResultRow) = .empty;

        for (sub_rows.rows) |row| {
            // Apply WHERE filter if any (by building a virtual schema)
            const out_vals = self.allocator.alloc([]const u8, num_out_cols) catch return ExecError.OutOfMemory;
            if (all_star) {
                for (0..num_out_cols) |ci| {
                    out_vals[ci] = self.allocator.dupe(u8, row.values[ci]) catch return ExecError.OutOfMemory;
                }
            } else {
                for (0..out_col_count) |ci| {
                    out_vals[ci] = self.allocator.dupe(u8, row.values[col_indices[ci]]) catch return ExecError.OutOfMemory;
                }
            }
            result_rows.append(self.allocator, .{ .values = out_vals }) catch return ExecError.OutOfMemory;
        }

        // Build column names
        const col_names = self.allocator.alloc([]const u8, num_out_cols) catch return ExecError.OutOfMemory;
        if (all_star) {
            for (sub_rows.columns, 0..) |sc, i| {
                col_names[i] = self.allocator.dupe(u8, sc) catch return ExecError.OutOfMemory;
            }
        } else {
            for (outer_sel.columns, 0..) |col, i| {
                const alias_name = if (outer_sel.aliases) |aliases| (if (i < aliases.len) aliases[i] else null) else null;
                col_names[i] = if (alias_name) |a|
                    self.allocator.dupe(u8, a) catch return ExecError.OutOfMemory
                else switch (col) {
                    .named => |name| self.allocator.dupe(u8, name) catch return ExecError.OutOfMemory,
                    else => self.allocator.dupe(u8, "?") catch return ExecError.OutOfMemory,
                };
            }
        }

        // Apply LIMIT/OFFSET
        var rows = result_rows.toOwnedSlice(self.allocator) catch return ExecError.OutOfMemory;
        const offset = outer_sel.offset orelse 0;
        if (offset > 0 and offset < rows.len) {
            for (rows[0..@intCast(offset)]) |row| self.freeRow(row);
            const remaining = self.allocator.alloc(ResultRow, rows.len - @as(usize, @intCast(offset))) catch return ExecError.OutOfMemory;
            @memcpy(remaining, rows[@intCast(offset)..]);
            self.allocator.free(rows);
            rows = remaining;
        }
        if (outer_sel.limit) |lim| {
            if (lim < rows.len) {
                for (rows[@intCast(lim)..]) |row| self.freeRow(row);
                const limited = self.allocator.alloc(ResultRow, @intCast(lim)) catch return ExecError.OutOfMemory;
                @memcpy(limited, rows[0..@intCast(lim)]);
                self.allocator.free(rows);
                rows = limited;
            }
        }

        return ExecResult{ .rows = .{ .columns = col_names, .rows = rows } };
    }

    fn execCTESelect(self: *Self, cs: ast.CTESelect) ExecError!ExecResult {
        // Transform the main SELECT by replacing CTE name references with subqueries
        var main = cs.select;
        for (cs.ctes) |cte| {
            if (std.mem.eql(u8, main.table_name, cte.name)) {
                const sub = self.allocator.create(ast.Select) catch return ExecError.OutOfMemory;
                sub.* = cte.query;
                main.subquery = sub;
                break;
            }
        }
        return self.execSelect(main);
    }

    fn execSelect(self: *Self, sel: ast.Select) ExecError!ExecResult {
        // Handle derived table: FROM (SELECT ...) AS alias
        if (sel.subquery) |sub| {
            return self.execDerivedTable(sel, sub);
        }

        // Route to JOIN handler if joins are present
        if (sel.joins) |join_list| {
            if (join_list.len > 0) {
                return self.execSelectJoin(sel);
            }
        }

        const result = self.catalog.openTable(sel.table_name) catch {
            return ExecError.StorageError;
        } orelse {
            // Check if it's a view
            if (self.views.get(sel.table_name)) |view_sql| {
                return self.execViewSelect(sel, view_sql);
            }
            return ExecError.TableNotFound;
        };
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

        // Check for window functions
        var has_window = false;
        for (sel.columns) |col| {
            if (col == .window_function) {
                has_window = true;
                break;
            }
        }
        if (has_window) {
            return self.execSelectWindow(sel, &table, schema);
        }

        // Determine which columns to output
        const col_proj = try self.resolveSelectColumns(sel.columns, schema);
        defer self.allocator.free(col_proj);

        // Build column name headers (prefer alias if present)
        const col_names = self.allocator.alloc([]const u8, col_proj.len) catch {
            return ExecError.OutOfMemory;
        };
        var func_name_buf: [32]u8 = undefined;
        for (col_proj, 0..) |cp, i| {
            const alias_name = if (sel.aliases) |aliases| (if (i < aliases.len) aliases[i] else null) else null;
            const name_src = alias_name orelse expr_eval.projectionColumnName(cp, schema, &func_name_buf);
            col_names[i] = self.allocator.dupe(u8, name_src) catch {
                for (col_names[0..i]) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);
                return ExecError.OutOfMemory;
            };
        }

        // Use explicit txn or begin implicit for read
        const txn = self.current_txn orelse self.beginImplicitTxn();

        // READ COMMITTED: refresh snapshot before each statement
        if (txn) |t| {
            if (t.isolation_level == .read_committed) {
                if (self.txn_manager) |tm| {
                    tm.refreshSnapshot(t) catch {};
                }
            }
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

        // Try index scan first
        const used_index = self.tryIndexScan(sel, &table, schema, col_proj, txn, &rows);

        if (!used_index) {
            // Planner chose seq_scan — try vectorized execution (reuse already-opened table)
            if (self.current_params == null and vec_exec_mod.canUseVectorized(sel)) {
                // Free col_names from scalar setup — vectorized builds its own
                for (col_names) |cn| self.allocator.free(cn);
                self.allocator.free(col_names);

                const exec_result = vec_exec_mod.execSelectVectorized(
                    self.allocator,
                    sel,
                    &table,
                    schema,
                    txn,
                ) catch |err| {
                    self.abortImplicitTxn(txn);
                    return err;
                };

                self.commitImplicitTxn(txn);
                return exec_result;
            }

            // Fall back to scalar sequential scan
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
                const formatted = self.allocator.alloc([]const u8, col_proj.len) catch {
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                for (col_proj, 0..) |cp, i| {
                    formatted[i] = expr_eval.formatProjection(self.allocator, cp, row.values, schema, self.current_params) catch {
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
        }

        self.commitImplicitTxn(txn);

        // ORDER BY: sort result rows
        var order_by_applied = false;
        var limit_applied = false;

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

            // Top-N optimization: when ORDER BY + LIMIT, use bounded partial sort
            if (sel.limit) |limit_val| {
                const offset_val = sel.offset orelse 0;
                const n: usize = @intCast(@min(limit_val + offset_val, rows.items.len));
                if (n > 0 and n < rows.items.len) {
                    // Bounded sort: maintain a sorted window of size N
                    // First, sort the first N elements (initial window)
                    const items = rows.items;
                    {
                        var si: usize = 1;
                        while (si < n) : (si += 1) {
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

                    // For each remaining row, check if it beats the worst (last) in the window
                    for (items[n..]) |*item| {
                        if (orderCompareRows(item.*, items[n - 1], order_clauses, order_indices) == .lt) {
                            // Swap out the worst element and free it
                            const evicted = items[n - 1];
                            items[n - 1] = item.*;
                            item.* = evicted;

                            // Re-sort: bubble the new element to its correct position
                            var j: usize = n - 1;
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

                    // Free rows beyond the window
                    for (items[n..]) |row| {
                        for (row.values) |v| self.allocator.free(v);
                        self.allocator.free(row.values);
                    }
                    rows.shrinkRetainingCapacity(n);

                    order_by_applied = true;
                    limit_applied = true;
                } else {
                    // N >= rows.len: just sort everything, limit is a no-op
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
                    order_by_applied = true;
                    limit_applied = true;
                }
            }

            // Standard full sort (no LIMIT or Top-N not applicable)
            if (!order_by_applied) {
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
        }

        // DISTINCT: dedup rows
        if (sel.distinct) {
            var seen = std.StringHashMap(void).init(self.allocator);
            defer {
                var key_iter = seen.keyIterator();
                while (key_iter.next()) |k| self.allocator.free(k.*);
                seen.deinit();
            }
            var write_idx: usize = 0;
            for (rows.items) |row| {
                // Build key from all column values concatenated with null separator
                var key_parts: std.ArrayList(u8) = .empty;
                for (row.values, 0..) |v, vi| {
                    if (vi > 0) key_parts.append(self.allocator, 0) catch {
                        key_parts.deinit(self.allocator);
                        return ExecError.OutOfMemory;
                    };
                    key_parts.appendSlice(self.allocator, v) catch {
                        key_parts.deinit(self.allocator);
                        return ExecError.OutOfMemory;
                    };
                }
                const key = key_parts.toOwnedSlice(self.allocator) catch {
                    key_parts.deinit(self.allocator);
                    return ExecError.OutOfMemory;
                };

                if (seen.contains(key)) {
                    // Duplicate — free this row
                    self.allocator.free(key);
                    for (row.values) |v| self.allocator.free(v);
                    self.allocator.free(row.values);
                } else {
                    seen.put(key, {}) catch {
                        self.allocator.free(key);
                        return ExecError.OutOfMemory;
                    };
                    rows.items[write_idx] = row;
                    write_idx += 1;
                }
            }
            rows.shrinkRetainingCapacity(write_idx);
        }

        // OFFSET: skip rows
        if (sel.offset) |offset_val| {
            const off: usize = @intCast(@min(offset_val, rows.items.len));
            // Free skipped rows
            for (rows.items[0..off]) |row| {
                for (row.values) |v| self.allocator.free(v);
                self.allocator.free(row.values);
            }
            // Shift remaining rows down
            const remaining = rows.items.len - off;
            std.mem.copyForwards(ResultRow, rows.items[0..remaining], rows.items[off..]);
            rows.shrinkRetainingCapacity(remaining);
        }

        // LIMIT: truncate (skip if already applied by Top-N)
        if (!limit_applied) {
            if (sel.limit) |limit_val| {
                const limit: usize = @intCast(@min(limit_val, rows.items.len));
                // Free excess rows
                for (rows.items[limit..]) |row| {
                    for (row.values) |v| self.allocator.free(v);
                    self.allocator.free(row.values);
                }
                rows.shrinkRetainingCapacity(limit);
            }
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
        distinct_sets: []?std.StringHashMapUnmanaged(void), // per-column distinct value sets
    };

    fn initGroupState(allocator: std.mem.Allocator, num_cols: usize, group_vals: [][]const u8, distinct_flags: []const bool) ExecError!GroupState {
        var state: GroupState = .{
            .count = 0,
            .sums = allocator.alloc(f64, num_cols) catch return ExecError.OutOfMemory,
            .mins = allocator.alloc(?f64, num_cols) catch return ExecError.OutOfMemory,
            .maxs = allocator.alloc(?f64, num_cols) catch return ExecError.OutOfMemory,
            .str_mins = allocator.alloc(?[]const u8, num_cols) catch return ExecError.OutOfMemory,
            .str_maxs = allocator.alloc(?[]const u8, num_cols) catch return ExecError.OutOfMemory,
            .non_null_counts = allocator.alloc(u64, num_cols) catch return ExecError.OutOfMemory,
            .group_values = group_vals,
            .distinct_sets = allocator.alloc(?std.StringHashMapUnmanaged(void), num_cols) catch return ExecError.OutOfMemory,
        };
        for (0..num_cols) |i| {
            state.sums[i] = 0;
            state.mins[i] = null;
            state.maxs[i] = null;
            state.str_mins[i] = null;
            state.str_maxs[i] = null;
            state.non_null_counts[i] = 0;
            state.distinct_sets[i] = if (distinct_flags.len > i and distinct_flags[i]) std.StringHashMapUnmanaged(void).empty else null;
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
        for (0..state.distinct_sets.len) |di| {
            if (state.distinct_sets[di]) |*set| {
                var it = set.keyIterator();
                while (it.next()) |k| self.allocator.free(k.*);
                set.deinit(self.allocator);
            }
        }
        self.allocator.free(state.distinct_sets);
        for (state.group_values) |gv| self.allocator.free(gv);
        self.allocator.free(state.group_values);
    }

    fn accumulateAgg(self: *Self, state: *GroupState, col_idx: usize, val: Value) void {
        if (val == .null_value) return;

        // For DISTINCT aggregates, check if value already seen
        if (state.distinct_sets[col_idx]) |*set| {
            const key = formatValue(self.allocator, val) catch return;
            const gop = set.getOrPut(self.allocator, key) catch {
                self.allocator.free(key);
                return;
            };
            if (gop.found_existing) {
                self.allocator.free(key);
                return; // skip duplicate
            }
        }

        state.non_null_counts[col_idx] += 1;

        const num_val: ?f64 = switch (val) {
            .smallint => |v| @floatFromInt(v),
            .integer => |v| @floatFromInt(v),
            .bigint => |v| @floatFromInt(v),
            .decimal => |v| @floatFromInt(v),
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

    fn execSelectWindow(
        self: *Self,
        sel: ast.Select,
        table: *Table,
        schema: *const Schema,
    ) ExecError!ExecResult {
        const txn = self.current_txn orelse self.beginImplicitTxn();

        // READ COMMITTED: refresh snapshot
        if (txn) |t| {
            if (t.isolation_level == .read_committed) {
                if (self.txn_manager) |tm| {
                    tm.refreshSnapshot(t) catch {};
                }
            }
        }

        // Collect all rows
        var all_rows: std.ArrayList([]Value) = .empty;
        defer {
            for (all_rows.items) |vals| self.allocator.free(vals);
            all_rows.deinit(self.allocator);
        }

        var iter = table.scanWithTxn(txn) catch return ExecError.StorageError;
        while (iter.next() catch return ExecError.StorageError) |row| {
            // Apply WHERE filter
            const matches = if (sel.where_clause) |where|
                self.evalWhere(where, schema, row.values)
            else
                true;
            if (matches) {
                all_rows.append(self.allocator, row.values) catch return ExecError.OutOfMemory;
            } else {
                iter.freeValues(row.values);
            }
        }

        self.commitImplicitTxn(txn);

        const num_rows = all_rows.items.len;
        const num_cols = sel.columns.len;

        // Build column headers
        const col_names = self.allocator.alloc([]const u8, num_cols) catch return ExecError.OutOfMemory;
        for (sel.columns, 0..) |col, ci| {
            const alias_name = if (sel.aliases) |aliases| (if (ci < aliases.len) aliases[ci] else null) else null;
            col_names[ci] = if (alias_name) |a|
                self.allocator.dupe(u8, a) catch return ExecError.OutOfMemory
            else switch (col) {
                .named => |name| self.allocator.dupe(u8, name) catch return ExecError.OutOfMemory,
                .qualified => |q| self.allocator.dupe(u8, q.column) catch return ExecError.OutOfMemory,
                .window_function => |wf| switch (wf.func) {
                    .row_number => self.allocator.dupe(u8, "row_number") catch return ExecError.OutOfMemory,
                    .rank => self.allocator.dupe(u8, "rank") catch return ExecError.OutOfMemory,
                    .dense_rank => self.allocator.dupe(u8, "dense_rank") catch return ExecError.OutOfMemory,
                },
                else => self.allocator.dupe(u8, "?") catch return ExecError.OutOfMemory,
            };
        }

        // For each window function column, compute the window values
        // First, resolve non-window column indices
        const col_indices = self.allocator.alloc(?usize, num_cols) catch return ExecError.OutOfMemory;
        defer self.allocator.free(col_indices);
        for (sel.columns, 0..) |col, ci| {
            switch (col) {
                .named => |name| {
                    col_indices[ci] = resolveColumnIndex(schema, name) orelse return ExecError.ColumnNotFound;
                },
                .qualified => |q| {
                    col_indices[ci] = resolveColumnIndex(schema, q.column) orelse return ExecError.ColumnNotFound;
                },
                .window_function => {
                    col_indices[ci] = null; // computed separately
                },
                else => {
                    col_indices[ci] = null;
                },
            }
        }

        // Compute window function values for each row
        // window_vals[col_idx][row_idx] = computed value
        const window_vals = self.allocator.alloc(?[]u64, num_cols) catch return ExecError.OutOfMemory;
        defer {
            for (window_vals) |wv| if (wv) |v| self.allocator.free(v);
            self.allocator.free(window_vals);
        }

        for (sel.columns, 0..) |col, ci| {
            if (col != .window_function) {
                window_vals[ci] = null;
                continue;
            }
            const wf = col.window_function;

            // Build row indices and sort by partition + order
            const indices = self.allocator.alloc(usize, num_rows) catch return ExecError.OutOfMemory;
            defer self.allocator.free(indices);
            for (0..num_rows) |i| indices[i] = i;

            // Sort indices by partition_by columns then order_by columns
            const SortCtx = struct {
                rows: []const []Value,
                partition_cols: ?[]const usize,
                order_cols: ?[]const usize,
                order_asc: ?[]const bool,
            };

            var part_indices: ?[]usize = null;
            defer if (part_indices) |pi| self.allocator.free(pi);
            if (wf.spec.partition_by) |pb| {
                const pi = self.allocator.alloc(usize, pb.len) catch return ExecError.OutOfMemory;
                for (pb, 0..) |pname, i| {
                    pi[i] = resolveColumnIndex(schema, pname) orelse return ExecError.ColumnNotFound;
                }
                part_indices = pi;
            }

            var ord_indices: ?[]usize = null;
            var ord_asc: ?[]bool = null;
            defer if (ord_indices) |oi| self.allocator.free(oi);
            defer if (ord_asc) |oa| self.allocator.free(oa);
            if (wf.spec.order_by) |ob| {
                const oi = self.allocator.alloc(usize, ob.len) catch return ExecError.OutOfMemory;
                const oa = self.allocator.alloc(bool, ob.len) catch return ExecError.OutOfMemory;
                for (ob, 0..) |o, i| {
                    oi[i] = resolveColumnIndex(schema, o.column) orelse return ExecError.ColumnNotFound;
                    oa[i] = o.ascending;
                }
                ord_indices = oi;
                ord_asc = oa;
            }

            const ctx: SortCtx = .{
                .rows = all_rows.items,
                .partition_cols = part_indices,
                .order_cols = ord_indices,
                .order_asc = ord_asc,
            };

            std.mem.sortUnstable(usize, indices, ctx, struct {
                fn lessThan(c: SortCtx, a: usize, b: usize) bool {
                    const ra = c.rows[a];
                    const rb = c.rows[b];
                    // Sort by partition columns first
                    if (c.partition_cols) |pcols| {
                        for (pcols) |pc| {
                            const cmp = orderValues(ra[pc], rb[pc]);
                            if (cmp != .eq) return cmp == .lt;
                        }
                    }
                    // Then by order columns
                    if (c.order_cols) |ocols| {
                        for (ocols, 0..) |oc, oi| {
                            const cmp = orderValues(ra[oc], rb[oc]);
                            if (cmp != .eq) {
                                const asc = if (c.order_asc) |oa| oa[oi] else true;
                                return if (asc) cmp == .lt else cmp == .gt;
                            }
                        }
                    }
                    return false;
                }
            }.lessThan);

            // Assign window function values
            const vals = self.allocator.alloc(u64, num_rows) catch return ExecError.OutOfMemory;

            var rank_val: u64 = 0;
            var dense_val: u64 = 0;
            var i: usize = 0;
            while (i < num_rows) {
                const row_idx = indices[i];
                const is_new_partition = if (i == 0) true else blk: {
                    if (part_indices) |pcols| {
                        const prev = all_rows.items[indices[i - 1]];
                        const curr = all_rows.items[row_idx];
                        for (pcols) |pc| {
                            if (orderValues(prev[pc], curr[pc]) != .eq) break :blk true;
                        }
                    }
                    break :blk false;
                };

                if (is_new_partition) {
                    rank_val = 1;
                    dense_val = 1;
                } else {
                    rank_val += 1;
                    // Check if order values changed from previous row
                    const prev = all_rows.items[indices[i - 1]];
                    const curr = all_rows.items[row_idx];
                    var order_changed = false;
                    if (ord_indices) |ocols| {
                        for (ocols) |oc| {
                            if (orderValues(prev[oc], curr[oc]) != .eq) {
                                order_changed = true;
                                break;
                            }
                        }
                    }
                    if (order_changed) dense_val += 1;
                }

                switch (wf.func) {
                    .row_number => vals[row_idx] = rank_val,
                    .rank => {
                        // Rank: same for ties, skip after
                        if (i > 0 and !is_new_partition) {
                            const prev = all_rows.items[indices[i - 1]];
                            const curr = all_rows.items[row_idx];
                            var same = true;
                            if (ord_indices) |ocols| {
                                for (ocols) |oc| {
                                    if (orderValues(prev[oc], curr[oc]) != .eq) {
                                        same = false;
                                        break;
                                    }
                                }
                            }
                            if (same) {
                                vals[row_idx] = vals[indices[i - 1]];
                            } else {
                                vals[row_idx] = rank_val;
                            }
                        } else {
                            vals[row_idx] = rank_val;
                        }
                    },
                    .dense_rank => vals[row_idx] = dense_val,
                }
                i += 1;
            }

            window_vals[ci] = vals;
        }

        // Build result rows in original order
        const result_rows = self.allocator.alloc(ResultRow, num_rows) catch return ExecError.OutOfMemory;
        for (0..num_rows) |ri| {
            const vals = self.allocator.alloc([]const u8, num_cols) catch return ExecError.OutOfMemory;
            for (0..num_cols) |ci| {
                if (window_vals[ci]) |wv| {
                    vals[ci] = std.fmt.allocPrint(self.allocator, "{d}", .{wv[ri]}) catch return ExecError.OutOfMemory;
                } else if (col_indices[ci]) |idx| {
                    vals[ci] = expr_eval.formatValue(self.allocator, all_rows.items[ri][idx]) catch return ExecError.OutOfMemory;
                } else {
                    vals[ci] = self.allocator.dupe(u8, "NULL") catch return ExecError.OutOfMemory;
                }
            }
            result_rows[ri] = .{ .values = vals };
        }

        return .{ .rows = .{ .columns = col_names, .rows = result_rows } };
    }

    fn orderValues(a: Value, b: Value) std.math.Order {
        const tag_a = std.meta.activeTag(a);
        const tag_b = std.meta.activeTag(b);
        if (a == .null_value and b == .null_value) return .eq;
        if (a == .null_value) return .lt;
        if (b == .null_value) return .gt;
        if (tag_a == tag_b) {
            return switch (a) {
                .smallint => |v| std.math.order(v, b.smallint),
                .integer => |v| std.math.order(v, b.integer),
                .bigint => |v| std.math.order(v, b.bigint),
                .float => |v| std.math.order(v, b.float),
                .boolean => |v| std.math.order(@intFromBool(v), @intFromBool(b.boolean)),
                .bytes => |v| std.mem.order(u8, v, b.bytes),
                .null_value => .eq,
                .date => |v| std.math.order(v, b.date),
                .timestamp => |v| std.math.order(v, b.timestamp),
                .decimal => |v| std.math.order(v, b.decimal),
                .uuid => |v| std.mem.order(u8, v[0..16], b.uuid[0..16]),
            };
        }
        // Cross-type int comparison
        const int_a = switch (a) {
            .smallint => |v| @as(i64, v),
            .integer => |v| @as(i64, v),
            .bigint => |v| v,
            else => return .lt,
        };
        const int_b = switch (b) {
            .smallint => |v| @as(i64, v),
            .integer => |v| @as(i64, v),
            .bigint => |v| v,
            else => return .gt,
        };
        return std.math.order(int_a, int_b);
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
                .all_columns, .expression, .window_function => return ExecError.ColumnNotFound,
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

        // Build distinct flags for each column
        const distinct_flags = self.allocator.alloc(bool, num_cols) catch return ExecError.OutOfMemory;
        defer self.allocator.free(distinct_flags);
        for (sel.columns, 0..) |col, i| {
            distinct_flags[i] = switch (col) {
                .aggregate => |agg| agg.distinct,
                else => false,
            };
        }

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
            const state = initGroupState(self.allocator, num_cols, empty_vals, distinct_flags) catch return ExecError.OutOfMemory;
            const key = self.allocator.dupe(u8, "") catch return ExecError.OutOfMemory;
            groups.put(key, state) catch return ExecError.OutOfMemory;
        }

        // Collect matching rows via index scan or seq scan
        var collected: std.ArrayList(CollectedRow) = .empty;
        defer {
            for (collected.items) |entry| self.allocator.free(entry.values);
            collected.deinit(self.allocator);
        }

        self.collectMatchingRows(sel.table_name, table, schema, sel.where_clause, txn, &collected) catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        for (collected.items) |entry| {
            const row_values = entry.values;

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
                    const fv = formatValue(self.allocator, row_values[gi]) catch {
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
                    gv[idx] = formatValue(self.allocator, row_values[gi]) catch {
                        for (gv[0..idx]) |v| self.allocator.free(v);
                        self.allocator.free(gv);
                        self.allocator.free(group_key);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                }
                const state = initGroupState(self.allocator, num_cols, gv, distinct_flags) catch {
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
                    const fv2 = formatValue(self.allocator, row_values[gi]) catch {
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
                self.accumulateAgg(state_ptr, i, row_values[ci]);
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
            const alias_name = if (sel.aliases) |aliases| (if (i < aliases.len) aliases[i] else null) else null;
            col_names[i] = if (alias_name) |a|
                self.allocator.dupe(u8, a) catch return ExecError.OutOfMemory
            else switch (col) {
                .aggregate => |agg| blk: {
                    const func_name = switch (agg.func) {
                        .count => "count",
                        .sum => "sum",
                        .avg => "avg",
                        .min => "min",
                        .max => "max",
                    };
                    const dist_prefix: []const u8 = if (agg.distinct) "DISTINCT " else "";
                    break :blk if (agg.column) |cn|
                        std.fmt.allocPrint(self.allocator, "{s}({s}{s})", .{ func_name, dist_prefix, cn }) catch return ExecError.OutOfMemory
                    else
                        std.fmt.allocPrint(self.allocator, "{s}(*)", .{func_name}) catch return ExecError.OutOfMemory;
                },
                .named => |name| self.allocator.dupe(u8, name) catch return ExecError.OutOfMemory,
                .qualified => |q| self.allocator.dupe(u8, q.column) catch return ExecError.OutOfMemory,
                .all_columns => unreachable,
                .expression, .window_function => return ExecError.ColumnNotFound,
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
                    .expression, .window_function => return ExecError.ColumnNotFound,
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
    // JOIN execution (nested loop join with optional index lookup)
    // ============================================================

    const IndexJoinInfo = struct {
        index_id: PageId,
        left_col_idx: usize, // column index in left schema
        right_col_idx: usize, // column index in right schema (the indexed one)
    };

    /// Check if the right table has an index on the join column from the ON condition.
    /// Returns IndexJoinInfo if an index nested-loop join is possible.
    fn tryIndexNestedLoopJoin(
        self: *Self,
        on_condition: *const ast.Expression,
        left_table_name: []const u8,
        right_table_name: []const u8,
        left_schema: *const Schema,
        right_schema: *const Schema,
        right_table_id: PageId,
    ) ?IndexJoinInfo {
        // ON condition must be a simple equality comparison
        if (on_condition.* != .comparison) return null;
        const cmp = on_condition.comparison;
        if (cmp.op != .eq) return null;

        // Extract column names and their table associations
        const left_col = self.extractJoinColumn(cmp.left, cmp.right, left_table_name, right_table_name, left_schema, right_schema);
        if (left_col == null) return null;
        const right_col = self.extractJoinColumn(cmp.left, cmp.right, right_table_name, left_table_name, right_schema, left_schema);
        if (right_col == null) return null;

        // Check if right table has an index on right_col
        const indexes = self.catalog.getIndexesForTable(right_table_id) catch return null;
        defer self.catalog.freeIndexList(indexes);

        for (indexes) |idx| {
            if (idx.column_ordinal == right_col.?) {
                // Only support integer indexes for now (matches btree key type)
                if (idx.column_ordinal < right_schema.columns.len and
                    right_schema.columns[idx.column_ordinal].col_type == .integer)
                {
                    return .{
                        .index_id = idx.index_id,
                        .left_col_idx = left_col.?,
                        .right_col_idx = right_col.?,
                    };
                }
            }
        }

        return null;
    }

    const EquiJoinKeys = struct {
        left_col_idx: usize,
        right_col_idx: usize,
    };

    /// Try to extract equi-join column indices from a simple ON condition (e.g. a.id = b.id).
    /// Returns left and right column indices if the ON is a simple equality comparison.
    fn tryExtractEquiJoinKeys(
        self: *Self,
        on_condition: *const ast.Expression,
        left_table_name: []const u8,
        right_table_name: []const u8,
        left_schema: *const Schema,
        right_schema: *const Schema,
    ) ?EquiJoinKeys {
        if (on_condition.* != .comparison) return null;
        const cmp = on_condition.comparison;
        if (cmp.op != .eq) return null;

        const left_col = self.extractJoinColumn(cmp.left, cmp.right, left_table_name, right_table_name, left_schema, right_schema);
        if (left_col == null) return null;
        const right_col = self.extractJoinColumn(cmp.left, cmp.right, right_table_name, left_table_name, right_schema, left_schema);
        if (right_col == null) return null;

        return .{
            .left_col_idx = left_col.?,
            .right_col_idx = right_col.?,
        };
    }

    /// From an ON condition `expr_a op expr_b`, extract the column index for `target_table`.
    /// Returns the column ordinal in target_schema if one side references target_table.
    fn extractJoinColumn(
        _: *Self,
        expr_a: *const ast.Expression,
        expr_b: *const ast.Expression,
        target_table: []const u8,
        other_table: []const u8,
        target_schema: *const Schema,
        other_schema: *const Schema,
    ) ?usize {
        // Try expr_a as target_table column
        if (getJoinColumnIdx(expr_a, target_table, target_schema)) |idx| {
            // Verify expr_b references the other table
            if (getJoinColumnIdx(expr_b, other_table, other_schema) != null) {
                return idx;
            }
        }
        // Try expr_b as target_table column
        if (getJoinColumnIdx(expr_b, target_table, target_schema)) |idx| {
            if (getJoinColumnIdx(expr_a, other_table, other_schema) != null) {
                return idx;
            }
        }
        return null;
    }

    fn getJoinColumnIdx(expr: *const ast.Expression, table_name: []const u8, schema: *const Schema) ?usize {
        switch (expr.*) {
            .qualified_ref => |qr| {
                if (std.ascii.eqlIgnoreCase(qr.table, table_name)) {
                    return resolveColumnIndex(schema, qr.column);
                }
            },
            .column_ref => |name| {
                // Unqualified: try to resolve in the given schema
                return resolveColumnIndex(schema, name);
            },
            else => {},
        }
        return null;
    }

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
        const col_proj = try self.resolveSelectColumnsJoin(sel.columns, combined_schema, sel.table_name, join.table_name, left_schema.columns.len);
        defer self.allocator.free(col_proj);

        // Build column headers
        var join_func_buf: [32]u8 = undefined;
        const col_names = self.allocator.alloc([]const u8, col_proj.len) catch return ExecError.OutOfMemory;
        for (col_proj, 0..) |cp, i| {
            const alias_name = if (sel.aliases) |aliases| (if (i < aliases.len) aliases[i] else null) else null;
            const name_src = alias_name orelse expr_eval.projectionColumnName(cp, combined_schema, &join_func_buf);
            col_names[i] = self.allocator.dupe(u8, name_src) catch {
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

        // Try index nested-loop join: if right table has an index on the join column,
        // use btree point lookup instead of materializing all right rows.
        const index_join = if (join.on_condition) |on_cond|
            self.tryIndexNestedLoopJoin(
                on_cond,
                sel.table_name,
                join.table_name,
                left_schema,
                right_schema,
                right_table.table_id,
            )
        else
            null;

        if (index_join) |ij| {
            // Index nested-loop join: scan left, lookup right via index
            var btree = btree_mod.BTree.open(right_table.buffer_pool, ij.index_id, self.catalog.alloc_manager);

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

                // Extract join key from left row
                const left_val = left_row.values[ij.left_col_idx];
                const key: i32 = switch (left_val) {
                    .integer => |v| v,
                    else => continue, // non-integer key, skip
                };

                // Lookup in right table's index
                const right_tid = btree.search(key) catch continue;

                const combined_values = self.allocator.alloc(Value, combined_count) catch {
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                @memcpy(combined_values[0..left_schema.columns.len], left_row.values);

                var matched = false;

                if (right_tid) |rtid| {
                    // Fetch right row by TID
                    const right_vals_opt = if (txn) |t|
                        (right_table.getTupleTxn(rtid, t) catch null)
                    else
                        (right_table.getTuple(rtid, null) catch null);

                    if (right_vals_opt) |right_vals| {
                        defer self.allocator.free(right_vals);
                        @memcpy(combined_values[left_schema.columns.len..], right_vals);

                        // Verify ON condition (handles residual checks beyond the index key)
                        const on_match = if (join.on_condition) |on_cond| self.evalJoinCondition(on_cond, sel.table_name, join.table_name, left_schema, right_schema, left_row.values, right_vals) else true;

                        if (on_match) {
                            if (sel.where_clause) |where| {
                                if (!self.evalWhere(where, combined_schema, combined_values)) {
                                    // WHERE failed, not a match
                                } else {
                                    matched = true;
                                    try self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values, &rows);
                                }
                            } else {
                                matched = true;
                                try self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values, &rows);
                            }
                        }
                    }
                }

                // LEFT JOIN: emit row with NULLs for right side if no match
                if (join.join_type == .left and !matched) {
                    for (combined_values[left_schema.columns.len..]) |*v| {
                        v.* = .{ .null_value = {} };
                    }

                    const emit = if (sel.where_clause) |where|
                        self.evalWhere(where, combined_schema, combined_values)
                    else
                        true;

                    if (emit) {
                        try self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values, &rows);
                    }
                }

                self.allocator.free(combined_values);
            }
        } else if (if (join.on_condition) |on_cond| self.tryExtractEquiJoinKeys(on_cond, sel.table_name, join.table_name, left_schema, right_schema) else null) |eq_keys| {
            // Hash join: O(n+m) for equi-joins without an index
            try self.execHashJoin(
                eq_keys,
                join,
                sel,
                &left_table,
                &right_table,
                left_schema,
                right_schema,
                combined_schema,
                combined_count,
                col_proj,
                txn,
                &rows,
            );
        } else {
            // Fallback: materialize right table rows for nested loop
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
                    right_rows.append(self.allocator, row.values) catch {
                        riter.freeValues(row.values);
                        for (col_names) |cn| self.allocator.free(cn);
                        self.allocator.free(col_names);
                        self.abortImplicitTxn(txn);
                        return ExecError.OutOfMemory;
                    };
                }
            }

            // Track matched right rows for RIGHT JOIN
            const right_matched: ?[]bool = if (join.join_type == .right) blk: {
                const rm = self.allocator.alloc(bool, right_rows.items.len) catch break :blk null;
                @memset(rm, false);
                break :blk rm;
            } else null;
            defer if (right_matched) |rm| self.allocator.free(rm);

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

                const combined_values = self.allocator.alloc(Value, combined_count) catch {
                    for (col_names) |cn| self.allocator.free(cn);
                    self.allocator.free(col_names);
                    self.abortImplicitTxn(txn);
                    return ExecError.OutOfMemory;
                };
                @memcpy(combined_values[0..left_schema.columns.len], left_row.values);

                var matched = false;
                for (right_rows.items, 0..) |right_vals, ri| {
                    @memcpy(combined_values[left_schema.columns.len..], right_vals);

                    const on_match = if (join.on_condition) |on_cond| self.evalJoinCondition(on_cond, sel.table_name, join.table_name, left_schema, right_schema, left_row.values, right_vals) else true;
                    if (!on_match) continue;

                    if (sel.where_clause) |where| {
                        if (!self.evalWhere(where, combined_schema, combined_values)) continue;
                    }

                    matched = true;
                    if (right_matched) |rm| rm[ri] = true;
                    try self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values, &rows);
                }

                // LEFT JOIN: emit row with NULLs for right side if no match
                if (join.join_type == .left and !matched) {
                    for (combined_values[left_schema.columns.len..]) |*v| {
                        v.* = .{ .null_value = {} };
                    }

                    const emit = if (sel.where_clause) |where|
                        self.evalWhere(where, combined_schema, combined_values)
                    else
                        true;

                    if (emit) {
                        try self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values, &rows);
                    }
                }

                self.allocator.free(combined_values);
            }

            // RIGHT JOIN: emit unmatched right rows with NULLs on the left side
            if (join.join_type == .right) {
                if (right_matched) |rm| {
                    for (right_rows.items, 0..) |right_vals, ri| {
                        if (rm[ri]) continue;
                        const combined_values2 = self.allocator.alloc(Value, combined_count) catch return ExecError.OutOfMemory;
                        for (combined_values2[0..left_schema.columns.len]) |*v| {
                            v.* = .{ .null_value = {} };
                        }
                        @memcpy(combined_values2[left_schema.columns.len..], right_vals);

                        const emit = if (sel.where_clause) |where|
                            self.evalWhere(where, combined_schema, combined_values2)
                        else
                            true;

                        if (emit) {
                            try self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values2, &rows);
                        }
                        self.allocator.free(combined_values2);
                    }
                }
            }
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

    /// Execute a hash join for equi-join conditions.
    /// Build phase: hash the smaller table's join keys into a hash map.
    /// Probe phase: scan the larger table and look up matches.
    /// For LEFT JOIN, the left table is always the probe side.
    fn execHashJoin(
        self: *Self,
        eq_keys: EquiJoinKeys,
        join: ast.JoinClause,
        sel: ast.Select,
        left_table: *Table,
        right_table: *Table,
        left_schema: *const Schema,
        right_schema: *const Schema,
        combined_schema: *const Schema,
        combined_count: usize,
        col_proj: []const ProjectionColumn,
        txn: ?*Transaction,
        rows: *std.ArrayList(ResultRow),
    ) ExecError!void {
        // Decide build vs probe side.
        // LEFT JOIN: always build right, probe left (to preserve all left rows).
        // INNER JOIN: build the smaller table.
        const build_right = if (join.join_type == .left)
            true
        else if (join.join_type == .right)
            false // RIGHT JOIN: build left, probe right
        else blk: {
            const left_count = left_table.tupleCount() catch 0;
            const right_count = right_table.tupleCount() catch 0;
            break :blk right_count <= left_count;
        };

        const build_col_idx = if (build_right) eq_keys.right_col_idx else eq_keys.left_col_idx;
        const probe_col_idx = if (build_right) eq_keys.left_col_idx else eq_keys.right_col_idx;
        var build_table = if (build_right) right_table else left_table;
        var probe_table = if (build_right) left_table else right_table;

        // Build phase: scan build table, hash join keys into a HashMap.
        // Key: hash of join column value, Value: list of row value arrays.
        var hash_map = std.AutoHashMap(u64, std.ArrayListUnmanaged([]Value)).init(self.allocator);
        defer {
            var it = hash_map.iterator();
            while (it.next()) |entry| {
                for (entry.value_ptr.items) |row_vals| {
                    self.allocator.free(row_vals);
                }
                entry.value_ptr.deinit(self.allocator);
            }
            hash_map.deinit();
        }

        {
            var build_iter = build_table.scanWithTxn(txn) catch return ExecError.StorageError;
            while (build_iter.next() catch return ExecError.StorageError) |row| {
                const key_val = row.values[build_col_idx];
                if (key_val == .null_value) {
                    // NULL never matches in SQL; skip
                    self.allocator.free(row.values);
                    continue;
                }
                const h = key_val.hash();
                const gop = hash_map.getOrPut(h) catch {
                    self.allocator.free(row.values);
                    return ExecError.OutOfMemory;
                };
                if (!gop.found_existing) {
                    gop.value_ptr.* = .empty;
                }
                gop.value_ptr.append(self.allocator, row.values) catch {
                    self.allocator.free(row.values);
                    return ExecError.OutOfMemory;
                };
            }
        }

        // Probe phase: scan probe table, look up matching buckets.
        {
            var probe_iter = probe_table.scanWithTxn(txn) catch return ExecError.StorageError;
            while (probe_iter.next() catch return ExecError.StorageError) |probe_row| {
                defer probe_iter.freeValues(probe_row.values);

                const probe_key = probe_row.values[probe_col_idx];
                var matched = false;

                if (probe_key != .null_value) {
                    const h = probe_key.hash();
                    if (hash_map.get(h)) |bucket| {
                        for (bucket.items) |build_vals| {
                            // Verify equality (handle hash collisions)
                            if (!Value.eqlForJoin(probe_key, build_vals[build_col_idx])) continue;

                            // Assemble combined row: left first, right second
                            const combined_values = self.allocator.alloc(Value, combined_count) catch return ExecError.OutOfMemory;
                            if (build_right) {
                                // probe=left, build=right
                                @memcpy(combined_values[0..left_schema.columns.len], probe_row.values);
                                @memcpy(combined_values[left_schema.columns.len..], build_vals);
                            } else {
                                // probe=right, build=left
                                @memcpy(combined_values[0..left_schema.columns.len], build_vals);
                                @memcpy(combined_values[left_schema.columns.len..], probe_row.values);
                            }

                            // Evaluate residual ON condition
                            const left_vals = combined_values[0..left_schema.columns.len];
                            const right_vals = combined_values[left_schema.columns.len..];
                            const on_match = self.evalJoinCondition(
                                join.on_condition.?,
                                sel.table_name,
                                join.table_name,
                                left_schema,
                                right_schema,
                                left_vals,
                                right_vals,
                            );

                            if (on_match) {
                                const emit = if (sel.where_clause) |where|
                                    self.evalWhere(where, combined_schema, combined_values)
                                else
                                    true;

                                if (emit) {
                                    matched = true;
                                    self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values, rows) catch {
                                        self.allocator.free(combined_values);
                                        return ExecError.OutOfMemory;
                                    };
                                }
                            }

                            self.allocator.free(combined_values);
                        }
                    }
                }

                // LEFT JOIN: emit row with NULLs for right side if no match
                if (join.join_type == .left and !matched and build_right) {
                    const combined_values = self.allocator.alloc(Value, combined_count) catch return ExecError.OutOfMemory;
                    @memcpy(combined_values[0..left_schema.columns.len], probe_row.values);
                    for (combined_values[left_schema.columns.len..]) |*v| {
                        v.* = .{ .null_value = {} };
                    }

                    const emit = if (sel.where_clause) |where|
                        self.evalWhere(where, combined_schema, combined_values)
                    else
                        true;

                    if (emit) {
                        self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values, rows) catch {
                            self.allocator.free(combined_values);
                            return ExecError.OutOfMemory;
                        };
                    }

                    self.allocator.free(combined_values);
                }

                // RIGHT JOIN: emit row with NULLs for left side if no match
                if (join.join_type == .right and !matched and !build_right) {
                    const combined_values = self.allocator.alloc(Value, combined_count) catch return ExecError.OutOfMemory;
                    for (combined_values[0..left_schema.columns.len]) |*v| {
                        v.* = .{ .null_value = {} };
                    }
                    @memcpy(combined_values[left_schema.columns.len..], probe_row.values);

                    const emit = if (sel.where_clause) |where|
                        self.evalWhere(where, combined_schema, combined_values)
                    else
                        true;

                    if (emit) {
                        self.formatAndAppendJoinRow(col_proj, combined_schema, combined_values, rows) catch {
                            self.allocator.free(combined_values);
                            return ExecError.OutOfMemory;
                        };
                    }

                    self.allocator.free(combined_values);
                }
            }
        }
    }

    /// Format selected columns from combined values and append to result rows.
    fn formatAndAppendJoinRow(
        self: *Self,
        col_proj: []const ProjectionColumn,
        combined_schema: *const Schema,
        combined_values: []const Value,
        rows: *std.ArrayList(ResultRow),
    ) ExecError!void {
        const formatted = self.allocator.alloc([]const u8, col_proj.len) catch return ExecError.OutOfMemory;
        errdefer self.allocator.free(formatted);
        for (col_proj, 0..) |cp, i| {
            formatted[i] = expr_eval.formatProjection(self.allocator, cp, combined_values, combined_schema, self.current_params) catch {
                for (formatted[0..i]) |f| self.allocator.free(f);
                self.allocator.free(formatted);
                return ExecError.OutOfMemory;
            };
        }
        rows.append(self.allocator, .{ .values = formatted }) catch {
            for (formatted) |f| self.allocator.free(f);
            self.allocator.free(formatted);
            return ExecError.OutOfMemory;
        };
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
            .literal => |lit| return litToStorageValue(lit, null),
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
    fn execDeleteWithParams(self: *Self, del: ast.Delete, params: []const ast.LiteralValue) ExecError!ExecResult {
        self.current_params = params;
        defer self.current_params = null;
        return self.execDelete(del);
    }

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

        // READ COMMITTED: refresh snapshot before each statement
        if (txn) |t| {
            if (t.isolation_level == .read_committed) {
                if (self.txn_manager) |tm| {
                    tm.refreshSnapshot(t) catch {};
                }
            }
        }

        // Collect TIDs and values to delete (can't delete while scanning)
        var to_delete: std.ArrayList(CollectedRow) = .empty;
        defer {
            for (to_delete.items) |entry| self.allocator.free(entry.values);
            to_delete.deinit(self.allocator);
        }

        self.collectMatchingRows(del.table_name, &table, schema, del.where_clause, txn, &to_delete) catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        // Enforce FOREIGN KEY: check no child rows reference deleted values
        for (to_delete.items) |entry| {
            if (!self.validateForeignKeyDelete(del.table_name, schema, entry.values)) {
                self.abortImplicitTxn(txn);
                return ExecError.ForeignKeyViolation;
            }
        }

        // Now delete collected tuples
        var deleted: u64 = 0;
        for (to_delete.items) |entry| {
            if (table.deleteTuple(txn, entry.tid) catch |err| {
                if (err == table_mod.TableError.WriteConflict) {
                    self.abortImplicitTxn(txn);
                    return ExecError.WriteConflict;
                }
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            }) {
                // Maintain indexes
                self.maintainIndexesDelete(table.table_id, schema, entry.values);
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
    fn execUpdateWithParams(self: *Self, upd: ast.Update, params: []const ast.LiteralValue) ExecError!ExecResult {
        self.current_params = params;
        defer self.current_params = null;
        return self.execUpdate(upd);
    }

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

        // READ COMMITTED: refresh snapshot before each statement
        if (txn) |t| {
            if (t.isolation_level == .read_committed) {
                if (self.txn_manager) |tm| {
                    tm.refreshSnapshot(t) catch {};
                }
            }
        }

        // Scan and collect rows to update (TID + current values)
        var to_update: std.ArrayList(CollectedRow) = .empty;
        defer {
            for (to_update.items) |entry| {
                self.allocator.free(entry.values);
            }
            to_update.deinit(self.allocator);
        }

        self.collectMatchingRows(upd.table_name, &table, schema, upd.where_clause, txn, &to_update) catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

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

            // Track which values were allocated by expression eval
            var owned_bytes: [64]?[]const u8 = .{null} ** 64;

            // Apply SET assignments — evaluate expressions against current row values
            for (upd.assignments, set_indices) |assign, ci| {
                const expr_result = expr_eval.evalExprToValue(self.allocator, assign.value, schema, entry.values, self.current_params);
                defer expr_result.deinit(self.allocator);
                if (expr_result.owned) {
                    switch (expr_result.value) {
                        .bytes => |b| {
                            const duped = self.allocator.dupe(u8, b) catch {
                                self.abortImplicitTxn(txn);
                                return ExecError.OutOfMemory;
                            };
                            new_values[ci] = .{ .bytes = duped };
                            owned_bytes[ci] = duped;
                        },
                        else => new_values[ci] = expr_result.value,
                    }
                } else {
                    new_values[ci] = expr_result.value;
                }
            }

            // Enforce NOT NULL constraints on updated values
            for (schema.columns, new_values) |col, val| {
                if (!col.nullable and val == .null_value) {
                    self.abortImplicitTxn(txn);
                    return ExecError.NotNullViolation;
                }
            }

            // Enforce CHECK constraints on updated values
            if (self.check_constraints.get(upd.table_name)) |checks| {
                if (!self.validateChecks(checks, schema, new_values)) {
                    self.abortImplicitTxn(txn);
                    return ExecError.CheckViolation;
                }
            }

            // Delete old tuple (MVCC: sets xmax)
            _ = table.deleteTuple(txn, entry.tid) catch |err| {
                if (err == table_mod.TableError.WriteConflict) {
                    self.abortImplicitTxn(txn);
                    return ExecError.WriteConflict;
                }
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            };

            // Maintain indexes: remove old, insert new
            self.maintainIndexesDelete(table.table_id, schema, entry.values);

            // Insert new tuple (MVCC: new xmin)
            const new_tid = table.insertTuple(txn, new_values) catch {
                self.abortImplicitTxn(txn);
                return ExecError.StorageError;
            };

            self.maintainIndexesInsert(table.table_id, schema, new_values, new_tid);

            // Free owned expression result bytes
            for (&owned_bytes) |*ob| {
                if (ob.*) |b| {
                    self.allocator.free(b);
                    ob.* = null;
                }
            }

            updated += 1;
        }

        self.commitImplicitTxn(txn);
        return .{ .row_count = updated };
    }

    // ============================================================
    // WHERE evaluation
    // ============================================================
    fn evalWhere(self: *Self, expr: *const ast.Expression, schema: *const Schema, values: []const Value) bool {
        return self.evalExprWithParams(expr, schema, values, self.current_params);
    }

    fn evalExprWithParams(self: *Self, expr: *const ast.Expression, schema: *const Schema, values: []const Value, params: ?[]const ast.LiteralValue) bool {
        switch (expr.*) {
            .comparison => |cmp| {
                const left_res = expr_eval.evalExprToValue(self.allocator, cmp.left, schema, values, params);
                defer left_res.deinit(self.allocator);
                const right_res = expr_eval.evalExprToValue(self.allocator, cmp.right, schema, values, params);
                defer right_res.deinit(self.allocator);
                return expr_eval.compareValues(left_res.value, cmp.op, right_res.value);
            },
            .and_expr => |a| {
                return self.evalExprWithParams(a.left, schema, values, params) and self.evalExprWithParams(a.right, schema, values, params);
            },
            .or_expr => |o| {
                return self.evalExprWithParams(o.left, schema, values, params) or self.evalExprWithParams(o.right, schema, values, params);
            },
            .not_expr => |n| {
                return !self.evalExprWithParams(n.operand, schema, values, params);
            },
            .between_expr => |b| {
                const val = expr_eval.evalExprToValue(self.allocator, b.value, schema, values, params);
                defer val.deinit(self.allocator);
                const low = expr_eval.evalExprToValue(self.allocator, b.low, schema, values, params);
                defer low.deinit(self.allocator);
                const high = expr_eval.evalExprToValue(self.allocator, b.high, schema, values, params);
                defer high.deinit(self.allocator);
                return expr_eval.compareValues(val.value, .gte, low.value) and expr_eval.compareValues(val.value, .lte, high.value);
            },
            .like_expr => |l| {
                const val = expr_eval.evalExprToValue(self.allocator, l.value, schema, values, params);
                defer val.deinit(self.allocator);
                const pat = expr_eval.evalExprToValue(self.allocator, l.pattern, schema, values, params);
                defer pat.deinit(self.allocator);
                const text = switch (val.value) {
                    .bytes => |s| s,
                    else => return false,
                };
                const pattern = switch (pat.value) {
                    .bytes => |s| s,
                    else => return false,
                };
                return expr_eval.matchLike(text, pattern);
            },
            .in_list => |il| {
                const val = expr_eval.evalExprToValue(self.allocator, il.value, schema, values, params);
                defer val.deinit(self.allocator);
                for (il.items) |item| {
                    const item_val = expr_eval.evalExprToValue(self.allocator, item, schema, values, params);
                    defer item_val.deinit(self.allocator);
                    if (expr_eval.compareValues(val.value, .eq, item_val.value)) return true;
                }
                return false;
            },
            .in_subquery => |isq| {
                const val_res = expr_eval.evalExprToValue(self.allocator, isq.value, schema, values, params);
                defer val_res.deinit(self.allocator);
                const sub_result = self.execSelect(isq.subquery.*) catch return false;
                defer self.freeResult(.{ .rows = sub_result.rows });
                for (sub_result.rows.rows) |row| {
                    if (row.values.len > 0) {
                        const sub_val = self.parseSubqueryValue(row.values[0], val_res.value);
                        if (expr_eval.compareValues(val_res.value, .eq, sub_val)) return true;
                    }
                }
                return false;
            },
            .exists_subquery => |esq| {
                const sub_result = self.execSelect(esq.subquery.*) catch return false;
                defer self.freeResult(.{ .rows = sub_result.rows });
                return sub_result.rows.rows.len > 0;
            },
            .literal => |lit| {
                const resolved = resolveParam(lit, params);
                // Bare literal in WHERE - treat as truthy
                return switch (resolved) {
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
            .is_null => |isn| {
                const operand = expr_eval.evalExprToValue(self.allocator, isn.operand, schema, values, params);
                defer operand.deinit(self.allocator);
                const result = operand.value == .null_value;
                return if (isn.negated) !result else result;
            },
            .case_expr, .function_call, .arithmetic, .unary_minus, .cast_expr => {
                const result = expr_eval.evalExprToValue(self.allocator, expr, schema, values, params);
                defer result.deinit(self.allocator);
                return switch (result.value) {
                    .null_value => false,
                    .boolean => |b| b,
                    .integer => |i| i != 0,
                    else => true,
                };
            },
        }
    }

    /// Parse a string from a subquery result row into a Value that can be compared
    /// with the given reference value. Tries to match the type of the reference.
    fn parseSubqueryValue(_: *Self, str: []const u8, ref_val: Value) Value {
        // Try to match the type of the reference value
        switch (ref_val) {
            .integer => {
                const v = std.fmt.parseInt(i32, str, 10) catch return Value{ .bytes = str };
                return .{ .integer = v };
            },
            .bigint => {
                const v = std.fmt.parseInt(i64, str, 10) catch return Value{ .bytes = str };
                return .{ .bigint = v };
            },
            .float => {
                const v = std.fmt.parseFloat(f64, str) catch return Value{ .bytes = str };
                return .{ .float = v };
            },
            .bytes => return .{ .bytes = str },
            .boolean => {
                if (std.mem.eql(u8, str, "true")) return .{ .boolean = true };
                if (std.mem.eql(u8, str, "false")) return .{ .boolean = false };
                return .{ .bytes = str };
            },
            .null_value => return .{ .null_value = {} },
            .smallint => {
                const v = std.fmt.parseInt(i16, str, 10) catch return Value{ .bytes = str };
                return .{ .smallint = v };
            },
            .date => {
                const v = std.fmt.parseInt(i64, str, 10) catch return Value{ .bytes = str };
                return .{ .date = v };
            },
            .timestamp => {
                const v = std.fmt.parseInt(i64, str, 10) catch return Value{ .bytes = str };
                return .{ .timestamp = v };
            },
            .decimal => {
                const v = std.fmt.parseInt(i64, str, 10) catch return Value{ .bytes = str };
                return .{ .decimal = v };
            },
            .uuid => return .{ .uuid = str },
        }
    }


    fn litToStorageValue(lit: ast.LiteralValue, params: ?[]const ast.LiteralValue) Value {
        const resolved = resolveParam(lit, params);
        return switch (resolved) {
            .integer => |i| .{ .integer = @intCast(i) },
            .float => |f| .{ .float = f },
            .string => |s| .{ .bytes = s },
            .boolean => |b| .{ .boolean = b },
            .null_value => .{ .null_value = {} },
            .parameter => .{ .null_value = {} }, // unresolved — shouldn't happen
        };
    }

    fn compareValues(left: Value, op: ast.CompOp, right: Value) bool {
        // Handle null comparisons
        if (left == .null_value or right == .null_value) return false;

        switch (left) {
            .smallint => |li| {
                const li_wide: i64 = li;
                const ri: i64 = switch (right) {
                    .smallint => |v| v,
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
            .integer => |li| {
                const li_wide: i64 = li;
                const ri: i64 = switch (right) {
                    .smallint => |v| v,
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
            .bigint, .date, .timestamp, .decimal => |li| {
                const ri: i64 = switch (right) {
                    .smallint => |v| v,
                    .integer => |v| v,
                    .bigint, .date, .timestamp, .decimal => |v| v,
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
                    .smallint => |v| @floatFromInt(v),
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
            .uuid => |lu| {
                const ru = switch (right) {
                    .uuid => |v| v,
                    else => return false,
                };
                const cmp = std.mem.order(u8, lu[0..16], ru[0..16]);
                return switch (op) {
                    .eq => cmp == .eq,
                    .neq => cmp != .eq,
                    .lt => cmp == .lt,
                    .gt => cmp == .gt,
                    .lte => cmp != .gt,
                    .gte => cmp != .lt,
                };
            },
            .null_value => return false,
        }
    }

    // ============================================================
    // Helpers
    // ============================================================

    const ProjectionColumn = expr_eval.ProjectionColumn;

    fn resolveSelectColumns(self: *Self, sel_cols: []const ast.SelectColumn, schema: *const Schema) ExecError![]ProjectionColumn {
        return self.resolveSelectColumnsJoin(sel_cols, schema, null, null, 0);
    }

    fn resolveSelectColumnsJoin(
        self: *Self,
        sel_cols: []const ast.SelectColumn,
        schema: *const Schema,
        left_table: ?[]const u8,
        right_table: ?[]const u8,
        left_col_count: usize,
    ) ExecError![]ProjectionColumn {
        if (sel_cols.len == 1 and sel_cols[0] == .all_columns) {
            // SELECT * — all columns
            const proj = self.allocator.alloc(ProjectionColumn, schema.columns.len) catch {
                return ExecError.OutOfMemory;
            };
            for (0..schema.columns.len) |i| {
                proj[i] = .{ .index = i };
            }
            return proj;
        }

        const proj = self.allocator.alloc(ProjectionColumn, sel_cols.len) catch {
            return ExecError.OutOfMemory;
        };

        for (sel_cols, 0..) |sc, i| {
            switch (sc) {
                .named => |name| {
                    var found = false;
                    for (schema.columns, 0..) |col, ci| {
                        if (std.ascii.eqlIgnoreCase(col.name, name)) {
                            proj[i] = .{ .index = ci };
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        self.allocator.free(proj);
                        return ExecError.ColumnNotFound;
                    }
                },
                .qualified => |q| {
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
                            proj[i] = .{ .index = ci };
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        self.allocator.free(proj);
                        return ExecError.ColumnNotFound;
                    }
                },
                .expression => |expr| {
                    proj[i] = .{ .expression = expr };
                },
                else => {
                    self.allocator.free(proj);
                    return ExecError.ColumnNotFound;
                },
            }
        }

        return proj;
    }

    fn mapDataType(dt: ast.DataType) ColumnType {
        return switch (dt) {
            .int, .integer, .serial => .integer,
            .smallint => .smallint,
            .bigint => .bigint,
            .float => .float,
            .boolean => .boolean,
            .varchar => .varchar,
            .text => .text,
            .date => .date,
            .timestamp => .timestamp,
            .decimal => .decimal,
            .uuid => .uuid,
            .json => .json,
        };
    }

    /// Resolve a parameter reference to its bound value, or return the literal as-is.
    fn resolveParam(lit: ast.LiteralValue, params: ?[]const ast.LiteralValue) ast.LiteralValue {
        return switch (lit) {
            .parameter => |idx| if (params) |p| p[idx] else lit,
            else => lit,
        };
    }

    fn litToValue(allocator: std.mem.Allocator, lit: ast.LiteralValue, col_type: ColumnType) !Value {
        return switch (lit) {
            .integer => |i| switch (col_type) {
                .smallint => Value{ .smallint = @intCast(i) },
                .integer => Value{ .integer = @intCast(i) },
                .bigint => Value{ .bigint = i },
                .float => Value{ .float = @floatFromInt(i) },
                .decimal => Value{ .decimal = i }, // will be scaled by caller
                else => error.TypeMismatch,
            },
            .float => |f| switch (col_type) {
                .float => Value{ .float = f },
                .decimal => Value{ .decimal = @intFromFloat(f) }, // will be scaled by caller
                else => error.TypeMismatch,
            },
            .string => |s| switch (col_type) {
                .varchar, .text, .json => Value{ .bytes = s },
                .date => Value{ .date = parseDateToEpochDays(s) orelse return error.TypeMismatch },
                .timestamp => Value{ .timestamp = parseTimestampToEpochSecs(s) orelse return error.TypeMismatch },
                .uuid => blk: {
                    const uuid_mem = allocator.alloc(u8, 16) catch return error.TypeMismatch;
                    var uuid_buf: [16]u8 = undefined;
                    if (!tuple_mod.parseUuidString(s, &uuid_buf)) {
                        allocator.free(uuid_mem);
                        return error.TypeMismatch;
                    }
                    @memcpy(uuid_mem, &uuid_buf);
                    break :blk Value{ .uuid = uuid_mem };
                },
                else => error.TypeMismatch,
            },
            .boolean => |b| switch (col_type) {
                .boolean => Value{ .boolean = b },
                else => error.TypeMismatch,
            },
            .null_value => Value{ .null_value = {} },
            .parameter => error.TypeMismatch, // unresolved parameter
        };
    }

    fn formatValue(allocator: std.mem.Allocator, val: Value) ![]const u8 {
        return switch (val) {
            .null_value => try allocator.dupe(u8, "NULL"),
            .boolean => |b| try allocator.dupe(u8, if (b) "true" else "false"),
            .smallint => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .integer => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .bigint => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .float => |f| try std.fmt.allocPrint(allocator, "{d:.6}", .{f}),
            .bytes => |s| try allocator.dupe(u8, s),
            .date => |d| formatEpochDays(allocator, d),
            .timestamp => |t| formatEpochSecs(allocator, t),
            .decimal => |d| try std.fmt.allocPrint(allocator, "{d}", .{d}),
            .uuid => |u| blk: {
                var buf: [36]u8 = undefined;
                const s = tuple_mod.formatUuidBytes(u, &buf) catch return error.OutOfMemory;
                break :blk try allocator.dupe(u8, s);
            },
        };
    }

    fn parseDateToEpochDays(s: []const u8) ?i64 {
        // Parse YYYY-MM-DD
        if (s.len != 10 or s[4] != '-' or s[7] != '-') return null;
        const year = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
        const month = std.fmt.parseInt(u32, s[5..7], 10) catch return null;
        const day = std.fmt.parseInt(u32, s[8..10], 10) catch return null;
        if (month < 1 or month > 12 or day < 1 or day > 31) return null;
        return civilToEpochDays(year, month, day);
    }

    fn parseTimestampToEpochSecs(s: []const u8) ?i64 {
        // Parse YYYY-MM-DD HH:MM:SS
        if (s.len != 19 or s[4] != '-' or s[7] != '-' or s[10] != ' ' or s[13] != ':' or s[16] != ':') return null;
        const year = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
        const month = std.fmt.parseInt(u32, s[5..7], 10) catch return null;
        const day = std.fmt.parseInt(u32, s[8..10], 10) catch return null;
        const hour = std.fmt.parseInt(u32, s[11..13], 10) catch return null;
        const minute = std.fmt.parseInt(u32, s[14..16], 10) catch return null;
        const second = std.fmt.parseInt(u32, s[17..19], 10) catch return null;
        if (month < 1 or month > 12 or day < 1 or day > 31) return null;
        if (hour > 23 or minute > 59 or second > 59) return null;
        const days = civilToEpochDays(year, month, day) orelse return null;
        return days * 86400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    }

    fn civilToEpochDays(y_raw: i32, m: u32, d: u32) ?i64 {
        // Algorithm based on Howard Hinnant's date algorithms
        var y: i64 = y_raw;
        var m_adj = m;
        if (m_adj <= 2) {
            y -= 1;
            m_adj += 9;
        } else {
            m_adj -= 3;
        }
        const era: i64 = @divFloor(y, 400);
        const yoe: i64 = y - era * 400;
        const doy: i64 = @divFloor(@as(i64, @intCast(153 * m_adj + 2)), 5) + @as(i64, @intCast(d)) - 1;
        const doe: i64 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
        return era * 146097 + doe - 719468;
    }

    fn formatEpochDays(allocator: std.mem.Allocator, epoch_days: i64) ![]const u8 {
        // Inverse of civilToEpochDays
        const z = epoch_days + 719468;
        const era = @divFloor(z, 146097);
        const doe = z - era * 146097;
        const yoe_raw = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
        const y = yoe_raw + era * 400;
        const doy = doe - (365 * yoe_raw + @divFloor(yoe_raw, 4) - @divFloor(yoe_raw, 100));
        const mp = @divFloor(5 * doy + 2, 153);
        const d = doy - @divFloor(153 * mp + 2, 5) + 1;
        const m = if (mp < 10) mp + 3 else mp - 9;
        const yr = if (m <= 2) y + 1 else y;
        var buf: [11]u8 = undefined;
        _ = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{
            @as(u32, @intCast(yr)), @as(u32, @intCast(m)), @as(u32, @intCast(d)),
        }) catch return error.OutOfMemory;
        return allocator.dupe(u8, buf[0..10]);
    }

    fn formatEpochSecs(allocator: std.mem.Allocator, epoch_secs: i64) ![]const u8 {
        const days = @divFloor(epoch_secs, 86400);
        const rem = @mod(epoch_secs, 86400);
        const hour = @divFloor(rem, 3600);
        const min = @divFloor(@mod(rem, 3600), 60);
        const sec = @mod(rem, 60);
        const date_str = try formatEpochDays(allocator, days);
        defer allocator.free(date_str);
        return std.fmt.allocPrint(allocator, "{s} {d:0>2}:{d:0>2}:{d:0>2}", .{
            date_str, @as(u32, @intCast(hour)), @as(u32, @intCast(min)), @as(u32, @intCast(sec)),
        });
    }

    fn findConflictingRow(self: *Self, table: *Table, schema: *const Schema, values: []const Value, conflict_indices: []const usize) bool {
        _ = schema;
        var iter = table.scan() catch return false;
        while (iter.next() catch null) |row| {
            defer self.allocator.free(row.values);

            var all_match = true;
            for (conflict_indices) |idx| {
                if (!valuesEqual(values[idx], row.values[idx])) {
                    all_match = false;
                    break;
                }
            }
            if (all_match) return true;
        }
        return false;
    }

    fn buildConflictWhere(self: *Self, schema: *const Schema, values: []const Value, conflict_indices: []const usize) ?*const ast.Expression {
        var result: ?*const ast.Expression = null;
        for (conflict_indices) |idx| {
            const col_name = schema.columns[idx].name;
            // Build: col = value
            const col_ref = self.allocator.create(ast.Expression) catch return null;
            col_ref.* = .{ .column_ref = col_name };

            const lit_expr = self.allocator.create(ast.Expression) catch return null;
            lit_expr.* = .{ .literal = valueTo_astLiteral(values[idx]) };

            const cmp = self.allocator.create(ast.Expression) catch return null;
            cmp.* = .{ .comparison = .{ .left = col_ref, .op = .eq, .right = lit_expr } };

            if (result) |prev| {
                const and_expr = self.allocator.create(ast.Expression) catch return null;
                and_expr.* = .{ .and_expr = .{ .left = prev, .right = cmp } };
                result = and_expr;
            } else {
                result = cmp;
            }
        }
        return result;
    }

    fn valueTo_astLiteral(val: Value) ast.LiteralValue {
        return switch (val) {
            .null_value => .{ .null_value = {} },
            .boolean => |b| .{ .boolean = b },
            .smallint => |i| .{ .integer = i },
            .integer => |i| .{ .integer = i },
            .bigint => |i| .{ .integer = i },
            .float => |f| .{ .float = f },
            .decimal => |d| .{ .integer = d },
            .bytes => |s| .{ .string = s },
            .date => |d| .{ .integer = d },
            .timestamp => |t| .{ .integer = t },
            .uuid => |u| .{ .string = u },
        };
    }
};

// ============================================================
// Tests
// ============================================================

const disk_manager_mod = @import("../storage/disk_manager.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const alloc_map_mod = @import("../storage/alloc_map.zig");
const DiskManager = disk_manager_mod.DiskManager;
const BufferPool = buffer_pool_mod.BufferPool;
const AllocManager = alloc_map_mod.AllocManager;

test "executor create table and insert" {
    const test_file = "test_exec_basic.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
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

test "executor ROLLBACK without BEGIN returns error" {
    const test_file = "test_exec_rollback_nobegin.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const result = exec.execute("ROLLBACK");
    try std.testing.expectError(ExecError.TransactionError, result);
}

test "executor COMMIT without BEGIN returns error" {
    const test_file = "test_exec_commit_nobegin.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const result = exec.execute("COMMIT");
    try std.testing.expectError(ExecError.TransactionError, result);
}

test "executor nested BEGIN returns error" {
    const test_file = "test_exec_nested_begin.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const b1 = try exec.execute("BEGIN");
    exec.freeResult(b1);

    // Second BEGIN should fail
    const b2 = exec.execute("BEGIN");
    try std.testing.expectError(ExecError.TransactionError, b2);

    // Clean up — rollback the first txn
    const rb = try exec.execute("ROLLBACK");
    exec.freeResult(rb);
}

test "executor SELECT on empty table returns no rows" {
    const test_file = "test_exec_empty_select.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE empty_t (id INT, name TEXT)");
    exec.freeResult(ct);

    const sel = try exec.execute("SELECT * FROM empty_t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 0), sel.rows.rows.len);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.columns.len);
}

test "executor DELETE on empty table returns zero" {
    const test_file = "test_exec_empty_delete.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE empty_t (id INT)");
    exec.freeResult(ct);

    const del = try exec.execute("DELETE FROM empty_t WHERE id = 1");
    defer exec.freeResult(del);
    try std.testing.expectEqual(@as(u64, 0), del.row_count);
}

test "executor UPDATE on empty table returns zero" {
    const test_file = "test_exec_empty_update.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE empty_t (id INT, val INT)");
    exec.freeResult(ct);

    const upd = try exec.execute("UPDATE empty_t SET val = 99");
    defer exec.freeResult(upd);
    try std.testing.expectEqual(@as(u64, 0), upd.row_count);
}

test "executor insert then delete then rollback restores row" {
    const test_file = "test_exec_ins_del_rollback.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    // Create table and insert a committed row
    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1, 'Alice')");
    exec.freeResult(ins);

    // BEGIN, DELETE, ROLLBACK — row should reappear
    const b = try exec.execute("BEGIN");
    exec.freeResult(b);
    const del = try exec.execute("DELETE FROM t WHERE id = 1");
    exec.freeResult(del);

    // Within txn, row should be gone
    const sel1 = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(sel1);
    try std.testing.expectEqual(@as(usize, 0), sel1.rows.rows.len);

    const rb = try exec.execute("ROLLBACK");
    exec.freeResult(rb);

    // After rollback, row should be back
    const sel2 = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(sel2);
    try std.testing.expectEqual(@as(usize, 1), sel2.rows.rows.len);
    try std.testing.expectEqualStrings("Alice", sel2.rows.rows[0].values[1]);
}

test "executor CREATE TABLE duplicate name fails" {
    const test_file = "test_exec_dup_table.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct1);

    const ct2 = exec.execute("CREATE TABLE t (id INT)");
    try std.testing.expectError(ExecError.TableAlreadyExists, ct2);
}

test "executor INSERT column count mismatch" {
    const test_file = "test_exec_col_mismatch.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    // Too few values
    const result = exec.execute("INSERT INTO t VALUES (1)");
    try std.testing.expectError(ExecError.ColumnCountMismatch, result);
}

test "executor UPDATE WHERE matches zero rows" {
    const test_file = "test_exec_update_zero.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1, 'Alice')");
    exec.freeResult(ins);

    // UPDATE with WHERE that matches nothing
    const upd = try exec.execute("UPDATE t SET name = 'Bob' WHERE id = 999");
    defer exec.freeResult(upd);
    try std.testing.expectEqual(@as(u64, 0), upd.row_count);

    // Original should be unchanged
    const sel = try exec.execute("SELECT name FROM t WHERE id = 1");
    defer exec.freeResult(sel);
    try std.testing.expectEqualStrings("Alice", sel.rows.rows[0].values[0]);
}

test "executor multiple inserts then delete all" {
    const test_file = "test_exec_delete_all.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        const sql = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i + 1}) catch unreachable;
        defer std.testing.allocator.free(sql);
        const r = try exec.execute(sql);
        exec.freeResult(r);
    }

    // DELETE all (no WHERE)
    const del = try exec.execute("DELETE FROM t");
    defer exec.freeResult(del);
    try std.testing.expectEqual(@as(u64, 10), del.row_count);

    // Table should be empty
    const sel = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 0), sel.rows.rows.len);
}

test "executor INSERT into nonexistent table" {
    const test_file = "test_exec_ins_noent.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const result = exec.execute("INSERT INTO nonexistent VALUES (1)");
    try std.testing.expectError(ExecError.TableNotFound, result);
}

test "executor DELETE from nonexistent table" {
    const test_file = "test_exec_del_noent.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const result = exec.execute("DELETE FROM nonexistent WHERE id = 1");
    try std.testing.expectError(ExecError.TableNotFound, result);
}

test "executor parse error" {
    const test_file = "test_exec_parse_err.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const result = exec.execute("THIS IS NOT SQL");
    try std.testing.expectError(ExecError.ParseError, result);
}

test "executor SELECT COUNT on empty table" {
    const test_file = "test_exec_count_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    const sel = try exec.execute("SELECT COUNT(*) FROM t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
    try std.testing.expectEqualStrings("0", sel.rows.rows[0].values[0]);
}

test "executor DROP TABLE" {
    const test_file = "test_exec_drop.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);

    // Insert a row to confirm table works
    const ins = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(ins);

    // DROP TABLE
    const drop = try exec.execute("DROP TABLE users");
    defer exec.freeResult(drop);
    try std.testing.expectEqualStrings("DROP TABLE", drop.message);

    // Table should no longer exist
    const result = exec.execute("SELECT * FROM users");
    try std.testing.expectError(ExecError.TableNotFound, result);
}

test "executor DROP TABLE nonexistent fails" {
    const test_file = "test_exec_drop_noent.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const result = exec.execute("DROP TABLE nonexistent");
    try std.testing.expectError(ExecError.TableNotFound, result);
}

test "executor SELECT with column aliases" {
    const test_file = "test_exec_alias.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(ins);

    const sel = try exec.execute("SELECT id AS user_id, name AS user_name FROM users");
    defer exec.freeResult(sel);
    try std.testing.expectEqualStrings("user_id", sel.rows.columns[0]);
    try std.testing.expectEqualStrings("user_name", sel.rows.columns[1]);
    try std.testing.expectEqualStrings("1", sel.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("Alice", sel.rows.rows[0].values[1]);
}

test "executor SELECT DISTINCT" {
    const test_file = "test_exec_distinct.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, color TEXT)");
    exec.freeResult(ct);
    const r1 = try exec.execute("INSERT INTO t VALUES (1, 'red')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO t VALUES (2, 'blue')");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO t VALUES (3, 'red')");
    exec.freeResult(r3);
    const r4 = try exec.execute("INSERT INTO t VALUES (4, 'blue')");
    exec.freeResult(r4);

    // DISTINCT should return 2 unique colors
    const sel = try exec.execute("SELECT DISTINCT color FROM t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);
}

test "executor BETWEEN" {
    const test_file = "test_exec_between.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const r1 = try exec.execute("INSERT INTO t VALUES (1, 'a')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO t VALUES (5, 'b')");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO t VALUES (10, 'c')");
    exec.freeResult(r3);
    const r4 = try exec.execute("INSERT INTO t VALUES (15, 'd')");
    exec.freeResult(r4);

    // BETWEEN 3 AND 12 should return rows with id 5 and 10
    const sel = try exec.execute("SELECT * FROM t WHERE id BETWEEN 3 AND 12");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);
}

test "executor LIKE" {
    const test_file = "test_exec_like.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const r1 = try exec.execute("INSERT INTO t VALUES (1, 'Alice')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO t VALUES (2, 'Bob')");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO t VALUES (3, 'Anna')");
    exec.freeResult(r3);

    // LIKE 'A%' should match Alice and Anna
    const sel = try exec.execute("SELECT * FROM t WHERE name LIKE 'A%'");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);

    // LIKE '_o_' should match Bob
    const sel2 = try exec.execute("SELECT * FROM t WHERE name LIKE '_ob'");
    defer exec.freeResult(sel2);
    try std.testing.expectEqual(@as(usize, 1), sel2.rows.rows.len);
    try std.testing.expectEqualStrings("Bob", sel2.rows.rows[0].values[1]);
}

test "executor IN list" {
    const test_file = "test_exec_in.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const r1 = try exec.execute("INSERT INTO t VALUES (1, 'Alice')");
    exec.freeResult(r1);
    const r2 = try exec.execute("INSERT INTO t VALUES (2, 'Bob')");
    exec.freeResult(r2);
    const r3 = try exec.execute("INSERT INTO t VALUES (3, 'Charlie')");
    exec.freeResult(r3);

    // IN (1, 3) should return Alice and Charlie
    const sel = try exec.execute("SELECT * FROM t WHERE id IN (1, 3)");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);
}

test "executor ALTER TABLE ADD COLUMN" {
    const test_file = "test_exec_alter_add.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    // Create table and insert row BEFORE adding column
    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(ins);

    // ALTER TABLE ADD COLUMN
    const alt = try exec.execute("ALTER TABLE users ADD COLUMN email TEXT");
    defer exec.freeResult(alt);
    try std.testing.expectEqualStrings("ALTER TABLE", alt.message);

    // Existing row should have NULL for new column
    const sel = try exec.execute("SELECT * FROM users");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 3), sel.rows.columns.len);
    try std.testing.expectEqualStrings("email", sel.rows.columns[2]);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
    try std.testing.expectEqualStrings("1", sel.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("Alice", sel.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("NULL", sel.rows.rows[0].values[2]);
}

test "executor ALTER TABLE ADD COLUMN then insert" {
    const test_file = "test_exec_alter_ins.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);
    const ins1 = try exec.execute("INSERT INTO t VALUES (1)");
    exec.freeResult(ins1);

    // Add column
    const alt = try exec.execute("ALTER TABLE t ADD name TEXT");
    exec.freeResult(alt);

    // Insert with new column
    const ins2 = try exec.execute("INSERT INTO t VALUES (2, 'Bob')");
    exec.freeResult(ins2);

    // Select should show both rows
    const sel = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.columns.len);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);
}

test "executor ALTER TABLE nonexistent fails" {
    const test_file = "test_exec_alter_noent.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const result = exec.execute("ALTER TABLE nonexistent ADD col INT");
    try std.testing.expectError(ExecError.TableNotFound, result);
}

test "executor IN subquery" {
    const test_file = "test_exec_in_sub.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE active_ids (user_id INT)");
    exec.freeResult(ct2);

    const ins1 = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO users VALUES (2, 'Bob')");
    exec.freeResult(ins2);
    const ins3 = try exec.execute("INSERT INTO users VALUES (3, 'Charlie')");
    exec.freeResult(ins3);

    const ins4 = try exec.execute("INSERT INTO active_ids VALUES (1)");
    exec.freeResult(ins4);
    const ins5 = try exec.execute("INSERT INTO active_ids VALUES (3)");
    exec.freeResult(ins5);

    // IN (SELECT ...) — should return Alice and Charlie
    const sel = try exec.execute("SELECT name FROM users WHERE id IN (SELECT user_id FROM active_ids)");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);
}

test "executor EXISTS subquery" {
    const test_file = "test_exec_exists.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE orders (id INT, user_id INT)");
    exec.freeResult(ct2);

    const ins1 = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO users VALUES (2, 'Bob')");
    exec.freeResult(ins2);
    const ins3 = try exec.execute("INSERT INTO orders VALUES (1, 1)");
    exec.freeResult(ins3);

    // EXISTS on non-empty table — should return both users since subquery is not correlated
    const sel = try exec.execute("SELECT * FROM users WHERE EXISTS (SELECT * FROM orders)");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.rows.len);

    // EXISTS on empty result — drop orders, empty it
    const del = try exec.execute("DELETE FROM orders");
    exec.freeResult(del);
    const sel2 = try exec.execute("SELECT * FROM users WHERE EXISTS (SELECT * FROM orders)");
    defer exec.freeResult(sel2);
    try std.testing.expectEqual(@as(usize, 0), sel2.rows.rows.len);
}

test "executor IN subquery empty result" {
    const test_file = "test_exec_in_sub_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE empty_t (user_id INT)");
    exec.freeResult(ct2);

    const ins1 = try exec.execute("INSERT INTO users VALUES (1, 'Alice')");
    exec.freeResult(ins1);

    // IN (SELECT ...) from empty table — should return 0 rows
    const sel = try exec.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM empty_t)");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 0), sel.rows.rows.len);
}

test "executor CREATE INDEX and DROP INDEX" {
    const test_file = "test_exec_idx.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);

    // CREATE INDEX
    const r1 = try exec.execute("CREATE INDEX idx_users_id ON users (id)");
    defer exec.freeResult(r1);
    try std.testing.expectEqualStrings("CREATE INDEX", r1.message);

    // CREATE UNIQUE INDEX
    const r2 = try exec.execute("CREATE UNIQUE INDEX idx_users_name ON users (name)");
    defer exec.freeResult(r2);
    try std.testing.expectEqualStrings("CREATE INDEX", r2.message);

    // Duplicate should fail
    try std.testing.expectError(ExecError.IndexAlreadyExists, exec.execute("CREATE INDEX idx_users_id ON users (id)"));

    // DROP INDEX
    const r3 = try exec.execute("DROP INDEX idx_users_id");
    defer exec.freeResult(r3);
    try std.testing.expectEqualStrings("DROP INDEX", r3.message);

    // DROP nonexistent should fail
    try std.testing.expectError(ExecError.IndexNotFound, exec.execute("DROP INDEX nope"));
}

test "executor index maintained on insert" {
    const test_file = "test_exec_idx_ins.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Insert rows — should be indexed
    const ins1 = try exec.execute("INSERT INTO t VALUES (10, 'a')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES (20, 'b')");
    exec.freeResult(ins2);

    // Verify via btree search
    const idx_entry = (try catalog.findIndex("idx_t_id")).?;
    defer catalog.freeIndexEntry(idx_entry);
    var btree = btree_mod.BTree.open(&bp, idx_entry.index_id, &am);
    const found10 = try btree.search(10);
    try std.testing.expect(found10 != null);
    const found20 = try btree.search(20);
    try std.testing.expect(found20 != null);
    const found99 = try btree.search(99);
    try std.testing.expect(found99 == null);
}

test "executor index maintained on delete" {
    const test_file = "test_exec_idx_del.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    const ins = try exec.execute("INSERT INTO t VALUES (42, 'x')");
    exec.freeResult(ins);

    // Verify indexed
    const idx_entry = (try catalog.findIndex("idx_t_id")).?;
    defer catalog.freeIndexEntry(idx_entry);
    var btree = btree_mod.BTree.open(&bp, idx_entry.index_id, &am);
    try std.testing.expect((try btree.search(42)) != null);

    // Delete
    const del = try exec.execute("DELETE FROM t WHERE id = 42");
    exec.freeResult(del);

    // Verify removed from index
    try std.testing.expect((try btree.search(42)) == null);
}

test "executor index maintained on update" {
    const test_file = "test_exec_idx_upd.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();
    var exec = Executor.initWithMvcc(std.testing.allocator, &catalog, &tm, &undo);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    const ins = try exec.execute("INSERT INTO t VALUES (1, 'x')");
    exec.freeResult(ins);

    // Update id from 1 to 99
    const upd = try exec.execute("UPDATE t SET id = 99 WHERE id = 1");
    exec.freeResult(upd);

    const idx_entry = (try catalog.findIndex("idx_t_id")).?;
    defer catalog.freeIndexEntry(idx_entry);
    var btree = btree_mod.BTree.open(&bp, idx_entry.index_id, &am);

    // Old key gone, new key present
    try std.testing.expect((try btree.search(1)) == null);
    try std.testing.expect((try btree.search(99)) != null);
}

test "executor backfill on CREATE INDEX" {
    const test_file = "test_exec_idx_backfill.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    // Insert rows first, THEN create index
    const ins1 = try exec.execute("INSERT INTO t VALUES (5, 'a')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES (15, 'b')");
    exec.freeResult(ins2);
    const ins3 = try exec.execute("INSERT INTO t VALUES (25, 'c')");
    exec.freeResult(ins3);

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // All existing rows should be in the index
    const idx_entry = (try catalog.findIndex("idx_t_id")).?;
    defer catalog.freeIndexEntry(idx_entry);
    var btree = btree_mod.BTree.open(&bp, idx_entry.index_id, &am);

    try std.testing.expect((try btree.search(5)) != null);
    try std.testing.expect((try btree.search(15)) != null);
    try std.testing.expect((try btree.search(25)) != null);
    try std.testing.expect((try btree.search(99)) == null);
}

test "executor EXPLAIN seq scan" {
    const test_file = "test_exec_explain_seq.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);

    const result = try exec.execute("EXPLAIN SELECT * FROM users WHERE id = 1");
    defer exec.freeResult(result);

    try std.testing.expectEqualStrings("QUERY PLAN", result.rows.columns[0]);
    try std.testing.expect(result.rows.rows.len == 1);
    // No indexes → should show Filter + Seq Scan
    try std.testing.expect(std.mem.indexOf(u8, result.rows.rows[0].values[0], "Seq Scan") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.rows.rows[0].values[0], "Filter") != null);
}

test "executor EXPLAIN index scan" {
    const test_file = "test_exec_explain_idx.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);

    // Insert enough rows for index scan to be cheaper
    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const ins = try exec.execute("INSERT INTO users VALUES (1, 'test')");
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_users_id ON users (id)");
    exec.freeResult(ci);

    const result = try exec.execute("EXPLAIN SELECT * FROM users WHERE id = 50");
    defer exec.freeResult(result);

    try std.testing.expectEqualStrings("QUERY PLAN", result.rows.columns[0]);
    // With index + 100 rows → should show Index Scan
    try std.testing.expect(std.mem.indexOf(u8, result.rows.rows[0].values[0], "Index Scan") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.rows.rows[0].values[0], "idx_users_id") != null);
}

test "executor EXPLAIN with filter" {
    const test_file = "test_exec_explain_flt.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    const result = try exec.execute("EXPLAIN SELECT * FROM t WHERE id = 1");
    defer exec.freeResult(result);

    // Should show Filter node wrapping scan
    try std.testing.expect(std.mem.indexOf(u8, result.rows.rows[0].values[0], "Filter") != null);
}

test "executor index scan point lookup" {
    const test_file = "test_exec_iscan_point.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct);

    // Insert enough rows for index scan to kick in
    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO users VALUES ({d}, 'user{d}')", .{ i, i }) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_users_id ON users (id)");
    exec.freeResult(ci);

    // Point lookup: SELECT * FROM users WHERE id = 25
    const result = try exec.execute("SELECT * FROM users WHERE id = 25");
    defer exec.freeResult(result);

    try std.testing.expect(result.rows.rows.len == 1);
    try std.testing.expectEqualStrings("25", result.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("user25", result.rows.rows[0].values[1]);
}

test "executor index scan range" {
    const test_file = "test_exec_iscan_range.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, val TEXT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d}, 'v{d}')", .{ i, i }) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Range scan: id >= 95
    const result = try exec.execute("SELECT * FROM t WHERE id >= 95");
    defer exec.freeResult(result);

    // Should get rows 95, 96, 97, 98, 99
    try std.testing.expectEqual(@as(usize, 5), result.rows.rows.len);
}

test "executor index scan with residual filter" {
    const test_file = "test_exec_iscan_resid.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const name = if (@mod(i, 2) == 0) "even" else "odd";
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d}, '{s}')", .{ i, name }) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Point lookup with index on id, but residual filter on name
    const result = try exec.execute("SELECT * FROM t WHERE id = 10 AND name = 'even'");
    defer exec.freeResult(result);

    try std.testing.expect(result.rows.rows.len == 1);
    try std.testing.expectEqualStrings("10", result.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("even", result.rows.rows[0].values[1]);
}

test "executor index scan same results as seq scan" {
    const test_file = "test_exec_iscan_same.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 20) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    // Query without index (seq scan) — small table, planner will use seq scan
    const seq_result = try exec.execute("SELECT * FROM t WHERE id = 10");
    defer exec.freeResult(seq_result);

    // Now create index — with 20 rows, planner picks index scan (cost 5.5 < 20)
    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    const idx_result = try exec.execute("SELECT * FROM t WHERE id = 10");
    defer exec.freeResult(idx_result);

    // Both should return exactly one row with id=10
    try std.testing.expectEqual(seq_result.rows.rows.len, idx_result.rows.rows.len);
    try std.testing.expectEqualStrings(seq_result.rows.rows[0].values[0], idx_result.rows.rows[0].values[0]);
}

test "executor index scan with ORDER BY and LIMIT" {
    const test_file = "test_exec_iscan_order.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, val TEXT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d}, 'v{d}')", .{ i, i }) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Range scan with ORDER BY DESC and LIMIT
    const result = try exec.execute("SELECT * FROM t WHERE id >= 90 ORDER BY id DESC LIMIT 3");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 3), result.rows.rows.len);
    try std.testing.expectEqualStrings("99", result.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("98", result.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("97", result.rows.rows[2].values[0]);
}

test "executor index scan on empty table returns no rows" {
    const test_file = "test_exec_iscan_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);
    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // No inserts — query should return 0 rows
    const result = try exec.execute("SELECT * FROM t WHERE id = 42");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 0), result.rows.rows.len);
}

test "executor index scan point lookup no match" {
    const test_file = "test_exec_iscan_nomatch.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // 999 not in 0..49
    const result = try exec.execute("SELECT * FROM t WHERE id = 999");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 0), result.rows.rows.len);
}

test "executor index scan with zero and boundary keys" {
    const test_file = "test_exec_iscan_zero.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    // Insert values including 0 and boundary, plus filler for index scan
    const vals = [_]i32{ 0, 1, 2, 5, 10, 50, 99 };
    for (vals) |v| {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{v}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }
    // Add filler rows to make index scan viable
    var i: i32 = 100;
    while (i < 120) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Lookup 0 (boundary)
    const r1 = try exec.execute("SELECT * FROM t WHERE id = 0");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 1), r1.rows.rows.len);
    try std.testing.expectEqualStrings("0", r1.rows.rows[0].values[0]);

    // Lookup 1
    const r2 = try exec.execute("SELECT * FROM t WHERE id = 1");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);
    try std.testing.expectEqualStrings("1", r2.rows.rows[0].values[0]);
}

test "executor drop index falls back to seq scan" {
    const test_file = "test_exec_drop_fallback.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Query with index
    const r1 = try exec.execute("SELECT * FROM t WHERE id = 25");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 1), r1.rows.rows.len);

    // Drop index
    const di = try exec.execute("DROP INDEX idx_t_id");
    exec.freeResult(di);

    // Same query still works (seq scan fallback)
    const r2 = try exec.execute("SELECT * FROM t WHERE id = 25");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);

    // EXPLAIN shows Seq Scan
    const expl = try exec.execute("EXPLAIN SELECT * FROM t WHERE id = 25");
    defer exec.freeResult(expl);
    try std.testing.expect(std.mem.indexOf(u8, expl.rows.rows[0].values[0], "Seq Scan") != null);
}

test "executor uses correct index when multiple exist" {
    const test_file = "test_exec_multi_idx.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, val INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d}, {d})", .{ i, i * 10 }) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci1 = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci1);
    const ci2 = try exec.execute("CREATE INDEX idx_t_val ON t (val)");
    exec.freeResult(ci2);

    // Query on val column — should use idx_t_val
    const result = try exec.execute("SELECT * FROM t WHERE val = 100");
    defer exec.freeResult(result);
    try std.testing.expectEqual(@as(usize, 1), result.rows.rows.len);
    try std.testing.expectEqualStrings("10", result.rows.rows[0].values[0]); // id=10, val=100

    // EXPLAIN shows idx_t_val
    const expl = try exec.execute("EXPLAIN SELECT * FROM t WHERE val = 100");
    defer exec.freeResult(expl);
    try std.testing.expect(std.mem.indexOf(u8, expl.rows.rows[0].values[0], "idx_t_val") != null);
}

test "executor index scan BETWEEN range" {
    const test_file = "test_exec_iscan_between.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // BETWEEN 45 AND 55 → 11 rows (45,46,...,55)
    const result = try exec.execute("SELECT * FROM t WHERE id BETWEEN 45 AND 55");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 11), result.rows.rows.len);
}

test "executor BETWEEN with inverted bounds returns empty" {
    const test_file = "test_exec_iscan_between_inv.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // BETWEEN 100 AND 50 — inverted bounds → 0 rows
    const result = try exec.execute("SELECT * FROM t WHERE id BETWEEN 100 AND 50");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 0), result.rows.rows.len);
}

test "executor reverse comparison 25 = id uses index" {
    const test_file = "test_exec_iscan_reverse.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Reversed comparison: 25 = id
    const result = try exec.execute("SELECT * FROM t WHERE 25 = id");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 1), result.rows.rows.len);
    try std.testing.expectEqualStrings("25", result.rows.rows[0].values[0]);
}

test "executor index scan with DISTINCT" {
    const test_file = "test_exec_iscan_distinct.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, category TEXT)");
    exec.freeResult(ct);

    // 60 rows: id=0..59, category alternates "a"/"b"
    var i: i32 = 0;
    while (i < 60) : (i += 1) {
        const cat = if (@mod(i, 2) == 0) "a" else "b";
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d}, '{s}')", .{ i, cat }) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // SELECT DISTINCT category WHERE id >= 50 → 10 rows (50..59), alternating a/b → 2 distinct
    const result = try exec.execute("SELECT DISTINCT category FROM t WHERE id >= 50");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 2), result.rows.rows.len);
}

test "executor insert duplicate key into non-unique index" {
    const test_file = "test_exec_dup_nonunique.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    // Non-unique index on id
    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Insert two rows with same key
    const ins1 = try exec.execute("INSERT INTO t VALUES (1, 'first')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES (1, 'second')");
    exec.freeResult(ins2);

    // Both inserts succeeded — index point lookup returns at least 1 row
    const result = try exec.execute("SELECT * FROM t WHERE id = 1");
    defer exec.freeResult(result);
    try std.testing.expect(result.rows.rows.len >= 1);
}

test "executor duplicate key index vs seq scan divergence" {
    const test_file = "test_exec_dup_diverge.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    // 30 rows: 3 copies of ids 0..9
    var copy: u32 = 0;
    while (copy < 3) : (copy += 1) {
        var i: i32 = 0;
        while (i < 10) : (i += 1) {
            const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
            defer std.testing.allocator.free(buf);
            const ins = try exec.execute(buf);
            exec.freeResult(ins);
        }
    }

    // Without index: seq scan should find all 3 copies
    const seq_result = try exec.execute("SELECT * FROM t WHERE id = 5");
    defer exec.freeResult(seq_result);
    try std.testing.expectEqual(@as(usize, 3), seq_result.rows.rows.len);

    // Create index (backfill silently drops dupes — btree replaces on same key)
    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // With index: btree point lookup returns 1 TID (last inserted for key=5)
    const idx_result = try exec.execute("SELECT * FROM t WHERE id = 5");
    defer exec.freeResult(idx_result);
    try std.testing.expectEqual(@as(usize, 1), idx_result.rows.rows.len);
}

test "executor index scan after deleting all rows" {
    const test_file = "test_exec_iscan_del_all.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Delete all rows
    const del = try exec.execute("DELETE FROM t");
    exec.freeResult(del);

    // Index scan should return 0 rows
    const result = try exec.execute("SELECT * FROM t WHERE id = 25");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 0), result.rows.rows.len);
}

test "executor index on non-integer column ignored by planner" {
    const test_file = "test_exec_idx_text.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d}, 'test')", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    // Create index on TEXT column name
    const ci = try exec.execute("CREATE INDEX idx_t_name ON t (name)");
    exec.freeResult(ci);

    // EXPLAIN should show Seq Scan (planner can't use TEXT index)
    const expl = try exec.execute("EXPLAIN SELECT * FROM t WHERE name = 'test'");
    defer exec.freeResult(expl);
    try std.testing.expect(std.mem.indexOf(u8, expl.rows.rows[0].values[0], "Seq Scan") != null);
}

test "executor index scan large range" {
    const test_file = "test_exec_iscan_large.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // id >= 1 → 99 rows (1..99)
    const result = try exec.execute("SELECT * FROM t WHERE id >= 1");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 99), result.rows.rows.len);
}

test "executor update indexed column preserves consistency" {
    const test_file = "test_exec_iscan_upd.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // Update id=25 to id=999
    const upd = try exec.execute("UPDATE t SET id = 999 WHERE id = 25");
    exec.freeResult(upd);

    // Old key gone
    const r1 = try exec.execute("SELECT * FROM t WHERE id = 25");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 0), r1.rows.rows.len);

    // New key present
    const r2 = try exec.execute("SELECT * FROM t WHERE id = 999");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);
}

test "executor EXPLAIN BETWEEN shows range scan" {
    const test_file = "test_exec_expl_between.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 200) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    const result = try exec.execute("EXPLAIN SELECT * FROM t WHERE id BETWEEN 50 AND 100");
    defer exec.freeResult(result);

    try std.testing.expect(std.mem.indexOf(u8, result.rows.rows[0].values[0], "Index Scan") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.rows.rows[0].values[0], "idx_t_id") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.rows.rows[0].values[0], "range key=50..100") != null);
}

test "executor EXPLAIN after DROP INDEX shows seq scan" {
    const test_file = "test_exec_expl_drop.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    const ci = try exec.execute("CREATE INDEX idx_t_id ON t (id)");
    exec.freeResult(ci);

    // EXPLAIN with index → shows Index Scan
    const expl1 = try exec.execute("EXPLAIN SELECT * FROM t WHERE id = 50");
    defer exec.freeResult(expl1);
    try std.testing.expect(std.mem.indexOf(u8, expl1.rows.rows[0].values[0], "Index Scan") != null);

    // Drop index
    const di = try exec.execute("DROP INDEX idx_t_id");
    exec.freeResult(di);

    // EXPLAIN without index → shows Seq Scan
    const expl2 = try exec.execute("EXPLAIN SELECT * FROM t WHERE id = 50");
    defer exec.freeResult(expl2);
    try std.testing.expect(std.mem.indexOf(u8, expl2.rows.rows[0].values[0], "Seq Scan") != null);
}

test "executor LIMIT 0 returns no rows" {
    const test_file = "test_exec_limit0.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1)");
    exec.freeResult(ins);

    const result = try exec.execute("SELECT * FROM t LIMIT 0");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 0), result.rows.rows.len);
}

test "executor SUM AVG MIN MAX on empty result" {
    const test_file = "test_exec_agg_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, price INT)");
    exec.freeResult(ct);

    // Table is empty → aggregates should return 1 row each

    // COUNT(*) on empty → 0
    const r1 = try exec.execute("SELECT COUNT(*) FROM t");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 1), r1.rows.rows.len);
    try std.testing.expectEqualStrings("0", r1.rows.rows[0].values[0]);

    // SUM on empty → 0.000000 (sum starts at 0)
    const r2 = try exec.execute("SELECT SUM(price) FROM t");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);
    try std.testing.expectEqualStrings("0.000000", r2.rows.rows[0].values[0]);

    // AVG on empty → NULL (non_null_count == 0)
    const r3 = try exec.execute("SELECT AVG(price) FROM t");
    defer exec.freeResult(r3);
    try std.testing.expectEqual(@as(usize, 1), r3.rows.rows.len);
    try std.testing.expectEqualStrings("NULL", r3.rows.rows[0].values[0]);

    // MIN on empty → NULL
    const r4 = try exec.execute("SELECT MIN(price) FROM t");
    defer exec.freeResult(r4);
    try std.testing.expectEqual(@as(usize, 1), r4.rows.rows.len);
    try std.testing.expectEqualStrings("NULL", r4.rows.rows[0].values[0]);

    // MAX on empty → NULL
    const r5 = try exec.execute("SELECT MAX(price) FROM t");
    defer exec.freeResult(r5);
    try std.testing.expectEqual(@as(usize, 1), r5.rows.rows.len);
    try std.testing.expectEqualStrings("NULL", r5.rows.rows[0].values[0]);
}

test "executor SUM AVG MIN MAX with WHERE matching nothing" {
    const test_file = "test_exec_agg_nomatch.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    var i: i32 = 1;
    while (i <= 10) : (i += 1) {
        const buf = std.fmt.allocPrint(std.testing.allocator, "INSERT INTO t VALUES ({d})", .{i}) catch unreachable;
        defer std.testing.allocator.free(buf);
        const ins = try exec.execute(buf);
        exec.freeResult(ins);
    }

    // WHERE matches nothing
    const r1 = try exec.execute("SELECT COUNT(*) FROM t WHERE id > 100");
    defer exec.freeResult(r1);
    try std.testing.expectEqualStrings("0", r1.rows.rows[0].values[0]);

    const r2 = try exec.execute("SELECT AVG(id) FROM t WHERE id > 100");
    defer exec.freeResult(r2);
    try std.testing.expectEqualStrings("NULL", r2.rows.rows[0].values[0]);
}

test "executor DROP system table fails" {
    const test_file = "test_exec_drop_sys.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    try std.testing.expectError(ExecError.StorageError, exec.execute("DROP TABLE gp_tables"));
    try std.testing.expectError(ExecError.StorageError, exec.execute("DROP TABLE gp_columns"));
    try std.testing.expectError(ExecError.StorageError, exec.execute("DROP TABLE gp_indexes"));
}

test "executor CREATE INDEX nonexistent table fails" {
    const test_file = "test_exec_cidx_no_tbl.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    try std.testing.expectError(ExecError.TableNotFound, exec.execute("CREATE INDEX idx ON nonexistent (id)"));
}

test "executor CREATE INDEX nonexistent column fails" {
    const test_file = "test_exec_cidx_no_col.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    try std.testing.expectError(ExecError.StorageError, exec.execute("CREATE INDEX idx ON t (nonexistent)"));
}

test "executor LIKE edge cases" {
    const test_file = "test_exec_like_edge.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    const ins1 = try exec.execute("INSERT INTO t VALUES (1, 'alice')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES (2, 'bob')");
    exec.freeResult(ins2);
    const ins3 = try exec.execute("INSERT INTO t VALUES (3, '')");
    exec.freeResult(ins3);
    const ins4 = try exec.execute("INSERT INTO t VALUES (4, 'a')");
    exec.freeResult(ins4);

    // '%' matches everything (including empty string)
    const r1 = try exec.execute("SELECT * FROM t WHERE name LIKE '%'");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 4), r1.rows.rows.len);

    // '_' matches exactly 1 character
    const r2 = try exec.execute("SELECT * FROM t WHERE name LIKE '_'");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);
    try std.testing.expectEqualStrings("a", r2.rows.rows[0].values[1]);

    // Ends-with pattern
    const r3 = try exec.execute("SELECT * FROM t WHERE name LIKE '%ce'");
    defer exec.freeResult(r3);
    try std.testing.expectEqual(@as(usize, 1), r3.rows.rows.len);
    try std.testing.expectEqualStrings("alice", r3.rows.rows[0].values[1]);

    // Starts-with pattern
    const r4 = try exec.execute("SELECT * FROM t WHERE name LIKE 'b%'");
    defer exec.freeResult(r4);
    try std.testing.expectEqual(@as(usize, 1), r4.rows.rows.len);
    try std.testing.expectEqualStrings("bob", r4.rows.rows[0].values[1]);
}

test "executor SELECT with no matching WHERE returns empty" {
    const test_file = "test_exec_no_match.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1)");
    exec.freeResult(ins);

    const result = try exec.execute("SELECT * FROM t WHERE id = 999");
    defer exec.freeResult(result);

    try std.testing.expectEqual(@as(usize, 0), result.rows.rows.len);
    // Columns should still be present
    try std.testing.expectEqual(@as(usize, 1), result.rows.columns.len);
}

test "executor LEFT JOIN with unmatched rows" {
    const test_file = "test_exec_lj_unmatched.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE users (id INT, name TEXT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE orders (id INT, user_id INT)");
    exec.freeResult(ct2);

    const ins1 = try exec.execute("INSERT INTO users VALUES (1, 'alice')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO users VALUES (2, 'bob')");
    exec.freeResult(ins2);
    // Only alice has an order
    const ins3 = try exec.execute("INSERT INTO orders VALUES (100, 1)");
    exec.freeResult(ins3);

    const result = try exec.execute("SELECT users.name, orders.id FROM users LEFT JOIN orders ON users.id = orders.user_id");
    defer exec.freeResult(result);

    // Should have 2 rows: alice with order, bob with NULL
    try std.testing.expectEqual(@as(usize, 2), result.rows.rows.len);
}

test "executor DELETE with WHERE matching nothing" {
    const test_file = "test_exec_del_nomatch.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1)");
    exec.freeResult(ins);

    // Delete with non-matching WHERE
    const del = try exec.execute("DELETE FROM t WHERE id = 999");
    defer exec.freeResult(del);
    try std.testing.expectEqual(@as(u64, 0), del.row_count);

    // Row should still exist
    const sel = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
}

test "executor UPDATE SET multiple columns" {
    const test_file = "test_exec_upd_multi.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT, val INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1, 'old', 10)");
    exec.freeResult(ins);

    const upd = try exec.execute("UPDATE t SET name = 'new', val = 20 WHERE id = 1");
    exec.freeResult(upd);

    const sel = try exec.execute("SELECT * FROM t WHERE id = 1");
    defer exec.freeResult(sel);
    try std.testing.expectEqual(@as(usize, 1), sel.rows.rows.len);
    try std.testing.expectEqualStrings("new", sel.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("20", sel.rows.rows[0].values[2]);
}

test "executor INSERT INTO nonexistent table" {
    const test_file = "test_exec_ins_noexist.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    try std.testing.expectError(ExecError.TableNotFound, exec.execute("INSERT INTO ghost VALUES (1)"));
}

test "executor SELECT FROM nonexistent table" {
    const test_file = "test_exec_sel_noexist.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();

    var exec = Executor.init(std.testing.allocator, &catalog);

    try std.testing.expectError(ExecError.TableNotFound, exec.execute("SELECT * FROM ghost"));
}

test "executor LOWER function" {
    const test_file = "test_exec_lower.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('Hello World')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT LOWER(name) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("hello world", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("lower", r.rows.columns[0]);
}

test "executor UPPER function" {
    const test_file = "test_exec_upper.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('Hello')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT UPPER(name) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("HELLO", r.rows.rows[0].values[0]);
}

test "executor LENGTH function" {
    const test_file = "test_exec_length.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('hello')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT LENGTH(name) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("5", r.rows.rows[0].values[0]);
}

test "executor TRIM function" {
    const test_file = "test_exec_trim.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('  hello  ')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT TRIM(name) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("hello", r.rows.rows[0].values[0]);
}

test "executor SUBSTRING function" {
    const test_file = "test_exec_substr.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('hello world')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT SUBSTRING(name, 1, 5) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("hello", r.rows.rows[0].values[0]);
}

test "executor CONCAT function" {
    const test_file = "test_exec_concat.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (first TEXT, last TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('hello', ' world')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT CONCAT(first, last) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("hello world", r.rows.rows[0].values[0]);
}

test "executor CASE expression" {
    const test_file = "test_exec_case.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins1 = try exec.execute("INSERT INTO t VALUES (10)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES (3)");
    exec.freeResult(ins2);

    const r = try exec.execute("SELECT CASE WHEN x > 5 THEN 'big' ELSE 'small' END FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    try std.testing.expectEqualStrings("big", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("small", r.rows.rows[1].values[0]);
}

test "executor nested functions" {
    const test_file = "test_exec_nested.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('Hello World')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT UPPER(SUBSTRING(name, 1, 5)) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("HELLO", r.rows.rows[0].values[0]);
}

test "executor function in WHERE" {
    const test_file = "test_exec_fn_where.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins1 = try exec.execute("INSERT INTO t VALUES ('Alice')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES ('Bob')");
    exec.freeResult(ins2);

    const r = try exec.execute("SELECT name FROM t WHERE UPPER(name) = 'ALICE'");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("Alice", r.rows.rows[0].values[0]);
}

test "executor function with alias" {
    const test_file = "test_exec_fn_alias.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('hello')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT UPPER(name) AS uname FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("uname", r.rows.columns[0]);
    try std.testing.expectEqualStrings("HELLO", r.rows.rows[0].values[0]);
}

test "executor CASE no else returns NULL" {
    const test_file = "test_exec_case_null.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT CASE WHEN x > 100 THEN 'big' END FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("NULL", r.rows.rows[0].values[0]);
}

test "executor arithmetic in SELECT" {
    const test_file = "test_exec_arith_select.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE products (price INT, quantity INT)");
    exec.freeResult(ct);
    const ins1 = try exec.execute("INSERT INTO products VALUES (10, 3)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO products VALUES (25, 2)");
    exec.freeResult(ins2);

    const r = try exec.execute("SELECT price * quantity FROM products");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    try std.testing.expectEqualStrings("30", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("50", r.rows.rows[1].values[0]);
}

test "executor arithmetic in WHERE" {
    const test_file = "test_exec_arith_where.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE items (a INT, b INT)");
    exec.freeResult(ct);
    const ins1 = try exec.execute("INSERT INTO items VALUES (3, 7)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO items VALUES (10, 1)");
    exec.freeResult(ins2);

    const r = try exec.execute("SELECT * FROM items WHERE a + b > 5");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
}

test "executor arithmetic with alias" {
    const test_file = "test_exec_arith_alias.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (5)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT x * 2 AS doubled FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("doubled", r.rows.columns[0]);
    try std.testing.expectEqualStrings("10", r.rows.rows[0].values[0]);
}

test "executor arithmetic division and subtraction" {
    const test_file = "test_exec_arith_div.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (20)");
    exec.freeResult(ins);

    const r1 = try exec.execute("SELECT x / 4 FROM t");
    defer exec.freeResult(r1);
    try std.testing.expectEqualStrings("5", r1.rows.rows[0].values[0]);

    const r2 = try exec.execute("SELECT x - 8 FROM t");
    defer exec.freeResult(r2);
    try std.testing.expectEqualStrings("12", r2.rows.rows[0].values[0]);
}

test "executor arithmetic float promotion" {
    const test_file = "test_exec_arith_float.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x FLOAT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (3.5)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT x * 2 FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("7.000000", r.rows.rows[0].values[0]);
}

test "executor multi-row INSERT" {
    const test_file = "test_exec_multi_insert.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);

    const ins = try exec.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");
    try std.testing.expectEqual(@as(usize, 3), ins.row_count);
    exec.freeResult(ins);

    const r = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 3), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("alice", r.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("2", r.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("bob", r.rows.rows[1].values[1]);
    try std.testing.expectEqualStrings("3", r.rows.rows[2].values[0]);
    try std.testing.expectEqualStrings("charlie", r.rows.rows[2].values[1]);
}

test "executor IS NULL and IS NOT NULL" {
    const test_file = "test_exec_is_null.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const ins1 = try exec.execute("INSERT INTO t VALUES (1, 'alice')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES (2, NULL)");
    exec.freeResult(ins2);

    const r1 = try exec.execute("SELECT * FROM t WHERE name IS NULL");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 1), r1.rows.rows.len);
    try std.testing.expectEqualStrings("2", r1.rows.rows[0].values[0]);

    const r2 = try exec.execute("SELECT * FROM t WHERE name IS NOT NULL");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);
    try std.testing.expectEqualStrings("1", r2.rows.rows[0].values[0]);
}

test "executor COALESCE" {
    const test_file = "test_exec_coalesce.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (a INT, b INT)");
    exec.freeResult(ct);
    const ins1 = try exec.execute("INSERT INTO t VALUES (NULL, 10)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES (5, 10)");
    exec.freeResult(ins2);

    const r = try exec.execute("SELECT COALESCE(a, b) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    try std.testing.expectEqualStrings("10", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("5", r.rows.rows[1].values[0]);
}

test "executor NULLIF" {
    const test_file = "test_exec_nullif.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins1 = try exec.execute("INSERT INTO t VALUES (0)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t VALUES (5)");
    exec.freeResult(ins2);

    const r = try exec.execute("SELECT NULLIF(x, 0) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("NULL", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("5", r.rows.rows[1].values[0]);
}

test "executor ABS and MOD" {
    const test_file = "test_exec_abs_mod.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (-7), (10)");
    exec.freeResult(ins);

    const r1 = try exec.execute("SELECT ABS(x) FROM t");
    defer exec.freeResult(r1);
    try std.testing.expectEqualStrings("7", r1.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("10", r1.rows.rows[1].values[0]);

    const r2 = try exec.execute("SELECT MOD(x, 3) FROM t WHERE x > 0");
    defer exec.freeResult(r2);
    try std.testing.expectEqualStrings("1", r2.rows.rows[0].values[0]);
}

test "executor ROUND CEIL FLOOR" {
    const test_file = "test_exec_round.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x FLOAT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (3.7)");
    exec.freeResult(ins);

    const r1 = try exec.execute("SELECT ROUND(x) FROM t");
    defer exec.freeResult(r1);
    try std.testing.expectEqualStrings("4.000000", r1.rows.rows[0].values[0]);

    const r2 = try exec.execute("SELECT CEIL(x) FROM t");
    defer exec.freeResult(r2);
    try std.testing.expectEqualStrings("4.000000", r2.rows.rows[0].values[0]);

    const r3 = try exec.execute("SELECT FLOOR(x) FROM t");
    defer exec.freeResult(r3);
    try std.testing.expectEqualStrings("3.000000", r3.rows.rows[0].values[0]);
}

test "executor CAST int to float" {
    const test_file = "test_exec_cast_itof.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (42)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT CAST(x AS FLOAT) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("42.000000", r.rows.rows[0].values[0]);
}

test "executor CAST float to int" {
    const test_file = "test_exec_cast_ftoi.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x FLOAT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (3.7)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT CAST(x AS INT) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("3", r.rows.rows[0].values[0]);
}

test "executor CAST int to text" {
    const test_file = "test_exec_cast_itot.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (99)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT CAST(x AS TEXT) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("99", r.rows.rows[0].values[0]);
}

test "executor CAST text to int" {
    const test_file = "test_exec_cast_ttoi.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (s TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('123')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT CAST(s AS INT) FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("123", r.rows.rows[0].values[0]);
}

test "executor NOT LIKE" {
    const test_file = "test_exec_not_like.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('alice'), ('bob'), ('alex')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT * FROM t WHERE name NOT LIKE 'al%'");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("bob", r.rows.rows[0].values[0]);
}

test "executor NOT IN" {
    const test_file = "test_exec_not_in.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT * FROM t WHERE x NOT IN (2, 4)");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 3), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("3", r.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("5", r.rows.rows[2].values[0]);
}

test "executor NOT BETWEEN" {
    const test_file = "test_exec_not_between.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1), (5), (10), (15)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT * FROM t WHERE x NOT BETWEEN 3 AND 12");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("15", r.rows.rows[1].values[0]);
}

test "executor || concat operator" {
    const test_file = "test_exec_concat_op.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (first TEXT, last TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('hello', ' world'), ('foo', 'bar')");
    exec.freeResult(ins);

    // || in SELECT
    const r1 = try exec.execute("SELECT first || last FROM t");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 2), r1.rows.rows.len);
    try std.testing.expectEqualStrings("hello world", r1.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("foobar", r1.rows.rows[1].values[0]);

    // || in WHERE
    const r2 = try exec.execute("SELECT * FROM t WHERE first || last = 'foobar'");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);
    try std.testing.expectEqualStrings("foo", r2.rows.rows[0].values[0]);

    // chained ||
    const r3 = try exec.execute("SELECT first || ' ' || last FROM t");
    defer exec.freeResult(r3);
    try std.testing.expectEqualStrings("hello  world", r3.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("foo bar", r3.rows.rows[1].values[0]);
}

test "executor UNION removes duplicates" {
    const test_file = "test_exec_union.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE t1 (x INT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE t2 (y INT)");
    exec.freeResult(ct2);
    const ins1 = try exec.execute("INSERT INTO t1 VALUES (1), (2), (3)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t2 VALUES (2), (3), (4)");
    exec.freeResult(ins2);

    // UNION should deduplicate: {1, 2, 3, 4}
    const r = try exec.execute("SELECT x FROM t1 UNION SELECT y FROM t2");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 4), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("2", r.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("3", r.rows.rows[2].values[0]);
    try std.testing.expectEqualStrings("4", r.rows.rows[3].values[0]);
}

test "executor UNION ALL keeps duplicates" {
    const test_file = "test_exec_union_all.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE t1 (x INT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE t2 (y INT)");
    exec.freeResult(ct2);
    const ins1 = try exec.execute("INSERT INTO t1 VALUES (1), (2), (3)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t2 VALUES (2), (3), (4)");
    exec.freeResult(ins2);

    // UNION ALL keeps all rows: {1, 2, 3, 2, 3, 4}
    const r = try exec.execute("SELECT x FROM t1 UNION ALL SELECT y FROM t2");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 6), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("2", r.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("3", r.rows.rows[2].values[0]);
    try std.testing.expectEqualStrings("2", r.rows.rows[3].values[0]);
    try std.testing.expectEqualStrings("3", r.rows.rows[4].values[0]);
    try std.testing.expectEqualStrings("4", r.rows.rows[5].values[0]);
}

test "executor INSERT INTO SELECT" {
    const test_file = "test_exec_insert_select.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE src (x INT, name TEXT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE dst (x INT, name TEXT)");
    exec.freeResult(ct2);
    const ins = try exec.execute("INSERT INTO src VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");
    exec.freeResult(ins);

    // INSERT INTO dst SELECT * FROM src WHERE x > 1
    const r1 = try exec.execute("INSERT INTO dst SELECT * FROM src WHERE x > 1");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(u64, 2), r1.row_count);

    // Verify dst has the right rows
    const r2 = try exec.execute("SELECT * FROM dst");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 2), r2.rows.rows.len);
    try std.testing.expectEqualStrings("2", r2.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("bob", r2.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("3", r2.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("charlie", r2.rows.rows[1].values[1]);
}

test "executor LIMIT OFFSET" {
    const test_file = "test_exec_offset.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)");
    exec.freeResult(ins);

    // LIMIT 2 OFFSET 2 -> rows 3, 4
    const r = try exec.execute("SELECT * FROM t LIMIT 2 OFFSET 2");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    try std.testing.expectEqualStrings("3", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("4", r.rows.rows[1].values[0]);

    // LIMIT 10 OFFSET 3 -> rows 4, 5
    const r2 = try exec.execute("SELECT * FROM t LIMIT 10 OFFSET 3");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 2), r2.rows.rows.len);
    try std.testing.expectEqualStrings("4", r2.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("5", r2.rows.rows[1].values[0]);
}

test "executor INSERT with column list" {
    const test_file = "test_exec_insert_cols.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (a INT, b TEXT, c INT)");
    exec.freeResult(ct);

    // Insert with only some columns — missing ones get NULL
    const ins = try exec.execute("INSERT INTO t (b, a) VALUES ('hello', 42)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("42", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("hello", r.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("NULL", r.rows.rows[0].values[2]);
}

test "executor UPDATE with expression" {
    const test_file = "test_exec_update_expr.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT, name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (10, 'alice'), (20, 'bob')");
    exec.freeResult(ins);

    // UPDATE t SET x = x + 5 WHERE name = 'alice'
    const upd1 = try exec.execute("UPDATE t SET x = x + 5 WHERE name = 'alice'");
    defer exec.freeResult(upd1);
    try std.testing.expectEqual(@as(u64, 1), upd1.row_count);

    const r = try exec.execute("SELECT x FROM t WHERE name = 'alice'");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("15", r.rows.rows[0].values[0]);

    // Verify bob unchanged
    const rb = try exec.execute("SELECT x FROM t WHERE name = 'bob'");
    defer exec.freeResult(rb);
    try std.testing.expectEqualStrings("20", rb.rows.rows[0].values[0]);

    // UPDATE with string concat expression
    const upd2 = try exec.execute("UPDATE t SET name = name || '_updated'");
    defer exec.freeResult(upd2);

    const r2 = try exec.execute("SELECT name FROM t WHERE x = 15");
    defer exec.freeResult(r2);
    try std.testing.expectEqualStrings("alice_updated", r2.rows.rows[0].values[0]);
}

test "executor RIGHT JOIN" {
    const test_file = "test_exec_right_join.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE t1 (id INT, val TEXT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE t2 (id INT, name TEXT)");
    exec.freeResult(ct2);
    const ins1 = try exec.execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t2 VALUES (2, 'x'), (3, 'y')");
    exec.freeResult(ins2);

    // RIGHT JOIN: all right rows, NULLs for unmatched left
    const r = try exec.execute("SELECT t1.val, t2.name FROM t1 RIGHT JOIN t2 ON t1.id = t2.id");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    // id=2 matches: val='b', name='x'
    try std.testing.expectEqualStrings("b", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("x", r.rows.rows[0].values[1]);
    // id=3 no match: val=NULL, name='y'
    try std.testing.expectEqualStrings("NULL", r.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("y", r.rows.rows[1].values[1]);
}

test "executor CROSS JOIN" {
    const test_file = "test_exec_cross_join.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE t1 (a INT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE t2 (b INT)");
    exec.freeResult(ct2);
    const ins1 = try exec.execute("INSERT INTO t1 VALUES (1), (2)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t2 VALUES (10), (20)");
    exec.freeResult(ins2);

    // CROSS JOIN: 2x2 = 4 rows
    const r = try exec.execute("SELECT * FROM t1 CROSS JOIN t2");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 4), r.rows.rows.len);
}

test "executor DEFAULT column values" {
    const test_file = "test_exec_default.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (x INT, status TEXT DEFAULT 'active', score INT DEFAULT 0)");
    exec.freeResult(ct);

    // Insert with only x — others get defaults
    const ins = try exec.execute("INSERT INTO t (x) VALUES (1)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("active", r.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("0", r.rows.rows[0].values[2]);
}

test "executor NOT NULL constraint on INSERT" {
    const test_file = "test_exec_notnull.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)");
    exec.freeResult(ct);

    // Valid insert should succeed
    const ins1 = try exec.execute("INSERT INTO t VALUES (1, 'alice')");
    exec.freeResult(ins1);

    // NULL in NOT NULL column should fail
    const result = exec.execute("INSERT INTO t VALUES (NULL, 'bob')");
    try std.testing.expectError(ExecError.NotNullViolation, result);

    // Insert with column list omitting NOT NULL column should fail (gets NULL default)
    const result2 = exec.execute("INSERT INTO t (name) VALUES ('carol')");
    try std.testing.expectError(ExecError.NotNullViolation, result2);

    // Verify only the valid row exists
    const r = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
}

test "executor NOT NULL constraint on UPDATE" {
    const test_file = "test_exec_notnull_upd.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)");
    exec.freeResult(ct);

    const ins = try exec.execute("INSERT INTO t VALUES (1, 'alice')");
    exec.freeResult(ins);

    // Setting NOT NULL column to NULL should fail
    const result = exec.execute("UPDATE t SET id = NULL WHERE name = 'alice'");
    try std.testing.expectError(ExecError.NotNullViolation, result);

    // Valid update should work
    const upd = try exec.execute("UPDATE t SET id = 2 WHERE name = 'alice'");
    exec.freeResult(upd);

    const r = try exec.execute("SELECT id FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("2", r.rows.rows[0].values[0]);
}

test "executor NOT NULL with DEFAULT" {
    const test_file = "test_exec_notnull_def.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT NOT NULL, status TEXT NOT NULL DEFAULT 'active')");
    exec.freeResult(ct);

    // Omitting status should use default and pass NOT NULL check
    const ins = try exec.execute("INSERT INTO t (id) VALUES (1)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("active", r.rows.rows[0].values[1]);
}

test "executor COUNT(DISTINCT col)" {
    const test_file = "test_exec_count_distinct.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (category TEXT, val INT)");
    exec.freeResult(ct);

    const ins = try exec.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 1), ('b', 1), ('a', 1)");
    exec.freeResult(ins);

    // COUNT(DISTINCT category) should be 2
    const r1 = try exec.execute("SELECT COUNT(DISTINCT category) FROM t");
    defer exec.freeResult(r1);
    try std.testing.expectEqualStrings("2", r1.rows.rows[0].values[0]);

    // COUNT(DISTINCT val) should be 2
    const r2 = try exec.execute("SELECT COUNT(DISTINCT val) FROM t");
    defer exec.freeResult(r2);
    try std.testing.expectEqualStrings("2", r2.rows.rows[0].values[0]);

    // Regular COUNT(val) should be 5
    const r3 = try exec.execute("SELECT COUNT(val) FROM t");
    defer exec.freeResult(r3);
    try std.testing.expectEqualStrings("5", r3.rows.rows[0].values[0]);

    // SUM(DISTINCT val) should be 3 (1 + 2)
    const r4 = try exec.execute("SELECT SUM(DISTINCT val) FROM t");
    defer exec.freeResult(r4);
    try std.testing.expectEqualStrings("3.000000", r4.rows.rows[0].values[0]);
}

test "executor COUNT(DISTINCT) with GROUP BY" {
    const test_file = "test_exec_count_distinct_gb.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (category TEXT, val INT)");
    exec.freeResult(ct);

    const ins = try exec.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('a', 1), ('b', 3), ('b', 3)");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT category, COUNT(DISTINCT val) FROM t GROUP BY category");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    // Group 'a' has distinct vals {1, 2} = 2
    try std.testing.expectEqualStrings("a", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("2", r.rows.rows[0].values[1]);
    // Group 'b' has distinct vals {3} = 1
    try std.testing.expectEqualStrings("b", r.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("1", r.rows.rows[1].values[1]);
}

test "executor string functions REPLACE, POSITION, LEFT, RIGHT, REVERSE, LPAD, RPAD" {
    const test_file = "test_exec_strfuncs.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES ('hello world')");
    exec.freeResult(ins);

    // REPLACE
    const r1 = try exec.execute("SELECT REPLACE(name, 'world', 'zig') FROM t");
    defer exec.freeResult(r1);
    try std.testing.expectEqualStrings("hello zig", r1.rows.rows[0].values[0]);

    // POSITION
    const r2 = try exec.execute("SELECT POSITION('world', name) FROM t");
    defer exec.freeResult(r2);
    try std.testing.expectEqualStrings("7", r2.rows.rows[0].values[0]);

    // LEFT
    const r3 = try exec.execute("SELECT LEFT(name, 5) FROM t");
    defer exec.freeResult(r3);
    try std.testing.expectEqualStrings("hello", r3.rows.rows[0].values[0]);

    // RIGHT
    const r4 = try exec.execute("SELECT RIGHT(name, 5) FROM t");
    defer exec.freeResult(r4);
    try std.testing.expectEqualStrings("world", r4.rows.rows[0].values[0]);

    // REVERSE
    const r5 = try exec.execute("SELECT REVERSE(name) FROM t");
    defer exec.freeResult(r5);
    try std.testing.expectEqualStrings("dlrow olleh", r5.rows.rows[0].values[0]);

    // LPAD
    const r6 = try exec.execute("SELECT LPAD(name, 15, '*') FROM t");
    defer exec.freeResult(r6);
    try std.testing.expectEqualStrings("****hello world", r6.rows.rows[0].values[0]);

    // RPAD
    const r7 = try exec.execute("SELECT RPAD(name, 15, '-') FROM t");
    defer exec.freeResult(r7);
    try std.testing.expectEqualStrings("hello world----", r7.rows.rows[0].values[0]);
}

test "executor EXCEPT and INTERSECT" {
    const test_file = "test_exec_except_intersect.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct1 = try exec.execute("CREATE TABLE t1 (id INT)");
    exec.freeResult(ct1);
    const ct2 = try exec.execute("CREATE TABLE t2 (id INT)");
    exec.freeResult(ct2);

    const ins1 = try exec.execute("INSERT INTO t1 VALUES (1), (2), (3)");
    exec.freeResult(ins1);
    const ins2 = try exec.execute("INSERT INTO t2 VALUES (2), (3), (4)");
    exec.freeResult(ins2);

    // EXCEPT: t1 - t2 = {1}
    const r1 = try exec.execute("SELECT id FROM t1 EXCEPT SELECT id FROM t2");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 1), r1.rows.rows.len);
    try std.testing.expectEqualStrings("1", r1.rows.rows[0].values[0]);

    // INTERSECT: t1 & t2 = {2, 3}
    const r2 = try exec.execute("SELECT id FROM t1 INTERSECT SELECT id FROM t2");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 2), r2.rows.rows.len);
}

test "executor ALTER TABLE RENAME COLUMN" {
    const test_file = "test_exec_rename_col.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1, 'alice')");
    exec.freeResult(ins);

    const alt = try exec.execute("ALTER TABLE t RENAME COLUMN name TO username");
    exec.freeResult(alt);

    // Query using the new column name
    const r = try exec.execute("SELECT username FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqualStrings("username", r.rows.columns[0]);
    try std.testing.expectEqualStrings("alice", r.rows.rows[0].values[0]);
}

test "executor CHECK constraint" {
    const test_file = "test_exec_check.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);
    defer exec.deinit();

    const ct = try exec.execute("CREATE TABLE t (age INT, CHECK (age > 0))");
    exec.freeResult(ct);

    // Valid insert
    const ins1 = try exec.execute("INSERT INTO t VALUES (25)");
    exec.freeResult(ins1);

    // CHECK violation
    const result = exec.execute("INSERT INTO t VALUES (-1)");
    try std.testing.expectError(ExecError.CheckViolation, result);

    // Verify only valid row exists
    const r = try exec.execute("SELECT * FROM t");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 1), r.rows.rows.len);
    try std.testing.expectEqualStrings("25", r.rows.rows[0].values[0]);
}

test "executor derived table (FROM subquery)" {
    const test_file = "test_exec_derived.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);

    const ct = try exec.execute("CREATE TABLE t (id INT, name TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");
    exec.freeResult(ins);

    // SELECT * FROM (SELECT id, name FROM t) AS sub
    const r = try exec.execute("SELECT * FROM (SELECT id, name FROM t) AS sub");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 3), r.rows.rows.len);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("alice", r.rows.rows[0].values[1]);

    // SELECT name FROM (SELECT id, name FROM t) AS sub
    const r2 = try exec.execute("SELECT name FROM (SELECT id, name FROM t) AS sub");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.columns.len);
    try std.testing.expectEqualStrings("alice", r2.rows.rows[0].values[0]);
}

test "executor CREATE VIEW and SELECT from view" {
    const test_file = "test_exec_view.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);
    defer exec.deinit();

    const ct = try exec.execute("CREATE TABLE employees (id INT, name TEXT, dept TEXT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO employees VALUES (1, 'Alice', 'Eng'), (2, 'Bob', 'Sales'), (3, 'Carol', 'Eng')");
    exec.freeResult(ins);

    // CREATE VIEW
    const cv = try exec.execute("CREATE VIEW eng_employees AS SELECT id, name FROM employees WHERE dept = 'Eng'");
    exec.freeResult(cv);

    // SELECT * FROM view
    const r = try exec.execute("SELECT * FROM eng_employees");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    try std.testing.expectEqualStrings("id", r.rows.columns[0]);
    try std.testing.expectEqualStrings("name", r.rows.columns[1]);
    try std.testing.expectEqualStrings("1", r.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("Alice", r.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("3", r.rows.rows[1].values[0]);
    try std.testing.expectEqualStrings("Carol", r.rows.rows[1].values[1]);

    // SELECT specific column from view
    const r2 = try exec.execute("SELECT name FROM eng_employees");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.columns.len);
    try std.testing.expectEqualStrings("Alice", r2.rows.rows[0].values[0]);
    try std.testing.expectEqualStrings("Carol", r2.rows.rows[1].values[0]);

    // DROP VIEW
    const dv = try exec.execute("DROP VIEW eng_employees");
    exec.freeResult(dv);

    // SELECT from dropped view should fail
    const err = exec.execute("SELECT * FROM eng_employees");
    try std.testing.expectError(ExecError.TableNotFound, err);
}

test "executor CREATE VIEW duplicate name" {
    const test_file = "test_exec_view_dup.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);
    defer exec.deinit();

    const ct = try exec.execute("CREATE TABLE t (id INT)");
    exec.freeResult(ct);

    const cv = try exec.execute("CREATE VIEW v AS SELECT id FROM t");
    exec.freeResult(cv);

    // Duplicate view name
    const err1 = exec.execute("CREATE VIEW v AS SELECT id FROM t");
    try std.testing.expectError(ExecError.TableAlreadyExists, err1);

    // View name same as existing table
    const err2 = exec.execute("CREATE VIEW t AS SELECT id FROM t");
    try std.testing.expectError(ExecError.TableAlreadyExists, err2);
}

test "executor FOREIGN KEY constraint" {
    const test_file = "test_exec_fk.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);
    defer exec.deinit();

    // Create parent table
    const ct1 = try exec.execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)");
    exec.freeResult(ct1);
    const ins1 = try exec.execute("INSERT INTO departments VALUES (1, 'Eng'), (2, 'Sales')");
    exec.freeResult(ins1);

    // Create child table with FK
    const ct2 = try exec.execute("CREATE TABLE employees (id INT, name TEXT, dept_id INT, FOREIGN KEY (dept_id) REFERENCES departments(id))");
    exec.freeResult(ct2);

    // Valid FK insert
    const ins2 = try exec.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
    exec.freeResult(ins2);

    // Invalid FK insert — dept_id 99 doesn't exist
    const err = exec.execute("INSERT INTO employees VALUES (2, 'Bob', 99)");
    try std.testing.expectError(ExecError.ForeignKeyViolation, err);

    // NULL FK value should be allowed
    const ins3 = try exec.execute("INSERT INTO employees (id, name) VALUES (3, 'Carol')");
    exec.freeResult(ins3);

    // Delete from parent that is referenced should fail
    const del_err = exec.execute("DELETE FROM departments WHERE id = 1");
    try std.testing.expectError(ExecError.ForeignKeyViolation, del_err);

    // Delete from parent that is NOT referenced should succeed
    const del = try exec.execute("DELETE FROM departments WHERE id = 2");
    defer exec.freeResult(del);
    try std.testing.expectEqual(@as(u64, 1), del.row_count);
}

test "executor window functions ROW_NUMBER, RANK, DENSE_RANK" {
    const test_file = "test_exec_window.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);
    defer exec.deinit();

    const ct = try exec.execute("CREATE TABLE scores (name TEXT, dept TEXT, score INT)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO scores VALUES ('Alice', 'Eng', 90), ('Bob', 'Eng', 85), ('Carol', 'Sales', 90), ('Dave', 'Sales', 80)");
    exec.freeResult(ins);

    // ROW_NUMBER() OVER(ORDER BY score DESC)
    const r1 = try exec.execute("SELECT name, ROW_NUMBER() OVER(ORDER BY score DESC) FROM scores");
    defer exec.freeResult(r1);
    try std.testing.expectEqual(@as(usize, 4), r1.rows.rows.len);
    try std.testing.expectEqualStrings("row_number", r1.rows.columns[1]);
    // Alice(90)=1, Bob(85)=3, Carol(90)=2, Dave(80)=4 (original order, numbered by sort)
    try std.testing.expectEqualStrings("1", r1.rows.rows[0].values[1]); // Alice
    try std.testing.expectEqualStrings("3", r1.rows.rows[1].values[1]); // Bob

    // ROW_NUMBER() OVER(PARTITION BY dept ORDER BY score DESC)
    const r2 = try exec.execute("SELECT name, dept, ROW_NUMBER() OVER(PARTITION BY dept ORDER BY score DESC) FROM scores");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 4), r2.rows.rows.len);
    // Within each partition, numbering restarts at 1
    // Eng: Alice(90)=1, Bob(85)=2; Sales: Carol(90)=1, Dave(80)=2
    for (r2.rows.rows) |row| {
        const rn = try std.fmt.parseInt(u64, row.values[2], 10);
        try std.testing.expect(rn == 1 or rn == 2);
    }

    // RANK() with ties
    const ct2 = try exec.execute("CREATE TABLE ranks (id INT, val INT)");
    exec.freeResult(ct2);
    const ins2 = try exec.execute("INSERT INTO ranks VALUES (1, 10), (2, 20), (3, 20), (4, 30)");
    exec.freeResult(ins2);

    const r3 = try exec.execute("SELECT id, RANK() OVER(ORDER BY val) FROM ranks");
    defer exec.freeResult(r3);
    // val=10 → rank 1, val=20 → rank 2, val=20 → rank 2, val=30 → rank 4
    try std.testing.expectEqualStrings("1", r3.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("2", r3.rows.rows[1].values[1]);
    try std.testing.expectEqualStrings("2", r3.rows.rows[2].values[1]);
    try std.testing.expectEqualStrings("4", r3.rows.rows[3].values[1]);

    // DENSE_RANK()
    const r4 = try exec.execute("SELECT id, DENSE_RANK() OVER(ORDER BY val) FROM ranks");
    defer exec.freeResult(r4);
    // val=10 → 1, val=20 → 2, val=20 → 2, val=30 → 3
    try std.testing.expectEqualStrings("1", r4.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("2", r4.rows.rows[1].values[1]);
    try std.testing.expectEqualStrings("2", r4.rows.rows[2].values[1]);
    try std.testing.expectEqualStrings("3", r4.rows.rows[3].values[1]);
}

test "executor DATE and TIMESTAMP types" {
    const test_file = "test_exec_datetime.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();
    var catalog = try Catalog.init(std.testing.allocator, &bp, &am);
    defer catalog.deinit();
    var exec = Executor.init(std.testing.allocator, &catalog);
    defer exec.deinit();

    // DATE type
    const ct = try exec.execute("CREATE TABLE events (id INT, event_date DATE)");
    exec.freeResult(ct);
    const ins = try exec.execute("INSERT INTO events VALUES (1, '2024-01-15'), (2, '2024-06-30')");
    exec.freeResult(ins);

    const r = try exec.execute("SELECT id, event_date FROM events");
    defer exec.freeResult(r);
    try std.testing.expectEqual(@as(usize, 2), r.rows.rows.len);
    try std.testing.expectEqualStrings("2024-01-15", r.rows.rows[0].values[1]);
    try std.testing.expectEqualStrings("2024-06-30", r.rows.rows[1].values[1]);

    // TIMESTAMP type
    const ct2 = try exec.execute("CREATE TABLE logs (id INT, created_at TIMESTAMP)");
    exec.freeResult(ct2);
    const ins2 = try exec.execute("INSERT INTO logs VALUES (1, '2024-01-15 10:30:00')");
    exec.freeResult(ins2);

    const r2 = try exec.execute("SELECT id, created_at FROM logs");
    defer exec.freeResult(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.rows.rows.len);
    try std.testing.expectEqualStrings("2024-01-15 10:30:00", r2.rows.rows[0].values[1]);

    // DATE comparison in WHERE
    const r3 = try exec.execute("SELECT id FROM events WHERE event_date > '2024-03-01'");
    defer exec.freeResult(r3);
    try std.testing.expectEqual(@as(usize, 1), r3.rows.rows.len);
    try std.testing.expectEqualStrings("2", r3.rows.rows[0].values[0]);
}
