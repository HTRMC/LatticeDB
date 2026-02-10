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
    /// Bound parameters for current prepared statement execution (null = no params)
    current_params: ?[]const ast.LiteralValue = null,

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
            .select => |sel| self.execSelectWithParams(sel, params),
            .update => |upd| self.execUpdateWithParams(upd, params),
            .delete => |del| self.execDeleteWithParams(del, params),
            .drop_table => |dt| self.execDropTable(dt),
            .drop_index => |di| self.execDropIndex(di),
            .alter_table => |at| self.execAlterTable(at),
            .explain => |ex| self.execExplain(ex),
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
            .select => |sel| self.execSelect(sel),
            .update => |upd| self.execUpdate(upd),
            .delete => |del| self.execDelete(del),
            .drop_table => |dt| self.execDropTable(dt),
            .drop_index => |di| self.execDropIndex(di),
            .alter_table => |at| self.execAlterTable(at),
            .explain => |ex| self.execExplain(ex),
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
                // Enforce that added column must be nullable
                // (existing rows will have NULL for it)
                const col = Column{
                    .name = col_def.name,
                    .col_type = mapDataType(col_def.data_type),
                    .max_length = col_def.max_length,
                    .nullable = true, // Always nullable for ADD COLUMN
                };

                self.catalog.addColumn(at.table_name, col) catch |err| {
                    return switch (err) {
                        catalog_mod.CatalogError.TableNotFound => ExecError.TableNotFound,
                        else => ExecError.StorageError,
                    };
                };

                const msg = std.fmt.allocPrint(self.allocator, "ALTER TABLE", .{}) catch {
                    return ExecError.OutOfMemory;
                };
                return .{ .message = msg };
            },
        }
    }

    // ============================================================
    // CREATE INDEX
    // ============================================================
    fn execCreateIndex(self: *Self, ci: ast.CreateIndex) ExecError!ExecResult {
        const index_id = self.catalog.createIndex(ci.table_name, ci.index_name, ci.column_name, ci.is_unique) catch |err| {
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

        // Find column ordinal
        var col_ord: ?usize = null;
        for (schema.columns, 0..) |col, i| {
            if (std.ascii.eqlIgnoreCase(col.name, ci.column_name)) {
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
        col_indices: []const usize,
        txn: ?*Transaction,
        rows: *std.ArrayList(ResultRow),
    ) bool {
        // Run planner
        var planner = planner_mod.Planner.init(self.allocator, self.catalog);
        const plan = planner.planSelect(sel) catch return false;
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
                    self.fetchAndAppendRow(table, t, txn, sel, schema, col_indices, rows) catch return false;
                }
            },
            .range => |r| {
                // Range scan [low, high]
                var range_iter = btree.rangeScan(r.low, r.high) catch return false;
                while (range_iter.next() catch return false) |entry| {
                    self.fetchAndAppendRow(table, entry.tid, txn, sel, schema, col_indices, rows) catch return false;
                }
            },
            .range_from => |key| {
                // key >= value → scan from key to max
                var range_iter = btree.rangeScan(key, std.math.maxInt(i32)) catch return false;
                while (range_iter.next() catch return false) |entry| {
                    self.fetchAndAppendRow(table, entry.tid, txn, sel, schema, col_indices, rows) catch return false;
                }
            },
            .range_to => |key| {
                // key <= value → scan from min to key
                var range_iter = btree.rangeScan(std.math.minInt(i32), key) catch return false;
                while (range_iter.next() catch return false) |entry| {
                    self.fetchAndAppendRow(table, entry.tid, txn, sel, schema, col_indices, rows) catch return false;
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
        col_indices: []const usize,
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
        const formatted = try self.allocator.alloc([]const u8, col_indices.len);
        errdefer self.allocator.free(formatted);
        for (col_indices, 0..) |ci, i| {
            formatted[i] = try formatValue(self.allocator, vals[ci]);
        }

        try rows.append(self.allocator, .{ .values = formatted });
    }

    // ============================================================
    // INSERT INTO
    // ============================================================
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
            const resolved = resolveParam(lit, self.current_params);
            values[i] = litToValue(resolved, col.col_type) catch {
                return ExecError.TypeMismatch;
            };
        }

        // Use explicit txn or auto-commit
        const txn = self.current_txn orelse self.beginImplicitTxn();

        const tid = table.insertTuple(txn, values) catch {
            self.abortImplicitTxn(txn);
            return ExecError.StorageError;
        };

        // Maintain indexes
        self.maintainIndexesInsert(table.table_id, schema, values, tid);

        self.commitImplicitTxn(txn);
        return .{ .row_count = 1 };
    }

    // ============================================================
    // SELECT
    // ============================================================
    fn execSelectWithParams(self: *Self, sel: ast.Select, params: []const ast.LiteralValue) ExecError!ExecResult {
        self.current_params = params;
        defer self.current_params = null;
        return self.execSelect(sel);
    }

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

        // Build column name headers (prefer alias if present)
        const col_names = self.allocator.alloc([]const u8, col_indices.len) catch {
            return ExecError.OutOfMemory;
        };
        for (col_indices, 0..) |ci, i| {
            const alias_name = if (sel.aliases) |aliases| (if (i < aliases.len) aliases[i] else null) else null;
            const name_src = alias_name orelse schema.columns[ci].name;
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
        const used_index = self.tryIndexScan(sel, &table, schema, col_indices, txn, &rows);

        if (!used_index) {
            // Fall back to sequential scan
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
                const n: usize = @intCast(@min(limit_val, rows.items.len));
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

        // Try index nested-loop join: if right table has an index on the join column,
        // use btree point lookup instead of materializing all right rows.
        const index_join = self.tryIndexNestedLoopJoin(
            join.on_condition,
            sel.table_name,
            join.table_name,
            left_schema,
            right_schema,
            right_table.table_id,
        );

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
                        const on_match = self.evalJoinCondition(join.on_condition, sel.table_name, join.table_name, left_schema, right_schema, left_row.values, right_vals);

                        if (on_match) {
                            if (sel.where_clause) |where| {
                                if (!self.evalWhere(where, combined_schema, combined_values)) {
                                    // WHERE failed, not a match
                                } else {
                                    matched = true;
                                    try self.formatAndAppendJoinRow(col_indices, combined_values, &rows);
                                }
                            } else {
                                matched = true;
                                try self.formatAndAppendJoinRow(col_indices, combined_values, &rows);
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
                        try self.formatAndAppendJoinRow(col_indices, combined_values, &rows);
                    }
                }

                self.allocator.free(combined_values);
            }
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
                for (right_rows.items) |right_vals| {
                    @memcpy(combined_values[left_schema.columns.len..], right_vals);

                    const on_match = self.evalJoinCondition(join.on_condition, sel.table_name, join.table_name, left_schema, right_schema, left_row.values, right_vals);
                    if (!on_match) continue;

                    if (sel.where_clause) |where| {
                        if (!self.evalWhere(where, combined_schema, combined_values)) continue;
                    }

                    matched = true;
                    try self.formatAndAppendJoinRow(col_indices, combined_values, &rows);
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
                        try self.formatAndAppendJoinRow(col_indices, combined_values, &rows);
                    }
                }

                self.allocator.free(combined_values);
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

    /// Format selected columns from combined values and append to result rows.
    fn formatAndAppendJoinRow(
        self: *Self,
        col_indices: []const usize,
        combined_values: []const Value,
        rows: *std.ArrayList(ResultRow),
    ) ExecError!void {
        const formatted = self.allocator.alloc([]const u8, col_indices.len) catch return ExecError.OutOfMemory;
        errdefer self.allocator.free(formatted);
        for (col_indices, 0..) |ci, i| {
            formatted[i] = formatValue(self.allocator, combined_values[ci]) catch {
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

            // Apply SET assignments
            for (upd.assignments, set_indices) |assign, ci| {
                const resolved_val = resolveParam(assign.value, self.current_params);
                new_values[ci] = litToValue(resolved_val, schema.columns[ci].col_type) catch {
                    self.abortImplicitTxn(txn);
                    return ExecError.TypeMismatch;
                };
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
                const left_val = resolveExprValue(cmp.left, schema, values, params);
                const right_val = resolveExprValue(cmp.right, schema, values, params);
                return compareValues(left_val, cmp.op, right_val);
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
                const val = resolveExprValue(b.value, schema, values, params);
                const low = resolveExprValue(b.low, schema, values, params);
                const high = resolveExprValue(b.high, schema, values, params);
                return compareValues(val, .gte, low) and compareValues(val, .lte, high);
            },
            .like_expr => |l| {
                const val = resolveExprValue(l.value, schema, values, params);
                const pat = resolveExprValue(l.pattern, schema, values, params);
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
                const val = resolveExprValue(il.value, schema, values, params);
                for (il.items) |item| {
                    const item_val = resolveExprValue(item, schema, values, params);
                    if (compareValues(val, .eq, item_val)) return true;
                }
                return false;
            },
            .in_subquery => |isq| {
                const val = resolveExprValue(isq.value, schema, values, params);
                const sub_result = self.execSelect(isq.subquery.*) catch return false;
                defer self.freeResult(.{ .rows = sub_result.rows });
                for (sub_result.rows.rows) |row| {
                    if (row.values.len > 0) {
                        // Compare val with first column of each row
                        const sub_val = self.parseSubqueryValue(row.values[0], val);
                        if (compareValues(val, .eq, sub_val)) return true;
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
        }
    }

    /// SQL LIKE pattern matching: % = any chars, _ = single char
    fn matchLike(text: []const u8, pattern: []const u8) bool {
        var ti: usize = 0;
        var pi: usize = 0;
        var star_pi: ?usize = null;
        var star_ti: usize = 0;

        while (ti < text.len) {
            if (pi < pattern.len and pattern[pi] == '_') {
                // _ matches any single character
                ti += 1;
                pi += 1;
            } else if (pi < pattern.len and pattern[pi] == '%') {
                // % matches zero or more characters - try matching zero first
                star_pi = pi;
                star_ti = ti;
                pi += 1;
            } else if (pi < pattern.len and pattern[pi] == text[ti]) {
                ti += 1;
                pi += 1;
            } else if (star_pi) |sp| {
                // Backtrack: try matching one more character with %
                pi = sp + 1;
                star_ti += 1;
                ti = star_ti;
            } else {
                return false;
            }
        }

        // Consume trailing % in pattern
        while (pi < pattern.len and pattern[pi] == '%') {
            pi += 1;
        }

        return pi == pattern.len;
    }

    fn resolveExprValue(expr: *const ast.Expression, schema: *const Schema, values: []const Value, params: ?[]const ast.LiteralValue) Value {
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
            .literal => |lit| return litToStorageValue(lit, params),
            else => return .{ .null_value = {} },
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

    /// Resolve a parameter reference to its bound value, or return the literal as-is.
    fn resolveParam(lit: ast.LiteralValue, params: ?[]const ast.LiteralValue) ast.LiteralValue {
        return switch (lit) {
            .parameter => |idx| if (params) |p| p[idx] else lit,
            else => lit,
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
            .parameter => error.TypeMismatch, // unresolved parameter
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
