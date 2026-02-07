const std = @import("std");
const page_mod = @import("page.zig");
const buffer_pool_mod = @import("buffer_pool.zig");
const tuple_mod = @import("tuple.zig");
const table_mod = @import("table.zig");
const btree_mod = @import("../index/btree.zig");

const PageId = page_mod.PageId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const BufferPool = buffer_pool_mod.BufferPool;
const Schema = tuple_mod.Schema;
const Column = tuple_mod.Column;
const ColumnType = tuple_mod.ColumnType;
const Value = tuple_mod.Value;
const Table = table_mod.Table;

pub const CatalogError = error{
    TableAlreadyExists,
    TableNotFound,
    SystemTableError,
    IndexAlreadyExists,
    IndexNotFound,
    OutOfMemory,
    BufferPoolError,
    SerializationError,
    StorageError,
};

/// Maximum length for table and column names
const MAX_NAME_LEN = 64;

/// Entry in the tables catalog (pg_class equivalent)
pub const TableEntry = struct {
    table_id: PageId,
    name: []const u8,
    column_count: u16,
};

/// Entry in the columns catalog (pg_attribute equivalent)
pub const ColumnEntry = struct {
    table_id: PageId,
    ordinal: u16,
    name: []const u8,
    col_type: ColumnType,
    max_length: u16,
    nullable: bool,
};

/// Entry in the indexes catalog
pub const IndexEntry = struct {
    index_id: PageId,
    table_id: PageId,
    index_name: []const u8,
    column_ordinal: u16,
    is_unique: bool,
};

/// System catalog - stores metadata about all tables and columns.
/// Uses two internal heap tables:
///   - gp_tables: (table_id: integer, name: varchar, column_count: integer)
///   - gp_columns: (table_id: integer, ordinal: integer, name: varchar,
///                   col_type: integer, max_length: integer, nullable: integer)
pub const Catalog = struct {
    allocator: std.mem.Allocator,
    buffer_pool: *BufferPool,

    /// The heap table storing table metadata
    tables_table: Table,
    /// The heap table storing column metadata
    columns_table: Table,
    /// The heap table storing index metadata
    indexes_table: Table,

    /// Tracks all heap-allocated schemas so they can be freed on deinit
    owned_schemas: std.ArrayList(*const Schema),

    /// Schema for the gp_tables catalog table
    const tables_schema = Schema{
        .columns = &.{
            .{ .name = "table_id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = MAX_NAME_LEN, .nullable = false },
            .{ .name = "column_count", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    /// Schema for the gp_indexes catalog table
    const indexes_schema = Schema{
        .columns = &.{
            .{ .name = "index_id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "table_id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "index_name", .col_type = .varchar, .max_length = MAX_NAME_LEN, .nullable = false },
            .{ .name = "column_ordinal", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "is_unique", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    /// Schema for the gp_columns catalog table
    const columns_schema = Schema{
        .columns = &.{
            .{ .name = "table_id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "ordinal", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = MAX_NAME_LEN, .nullable = false },
            .{ .name = "col_type", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "max_length", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "nullable", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    const Self = @This();

    /// Initialize a new catalog - creates the three system tables
    pub fn init(allocator: std.mem.Allocator, buffer_pool: *BufferPool) CatalogError!Self {
        var tables_table = Table.create(allocator, buffer_pool, &tables_schema) catch {
            return CatalogError.StorageError;
        };

        var columns_table = Table.create(allocator, buffer_pool, &columns_schema) catch {
            return CatalogError.StorageError;
        };

        const indexes_table = Table.create(allocator, buffer_pool, &indexes_schema) catch {
            return CatalogError.StorageError;
        };

        // Record the catalog tables themselves in gp_tables
        // so they are discoverable
        const tables_vals = [_]Value{
            .{ .integer = @bitCast(tables_table.table_id) },
            .{ .bytes = "gp_tables" },
            .{ .integer = @intCast(tables_schema.columns.len) },
        };
        _ = tables_table.insertTuple(null, &tables_vals) catch {
            return CatalogError.StorageError;
        };

        const columns_vals = [_]Value{
            .{ .integer = @bitCast(columns_table.table_id) },
            .{ .bytes = "gp_columns" },
            .{ .integer = @intCast(columns_schema.columns.len) },
        };
        _ = tables_table.insertTuple(null, &columns_vals) catch {
            return CatalogError.StorageError;
        };

        const indexes_vals = [_]Value{
            .{ .integer = @bitCast(indexes_table.table_id) },
            .{ .bytes = "gp_indexes" },
            .{ .integer = @intCast(indexes_schema.columns.len) },
        };
        _ = tables_table.insertTuple(null, &indexes_vals) catch {
            return CatalogError.StorageError;
        };

        return .{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .tables_table = tables_table,
            .columns_table = columns_table,
            .indexes_table = indexes_table,
            .owned_schemas = .empty,
        };
    }

    /// Free all resources owned by the catalog
    pub fn deinit(self: *Self) void {
        for (self.owned_schemas.items) |schema| {
            for (schema.columns) |col| {
                if (col.name.len > 0) {
                    self.allocator.free(col.name);
                }
            }
            self.allocator.free(schema.columns);
            self.allocator.destroy(@constCast(schema));
        }
        self.owned_schemas.deinit(self.allocator);
    }

    /// Open an existing catalog from known table IDs
    pub fn open(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        tables_table_id: PageId,
        columns_table_id: PageId,
        indexes_table_id: PageId,
    ) Self {
        return .{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .tables_table = Table.open(allocator, buffer_pool, &tables_schema, tables_table_id),
            .columns_table = Table.open(allocator, buffer_pool, &columns_schema, columns_table_id),
            .indexes_table = Table.open(allocator, buffer_pool, &indexes_schema, indexes_table_id),
            .owned_schemas = .empty,
        };
    }

    /// Create a new user table and register it in the catalog.
    /// Returns the PageId (table_id) of the newly created table.
    pub fn createTable(self: *Self, name: []const u8, columns: []const Column) CatalogError!PageId {
        // Check if a table with this name already exists
        if (try self.findTableId(name) != null) {
            return CatalogError.TableAlreadyExists;
        }

        // Build the schema for the new table
        const schema_columns = self.allocator.alloc(Column, columns.len) catch {
            return CatalogError.OutOfMemory;
        };

        for (columns, 0..) |col, i| {
            // Copy column name to owned memory
            const name_copy = self.allocator.dupe(u8, col.name) catch {
                // Free previously allocated names
                for (schema_columns[0..i]) |prev| {
                    self.allocator.free(prev.name);
                }
                self.allocator.free(schema_columns);
                return CatalogError.OutOfMemory;
            };
            schema_columns[i] = .{
                .name = name_copy,
                .col_type = col.col_type,
                .max_length = col.max_length,
                .nullable = col.nullable,
            };
        }

        const schema = self.allocator.create(Schema) catch {
            for (schema_columns) |col| {
                self.allocator.free(col.name);
            }
            self.allocator.free(schema_columns);
            return CatalogError.OutOfMemory;
        };
        schema.* = .{ .columns = schema_columns };

        // Track the schema for cleanup
        self.owned_schemas.append(self.allocator, schema) catch {
            self.allocator.destroy(schema);
            for (schema_columns) |col| {
                self.allocator.free(col.name);
            }
            self.allocator.free(schema_columns);
            return CatalogError.OutOfMemory;
        };

        // Create the actual heap table
        const table = Table.create(self.allocator, self.buffer_pool, schema) catch {
            // Schema is tracked in owned_schemas, will be freed by deinit
            return CatalogError.StorageError;
        };

        const table_id = table.table_id;

        // Insert into gp_tables
        const table_vals = [_]Value{
            .{ .integer = @bitCast(table_id) },
            .{ .bytes = name },
            .{ .integer = @intCast(columns.len) },
        };
        _ = self.tables_table.insertTuple(null, &table_vals) catch {
            return CatalogError.StorageError;
        };

        // Insert columns into gp_columns
        for (columns, 0..) |col, i| {
            const col_vals = [_]Value{
                .{ .integer = @bitCast(table_id) },
                .{ .integer = @intCast(i) },
                .{ .bytes = col.name },
                .{ .integer = @intCast(@intFromEnum(col.col_type)) },
                .{ .integer = @intCast(col.max_length) },
                .{ .integer = if (col.nullable) @as(i32, 1) else @as(i32, 0) },
            };
            _ = self.columns_table.insertTuple(null, &col_vals) catch {
                return CatalogError.StorageError;
            };
        }

        return table_id;
    }

    /// Add a column to an existing table. The new column must be nullable
    /// (existing rows will have NULL for it).
    pub fn addColumn(self: *Self, table_name: []const u8, col: Column) CatalogError!void {
        // Find the table entry in gp_tables
        var table_id: ?PageId = null;
        var old_col_count: u16 = 0;
        var delete_tid: ?page_mod.TupleId = null;

        {
            var iter = self.tables_table.scan() catch {
                return CatalogError.StorageError;
            };

            while (iter.next() catch { return CatalogError.StorageError; }) |row| {
                defer iter.freeValues(row.values);
                if (std.mem.eql(u8, row.values[1].bytes, table_name)) {
                    table_id = @bitCast(row.values[0].integer);
                    old_col_count = @intCast(row.values[2].integer);
                    delete_tid = row.tid;
                    break;
                }
            }
        }

        const tid = table_id orelse return CatalogError.TableNotFound;
        const dtid = delete_tid orelse return CatalogError.StorageError;

        const new_col_count = old_col_count + 1;

        // Delete old gp_tables row and insert updated one
        _ = self.tables_table.deleteTuple(null, dtid) catch {
            return CatalogError.StorageError;
        };
        const table_vals = [_]Value{
            .{ .integer = @bitCast(tid) },
            .{ .bytes = table_name },
            .{ .integer = @intCast(new_col_count) },
        };
        _ = self.tables_table.insertTuple(null, &table_vals) catch {
            return CatalogError.StorageError;
        };

        // Insert new column into gp_columns
        const col_vals = [_]Value{
            .{ .integer = @bitCast(tid) },
            .{ .integer = @intCast(old_col_count) }, // ordinal = old count (0-based)
            .{ .bytes = col.name },
            .{ .integer = @intCast(@intFromEnum(col.col_type)) },
            .{ .integer = @intCast(col.max_length) },
            .{ .integer = if (col.nullable) @as(i32, 1) else @as(i32, 0) },
        };
        _ = self.columns_table.insertTuple(null, &col_vals) catch {
            return CatalogError.StorageError;
        };
    }

    /// Drop a user table by name. Removes entries from gp_tables, gp_columns, and gp_indexes.
    /// System tables (gp_tables, gp_columns, gp_indexes) cannot be dropped.
    pub fn dropTable(self: *Self, name: []const u8) CatalogError!void {
        // Block dropping system tables
        if (std.mem.eql(u8, name, "gp_tables") or std.mem.eql(u8, name, "gp_columns") or std.mem.eql(u8, name, "gp_indexes")) {
            return CatalogError.SystemTableError;
        }

        // Find table_id from gp_tables and delete matching row
        var table_id: ?PageId = null;
        {
            var iter = self.tables_table.scan() catch {
                return CatalogError.StorageError;
            };

            while (iter.next() catch { return CatalogError.StorageError; }) |row| {
                defer iter.freeValues(row.values);
                if (std.mem.eql(u8, row.values[1].bytes, name)) {
                    table_id = @bitCast(row.values[0].integer);
                    _ = self.tables_table.deleteTuple(null, row.tid) catch {
                        return CatalogError.StorageError;
                    };
                    break;
                }
            }
        }

        const tid = table_id orelse return CatalogError.TableNotFound;

        // Delete all matching rows from gp_columns
        var to_delete: std.ArrayList(page_mod.TupleId) = .empty;
        defer to_delete.deinit(self.allocator);
        {
            var iter = self.columns_table.scan() catch {
                return CatalogError.StorageError;
            };

            while (iter.next() catch { return CatalogError.StorageError; }) |row| {
                defer iter.freeValues(row.values);
                const col_tid: PageId = @bitCast(row.values[0].integer);
                if (col_tid == tid) {
                    to_delete.append(self.allocator, row.tid) catch {
                        return CatalogError.OutOfMemory;
                    };
                }
            }
        }

        for (to_delete.items) |del_tid| {
            _ = self.columns_table.deleteTuple(null, del_tid) catch {
                return CatalogError.StorageError;
            };
        }

        // Cascade-delete indexes from gp_indexes
        var idx_to_delete: std.ArrayList(page_mod.TupleId) = .empty;
        defer idx_to_delete.deinit(self.allocator);
        {
            var idx_iter = self.indexes_table.scan() catch {
                return CatalogError.StorageError;
            };
            while (idx_iter.next() catch { return CatalogError.StorageError; }) |row| {
                defer idx_iter.freeValues(row.values);
                const idx_table_id: PageId = @bitCast(row.values[1].integer);
                if (idx_table_id == tid) {
                    idx_to_delete.append(self.allocator, row.tid) catch {
                        return CatalogError.OutOfMemory;
                    };
                }
            }
        }
        for (idx_to_delete.items) |del_tid| {
            _ = self.indexes_table.deleteTuple(null, del_tid) catch {
                return CatalogError.StorageError;
            };
        }
    }

    // ============================================================
    // Index management
    // ============================================================

    /// Create a new index. Validates table/column exist, creates BTree, inserts into gp_indexes.
    /// Returns the PageId (root page) of the new BTree index.
    pub fn createIndex(
        self: *Self,
        table_name: []const u8,
        index_name: []const u8,
        column_name: []const u8,
        is_unique: bool,
    ) CatalogError!PageId {
        // Check if index name already exists
        if (try self.findIndex(index_name)) |existing| {
            self.freeIndexEntry(existing);
            return CatalogError.IndexAlreadyExists;
        }

        // Validate table exists
        const table_id = (try self.findTableId(table_name)) orelse return CatalogError.TableNotFound;

        // Validate column exists and get ordinal
        const schema = (try self.getSchema(table_id)) orelse return CatalogError.TableNotFound;
        defer self.freeSchema(schema);

        var column_ordinal: ?u16 = null;
        for (schema.columns, 0..) |col, i| {
            if (std.ascii.eqlIgnoreCase(col.name, column_name)) {
                column_ordinal = @intCast(i);
                break;
            }
        }
        if (column_ordinal == null) return CatalogError.StorageError; // Column not found

        // Create the BTree
        const btree = btree_mod.BTree.create(self.buffer_pool) catch {
            return CatalogError.StorageError;
        };
        const index_id = btree.root_page_id;

        // Insert into gp_indexes
        const idx_vals = [_]Value{
            .{ .integer = @bitCast(index_id) },
            .{ .integer = @bitCast(table_id) },
            .{ .bytes = index_name },
            .{ .integer = @intCast(column_ordinal.?) },
            .{ .integer = if (is_unique) @as(i32, 1) else @as(i32, 0) },
        };
        _ = self.indexes_table.insertTuple(null, &idx_vals) catch {
            return CatalogError.StorageError;
        };

        return index_id;
    }

    /// Drop an index by name
    pub fn dropIndex(self: *Self, index_name: []const u8) CatalogError!void {
        var found = false;
        var iter = self.indexes_table.scan() catch {
            return CatalogError.StorageError;
        };
        while (iter.next() catch { return CatalogError.StorageError; }) |row| {
            defer iter.freeValues(row.values);
            if (std.mem.eql(u8, row.values[2].bytes, index_name)) {
                _ = self.indexes_table.deleteTuple(null, row.tid) catch {
                    return CatalogError.StorageError;
                };
                found = true;
                break;
            }
        }
        if (!found) return CatalogError.IndexNotFound;
    }

    /// Get all indexes for a given table_id. Caller must call freeIndexList.
    pub fn getIndexesForTable(self: *Self, table_id: PageId) CatalogError![]IndexEntry {
        var entries: std.ArrayList(IndexEntry) = .empty;
        defer entries.deinit(self.allocator);

        var iter = self.indexes_table.scan() catch {
            return CatalogError.StorageError;
        };
        while (iter.next() catch { return CatalogError.StorageError; }) |row| {
            defer iter.freeValues(row.values);
            const row_table_id: PageId = @bitCast(row.values[1].integer);
            if (row_table_id == table_id) {
                const name_copy = self.allocator.dupe(u8, row.values[2].bytes) catch {
                    return CatalogError.OutOfMemory;
                };
                entries.append(self.allocator, .{
                    .index_id = @bitCast(row.values[0].integer),
                    .table_id = row_table_id,
                    .index_name = name_copy,
                    .column_ordinal = @intCast(row.values[3].integer),
                    .is_unique = row.values[4].integer != 0,
                }) catch {
                    self.allocator.free(name_copy);
                    return CatalogError.OutOfMemory;
                };
            }
        }

        return entries.toOwnedSlice(self.allocator) catch {
            return CatalogError.OutOfMemory;
        };
    }

    /// Find an index by name. Caller must call freeIndexEntry if non-null.
    pub fn findIndex(self: *Self, index_name: []const u8) CatalogError!?IndexEntry {
        var iter = self.indexes_table.scan() catch {
            return CatalogError.StorageError;
        };
        while (iter.next() catch { return CatalogError.StorageError; }) |row| {
            defer iter.freeValues(row.values);
            if (std.mem.eql(u8, row.values[2].bytes, index_name)) {
                const name_copy = self.allocator.dupe(u8, row.values[2].bytes) catch {
                    return CatalogError.OutOfMemory;
                };
                return .{
                    .index_id = @bitCast(row.values[0].integer),
                    .table_id = @bitCast(row.values[1].integer),
                    .index_name = name_copy,
                    .column_ordinal = @intCast(row.values[3].integer),
                    .is_unique = row.values[4].integer != 0,
                };
            }
        }
        return null;
    }

    /// Free an IndexEntry returned by findIndex
    pub fn freeIndexEntry(self: *Self, entry: IndexEntry) void {
        self.allocator.free(entry.index_name);
    }

    /// Free an index list returned by getIndexesForTable
    pub fn freeIndexList(self: *Self, entries: []IndexEntry) void {
        for (entries) |entry| {
            self.allocator.free(entry.index_name);
        }
        self.allocator.free(entries);
    }

    /// Find a table's PageId by name. Returns null if not found.
    pub fn findTableId(self: *Self, name: []const u8) CatalogError!?PageId {
        var iter = self.tables_table.scan() catch {
            return CatalogError.StorageError;
        };

        while (iter.next() catch { return CatalogError.StorageError; }) |row| {
            defer iter.freeValues(row.values);
            // row.values[1] is the table name
            if (std.mem.eql(u8, row.values[1].bytes, name)) {
                const tid: PageId = @bitCast(row.values[0].integer);
                return tid;
            }
        }
        return null;
    }

    /// Get table entry by name
    pub fn getTable(self: *Self, name: []const u8) CatalogError!?TableEntry {
        var iter = self.tables_table.scan() catch {
            return CatalogError.StorageError;
        };

        while (iter.next() catch { return CatalogError.StorageError; }) |row| {
            defer iter.freeValues(row.values);
            if (std.mem.eql(u8, row.values[1].bytes, name)) {
                return .{
                    .table_id = @bitCast(row.values[0].integer),
                    .name = name,
                    .column_count = @intCast(row.values[2].integer),
                };
            }
        }
        return null;
    }

    /// Reconstruct a Schema for a table by reading gp_columns.
    /// Caller owns the returned Schema and must free it with freeSchema.
    pub fn getSchema(self: *Self, table_id: PageId) CatalogError!?*const Schema {
        // First find the column count
        var col_count: usize = 0;
        {
            var iter = self.tables_table.scan() catch {
                return CatalogError.StorageError;
            };
            var found = false;
            while (iter.next() catch { return CatalogError.StorageError; }) |row| {
                defer iter.freeValues(row.values);
                const tid: PageId = @bitCast(row.values[0].integer);
                if (tid == table_id) {
                    col_count = @intCast(row.values[2].integer);
                    found = true;
                    break;
                }
            }
            if (!found) return null;
        }

        // Allocate columns array
        const columns = self.allocator.alloc(Column, col_count) catch {
            return CatalogError.OutOfMemory;
        };
        errdefer {
            for (columns) |col| {
                if (col.name.len > 0) self.allocator.free(col.name);
            }
            self.allocator.free(columns);
        }
        @memset(columns, Column{ .name = "", .col_type = .integer, .max_length = 0, .nullable = false });

        // Read columns from gp_columns
        var iter = self.columns_table.scan() catch {
            return CatalogError.StorageError;
        };

        while (iter.next() catch { return CatalogError.StorageError; }) |row| {
            defer iter.freeValues(row.values);
            const tid: PageId = @bitCast(row.values[0].integer);
            if (tid == table_id) {
                const ordinal: usize = @intCast(row.values[1].integer);
                if (ordinal >= col_count) continue;

                const name_copy = self.allocator.dupe(u8, row.values[2].bytes) catch {
                    return CatalogError.OutOfMemory;
                };

                columns[ordinal] = .{
                    .name = name_copy,
                    .col_type = @enumFromInt(@as(u8, @intCast(row.values[3].integer))),
                    .max_length = @intCast(row.values[4].integer),
                    .nullable = row.values[5].integer != 0,
                };
            }
        }

        const schema = self.allocator.create(Schema) catch {
            return CatalogError.OutOfMemory;
        };
        schema.* = .{ .columns = columns };
        return schema;
    }

    /// Free a schema returned by getSchema
    pub fn freeSchema(self: *Self, schema: *const Schema) void {
        for (schema.columns) |col| {
            if (col.name.len > 0) {
                self.allocator.free(col.name);
            }
        }
        self.allocator.free(schema.columns);
        self.allocator.destroy(@constCast(schema));
    }

    /// Open a Table handle for a named table.
    /// Caller must call freeSchema on the returned schema when done.
    pub fn openTable(self: *Self, name: []const u8) CatalogError!?struct { table: Table, schema: *const Schema } {
        const table_id = try self.findTableId(name) orelse return null;
        const schema = try self.getSchema(table_id) orelse return null;
        return .{
            .table = Table.open(self.allocator, self.buffer_pool, schema, table_id),
            .schema = schema,
        };
    }

    /// List all table names. Caller must free each name and the slice.
    pub fn listTables(self: *Self) CatalogError![][]const u8 {
        var names: std.ArrayList([]const u8) = .empty;
        defer names.deinit(self.allocator);

        var iter = self.tables_table.scan() catch {
            return CatalogError.StorageError;
        };

        while (iter.next() catch { return CatalogError.StorageError; }) |row| {
            defer iter.freeValues(row.values);
            const name_copy = self.allocator.dupe(u8, row.values[1].bytes) catch {
                return CatalogError.OutOfMemory;
            };
            names.append(self.allocator, name_copy) catch {
                self.allocator.free(name_copy);
                return CatalogError.OutOfMemory;
            };
        }

        return names.toOwnedSlice(self.allocator) catch {
            return CatalogError.OutOfMemory;
        };
    }

    /// Free a list returned by listTables
    pub fn freeTableList(self: *Self, names: [][]const u8) void {
        for (names) |name| {
            self.allocator.free(name);
        }
        self.allocator.free(names);
    }
};

// Tests
const disk_manager_mod = @import("disk_manager.zig");
const DiskManager = disk_manager_mod.DiskManager;

test "catalog create and find table" {
    const test_file = "test_catalog_basic.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    // Create a user table
    const columns = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
        .{ .name = "email", .col_type = .text, .max_length = 0, .nullable = true },
    };

    const table_id = try catalog.createTable("users", &columns);
    try std.testing.expect(table_id != INVALID_PAGE_ID);

    // Find it by name
    const found_id = try catalog.findTableId("users");
    try std.testing.expectEqual(table_id, found_id.?);

    // Not found
    const missing = try catalog.findTableId("nonexistent");
    try std.testing.expect(missing == null);
}

test "catalog get table entry" {
    const test_file = "test_catalog_entry.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const columns = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "value", .col_type = .float, .max_length = 0, .nullable = false },
    };

    _ = try catalog.createTable("measurements", &columns);

    const entry = (try catalog.getTable("measurements")).?;
    try std.testing.expectEqualStrings("measurements", entry.name);
    try std.testing.expectEqual(@as(u16, 2), entry.column_count);
}

test "catalog duplicate table name" {
    const test_file = "test_catalog_dup.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const columns = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
    };

    _ = try catalog.createTable("users", &columns);

    // Should fail with duplicate
    const result = catalog.createTable("users", &columns);
    try std.testing.expectError(CatalogError.TableAlreadyExists, result);
}

test "catalog reconstruct schema" {
    const test_file = "test_catalog_schema.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const columns = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "name", .col_type = .varchar, .max_length = 100, .nullable = false },
        .{ .name = "score", .col_type = .float, .max_length = 0, .nullable = true },
    };

    const table_id = try catalog.createTable("students", &columns);

    // Reconstruct schema
    const schema = (try catalog.getSchema(table_id)).?;
    defer catalog.freeSchema(schema);

    try std.testing.expectEqual(@as(usize, 3), schema.columns.len);
    try std.testing.expectEqualStrings("id", schema.columns[0].name);
    try std.testing.expectEqual(ColumnType.integer, schema.columns[0].col_type);
    try std.testing.expectEqual(false, schema.columns[0].nullable);

    try std.testing.expectEqualStrings("name", schema.columns[1].name);
    try std.testing.expectEqual(ColumnType.varchar, schema.columns[1].col_type);
    try std.testing.expectEqual(@as(u16, 100), schema.columns[1].max_length);

    try std.testing.expectEqualStrings("score", schema.columns[2].name);
    try std.testing.expectEqual(ColumnType.float, schema.columns[2].col_type);
    try std.testing.expectEqual(true, schema.columns[2].nullable);
}

test "catalog open table and use it" {
    const test_file = "test_catalog_open.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const columns = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
    };

    _ = try catalog.createTable("products", &columns);

    // Open the table through catalog
    const result = (try catalog.openTable("products")).?;
    defer catalog.freeSchema(result.schema);
    var table = result.table;

    // Insert a row
    const vals = [_]Value{ .{ .integer = 1 }, .{ .bytes = "Widget" } };
    const tid = table.insertTuple(null, &vals) catch unreachable;

    // Read it back
    const row = (table.getTuple(tid, null) catch unreachable).?;
    defer std.testing.allocator.free(row);

    try std.testing.expectEqual(@as(i32, 1), row[0].integer);
    try std.testing.expectEqualStrings("Widget", row[1].bytes);
}

test "catalog list tables" {
    const test_file = "test_catalog_list.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const col = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
    };

    _ = try catalog.createTable("alpha", &col);
    _ = try catalog.createTable("beta", &col);
    _ = try catalog.createTable("gamma", &col);

    const names = try catalog.listTables();
    defer catalog.freeTableList(names);

    // Should have gp_tables, gp_columns, gp_indexes, alpha, beta, gamma = 6
    try std.testing.expectEqual(@as(usize, 6), names.len);

    // Check that our user tables are present
    var found_alpha = false;
    var found_beta = false;
    var found_gamma = false;
    for (names) |name| {
        if (std.mem.eql(u8, name, "alpha")) found_alpha = true;
        if (std.mem.eql(u8, name, "beta")) found_beta = true;
        if (std.mem.eql(u8, name, "gamma")) found_gamma = true;
    }
    try std.testing.expect(found_alpha);
    try std.testing.expect(found_beta);
    try std.testing.expect(found_gamma);
}

test "catalog drop table" {
    const test_file = "test_catalog_drop.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const col = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
    };

    _ = try catalog.createTable("mytable", &col);
    try std.testing.expect((try catalog.findTableId("mytable")) != null);

    // Drop the table
    try catalog.dropTable("mytable");

    // Should no longer be found
    try std.testing.expect((try catalog.findTableId("mytable")) == null);

    // Drop nonexistent should fail
    try std.testing.expectError(CatalogError.TableNotFound, catalog.dropTable("mytable"));

    // Drop system table should fail
    try std.testing.expectError(CatalogError.SystemTableError, catalog.dropTable("gp_tables"));
    try std.testing.expectError(CatalogError.SystemTableError, catalog.dropTable("gp_indexes"));
}

test "catalog create and find index" {
    const test_file = "test_catalog_idx.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const col = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
    };
    _ = try catalog.createTable("users", &col);

    // Create index
    const idx_id = try catalog.createIndex("users", "idx_users_id", "id", false);
    try std.testing.expect(idx_id != INVALID_PAGE_ID);

    // Find it
    const entry = (try catalog.findIndex("idx_users_id")).?;
    defer catalog.freeIndexEntry(entry);
    try std.testing.expectEqualStrings("idx_users_id", entry.index_name);
    try std.testing.expectEqual(@as(u16, 0), entry.column_ordinal);
    try std.testing.expect(!entry.is_unique);

    // Not found
    const missing = try catalog.findIndex("nonexistent");
    try std.testing.expect(missing == null);

    // Duplicate should fail
    try std.testing.expectError(CatalogError.IndexAlreadyExists, catalog.createIndex("users", "idx_users_id", "id", false));
}

test "catalog list indexes for table" {
    const test_file = "test_catalog_idx_list.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const col = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        .{ .name = "val", .col_type = .integer, .max_length = 0, .nullable = false },
    };
    const table_id = try catalog.createTable("t", &col);

    _ = try catalog.createIndex("t", "idx_t_id", "id", true);
    _ = try catalog.createIndex("t", "idx_t_val", "val", false);

    const indexes = try catalog.getIndexesForTable(table_id);
    defer catalog.freeIndexList(indexes);

    try std.testing.expectEqual(@as(usize, 2), indexes.len);

    // Drop one
    try catalog.dropIndex("idx_t_id");
    const indexes2 = try catalog.getIndexesForTable(table_id);
    defer catalog.freeIndexList(indexes2);
    try std.testing.expectEqual(@as(usize, 1), indexes2.len);

    // Drop nonexistent
    try std.testing.expectError(CatalogError.IndexNotFound, catalog.dropIndex("nope"));
}

test "catalog drop table cascades indexes" {
    const test_file = "test_catalog_idx_cascade.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const col = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
    };
    const table_id = try catalog.createTable("t", &col);
    _ = try catalog.createIndex("t", "idx_t_id", "id", false);

    // Verify index exists
    const indexes = try catalog.getIndexesForTable(table_id);
    defer catalog.freeIndexList(indexes);
    try std.testing.expectEqual(@as(usize, 1), indexes.len);

    // Drop table — should cascade
    try catalog.dropTable("t");

    // Index should be gone
    const entry = try catalog.findIndex("idx_t_id");
    try std.testing.expect(entry == null);
}

test "catalog drop system table fails" {
    const test_file = "test_catalog_drop_sys.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    try std.testing.expectError(CatalogError.SystemTableError, catalog.dropTable("gp_tables"));
    try std.testing.expectError(CatalogError.SystemTableError, catalog.dropTable("gp_columns"));
    try std.testing.expectError(CatalogError.SystemTableError, catalog.dropTable("gp_indexes"));
}

test "catalog create index nonexistent table" {
    const test_file = "test_catalog_idx_no_table.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    try std.testing.expectError(CatalogError.TableNotFound, catalog.createIndex("nonexistent", "idx", "id", false));
}

test "catalog create index nonexistent column" {
    const test_file = "test_catalog_idx_no_col.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const col = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
    };
    _ = try catalog.createTable("t", &col);

    // Column "nope" doesn't exist
    try std.testing.expectError(CatalogError.StorageError, catalog.createIndex("t", "idx", "nope", false));
}

test "catalog getIndexesForTable empty" {
    const test_file = "test_catalog_idx_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const col = [_]Column{
        .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
    };
    const table_id = try catalog.createTable("t", &col);

    // No indexes created
    const indexes = try catalog.getIndexesForTable(table_id);
    defer catalog.freeIndexList(indexes);
    try std.testing.expectEqual(@as(usize, 0), indexes.len);
}

test "catalog findIndex nonexistent returns null" {
    const test_file = "test_catalog_find_idx_null.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const result = try catalog.findIndex("nonexistent_index");
    try std.testing.expect(result == null);
}

test "catalog findTableId nonexistent returns null" {
    const test_file = "test_catalog_find_tbl_null.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    const result = try catalog.findTableId("nonexistent_table");
    try std.testing.expect(result == null);
}

test "catalog drop nonexistent table fails" {
    const test_file = "test_catalog_drop_noexist.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();
    var catalog = try Catalog.init(std.testing.allocator, &bp);
    defer catalog.deinit();

    try std.testing.expectError(CatalogError.TableNotFound, catalog.dropTable("nonexistent"));
}
