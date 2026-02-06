const std = @import("std");

/// Column type as specified in SQL
pub const DataType = enum {
    int,
    integer,
    bigint,
    float,
    boolean,
    varchar,
    text,
};

/// Column definition in CREATE TABLE
pub const ColumnDef = struct {
    name: []const u8,
    data_type: DataType,
    max_length: u16, // for VARCHAR(n)
    nullable: bool, // default true unless PRIMARY KEY
    is_primary_key: bool,
};

/// Comparison operators in WHERE clauses
pub const CompOp = enum {
    eq, // =
    neq, // != or <>
    lt, // <
    gt, // >
    lte, // <=
    gte, // >=
};

/// A literal value in SQL
pub const LiteralValue = union(enum) {
    integer: i64,
    float: f64,
    string: []const u8,
    boolean: bool,
    null_value: void,
};

/// Expression node for WHERE clauses
pub const Expression = union(enum) {
    /// Column reference: just a name
    column_ref: []const u8,
    /// Literal value
    literal: LiteralValue,
    /// Comparison: left op right
    comparison: struct {
        left: *const Expression,
        op: CompOp,
        right: *const Expression,
    },
    /// AND
    and_expr: struct {
        left: *const Expression,
        right: *const Expression,
    },
    /// OR
    or_expr: struct {
        left: *const Expression,
        right: *const Expression,
    },
    /// NOT
    not_expr: struct {
        operand: *const Expression,
    },
};

/// What columns to select
pub const SelectColumn = union(enum) {
    /// SELECT *
    all_columns: void,
    /// SELECT col_name
    named: []const u8,
};

/// Top-level SQL statement
pub const Statement = union(enum) {
    create_table: CreateTable,
    insert: Insert,
    select: Select,
    update: Update,
    delete: Delete,
    drop_table: DropTable,
    begin_txn: void,
    commit_txn: void,
    rollback_txn: void,
};

pub const CreateTable = struct {
    table_name: []const u8,
    columns: []const ColumnDef,
};

pub const Insert = struct {
    table_name: []const u8,
    values: []const LiteralValue,
};

pub const OrderByClause = struct {
    column: []const u8,
    ascending: bool, // true = ASC (default), false = DESC
};

pub const Select = struct {
    columns: []const SelectColumn,
    table_name: []const u8,
    where_clause: ?*const Expression,
    order_by: ?[]const OrderByClause,
    limit: ?u64,
};

pub const Delete = struct {
    table_name: []const u8,
    where_clause: ?*const Expression,
};

pub const Update = struct {
    table_name: []const u8,
    assignments: []const SetClause,
    where_clause: ?*const Expression,
};

pub const SetClause = struct {
    column: []const u8,
    value: LiteralValue,
};

pub const DropTable = struct {
    table_name: []const u8,
};
