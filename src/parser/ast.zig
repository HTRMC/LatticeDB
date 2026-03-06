const std = @import("std");

/// Column type as specified in SQL
pub const DataType = enum {
    int,
    integer,
    smallint,
    bigint,
    float,
    boolean,
    varchar,
    text,
    date,
    timestamp,
    decimal,
    serial,
    uuid,
    json,
};

/// Column definition in CREATE TABLE
pub const ColumnDef = struct {
    name: []const u8,
    data_type: DataType,
    max_length: u16, // for VARCHAR(n), also precision for DECIMAL
    precision: u8 = 0, // DECIMAL(precision, scale)
    scale: u8 = 0, // DECIMAL(precision, scale)
    nullable: bool, // default true unless PRIMARY KEY
    is_primary_key: bool,
    default_value: ?LiteralValue, // DEFAULT literal
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
    parameter: u32, // 0-based parameter index
};

/// Expression node for WHERE clauses
pub const Expression = union(enum) {
    /// Column reference: just a name
    column_ref: []const u8,
    /// Qualified column reference: table.column
    qualified_ref: struct {
        table: []const u8,
        column: []const u8,
    },
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
    /// BETWEEN low AND high
    between_expr: struct {
        value: *const Expression,
        low: *const Expression,
        high: *const Expression,
    },
    /// LIKE pattern
    like_expr: struct {
        value: *const Expression,
        pattern: *const Expression,
    },
    /// IN (val, val, ...)
    in_list: struct {
        value: *const Expression,
        items: []const *const Expression,
    },
    /// IN (SELECT ...)
    in_subquery: struct {
        value: *const Expression,
        subquery: *const Select,
    },
    /// EXISTS (SELECT ...)
    exists_subquery: struct {
        subquery: *const Select,
    },
    /// CASE [operand] WHEN cond THEN result [ELSE default] END
    case_expr: struct {
        when_clauses: []const WhenClause,
        else_result: ?*const Expression,
    },
    /// Scalar function call: LOWER(expr), UPPER(expr), etc.
    function_call: struct {
        func: BuiltinFunction,
        args: []const *const Expression,
    },
    /// Arithmetic: left op right
    arithmetic: struct {
        left: *const Expression,
        op: ArithOp,
        right: *const Expression,
    },
    /// Unary minus: -expr
    unary_minus: struct {
        operand: *const Expression,
    },
    /// IS NULL / IS NOT NULL
    is_null: struct {
        operand: *const Expression,
        negated: bool, // true = IS NOT NULL
    },
    /// CAST(expr AS type)
    cast_expr: struct {
        operand: *const Expression,
        target_type: DataType,
    },
};

/// Arithmetic operators
pub const ArithOp = enum {
    add, // +
    sub, // -
    mul, // *
    div, // /
};

/// Built-in scalar function type
pub const BuiltinFunction = enum {
    lower,
    upper,
    trim,
    length,
    substring,
    concat,
    coalesce,
    nullif,
    abs,
    round,
    ceil,
    floor,
    mod,
    replace,
    position,
    left,
    right,
    reverse,
    lpad,
    rpad,
};

/// CASE WHEN clause
pub const WhenClause = struct {
    condition: *const Expression,
    result: *const Expression,
};

/// Aggregate function type
pub const AggregateFunc = enum {
    count,
    sum,
    avg,
    min,
    max,
};

/// Window function type
pub const WindowFunc = enum {
    row_number,
    rank,
    dense_rank,
};

/// Window specification: OVER(PARTITION BY ... ORDER BY ...)
pub const WindowSpec = struct {
    partition_by: ?[]const []const u8 = null,
    order_by: ?[]const OrderByClause = null,
};

/// What columns to select
pub const SelectColumn = union(enum) {
    /// SELECT *
    all_columns: void,
    /// SELECT col_name
    named: []const u8,
    /// SELECT table.col_name (qualified)
    qualified: struct {
        table: []const u8,
        column: []const u8,
    },
    /// SELECT COUNT(*), SUM(col), etc.
    aggregate: struct {
        func: AggregateFunc,
        column: ?[]const u8, // null for COUNT(*)
        distinct: bool = false,
    },
    /// SELECT expression (function call, CASE, etc.)
    expression: *const Expression,
    /// SELECT ROW_NUMBER() OVER(...), RANK() OVER(...), etc.
    window_function: struct {
        func: WindowFunc,
        spec: WindowSpec,
    },
};

/// Top-level SQL statement
pub const Statement = union(enum) {
    create_table: CreateTable,
    create_index: CreateIndex,
    insert: Insert,
    insert_select: InsertSelect,
    select: Select,
    update: Update,
    delete: Delete,
    drop_table: DropTable,
    drop_index: DropIndex,
    alter_table: AlterTable,
    explain: Explain,
    union_query: UnionQuery,
    create_view: CreateView,
    drop_view: []const u8, // view name
    cte_select: CTESelect,
    begin_txn: void,
    commit_txn: void,
    rollback_txn: void,
};

pub const CreateView = struct {
    view_name: []const u8,
    query: Select,
};

pub const CheckConstraint = struct {
    name: ?[]const u8, // optional constraint name
    expr: *const Expression,
};

pub const ForeignKey = struct {
    column: []const u8, // local column
    ref_table: []const u8, // referenced table
    ref_column: []const u8, // referenced column
};

pub const CreateTable = struct {
    table_name: []const u8,
    columns: []const ColumnDef,
    checks: []const CheckConstraint = &.{},
    foreign_keys: []const ForeignKey = &.{},
};

pub const Insert = struct {
    table_name: []const u8,
    columns: ?[]const []const u8, // optional column list
    rows: []const []const LiteralValue,
};

pub const InsertSelect = struct {
    table_name: []const u8,
    query: Select,
};

pub const OrderByClause = struct {
    column: []const u8,
    ascending: bool, // true = ASC (default), false = DESC
};

pub const JoinType = enum {
    inner,
    left,
    right,
    cross,
};

pub const JoinClause = struct {
    join_type: JoinType,
    table_name: []const u8,
    on_condition: ?*const Expression, // null for CROSS JOIN
};

pub const Select = struct {
    columns: []const SelectColumn,
    aliases: ?[]const ?[]const u8, // parallel to columns: alias name or null
    distinct: bool,
    table_name: []const u8,
    subquery: ?*const Select = null, // FROM (SELECT ...) AS alias
    joins: ?[]const JoinClause,
    where_clause: ?*const Expression,
    group_by: ?[]const []const u8,
    having_clause: ?*const Expression,
    order_by: ?[]const OrderByClause,
    limit: ?u64,
    offset: ?u64,
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
    value: *const Expression,
};

pub const DropTable = struct {
    table_name: []const u8,
};

pub const AlterAction = union(enum) {
    add_column: ColumnDef,
    drop_column: []const u8,
    rename_column: struct { old_name: []const u8, new_name: []const u8 },
};

pub const AlterTable = struct {
    table_name: []const u8,
    action: AlterAction,
};

pub const CreateIndex = struct {
    index_name: []const u8,
    table_name: []const u8,
    columns: []const []const u8, // one or more column names
    is_unique: bool,
};

pub const DropIndex = struct {
    index_name: []const u8,
};

pub const SetOpType = enum { @"union", except, intersect };

pub const UnionQuery = struct {
    left: Select,
    right: Select,
    all: bool, // true = UNION ALL / EXCEPT ALL / INTERSECT ALL
    op: SetOpType = .@"union",
};

pub const Explain = struct {
    select: Select,
};

pub const CTE = struct {
    name: []const u8,
    query: Select,
};

pub const CTESelect = struct {
    ctes: []const CTE,
    select: Select,
};
