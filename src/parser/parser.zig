const std = @import("std");
const lexer_mod = @import("lexer.zig");
const ast = @import("ast.zig");

const Lexer = lexer_mod.Lexer;
const Token = lexer_mod.Token;
const TokenType = lexer_mod.TokenType;

pub const ParseError = error{
    UnexpectedToken,
    UnexpectedEof,
    InvalidSyntax,
    OutOfMemory,
};

pub const Parser = struct {
    lexer: Lexer,
    current: Token,
    allocator: std.mem.Allocator,

    // Arena for AST allocations - caller frees everything at once
    arena_state: std.heap.ArenaAllocator,

    // Parameter tracking for prepared statements
    param_count: u32 = 0,
    param_names: [32][]const u8 = undefined,
    param_name_count: u32 = 0,
    has_positional: bool = false,
    has_named: bool = false,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, input: []const u8) Self {
        var p = Self{
            .lexer = Lexer.init(input),
            .current = undefined,
            .allocator = allocator,
            .arena_state = std.heap.ArenaAllocator.init(allocator),
        };
        p.current = p.lexer.nextToken();
        return p;
    }

    pub fn deinit(self: *Self) void {
        self.arena_state.deinit();
    }

    fn arena(self: *Self) std.mem.Allocator {
        return self.arena_state.allocator();
    }

    /// Parse a single SQL statement
    pub fn parse(self: *Self) ParseError!ast.Statement {
        const stmt = switch (self.current.type) {
            .kw_select => blk: {
                const sel = try self.parseSelect();
                // Check for UNION/EXCEPT/INTERSECT [ALL]
                const set_op: ?ast.SetOpType = switch (self.current.type) {
                    .kw_union => .@"union",
                    .kw_except => .except,
                    .kw_intersect => .intersect,
                    else => null,
                };
                if (set_op) |op| {
                    self.advance();
                    const all = self.current.type == .kw_all;
                    if (all) self.advance();
                    const right = try self.parseSelect();
                    break :blk ast.Statement{ .union_query = .{ .left = sel, .right = right, .all = all, .op = op } };
                }
                break :blk ast.Statement{ .select = sel };
            },
            .kw_insert => try self.parseInsert(),
            .kw_update => ast.Statement{ .update = try self.parseUpdate() },
            .kw_delete => ast.Statement{ .delete = try self.parseDelete() },
            .kw_create => blk: {
                const next = self.lexer.peek();
                if (next.type == .kw_index or next.type == .kw_unique) {
                    break :blk ast.Statement{ .create_index = try self.parseCreateIndex() };
                }
                if (next.type == .kw_view) {
                    self.advance(); // consume CREATE
                    self.advance(); // consume VIEW
                    const view_name = try self.expectIdentifier();
                    try self.expect(.kw_as);
                    const query = try self.parseSelect();
                    break :blk ast.Statement{ .create_view = .{ .view_name = view_name, .query = query } };
                }
                break :blk ast.Statement{ .create_table = try self.parseCreateTable() };
            },
            .kw_drop => blk: {
                const next = self.lexer.peek();
                if (next.type == .kw_index) {
                    break :blk ast.Statement{ .drop_index = try self.parseDropIndex() };
                }
                if (next.type == .kw_view) {
                    self.advance(); // consume DROP
                    self.advance(); // consume VIEW
                    break :blk ast.Statement{ .drop_view = try self.expectIdentifier() };
                }
                break :blk ast.Statement{ .drop_table = try self.parseDropTable() };
            },
            .kw_alter => ast.Statement{ .alter_table = try self.parseAlterTable() },
            .kw_explain => blk: {
                self.advance(); // consume EXPLAIN
                const sel = try self.parseSelect();
                break :blk ast.Statement{ .explain = .{ .select = sel } };
            },
            .kw_begin => blk: {
                self.advance();
                break :blk ast.Statement{ .begin_txn = {} };
            },
            .kw_commit => blk: {
                self.advance();
                break :blk ast.Statement{ .commit_txn = {} };
            },
            .kw_rollback => blk: {
                self.advance();
                break :blk ast.Statement{ .rollback_txn = {} };
            },
            .eof => return ParseError.UnexpectedEof,
            else => return ParseError.UnexpectedToken,
        };

        // Optional semicolon
        if (self.current.type == .semicolon) {
            self.advance();
        }

        return stmt;
    }

    // ============================================================
    // SELECT columns FROM table [WHERE expr]
    // ============================================================
    fn parseSelect(self: *Self) ParseError!ast.Select {
        try self.expect(.kw_select);

        // Optional DISTINCT
        const distinct = self.current.type == .kw_distinct;
        if (distinct) self.advance();

        // Parse column list
        const alloc = self.arena();
        var cols: std.ArrayList(ast.SelectColumn) = .empty;
        var alias_list: std.ArrayList(?[]const u8) = .empty;
        if (self.current.type == .op_star) {
            cols.append(alloc, .{ .all_columns = {} }) catch return ParseError.OutOfMemory;
            alias_list.append(alloc, null) catch return ParseError.OutOfMemory;
            self.advance();
        } else {
            cols.append(alloc, try self.parseSelectColumn()) catch return ParseError.OutOfMemory;
            // Check for AS alias
            if (self.current.type == .kw_as) {
                self.advance();
                alias_list.append(alloc, try self.expectIdentifier()) catch return ParseError.OutOfMemory;
            } else {
                alias_list.append(alloc, null) catch return ParseError.OutOfMemory;
            }
            while (self.current.type == .comma) {
                self.advance();
                cols.append(alloc, try self.parseSelectColumn()) catch return ParseError.OutOfMemory;
                if (self.current.type == .kw_as) {
                    self.advance();
                    alias_list.append(alloc, try self.expectIdentifier()) catch return ParseError.OutOfMemory;
                } else {
                    alias_list.append(alloc, null) catch return ParseError.OutOfMemory;
                }
            }
        }

        try self.expect(.kw_from);

        // Check for derived table: FROM (SELECT ...) [AS] alias
        var subquery: ?*const ast.Select = null;
        var table_name: []const u8 = undefined;
        if (self.current.type == .left_paren) {
            self.advance();
            const sub = try self.parseSelect();
            try self.expect(.right_paren);
            const sub_ptr = try alloc.create(ast.Select);
            sub_ptr.* = sub;
            subquery = sub_ptr;
            // Expect alias
            if (self.current.type == .kw_as) self.advance();
            table_name = try self.expectIdentifier();
        } else {
            table_name = try self.expectIdentifier();
        }

        // Optional JOIN clauses
        var joins: ?[]const ast.JoinClause = null;
        {
            var join_list: std.ArrayList(ast.JoinClause) = .empty;
            while (self.current.type == .kw_join or self.current.type == .kw_inner or
                self.current.type == .kw_left or self.current.type == .kw_right or
                self.current.type == .kw_cross)
            {
                var join_type: ast.JoinType = .inner;
                if (self.current.type == .kw_left) {
                    join_type = .left;
                    self.advance();
                } else if (self.current.type == .kw_right) {
                    join_type = .right;
                    self.advance();
                } else if (self.current.type == .kw_cross) {
                    join_type = .cross;
                    self.advance();
                } else if (self.current.type == .kw_inner) {
                    self.advance();
                }
                try self.expect(.kw_join);
                const join_table = try self.expectIdentifier();
                var on_cond: ?*const ast.Expression = null;
                if (join_type != .cross) {
                    try self.expect(.kw_on);
                    on_cond = try self.parseExpression();
                }
                join_list.append(alloc, .{
                    .join_type = join_type,
                    .table_name = join_table,
                    .on_condition = on_cond,
                }) catch return ParseError.OutOfMemory;
            }
            if (join_list.items.len > 0) {
                joins = join_list.toOwnedSlice(alloc) catch return ParseError.OutOfMemory;
            }
        }

        // Optional WHERE
        var where: ?*const ast.Expression = null;
        if (self.current.type == .kw_where) {
            self.advance();
            where = try self.parseExpression();
        }

        // Optional GROUP BY col [, col ...]
        var group_by: ?[]const []const u8 = null;
        if (self.current.type == .kw_group) {
            self.advance();
            try self.expect(.kw_by);

            var group_cols: std.ArrayList([]const u8) = .empty;
            group_cols.append(alloc, try self.expectIdentifier()) catch return ParseError.OutOfMemory;
            while (self.current.type == .comma) {
                self.advance();
                group_cols.append(alloc, try self.expectIdentifier()) catch return ParseError.OutOfMemory;
            }
            group_by = group_cols.toOwnedSlice(alloc) catch return ParseError.OutOfMemory;
        }

        // Optional HAVING expr (only valid with GROUP BY)
        var having: ?*const ast.Expression = null;
        if (self.current.type == .kw_having) {
            self.advance();
            having = try self.parseExpression();
        }

        // Optional ORDER BY col [ASC|DESC] [, ...]
        var order_by: ?[]const ast.OrderByClause = null;
        if (self.current.type == .kw_order) {
            self.advance();
            try self.expect(.kw_by);

            var order_cols: std.ArrayList(ast.OrderByClause) = .empty;
            order_cols.append(alloc, try self.parseOrderByColumn()) catch return ParseError.OutOfMemory;
            while (self.current.type == .comma) {
                self.advance();
                order_cols.append(alloc, try self.parseOrderByColumn()) catch return ParseError.OutOfMemory;
            }
            order_by = order_cols.toOwnedSlice(alloc) catch return ParseError.OutOfMemory;
        }

        // Optional LIMIT n [OFFSET m]
        var limit: ?u64 = null;
        var offset: ?u64 = null;
        if (self.current.type == .kw_limit) {
            self.advance();
            if (self.current.type != .integer_literal) return ParseError.UnexpectedToken;
            limit = std.fmt.parseInt(u64, self.current.text, 10) catch return ParseError.InvalidSyntax;
            self.advance();
            if (self.current.type == .kw_offset) {
                self.advance();
                if (self.current.type != .integer_literal) return ParseError.UnexpectedToken;
                offset = std.fmt.parseInt(u64, self.current.text, 10) catch return ParseError.InvalidSyntax;
                self.advance();
            }
        }

        // Check if any aliases were set
        var has_aliases = false;
        for (alias_list.items) |a| {
            if (a != null) {
                has_aliases = true;
                break;
            }
        }

        return .{
            .columns = cols.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
            .aliases = if (has_aliases) alias_list.toOwnedSlice(alloc) catch return ParseError.OutOfMemory else null,
            .distinct = distinct,
            .table_name = table_name,
            .subquery = subquery,
            .joins = joins,
            .where_clause = where,
            .group_by = group_by,
            .having_clause = having,
            .order_by = order_by,
            .limit = limit,
            .offset = offset,
        };
    }

    // ============================================================
    // INSERT INTO table VALUES (val, val, ...)
    // ============================================================
    fn parseInsert(self: *Self) ParseError!ast.Statement {
        try self.expect(.kw_insert);
        try self.expect(.kw_into);
        const table_name = try self.expectIdentifier();
        const alloc = self.arena();

        // Optional column list: INSERT INTO t (col1, col2) ...
        var columns: ?[]const []const u8 = null;
        if (self.current.type == .left_paren) {
            // Could be column list or VALUES row — peek to distinguish
            // Column list starts with identifier, values start with literal/minus
            const next = self.lexer.peek();
            if (next.type == .identifier) {
                self.advance(); // consume (
                var col_list: std.ArrayList([]const u8) = .empty;
                col_list.append(alloc, try self.expectIdentifier()) catch return ParseError.OutOfMemory;
                while (self.current.type == .comma) {
                    self.advance();
                    col_list.append(alloc, try self.expectIdentifier()) catch return ParseError.OutOfMemory;
                }
                try self.expect(.right_paren);
                columns = col_list.toOwnedSlice(alloc) catch return ParseError.OutOfMemory;
            }
        }

        // INSERT INTO ... SELECT ...
        if (self.current.type == .kw_select) {
            const query = try self.parseSelect();
            return .{ .insert_select = .{ .table_name = table_name, .query = query } };
        }

        try self.expect(.kw_values);

        var rows: std.ArrayList([]const ast.LiteralValue) = .empty;
        rows.append(alloc, try self.parseValueRow()) catch return ParseError.OutOfMemory;
        while (self.current.type == .comma) {
            self.advance();
            rows.append(alloc, try self.parseValueRow()) catch return ParseError.OutOfMemory;
        }

        return .{ .insert = .{
            .table_name = table_name,
            .columns = columns,
            .rows = rows.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
        } };
    }

    fn parseValueRow(self: *Self) ParseError![]const ast.LiteralValue {
        try self.expect(.left_paren);
        const alloc = self.arena();
        var vals: std.ArrayList(ast.LiteralValue) = .empty;
        vals.append(alloc, try self.parseLiteral()) catch return ParseError.OutOfMemory;
        while (self.current.type == .comma) {
            self.advance();
            vals.append(alloc, try self.parseLiteral()) catch return ParseError.OutOfMemory;
        }
        try self.expect(.right_paren);
        return vals.toOwnedSlice(alloc) catch return ParseError.OutOfMemory;
    }

    // ============================================================
    // DELETE FROM table [WHERE expr]
    // ============================================================
    fn parseDelete(self: *Self) ParseError!ast.Delete {
        try self.expect(.kw_delete);
        try self.expect(.kw_from);
        const table_name = try self.expectIdentifier();

        var where: ?*const ast.Expression = null;
        if (self.current.type == .kw_where) {
            self.advance();
            where = try self.parseExpression();
        }

        return .{
            .table_name = table_name,
            .where_clause = where,
        };
    }

    // ============================================================
    // UPDATE table SET col = val [, col = val] [WHERE expr]
    // ============================================================
    fn parseUpdate(self: *Self) ParseError!ast.Update {
        try self.expect(.kw_update);
        const table_name = try self.expectIdentifier();
        try self.expect(.kw_set);

        const alloc = self.arena();
        var assignments: std.ArrayList(ast.SetClause) = .empty;
        assignments.append(alloc, try self.parseSetClause()) catch return ParseError.OutOfMemory;
        while (self.current.type == .comma) {
            self.advance();
            assignments.append(alloc, try self.parseSetClause()) catch return ParseError.OutOfMemory;
        }

        var where: ?*const ast.Expression = null;
        if (self.current.type == .kw_where) {
            self.advance();
            where = try self.parseExpression();
        }

        return .{
            .table_name = table_name,
            .assignments = assignments.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
            .where_clause = where,
        };
    }

    fn parseWindowSpec(self: *Self) ParseError!ast.WindowSpec {
        const alloc = self.arena();
        try self.expect(.kw_over);
        try self.expect(.left_paren);

        var partition_by: ?[]const []const u8 = null;
        var order_by: ?[]const ast.OrderByClause = null;

        if (self.current.type == .kw_partition) {
            self.advance(); // consume PARTITION
            try self.expect(.kw_by);
            var parts: std.ArrayList([]const u8) = .empty;
            parts.append(alloc, try self.expectIdentifier()) catch return ParseError.OutOfMemory;
            while (self.current.type == .comma) {
                self.advance();
                parts.append(alloc, try self.expectIdentifier()) catch return ParseError.OutOfMemory;
            }
            partition_by = parts.toOwnedSlice(alloc) catch return ParseError.OutOfMemory;
        }

        if (self.current.type == .kw_order) {
            self.advance(); // consume ORDER
            try self.expect(.kw_by);
            var orders: std.ArrayList(ast.OrderByClause) = .empty;
            while (true) {
                const col = try self.expectIdentifier();
                var ascending = true;
                if (self.current.type == .kw_desc) {
                    ascending = false;
                    self.advance();
                } else if (self.current.type == .kw_asc) {
                    self.advance();
                }
                orders.append(alloc, .{ .column = col, .ascending = ascending }) catch return ParseError.OutOfMemory;
                if (self.current.type != .comma) break;
                self.advance();
            }
            order_by = orders.toOwnedSlice(alloc) catch return ParseError.OutOfMemory;
        }

        try self.expect(.right_paren);
        return .{ .partition_by = partition_by, .order_by = order_by };
    }

    fn parseSelectColumn(self: *Self) ParseError!ast.SelectColumn {
        // Check for window functions: ROW_NUMBER(), RANK(), DENSE_RANK()
        const win_func: ?ast.WindowFunc = switch (self.current.type) {
            .kw_row_number => .row_number,
            .kw_rank => .rank,
            .kw_dense_rank => .dense_rank,
            else => null,
        };
        if (win_func) |func| {
            self.advance(); // consume function name
            try self.expect(.left_paren);
            try self.expect(.right_paren);
            const spec = try self.parseWindowSpec();
            return .{ .window_function = .{ .func = func, .spec = spec } };
        }

        // Check for aggregate functions: COUNT, SUM, AVG, MIN, MAX
        const agg_func: ?ast.AggregateFunc = switch (self.current.type) {
            .kw_count => .count,
            .kw_sum => .sum,
            .kw_avg => .avg,
            .kw_min => .min,
            .kw_max => .max,
            else => null,
        };

        if (agg_func) |func| {
            self.advance(); // consume function name
            try self.expect(.left_paren);

            var distinct = false;
            if (self.current.type == .kw_distinct) {
                distinct = true;
                self.advance();
            }

            var column: ?[]const u8 = null;
            if (self.current.type == .op_star) {
                // COUNT(*)
                self.advance();
            } else {
                column = try self.expectIdentifier();
            }

            try self.expect(.right_paren);
            return .{ .aggregate = .{ .func = func, .column = column, .distinct = distinct } };
        }

        // Check for CASE or scalar function — parse as expression column
        if (self.isScalarFunctionToken() or self.current.type == .kw_case) {
            const alloc = self.arena();
            const expr = try self.parsePrimary();
            const expr_ptr = try alloc.create(ast.Expression);
            expr_ptr.* = expr.*;
            return .{ .expression = expr_ptr };
        }

        const first = try self.expectIdentifier();
        // Handle table.column — preserve as qualified reference
        if (self.current.type == .dot) {
            self.advance();
            return .{ .qualified = .{ .table = first, .column = try self.expectIdentifier() } };
        }
        // Check for arithmetic operator after identifier — parse as expression column
        if (self.isArithmeticOp()) {
            const alloc = self.arena();
            // Build a column_ref for the first identifier, then parse the rest as arithmetic
            const col_ref = try alloc.create(ast.Expression);
            col_ref.* = .{ .column_ref = first };
            const result = try self.parseArithmeticContinuation(col_ref);
            return .{ .expression = result };
        }
        return .{ .named = first };
    }

    fn isArithmeticOp(self: *Self) bool {
        return switch (self.current.type) {
            .op_plus, .op_minus, .op_star, .op_slash, .op_concat => true,
            else => false,
        };
    }

    fn parseArithmeticContinuation(self: *Self, initial_left: anytype) ParseError!*const ast.Expression {
        // We have the left operand already parsed. Continue with additive-level parsing.
        const alloc = self.arena();
        var left = initial_left;

        // First handle any pending multiplicative ops
        while (self.current.type == .op_star or self.current.type == .op_slash) {
            const op: ast.ArithOp = if (self.current.type == .op_star) .mul else .div;
            self.advance();
            const right = try self.parseUnary();
            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .arithmetic = .{ .left = left, .op = op, .right = right } };
            left = expr;
        }

        // Then handle additive ops and concat
        while (self.current.type == .op_plus or self.current.type == .op_minus or self.current.type == .op_concat) {
            if (self.current.type == .op_concat) {
                self.advance();
                const right = try self.parseMultiplicative();
                const args = try alloc.alloc(*const ast.Expression, 2);
                args[0] = left;
                args[1] = right;
                const expr = try alloc.create(ast.Expression);
                expr.* = .{ .function_call = .{ .func = .concat, .args = args } };
                left = expr;
            } else {
                const op: ast.ArithOp = if (self.current.type == .op_plus) .add else .sub;
                self.advance();
                const right = try self.parseMultiplicative();
                const expr = try alloc.create(ast.Expression);
                expr.* = .{ .arithmetic = .{ .left = left, .op = op, .right = right } };
                left = expr;
            }
        }

        return left;
    }

    fn isScalarFunctionToken(self: *Self) bool {
        return switch (self.current.type) {
            .kw_lower, .kw_upper, .kw_trim, .kw_length, .kw_substring, .kw_concat, .kw_coalesce, .kw_nullif, .kw_abs, .kw_round, .kw_ceil, .kw_floor, .kw_mod, .kw_cast, .kw_replace, .kw_position, .kw_reverse, .kw_lpad, .kw_rpad, .kw_left, .kw_right => true,
            else => false,
        };
    }

    fn parseOrderByColumn(self: *Self) ParseError!ast.OrderByClause {
        const column = try self.expectIdentifier();
        var ascending = true;
        if (self.current.type == .kw_asc) {
            self.advance();
        } else if (self.current.type == .kw_desc) {
            ascending = false;
            self.advance();
        }
        return .{ .column = column, .ascending = ascending };
    }

    fn parseSetClause(self: *Self) ParseError!ast.SetClause {
        const column = try self.expectIdentifier();
        try self.expect(.op_eq);
        const value = try self.parseExpression();
        return .{ .column = column, .value = value };
    }

    // ============================================================
    // CREATE TABLE name (col type, col type, ...)
    // ============================================================
    fn parseCreateTable(self: *Self) ParseError!ast.CreateTable {
        try self.expect(.kw_create);
        try self.expect(.kw_table);
        const table_name = try self.expectIdentifier();
        try self.expect(.left_paren);

        const alloc = self.arena();
        var cols: std.ArrayList(ast.ColumnDef) = .empty;
        var checks: std.ArrayList(ast.CheckConstraint) = .empty;
        var fkeys: std.ArrayList(ast.ForeignKey) = .empty;

        // Parse column defs and table-level constraints
        while (true) {
            if (self.current.type == .kw_check or self.current.type == .kw_constraint) {
                // Table-level CHECK or CONSTRAINT name CHECK/FOREIGN KEY
                var name: ?[]const u8 = null;
                if (self.current.type == .kw_constraint) {
                    self.advance();
                    name = try self.expectIdentifier();
                }
                if (self.current.type == .kw_foreign) {
                    // FOREIGN KEY (col) REFERENCES table(col)
                    self.advance(); // consume FOREIGN
                    try self.expect(.kw_key);
                    try self.expect(.left_paren);
                    const fk_col = try self.expectIdentifier();
                    try self.expect(.right_paren);
                    try self.expect(.kw_references);
                    const ref_table = try self.expectIdentifier();
                    try self.expect(.left_paren);
                    const ref_col = try self.expectIdentifier();
                    try self.expect(.right_paren);
                    fkeys.append(alloc, .{ .column = fk_col, .ref_table = ref_table, .ref_column = ref_col }) catch return ParseError.OutOfMemory;
                } else {
                    try self.expect(.kw_check);
                    try self.expect(.left_paren);
                    const expr = try self.parseExpression();
                    try self.expect(.right_paren);
                    checks.append(alloc, .{ .name = name, .expr = expr }) catch return ParseError.OutOfMemory;
                }
            } else if (self.current.type == .kw_foreign) {
                // FOREIGN KEY (col) REFERENCES table(col)
                self.advance(); // consume FOREIGN
                try self.expect(.kw_key);
                try self.expect(.left_paren);
                const fk_col = try self.expectIdentifier();
                try self.expect(.right_paren);
                try self.expect(.kw_references);
                const ref_table = try self.expectIdentifier();
                try self.expect(.left_paren);
                const ref_col = try self.expectIdentifier();
                try self.expect(.right_paren);
                fkeys.append(alloc, .{ .column = fk_col, .ref_table = ref_table, .ref_column = ref_col }) catch return ParseError.OutOfMemory;
            } else {
                cols.append(alloc, try self.parseColumnDef()) catch return ParseError.OutOfMemory;
            }
            if (self.current.type != .comma) break;
            self.advance();
        }

        try self.expect(.right_paren);

        return .{
            .table_name = table_name,
            .columns = cols.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
            .checks = checks.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
            .foreign_keys = fkeys.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
        };
    }

    // ============================================================
    // DROP TABLE name
    // ============================================================
    fn parseDropTable(self: *Self) ParseError!ast.DropTable {
        try self.expect(.kw_drop);
        try self.expect(.kw_table);
        const table_name = try self.expectIdentifier();
        return .{ .table_name = table_name };
    }

    // ============================================================
    // ALTER TABLE name ADD [COLUMN] coldef
    // ============================================================
    fn parseAlterTable(self: *Self) ParseError!ast.AlterTable {
        try self.expect(.kw_alter);
        try self.expect(.kw_table);
        const table_name = try self.expectIdentifier();

        if (self.current.type == .kw_add) {
            self.advance();
            if (self.current.type == .kw_column) self.advance();
            const col_def = try self.parseColumnDef();
            return .{ .table_name = table_name, .action = .{ .add_column = col_def } };
        } else if (self.current.type == .kw_drop) {
            self.advance();
            if (self.current.type == .kw_column) self.advance();
            const col_name = try self.expectIdentifier();
            return .{ .table_name = table_name, .action = .{ .drop_column = col_name } };
        } else if (self.current.type == .kw_rename) {
            self.advance();
            if (self.current.type == .kw_column) self.advance();
            const old_name = try self.expectIdentifier();
            try self.expect(.kw_to);
            const new_name = try self.expectIdentifier();
            return .{ .table_name = table_name, .action = .{ .rename_column = .{ .old_name = old_name, .new_name = new_name } } };
        }
        return ParseError.UnexpectedToken;
    }

    // ============================================================
    // CREATE [UNIQUE] INDEX name ON table (column)
    // ============================================================
    fn parseCreateIndex(self: *Self) ParseError!ast.CreateIndex {
        try self.expect(.kw_create);

        var is_unique = false;
        if (self.current.type == .kw_unique) {
            is_unique = true;
            self.advance();
        }
        try self.expect(.kw_index);
        const index_name = try self.expectIdentifier();
        // Expect ON keyword — "on" is kw_on
        try self.expect(.kw_on);
        const table_name = try self.expectIdentifier();
        try self.expect(.left_paren);
        const column_name = try self.expectIdentifier();
        try self.expect(.right_paren);

        return .{
            .index_name = index_name,
            .table_name = table_name,
            .column_name = column_name,
            .is_unique = is_unique,
        };
    }

    // ============================================================
    // DROP INDEX name
    // ============================================================
    fn parseDropIndex(self: *Self) ParseError!ast.DropIndex {
        try self.expect(.kw_drop);
        try self.expect(.kw_index);
        const index_name = try self.expectIdentifier();
        return .{ .index_name = index_name };
    }

    // ============================================================
    // Column definition: name TYPE [(length)] [PRIMARY KEY]
    // ============================================================
    fn parseColumnDef(self: *Self) ParseError!ast.ColumnDef {
        const name = try self.expectIdentifier();
        const data_type = try self.parseDataType();

        var max_length: u16 = 0;
        // Optional (length) for VARCHAR
        if (self.current.type == .left_paren) {
            self.advance();
            if (self.current.type != .integer_literal) return ParseError.UnexpectedToken;
            max_length = @intCast(std.fmt.parseInt(u16, self.current.text, 10) catch return ParseError.InvalidSyntax);
            self.advance();
            try self.expect(.right_paren);
        }

        var is_primary_key = false;
        var not_null = false;
        var default_value: ?ast.LiteralValue = null;

        // Parse optional constraints in any order: PRIMARY KEY, NOT NULL, DEFAULT
        while (true) {
            if (self.current.type == .kw_primary) {
                self.advance();
                try self.expect(.kw_key);
                is_primary_key = true;
            } else if (self.current.type == .kw_not) {
                self.advance();
                try self.expect(.kw_null);
                not_null = true;
            } else if (self.current.type == .kw_default) {
                self.advance();
                default_value = try self.parseLiteral();
            } else break;
        }

        return .{
            .name = name,
            .data_type = data_type,
            .max_length = max_length,
            .nullable = !is_primary_key and !not_null,
            .is_primary_key = is_primary_key,
            .default_value = default_value,
        };
    }

    fn parseDataType(self: *Self) ParseError!ast.DataType {
        const dt: ast.DataType = switch (self.current.type) {
            .kw_int => .int,
            .kw_integer => .integer,
            .kw_bigint => .bigint,
            .kw_float => .float,
            .kw_boolean => .boolean,
            .kw_varchar => .varchar,
            .kw_text => .text,
            else => return ParseError.UnexpectedToken,
        };
        self.advance();
        return dt;
    }

    // ============================================================
    // Expression parsing (WHERE clauses)
    // Precedence: OR < AND < NOT < comparison
    // ============================================================

    fn parseExpression(self: *Self) ParseError!*const ast.Expression {
        return self.parseOrExpr();
    }

    fn parseOrExpr(self: *Self) ParseError!*const ast.Expression {
        var left = try self.parseAndExpr();

        while (self.current.type == .kw_or) {
            self.advance();
            const right = try self.parseAndExpr();
            const expr = try self.arena().create(ast.Expression);
            expr.* = .{ .or_expr = .{ .left = left, .right = right } };
            left = expr;
        }

        return left;
    }

    fn parseAndExpr(self: *Self) ParseError!*const ast.Expression {
        var left = try self.parseNotExpr();

        while (self.current.type == .kw_and) {
            self.advance();
            const right = try self.parseNotExpr();
            const expr = try self.arena().create(ast.Expression);
            expr.* = .{ .and_expr = .{ .left = left, .right = right } };
            left = expr;
        }

        return left;
    }

    fn parseNotExpr(self: *Self) ParseError!*const ast.Expression {
        if (self.current.type == .kw_not) {
            self.advance();
            const operand = try self.parseComparison();
            const expr = try self.arena().create(ast.Expression);
            expr.* = .{ .not_expr = .{ .operand = operand } };
            return expr;
        }
        return self.parseComparison();
    }

    fn parseComparison(self: *Self) ParseError!*const ast.Expression {
        const left = try self.parseAdditive();
        const alloc = self.arena();

        // IS NULL / IS NOT NULL
        if (self.current.type == .kw_is) {
            self.advance();
            const negated = self.current.type == .kw_not;
            if (negated) self.advance();
            try self.expect(.kw_null);
            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .is_null = .{ .operand = left, .negated = negated } };
            return expr;
        }

        // NOT BETWEEN / NOT LIKE / NOT IN
        if (self.current.type == .kw_not) {
            const next = self.lexer.peek();
            if (next.type == .kw_between or next.type == .kw_like or next.type == .kw_in) {
                self.advance(); // consume NOT
                const inner = try self.parseComparisonInfix(left, alloc);
                const neg = try alloc.create(ast.Expression);
                neg.* = .{ .not_expr = .{ .operand = inner } };
                return neg;
            }
        }

        return self.parseComparisonInfix(left, alloc);
    }

    fn parseComparisonInfix(self: *Self, left: *const ast.Expression, alloc: std.mem.Allocator) ParseError!*const ast.Expression {
        // BETWEEN low AND high
        if (self.current.type == .kw_between) {
            self.advance();
            const low = try self.parseAdditive();
            try self.expect(.kw_and);
            const high = try self.parseAdditive();
            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .between_expr = .{ .value = left, .low = low, .high = high } };
            return expr;
        }

        // LIKE pattern
        if (self.current.type == .kw_like) {
            self.advance();
            const pattern = try self.parseAdditive();
            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .like_expr = .{ .value = left, .pattern = pattern } };
            return expr;
        }

        // IN (val, val, ...) or IN (SELECT ...)
        if (self.current.type == .kw_in) {
            self.advance();
            try self.expect(.left_paren);

            // Check for subquery
            if (self.current.type == .kw_select) {
                const subquery = try self.parseSelect();
                try self.expect(.right_paren);
                const sub_ptr = try alloc.create(ast.Select);
                sub_ptr.* = subquery;
                const expr = try alloc.create(ast.Expression);
                expr.* = .{ .in_subquery = .{
                    .value = left,
                    .subquery = sub_ptr,
                } };
                return expr;
            }

            var items: std.ArrayList(*const ast.Expression) = .empty;
            items.append(alloc, try self.parseAdditive()) catch return ParseError.OutOfMemory;
            while (self.current.type == .comma) {
                self.advance();
                items.append(alloc, try self.parseAdditive()) catch return ParseError.OutOfMemory;
            }
            try self.expect(.right_paren);

            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .in_list = .{
                .value = left,
                .items = items.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
            } };
            return expr;
        }

        const op: ?ast.CompOp = switch (self.current.type) {
            .op_eq => .eq,
            .op_neq => .neq,
            .op_lt => .lt,
            .op_gt => .gt,
            .op_lte => .lte,
            .op_gte => .gte,
            else => null,
        };

        if (op) |comp_op| {
            self.advance();
            const right = try self.parseAdditive();
            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .comparison = .{ .left = left, .op = comp_op, .right = right } };
            return expr;
        }

        return left;
    }

    fn parseAdditive(self: *Self) ParseError!*const ast.Expression {
        var left = try self.parseMultiplicative();
        const alloc = self.arena();

        while (self.current.type == .op_plus or self.current.type == .op_minus or self.current.type == .op_concat) {
            if (self.current.type == .op_concat) {
                // Desugar `a || b` into concat(a, b)
                self.advance();
                const right = try self.parseMultiplicative();
                const args = try alloc.alloc(*const ast.Expression, 2);
                args[0] = left;
                args[1] = right;
                const expr = try alloc.create(ast.Expression);
                expr.* = .{ .function_call = .{ .func = .concat, .args = args } };
                left = expr;
            } else {
                const op: ast.ArithOp = if (self.current.type == .op_plus) .add else .sub;
                self.advance();
                const right = try self.parseMultiplicative();
                const expr = try alloc.create(ast.Expression);
                expr.* = .{ .arithmetic = .{ .left = left, .op = op, .right = right } };
                left = expr;
            }
        }

        return left;
    }

    fn parseMultiplicative(self: *Self) ParseError!*const ast.Expression {
        var left = try self.parseUnary();
        const alloc = self.arena();

        while (self.current.type == .op_star or self.current.type == .op_slash) {
            const op: ast.ArithOp = if (self.current.type == .op_star) .mul else .div;
            self.advance();
            const right = try self.parseUnary();
            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .arithmetic = .{ .left = left, .op = op, .right = right } };
            left = expr;
        }

        return left;
    }

    fn parseUnary(self: *Self) ParseError!*const ast.Expression {
        if (self.current.type == .op_minus) {
            self.advance();
            const operand = try self.parseUnary();
            const expr = try self.arena().create(ast.Expression);
            expr.* = .{ .unary_minus = .{ .operand = operand } };
            return expr;
        }
        return self.parsePrimary();
    }

    fn parsePrimary(self: *Self) ParseError!*const ast.Expression {
        const expr = try self.arena().create(ast.Expression);

        switch (self.current.type) {
            .integer_literal => {
                const val = std.fmt.parseInt(i64, self.current.text, 10) catch return ParseError.InvalidSyntax;
                expr.* = .{ .literal = .{ .integer = val } };
                self.advance();
            },
            .float_literal => {
                const val = std.fmt.parseFloat(f64, self.current.text) catch return ParseError.InvalidSyntax;
                expr.* = .{ .literal = .{ .float = val } };
                self.advance();
            },
            .string_literal => {
                expr.* = .{ .literal = .{ .string = self.current.text } };
                self.advance();
            },
            .kw_true => {
                expr.* = .{ .literal = .{ .boolean = true } };
                self.advance();
            },
            .kw_false => {
                expr.* = .{ .literal = .{ .boolean = false } };
                self.advance();
            },
            .kw_null => {
                expr.* = .{ .literal = .{ .null_value = {} } };
                self.advance();
            },
            .identifier => {
                const name = self.current.text;
                self.advance();
                // Check for qualified ref: table.column
                if (self.current.type == .dot) {
                    self.advance();
                    const col_name = try self.expectIdentifier();
                    expr.* = .{ .qualified_ref = .{ .table = name, .column = col_name } };
                } else {
                    expr.* = .{ .column_ref = name };
                }
            },
            .kw_exists => {
                self.advance();
                try self.expect(.left_paren);
                const subquery = try self.parseSelect();
                try self.expect(.right_paren);
                const alloc = self.arena();
                const sub_ptr = try alloc.create(ast.Select);
                sub_ptr.* = subquery;
                expr.* = .{ .exists_subquery = .{ .subquery = sub_ptr } };
            },
            .kw_case => return self.parseCaseExpr(),
            .kw_cast => return self.parseCastExpr(),
            .kw_lower => return self.parseFunctionCall(.lower),
            .kw_upper => return self.parseFunctionCall(.upper),
            .kw_trim => return self.parseFunctionCall(.trim),
            .kw_length => return self.parseFunctionCall(.length),
            .kw_substring => return self.parseFunctionCall(.substring),
            .kw_concat => return self.parseFunctionCall(.concat),
            .kw_coalesce => return self.parseFunctionCall(.coalesce),
            .kw_nullif => return self.parseFunctionCall(.nullif),
            .kw_abs => return self.parseFunctionCall(.abs),
            .kw_round => return self.parseFunctionCall(.round),
            .kw_ceil => return self.parseFunctionCall(.ceil),
            .kw_floor => return self.parseFunctionCall(.floor),
            .kw_mod => return self.parseFunctionCall(.mod),
            .kw_replace => return self.parseFunctionCall(.replace),
            .kw_position => return self.parseFunctionCall(.position),
            .kw_reverse => return self.parseFunctionCall(.reverse),
            .kw_lpad => return self.parseFunctionCall(.lpad),
            .kw_rpad => return self.parseFunctionCall(.rpad),
            .kw_left => return self.parseFunctionCall(.left),
            .kw_right => return self.parseFunctionCall(.right),
            .positional_param => {
                const idx = try self.resolvePositionalParam();
                expr.* = .{ .literal = .{ .parameter = idx } };
            },
            .named_param => {
                const idx = try self.resolveNamedParam();
                expr.* = .{ .literal = .{ .parameter = idx } };
            },
            .left_paren => {
                self.advance();
                const inner = try self.parseExpression();
                try self.expect(.right_paren);
                return inner;
            },
            else => return ParseError.UnexpectedToken,
        }

        return expr;
    }

    /// Parse CASE WHEN cond THEN result [WHEN...] [ELSE default] END
    fn parseCaseExpr(self: *Self) ParseError!*const ast.Expression {
        const alloc = self.arena();
        self.advance(); // consume CASE

        // Simple CASE (CASE expr WHEN val THEN ...) is desugared to searched CASE
        var operand: ?*const ast.Expression = null;
        if (self.current.type != .kw_when) {
            operand = try self.parseExpression();
        }

        var when_clauses: std.ArrayList(ast.WhenClause) = .empty;
        while (self.current.type == .kw_when) {
            self.advance(); // consume WHEN
            const condition = if (operand) |op| blk: {
                // Simple CASE: desugar WHEN val -> WHEN operand = val
                const val = try self.parseExpression();
                const cmp = try alloc.create(ast.Expression);
                cmp.* = .{ .comparison = .{ .left = op, .op = .eq, .right = val } };
                break :blk @as(*const ast.Expression, cmp);
            } else try self.parseExpression();
            try self.expect(.kw_then);
            const result = try self.parseExpression();
            when_clauses.append(alloc, .{ .condition = condition, .result = result }) catch return ParseError.OutOfMemory;
        }

        if (when_clauses.items.len == 0) return ParseError.InvalidSyntax;

        var else_result: ?*const ast.Expression = null;
        if (self.current.type == .kw_else) {
            self.advance();
            else_result = try self.parseExpression();
        }

        try self.expect(.kw_end);

        const expr = try alloc.create(ast.Expression);
        expr.* = .{ .case_expr = .{
            .when_clauses = when_clauses.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
            .else_result = else_result,
        } };
        return expr;
    }

    /// Parse FUNC(arg1, arg2, ...)
    fn parseFunctionCall(self: *Self, func: ast.BuiltinFunction) ParseError!*const ast.Expression {
        const alloc = self.arena();
        self.advance(); // consume function name keyword
        try self.expect(.left_paren);

        var args: std.ArrayList(*const ast.Expression) = .empty;
        if (self.current.type != .right_paren) {
            args.append(alloc, try self.parseExpression()) catch return ParseError.OutOfMemory;
            while (self.current.type == .comma) {
                self.advance();
                args.append(alloc, try self.parseExpression()) catch return ParseError.OutOfMemory;
            }
        }
        try self.expect(.right_paren);

        // Validate argument count
        const n = args.items.len;
        switch (func) {
            .lower, .upper, .trim, .length, .reverse => if (n != 1) return ParseError.InvalidSyntax,
            .substring => if (n < 2 or n > 3) return ParseError.InvalidSyntax,
            .concat => if (n < 2) return ParseError.InvalidSyntax,
            .coalesce => if (n < 1) return ParseError.InvalidSyntax,
            .nullif, .mod, .left, .right, .position => if (n != 2) return ParseError.InvalidSyntax,
            .abs, .ceil, .floor => if (n != 1) return ParseError.InvalidSyntax,
            .round => if (n < 1 or n > 2) return ParseError.InvalidSyntax,
            .replace => if (n != 3) return ParseError.InvalidSyntax,
            .lpad, .rpad => if (n < 2 or n > 3) return ParseError.InvalidSyntax,
        }

        const expr = try alloc.create(ast.Expression);
        expr.* = .{ .function_call = .{
            .func = func,
            .args = args.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
        } };
        return expr;
    }

    /// Parse CAST(expr AS type)
    fn parseCastExpr(self: *Self) ParseError!*const ast.Expression {
        const alloc = self.arena();
        self.advance(); // consume CAST
        try self.expect(.left_paren);
        const operand = try self.parseExpression();
        try self.expect(.kw_as);
        const target_type = try self.parseDataType();
        // Skip optional VARCHAR(n) length
        if (target_type == .varchar and self.current.type == .left_paren) {
            self.advance();
            _ = self.current; // skip length
            if (self.current.type == .integer_literal) self.advance();
            try self.expect(.right_paren);
        }
        try self.expect(.right_paren);
        const expr = try alloc.create(ast.Expression);
        expr.* = .{ .cast_expr = .{ .operand = operand, .target_type = target_type } };
        return expr;
    }

    // ============================================================
    // Helpers
    // ============================================================

    fn parseLiteral(self: *Self) ParseError!ast.LiteralValue {
        switch (self.current.type) {
            .integer_literal => {
                const val = std.fmt.parseInt(i64, self.current.text, 10) catch return ParseError.InvalidSyntax;
                self.advance();
                return .{ .integer = val };
            },
            .float_literal => {
                const val = std.fmt.parseFloat(f64, self.current.text) catch return ParseError.InvalidSyntax;
                self.advance();
                return .{ .float = val };
            },
            .string_literal => {
                const text = self.current.text;
                self.advance();
                return .{ .string = text };
            },
            .kw_true => {
                self.advance();
                return .{ .boolean = true };
            },
            .kw_false => {
                self.advance();
                return .{ .boolean = false };
            },
            .kw_null => {
                self.advance();
                return .{ .null_value = {} };
            },
            .op_minus => {
                self.advance();
                switch (self.current.type) {
                    .integer_literal => {
                        const val = std.fmt.parseInt(i64, self.current.text, 10) catch return ParseError.InvalidSyntax;
                        self.advance();
                        return .{ .integer = -val };
                    },
                    .float_literal => {
                        const val = std.fmt.parseFloat(f64, self.current.text) catch return ParseError.InvalidSyntax;
                        self.advance();
                        return .{ .float = -val };
                    },
                    else => return ParseError.UnexpectedToken,
                }
            },
            .positional_param => return .{ .parameter = try self.resolvePositionalParam() },
            .named_param => return .{ .parameter = try self.resolveNamedParam() },
            else => return ParseError.UnexpectedToken,
        }
    }

    /// Resolve $N positional param: $1 -> index 0, $2 -> index 1, etc.
    fn resolvePositionalParam(self: *Self) ParseError!u32 {
        if (self.has_named) return ParseError.InvalidSyntax; // no mixing
        self.has_positional = true;
        const n = std.fmt.parseInt(u32, self.current.text, 10) catch return ParseError.InvalidSyntax;
        if (n == 0) return ParseError.InvalidSyntax; // $0 is invalid
        const idx = n - 1;
        if (n > self.param_count) self.param_count = n;
        self.advance();
        return idx;
    }

    /// Resolve @name named param: first occurrence assigns next index, subsequent reuses it.
    fn resolveNamedParam(self: *Self) ParseError!u32 {
        if (self.has_positional) return ParseError.InvalidSyntax; // no mixing
        self.has_named = true;
        const name = self.current.text;
        // Check if already seen
        for (self.param_names[0..self.param_name_count], 0..) |existing, i| {
            if (std.mem.eql(u8, existing, name)) {
                self.advance();
                return @intCast(i);
            }
        }
        // New named param
        if (self.param_name_count >= 32) return ParseError.InvalidSyntax;
        const idx = self.param_name_count;
        self.param_names[idx] = name;
        self.param_name_count += 1;
        self.param_count = self.param_name_count;
        self.advance();
        return idx;
    }

    fn advance(self: *Self) void {
        self.current = self.lexer.nextToken();
    }

    fn expect(self: *Self, expected: TokenType) ParseError!void {
        if (self.current.type != expected) {
            return ParseError.UnexpectedToken;
        }
        self.advance();
    }

    fn expectIdentifier(self: *Self) ParseError![]const u8 {
        if (self.current.type != .identifier) {
            return ParseError.UnexpectedToken;
        }
        const text = self.current.text;
        self.advance();
        return text;
    }
};

// Tests

test "parse CREATE TABLE" {
    var parser = Parser.init(std.testing.allocator, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email TEXT)");
    defer parser.deinit();

    const stmt = try parser.parse();
    const ct = stmt.create_table;

    try std.testing.expectEqualStrings("users", ct.table_name);
    try std.testing.expectEqual(@as(usize, 3), ct.columns.len);

    try std.testing.expectEqualStrings("id", ct.columns[0].name);
    try std.testing.expectEqual(ast.DataType.int, ct.columns[0].data_type);
    try std.testing.expect(ct.columns[0].is_primary_key);
    try std.testing.expect(!ct.columns[0].nullable);

    try std.testing.expectEqualStrings("name", ct.columns[1].name);
    try std.testing.expectEqual(ast.DataType.varchar, ct.columns[1].data_type);
    try std.testing.expectEqual(@as(u16, 255), ct.columns[1].max_length);

    try std.testing.expectEqualStrings("email", ct.columns[2].name);
    try std.testing.expectEqual(ast.DataType.text, ct.columns[2].data_type);
}

test "parse CREATE TABLE with NOT NULL" {
    var parser = Parser.init(std.testing.allocator, "CREATE TABLE t (id INT NOT NULL, name TEXT NOT NULL DEFAULT 'x', note TEXT)");
    defer parser.deinit();

    const stmt = try parser.parse();
    const ct = stmt.create_table;

    try std.testing.expect(!ct.columns[0].nullable);
    try std.testing.expect(!ct.columns[0].is_primary_key);
    try std.testing.expect(!ct.columns[1].nullable);
    try std.testing.expectEqualStrings("x", ct.columns[1].default_value.?.string);
    try std.testing.expect(ct.columns[2].nullable);
}

test "parse INSERT" {
    var parser = Parser.init(std.testing.allocator, "INSERT INTO users VALUES (1, 'alice', 'alice@example.com')");
    defer parser.deinit();

    const stmt = try parser.parse();
    const ins = stmt.insert;

    try std.testing.expectEqualStrings("users", ins.table_name);
    try std.testing.expectEqual(@as(usize, 1), ins.rows.len);
    try std.testing.expectEqual(@as(usize, 3), ins.rows[0].len);
    try std.testing.expectEqual(@as(i64, 1), ins.rows[0][0].integer);
    try std.testing.expectEqualStrings("alice", ins.rows[0][1].string);
    try std.testing.expectEqualStrings("alice@example.com", ins.rows[0][2].string);
}

test "parse SELECT with WHERE" {
    var parser = Parser.init(std.testing.allocator, "SELECT name, email FROM users WHERE id = 1");
    defer parser.deinit();

    const stmt = try parser.parse();
    const sel = stmt.select;

    try std.testing.expectEqualStrings("users", sel.table_name);
    try std.testing.expectEqual(@as(usize, 2), sel.columns.len);
    try std.testing.expectEqualStrings("name", sel.columns[0].named);
    try std.testing.expectEqualStrings("email", sel.columns[1].named);

    // WHERE id = 1
    const where = sel.where_clause.?;
    try std.testing.expectEqualStrings("id", where.comparison.left.column_ref);
    try std.testing.expectEqual(ast.CompOp.eq, where.comparison.op);
    try std.testing.expectEqual(@as(i64, 1), where.comparison.right.literal.integer);
}

test "parse SELECT *" {
    var parser = Parser.init(std.testing.allocator, "SELECT * FROM users");
    defer parser.deinit();

    const stmt = try parser.parse();
    const sel = stmt.select;

    try std.testing.expectEqual(@as(usize, 1), sel.columns.len);
    try std.testing.expectEqual(ast.SelectColumn{ .all_columns = {} }, sel.columns[0]);
}

test "parse DELETE with compound WHERE" {
    var parser = Parser.init(std.testing.allocator, "DELETE FROM users WHERE id > 5 AND name = 'bob'");
    defer parser.deinit();

    const stmt = try parser.parse();
    const del = stmt.delete;

    try std.testing.expectEqualStrings("users", del.table_name);

    // WHERE (id > 5) AND (name = 'bob')
    const where = del.where_clause.?;
    const left = where.and_expr.left;
    const right = where.and_expr.right;

    try std.testing.expectEqual(ast.CompOp.gt, left.comparison.op);
    try std.testing.expectEqualStrings("id", left.comparison.left.column_ref);

    try std.testing.expectEqual(ast.CompOp.eq, right.comparison.op);
    try std.testing.expectEqualStrings("name", right.comparison.left.column_ref);
    try std.testing.expectEqualStrings("bob", right.comparison.right.literal.string);
}

test "parse SELECT with ORDER BY and LIMIT" {
    {
        var p = Parser.init(std.testing.allocator, "SELECT * FROM users ORDER BY name DESC LIMIT 10");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        try std.testing.expectEqualStrings("users", sel.table_name);
        const ob = sel.order_by.?;
        try std.testing.expectEqual(@as(usize, 1), ob.len);
        try std.testing.expectEqualStrings("name", ob[0].column);
        try std.testing.expect(!ob[0].ascending);
        try std.testing.expectEqual(@as(u64, 10), sel.limit.?);
    }
    {
        // ORDER BY multiple columns, default ASC
        var p = Parser.init(std.testing.allocator, "SELECT id, name FROM t ORDER BY id, name ASC");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        const ob = sel.order_by.?;
        try std.testing.expectEqual(@as(usize, 2), ob.len);
        try std.testing.expect(ob[0].ascending);
        try std.testing.expect(ob[1].ascending);
        try std.testing.expect(sel.limit == null);
    }
}

test "parse UPDATE" {
    {
        // UPDATE with WHERE
        var p = Parser.init(std.testing.allocator, "UPDATE users SET name = 'Bob' WHERE id = 1");
        defer p.deinit();
        const stmt = try p.parse();
        const upd = stmt.update;
        try std.testing.expectEqualStrings("users", upd.table_name);
        try std.testing.expectEqual(@as(usize, 1), upd.assignments.len);
        try std.testing.expectEqualStrings("name", upd.assignments[0].column);
        try std.testing.expectEqualStrings("Bob", upd.assignments[0].value.literal.string);
        try std.testing.expect(upd.where_clause != null);
    }
    {
        // UPDATE multiple columns, no WHERE
        var p = Parser.init(std.testing.allocator, "UPDATE items SET price = 9.99, name = 'Widget'");
        defer p.deinit();
        const stmt = try p.parse();
        const upd = stmt.update;
        try std.testing.expectEqualStrings("items", upd.table_name);
        try std.testing.expectEqual(@as(usize, 2), upd.assignments.len);
        try std.testing.expectEqualStrings("price", upd.assignments[0].column);
        try std.testing.expectEqualStrings("name", upd.assignments[1].column);
        try std.testing.expect(upd.where_clause == null);
    }
}

test "parse SELECT with aggregates" {
    {
        var p = Parser.init(std.testing.allocator, "SELECT COUNT(*) FROM users");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        try std.testing.expectEqual(@as(usize, 1), sel.columns.len);
        const agg = sel.columns[0].aggregate;
        try std.testing.expectEqual(ast.AggregateFunc.count, agg.func);
        try std.testing.expect(agg.column == null);
    }
    {
        var p = Parser.init(std.testing.allocator, "SELECT SUM(price), AVG(price), MIN(id), MAX(id) FROM items");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        try std.testing.expectEqual(@as(usize, 4), sel.columns.len);
        try std.testing.expectEqual(ast.AggregateFunc.sum, sel.columns[0].aggregate.func);
        try std.testing.expectEqualStrings("price", sel.columns[0].aggregate.column.?);
        try std.testing.expectEqual(ast.AggregateFunc.avg, sel.columns[1].aggregate.func);
        try std.testing.expectEqual(ast.AggregateFunc.min, sel.columns[2].aggregate.func);
        try std.testing.expectEqual(ast.AggregateFunc.max, sel.columns[3].aggregate.func);
    }
    {
        // COUNT(column_name)
        var p = Parser.init(std.testing.allocator, "SELECT COUNT(name) FROM users");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        try std.testing.expectEqual(ast.AggregateFunc.count, sel.columns[0].aggregate.func);
        try std.testing.expectEqualStrings("name", sel.columns[0].aggregate.column.?);
    }
}

test "parse COUNT(DISTINCT col)" {
    var p = Parser.init(std.testing.allocator, "SELECT COUNT(DISTINCT name) FROM users");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const agg = sel.columns[0].aggregate;
    try std.testing.expectEqual(ast.AggregateFunc.count, agg.func);
    try std.testing.expectEqualStrings("name", agg.column.?);
    try std.testing.expect(agg.distinct);
}

test "parse SELECT with GROUP BY" {
    {
        var p = Parser.init(std.testing.allocator, "SELECT dept, COUNT(*) FROM employees GROUP BY dept");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        try std.testing.expectEqual(@as(usize, 2), sel.columns.len);
        try std.testing.expectEqualStrings("dept", sel.columns[0].named);
        try std.testing.expectEqual(ast.AggregateFunc.count, sel.columns[1].aggregate.func);
        const gb = sel.group_by.?;
        try std.testing.expectEqual(@as(usize, 1), gb.len);
        try std.testing.expectEqualStrings("dept", gb[0]);
    }
    {
        // GROUP BY with HAVING (simple column comparison)
        var p = Parser.init(std.testing.allocator, "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING dept = 'Sales'");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        try std.testing.expect(sel.group_by != null);
        try std.testing.expect(sel.having_clause != null);
    }
    {
        // GROUP BY multiple columns + ORDER BY
        var p = Parser.init(std.testing.allocator, "SELECT dept, role, COUNT(*) FROM emp GROUP BY dept, role ORDER BY dept");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        const gb = sel.group_by.?;
        try std.testing.expectEqual(@as(usize, 2), gb.len);
        try std.testing.expectEqualStrings("dept", gb[0]);
        try std.testing.expectEqualStrings("role", gb[1]);
        try std.testing.expect(sel.order_by != null);
    }
}

test "parse SELECT with JOIN" {
    {
        // INNER JOIN
        var p = Parser.init(std.testing.allocator, "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        const join_list = sel.joins.?;
        try std.testing.expectEqual(@as(usize, 1), join_list.len);
        try std.testing.expectEqual(ast.JoinType.inner, join_list[0].join_type);
        try std.testing.expectEqualStrings("orders", join_list[0].table_name);
        // ON condition should be a comparison of qualified refs
        const on_cond = join_list[0].on_condition.?;
        try std.testing.expectEqualStrings("users", on_cond.comparison.left.qualified_ref.table);
        try std.testing.expectEqualStrings("id", on_cond.comparison.left.qualified_ref.column);
    }
    {
        // LEFT JOIN
        var p = Parser.init(std.testing.allocator, "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
        defer p.deinit();
        const stmt = try p.parse();
        const sel = stmt.select;
        const join_list = sel.joins.?;
        try std.testing.expectEqual(ast.JoinType.left, join_list[0].join_type);
    }
}

test "parse BEGIN, COMMIT, ROLLBACK" {
    {
        var p = Parser.init(std.testing.allocator, "BEGIN;");
        defer p.deinit();
        const stmt = try p.parse();
        try std.testing.expect(stmt == .begin_txn);
    }
    {
        var p = Parser.init(std.testing.allocator, "COMMIT");
        defer p.deinit();
        const stmt = try p.parse();
        try std.testing.expect(stmt == .commit_txn);
    }
    {
        var p = Parser.init(std.testing.allocator, "rollback;");
        defer p.deinit();
        const stmt = try p.parse();
        try std.testing.expect(stmt == .rollback_txn);
    }
}

test "parse empty input" {
    var p = Parser.init(std.testing.allocator, "");
    defer p.deinit();
    try std.testing.expectError(ParseError.UnexpectedEof, p.parse());
}

test "parse whitespace only" {
    var p = Parser.init(std.testing.allocator, "   \t\n  ");
    defer p.deinit();
    try std.testing.expectError(ParseError.UnexpectedEof, p.parse());
}

test "parse SELECT without FROM" {
    var p = Parser.init(std.testing.allocator, "SELECT *");
    defer p.deinit();
    try std.testing.expectError(ParseError.UnexpectedToken, p.parse());
}

test "parse INSERT missing VALUES" {
    var p = Parser.init(std.testing.allocator, "INSERT INTO t");
    defer p.deinit();
    try std.testing.expectError(ParseError.UnexpectedToken, p.parse());
}

test "parse gibberish" {
    var p = Parser.init(std.testing.allocator, "GOBBLEDYGOOK");
    defer p.deinit();
    try std.testing.expectError(ParseError.UnexpectedToken, p.parse());
}

test "parse SELECT with GROUP BY no aggregates" {
    // Valid syntax — GROUP BY without aggregates is syntactically valid
    var p = Parser.init(std.testing.allocator, "SELECT name FROM t GROUP BY name");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    try std.testing.expect(sel.group_by != null);
    try std.testing.expectEqual(@as(usize, 1), sel.group_by.?.len);
    try std.testing.expectEqualStrings("name", sel.group_by.?[0]);
}

test "parse CREATE TABLE single column" {
    var p = Parser.init(std.testing.allocator, "CREATE TABLE t (id INT)");
    defer p.deinit();
    const stmt = try p.parse();
    const ct = stmt.create_table;
    try std.testing.expectEqualStrings("t", ct.table_name);
    try std.testing.expectEqual(@as(usize, 1), ct.columns.len);
}

test "parse DELETE without WHERE" {
    var p = Parser.init(std.testing.allocator, "DELETE FROM users");
    defer p.deinit();
    const stmt = try p.parse();
    const del = stmt.delete;
    try std.testing.expectEqualStrings("users", del.table_name);
    try std.testing.expect(del.where_clause == null);
}

test "parse UPDATE without WHERE" {
    var p = Parser.init(std.testing.allocator, "UPDATE t SET val = 42");
    defer p.deinit();
    const stmt = try p.parse();
    const upd = stmt.update;
    try std.testing.expectEqualStrings("t", upd.table_name);
    try std.testing.expect(upd.where_clause == null);
    try std.testing.expectEqual(@as(usize, 1), upd.assignments.len);
}

test "parse SELECT with aliases" {
    var p = Parser.init(std.testing.allocator, "SELECT name AS n, id AS i FROM users");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    try std.testing.expectEqual(@as(usize, 2), sel.columns.len);
    try std.testing.expect(sel.aliases != null);
    try std.testing.expectEqualStrings("n", sel.aliases.?[0].?);
    try std.testing.expectEqualStrings("i", sel.aliases.?[1].?);
}

test "parse SELECT DISTINCT" {
    var p = Parser.init(std.testing.allocator, "SELECT DISTINCT name FROM users");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    try std.testing.expect(sel.distinct);
    try std.testing.expectEqual(@as(usize, 1), sel.columns.len);
}

test "parse BETWEEN" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE id BETWEEN 1 AND 10");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    try std.testing.expect(where.* == .between_expr);
}

test "parse LIKE" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE name LIKE 'A%'");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    try std.testing.expect(where.* == .like_expr);
}

test "parse IN list" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE id IN (1, 2, 3)");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    try std.testing.expect(where.* == .in_list);
    try std.testing.expectEqual(@as(usize, 3), where.in_list.items.len);
}

test "parse ALTER TABLE ADD COLUMN" {
    {
        var p = Parser.init(std.testing.allocator, "ALTER TABLE users ADD COLUMN email TEXT");
        defer p.deinit();
        const stmt = try p.parse();
        const at = stmt.alter_table;
        try std.testing.expectEqualStrings("users", at.table_name);
        try std.testing.expectEqualStrings("email", at.action.add_column.name);
        try std.testing.expectEqual(ast.DataType.text, at.action.add_column.data_type);
    }
    {
        // Without COLUMN keyword
        var p = Parser.init(std.testing.allocator, "ALTER TABLE users ADD score FLOAT");
        defer p.deinit();
        const stmt = try p.parse();
        const at = stmt.alter_table;
        try std.testing.expectEqualStrings("score", at.action.add_column.name);
    }
}

test "parse IN subquery" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE id IN (SELECT id FROM other)");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    try std.testing.expect(where.* == .in_subquery);
    try std.testing.expectEqualStrings("other", where.in_subquery.subquery.table_name);
}

test "parse EXISTS subquery" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE EXISTS (SELECT * FROM other WHERE other.id = 1)");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    try std.testing.expect(where.* == .exists_subquery);
    try std.testing.expectEqualStrings("other", where.exists_subquery.subquery.table_name);
}

test "parse CREATE INDEX" {
    var p = Parser.init(std.testing.allocator, "CREATE INDEX idx_users_id ON users (id)");
    defer p.deinit();
    const stmt = try p.parse();
    const ci = stmt.create_index;
    try std.testing.expectEqualStrings("idx_users_id", ci.index_name);
    try std.testing.expectEqualStrings("users", ci.table_name);
    try std.testing.expectEqualStrings("id", ci.column_name);
    try std.testing.expect(!ci.is_unique);
}

test "parse CREATE UNIQUE INDEX" {
    var p = Parser.init(std.testing.allocator, "CREATE UNIQUE INDEX idx_u ON t (col)");
    defer p.deinit();
    const stmt = try p.parse();
    const ci = stmt.create_index;
    try std.testing.expectEqualStrings("idx_u", ci.index_name);
    try std.testing.expect(ci.is_unique);
}

test "parse DROP INDEX" {
    var p = Parser.init(std.testing.allocator, "DROP INDEX idx_users_id");
    defer p.deinit();
    const stmt = try p.parse();
    const di = stmt.drop_index;
    try std.testing.expectEqualStrings("idx_users_id", di.index_name);
}

test "parse EXPLAIN" {
    var p = Parser.init(std.testing.allocator, "EXPLAIN SELECT * FROM users WHERE id = 1");
    defer p.deinit();
    const stmt = try p.parse();
    const ex = stmt.explain;
    try std.testing.expectEqualStrings("users", ex.select.table_name);
    try std.testing.expect(ex.select.where_clause != null);
}

test "parse INSERT with positional params" {
    var p = Parser.init(std.testing.allocator, "INSERT INTO t VALUES ($1, $2, $3)");
    defer p.deinit();
    const stmt = try p.parse();
    const ins = stmt.insert;
    try std.testing.expectEqual(@as(usize, 1), ins.rows.len);
    try std.testing.expectEqual(@as(usize, 3), ins.rows[0].len);
    try std.testing.expectEqual(@as(u32, 0), ins.rows[0][0].parameter);
    try std.testing.expectEqual(@as(u32, 1), ins.rows[0][1].parameter);
    try std.testing.expectEqual(@as(u32, 2), ins.rows[0][2].parameter);
    try std.testing.expectEqual(@as(u32, 3), p.param_count);
}

test "parse SELECT with positional param in WHERE" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE id = $1");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    try std.testing.expectEqual(@as(u32, 0), where.comparison.right.literal.parameter);
    try std.testing.expectEqual(@as(u32, 1), p.param_count);
}

test "parse named params with reuse" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE a = @x OR b = @x");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    // Both @x should map to the same index 0
    const left_cmp = where.or_expr.left.comparison.right.literal.parameter;
    const right_cmp = where.or_expr.right.comparison.right.literal.parameter;
    try std.testing.expectEqual(@as(u32, 0), left_cmp);
    try std.testing.expectEqual(@as(u32, 0), right_cmp);
    try std.testing.expectEqual(@as(u32, 1), p.param_count);
}

test "parse mixing positional and named params fails" {
    var p = Parser.init(std.testing.allocator, "INSERT INTO t VALUES ($1, @name)");
    defer p.deinit();
    try std.testing.expectError(ParseError.InvalidSyntax, p.parse());
}

test "parse LOWER function in SELECT" {
    var p = Parser.init(std.testing.allocator, "SELECT LOWER(name) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    try std.testing.expectEqual(@as(usize, 1), sel.columns.len);
    try std.testing.expectEqual(ast.SelectColumn.expression, std.meta.activeTag(sel.columns[0]));
    const expr = sel.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.lower, expr.function_call.func);
    try std.testing.expectEqual(@as(usize, 1), expr.function_call.args.len);
}

test "parse UPPER function in WHERE" {
    var p = Parser.init(std.testing.allocator, "SELECT name FROM t WHERE UPPER(name) = 'FOO'");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    try std.testing.expect(sel.where_clause != null);
    // WHERE is a comparison: UPPER(name) = 'FOO'
    const cmp = sel.where_clause.?.comparison;
    try std.testing.expectEqual(ast.BuiltinFunction.upper, cmp.left.function_call.func);
}

test "parse CONCAT with multiple args" {
    var p = Parser.init(std.testing.allocator, "SELECT CONCAT(a, b, c) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const expr = sel.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.concat, expr.function_call.func);
    try std.testing.expectEqual(@as(usize, 3), expr.function_call.args.len);
}

test "parse SUBSTRING with 2 args" {
    var p = Parser.init(std.testing.allocator, "SELECT SUBSTRING(name, 1) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.substring, expr.function_call.func);
    try std.testing.expectEqual(@as(usize, 2), expr.function_call.args.len);
}

test "parse SUBSTRING with 3 args" {
    var p = Parser.init(std.testing.allocator, "SELECT SUBSTRING(name, 1, 3) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expectEqual(@as(usize, 3), expr.function_call.args.len);
}

test "parse searched CASE expression" {
    var p = Parser.init(std.testing.allocator, "SELECT CASE WHEN x > 5 THEN 'big' ELSE 'small' END FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    const ce = expr.case_expr;
    try std.testing.expectEqual(@as(usize, 1), ce.when_clauses.len);
    try std.testing.expect(ce.else_result != null);
}

test "parse simple CASE expression" {
    var p = Parser.init(std.testing.allocator, "SELECT CASE status WHEN 1 THEN 'on' WHEN 0 THEN 'off' END FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    const ce = expr.case_expr;
    // Simple CASE desugars to searched: each WHEN becomes a comparison
    try std.testing.expectEqual(@as(usize, 2), ce.when_clauses.len);
    try std.testing.expectEqual(ast.CompOp.eq, ce.when_clauses[0].condition.comparison.op);
    try std.testing.expect(ce.else_result == null);
}

test "parse CASE without WHEN fails" {
    var p = Parser.init(std.testing.allocator, "SELECT CASE END FROM t");
    defer p.deinit();
    try std.testing.expectError(ParseError.UnexpectedToken, p.parse());
}

test "parse nested function in SELECT" {
    var p = Parser.init(std.testing.allocator, "SELECT UPPER(SUBSTRING(name, 1, 3)) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.upper, expr.function_call.func);
    // The argument to UPPER is itself a function_call (SUBSTRING)
    const inner = expr.function_call.args[0];
    try std.testing.expectEqual(ast.BuiltinFunction.substring, inner.function_call.func);
}

test "parse LOWER with wrong arg count fails" {
    var p = Parser.init(std.testing.allocator, "SELECT LOWER(a, b) FROM t");
    defer p.deinit();
    try std.testing.expectError(ParseError.InvalidSyntax, p.parse());
}

test "parse CONCAT with one arg fails" {
    var p = Parser.init(std.testing.allocator, "SELECT CONCAT(a) FROM t");
    defer p.deinit();
    try std.testing.expectError(ParseError.InvalidSyntax, p.parse());
}

test "parse LENGTH function" {
    var p = Parser.init(std.testing.allocator, "SELECT LENGTH(name) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.length, expr.function_call.func);
}

test "parse TRIM function" {
    var p = Parser.init(std.testing.allocator, "SELECT TRIM(name) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.trim, expr.function_call.func);
}

test "parse function with alias" {
    var p = Parser.init(std.testing.allocator, "SELECT LOWER(name) AS lname FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    try std.testing.expectEqual(ast.SelectColumn.expression, std.meta.activeTag(sel.columns[0]));
    const alias = sel.aliases.?[0].?;
    try std.testing.expectEqualStrings("lname", alias);
}

test "parse arithmetic in WHERE" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE a + b > 10");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    // Should be comparison: (a + b) > 10
    try std.testing.expect(where.* == .comparison);
    const cmp = where.comparison;
    try std.testing.expectEqual(ast.CompOp.gt, cmp.op);
    try std.testing.expect(cmp.left.* == .arithmetic);
    try std.testing.expectEqual(ast.ArithOp.add, cmp.left.arithmetic.op);
}

test "parse arithmetic precedence" {
    // a + b * c should parse as a + (b * c)
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE a + b * c = 10");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    const cmp = where.comparison;
    const add = cmp.left.arithmetic;
    try std.testing.expectEqual(ast.ArithOp.add, add.op);
    // right side of add should be mul
    try std.testing.expect(add.right.* == .arithmetic);
    try std.testing.expectEqual(ast.ArithOp.mul, add.right.arithmetic.op);
}

test "parse arithmetic in SELECT column" {
    var p = Parser.init(std.testing.allocator, "SELECT price * quantity FROM orders");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    try std.testing.expectEqual(@as(usize, 1), sel.columns.len);
    try std.testing.expect(sel.columns[0] == .expression);
    const expr = sel.columns[0].expression;
    try std.testing.expect(expr.* == .arithmetic);
    try std.testing.expectEqual(ast.ArithOp.mul, expr.arithmetic.op);
}

test "parse unary minus" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE x = -5");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    const cmp = where.comparison;
    try std.testing.expect(cmp.right.* == .unary_minus);
}

test "parse arithmetic with parens" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE (a + b) * c = 10");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    const where = sel.where_clause.?;
    const cmp = where.comparison;
    const mul = cmp.left.arithmetic;
    try std.testing.expectEqual(ast.ArithOp.mul, mul.op);
    // left side of mul should be add (from parens)
    try std.testing.expect(mul.left.* == .arithmetic);
    try std.testing.expectEqual(ast.ArithOp.add, mul.left.arithmetic.op);
}

test "parse arithmetic with alias" {
    var p = Parser.init(std.testing.allocator, "SELECT a + b AS total FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const sel = stmt.select;
    try std.testing.expect(sel.columns[0] == .expression);
    const alias = sel.aliases.?[0].?;
    try std.testing.expectEqualStrings("total", alias);
}

test "parse multi-row INSERT" {
    var p = Parser.init(std.testing.allocator, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    defer p.deinit();
    const stmt = try p.parse();
    const ins = stmt.insert;
    try std.testing.expectEqual(@as(usize, 3), ins.rows.len);
    try std.testing.expectEqual(@as(i64, 1), ins.rows[0][0].integer);
    try std.testing.expectEqualStrings("a", ins.rows[0][1].string);
    try std.testing.expectEqual(@as(i64, 2), ins.rows[1][0].integer);
    try std.testing.expectEqualStrings("b", ins.rows[1][1].string);
    try std.testing.expectEqual(@as(i64, 3), ins.rows[2][0].integer);
    try std.testing.expectEqualStrings("c", ins.rows[2][1].string);
}

test "parse IS NULL" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE x IS NULL");
    defer p.deinit();
    const stmt = try p.parse();
    const where = stmt.select.where_clause.?;
    try std.testing.expect(where.* == .is_null);
    try std.testing.expect(!where.is_null.negated);
}

test "parse IS NOT NULL" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE x IS NOT NULL");
    defer p.deinit();
    const stmt = try p.parse();
    const where = stmt.select.where_clause.?;
    try std.testing.expect(where.* == .is_null);
    try std.testing.expect(where.is_null.negated);
}

test "parse COALESCE" {
    var p = Parser.init(std.testing.allocator, "SELECT COALESCE(a, b, 0) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expect(stmt.select.columns[0] == .expression);
    const expr = stmt.select.columns[0].expression;
    try std.testing.expect(expr.* == .function_call);
    try std.testing.expectEqual(ast.BuiltinFunction.coalesce, expr.function_call.func);
    try std.testing.expectEqual(@as(usize, 3), expr.function_call.args.len);
}

test "parse NULLIF" {
    var p = Parser.init(std.testing.allocator, "SELECT NULLIF(a, 0) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expect(stmt.select.columns[0] == .expression);
    const expr = stmt.select.columns[0].expression;
    try std.testing.expect(expr.* == .function_call);
    try std.testing.expectEqual(ast.BuiltinFunction.nullif, expr.function_call.func);
    try std.testing.expectEqual(@as(usize, 2), expr.function_call.args.len);
}

test "parse NULLIF wrong arg count fails" {
    var p = Parser.init(std.testing.allocator, "SELECT NULLIF(a) FROM t");
    defer p.deinit();
    try std.testing.expectError(ParseError.InvalidSyntax, p.parse());
}

test "parse ABS function" {
    var p = Parser.init(std.testing.allocator, "SELECT ABS(x) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.abs, expr.function_call.func);
}

test "parse ROUND with precision" {
    var p = Parser.init(std.testing.allocator, "SELECT ROUND(x, 2) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.round, expr.function_call.func);
    try std.testing.expectEqual(@as(usize, 2), expr.function_call.args.len);
}

test "parse MOD function" {
    var p = Parser.init(std.testing.allocator, "SELECT MOD(x, 3) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expectEqual(ast.BuiltinFunction.mod, expr.function_call.func);
    try std.testing.expectEqual(@as(usize, 2), expr.function_call.args.len);
}

test "parse CAST to INT" {
    var p = Parser.init(std.testing.allocator, "SELECT CAST(x AS INT) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expect(expr.* == .cast_expr);
    try std.testing.expectEqual(ast.DataType.int, expr.cast_expr.target_type);
}

test "parse CAST to FLOAT" {
    var p = Parser.init(std.testing.allocator, "SELECT CAST(x AS FLOAT) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expect(expr.* == .cast_expr);
    try std.testing.expectEqual(ast.DataType.float, expr.cast_expr.target_type);
}

test "parse CAST to TEXT" {
    var p = Parser.init(std.testing.allocator, "SELECT CAST(x AS TEXT) FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    const expr = stmt.select.columns[0].expression;
    try std.testing.expect(expr.* == .cast_expr);
    try std.testing.expectEqual(ast.DataType.text, expr.cast_expr.target_type);
}

test "parse CAST in WHERE" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE CAST(x AS INT) > 5");
    defer p.deinit();
    const stmt = try p.parse();
    const where = stmt.select.where_clause.?;
    try std.testing.expect(where.* == .comparison);
    try std.testing.expect(where.comparison.left.* == .cast_expr);
}

test "parse NOT LIKE" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE name NOT LIKE '%test%'");
    defer p.deinit();
    const stmt = try p.parse();
    const where = stmt.select.where_clause.?;
    try std.testing.expect(where.* == .not_expr);
    try std.testing.expect(where.not_expr.operand.* == .like_expr);
}

test "parse NOT IN" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE x NOT IN (1, 2, 3)");
    defer p.deinit();
    const stmt = try p.parse();
    const where = stmt.select.where_clause.?;
    try std.testing.expect(where.* == .not_expr);
    try std.testing.expect(where.not_expr.operand.* == .in_list);
}

test "parse NOT BETWEEN" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10");
    defer p.deinit();
    const stmt = try p.parse();
    const where = stmt.select.where_clause.?;
    try std.testing.expect(where.* == .not_expr);
    try std.testing.expect(where.not_expr.operand.* == .between_expr);
}

test "parse || concat in WHERE" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t WHERE a || b = 'hello'");
    defer p.deinit();
    const stmt = try p.parse();
    const where = stmt.select.where_clause.?;
    try std.testing.expect(where.* == .comparison);
    const lhs = where.comparison.left;
    try std.testing.expect(lhs.* == .function_call);
    try std.testing.expectEqual(ast.BuiltinFunction.concat, lhs.function_call.func);
    try std.testing.expectEqual(@as(usize, 2), lhs.function_call.args.len);
}

test "parse || concat in SELECT column" {
    var p = Parser.init(std.testing.allocator, "SELECT a || b FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expect(stmt.select.columns[0] == .expression);
    const expr = stmt.select.columns[0].expression;
    try std.testing.expect(expr.* == .function_call);
    try std.testing.expectEqual(ast.BuiltinFunction.concat, expr.function_call.func);
}

test "parse || concat chained" {
    var p = Parser.init(std.testing.allocator, "SELECT a || b || c FROM t");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expect(stmt.select.columns[0] == .expression);
    const expr = stmt.select.columns[0].expression;
    // (a || b) || c — outer is concat
    try std.testing.expect(expr.* == .function_call);
    try std.testing.expectEqual(ast.BuiltinFunction.concat, expr.function_call.func);
    // left arg is also concat
    try std.testing.expect(expr.function_call.args[0].* == .function_call);
}

test "parse UNION" {
    var p = Parser.init(std.testing.allocator, "SELECT a FROM t1 UNION SELECT b FROM t2");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expect(stmt == .union_query);
    try std.testing.expect(!stmt.union_query.all);
    try std.testing.expectEqualStrings("t1", stmt.union_query.left.table_name);
    try std.testing.expectEqualStrings("t2", stmt.union_query.right.table_name);
}

test "parse UNION ALL" {
    var p = Parser.init(std.testing.allocator, "SELECT a FROM t1 UNION ALL SELECT b FROM t2");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expect(stmt == .union_query);
    try std.testing.expect(stmt.union_query.all);
}

test "parse INSERT INTO SELECT" {
    var p = Parser.init(std.testing.allocator, "INSERT INTO t2 SELECT x FROM t1 WHERE x > 5");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expect(stmt == .insert_select);
    try std.testing.expectEqualStrings("t2", stmt.insert_select.table_name);
    try std.testing.expectEqualStrings("t1", stmt.insert_select.query.table_name);
    try std.testing.expect(stmt.insert_select.query.where_clause != null);
}

test "parse LIMIT OFFSET" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t LIMIT 10 OFFSET 20");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expectEqual(@as(u64, 10), stmt.select.limit.?);
    try std.testing.expectEqual(@as(u64, 20), stmt.select.offset.?);
}

test "parse LIMIT without OFFSET" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t LIMIT 5");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expectEqual(@as(u64, 5), stmt.select.limit.?);
    try std.testing.expect(stmt.select.offset == null);
}

test "parse DEFAULT column" {
    var p = Parser.init(std.testing.allocator, "CREATE TABLE t (x INT DEFAULT 42, name TEXT DEFAULT 'anon')");
    defer p.deinit();
    const stmt = try p.parse();
    const cols = stmt.create_table.columns;
    try std.testing.expectEqual(@as(i64, 42), cols[0].default_value.?.integer);
    try std.testing.expectEqualStrings("anon", cols[1].default_value.?.string);
}

test "parse RIGHT JOIN" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id");
    defer p.deinit();
    const stmt = try p.parse();
    const join_list = stmt.select.joins.?;
    try std.testing.expectEqual(ast.JoinType.right, join_list[0].join_type);
}

test "parse CROSS JOIN" {
    var p = Parser.init(std.testing.allocator, "SELECT * FROM t1 CROSS JOIN t2");
    defer p.deinit();
    const stmt = try p.parse();
    const join_list = stmt.select.joins.?;
    try std.testing.expectEqual(ast.JoinType.cross, join_list[0].join_type);
    try std.testing.expect(join_list[0].on_condition == null);
}

test "parse UPDATE with expression" {
    var p = Parser.init(std.testing.allocator, "UPDATE t SET x = x + 1 WHERE id = 5");
    defer p.deinit();
    const stmt = try p.parse();
    const upd = stmt.update;
    try std.testing.expectEqualStrings("x", upd.assignments[0].column);
    try std.testing.expect(upd.assignments[0].value.* == .arithmetic);
}

test "parse INSERT with column list" {
    var p = Parser.init(std.testing.allocator, "INSERT INTO t (a, b) VALUES (1, 2)");
    defer p.deinit();
    const stmt = try p.parse();
    try std.testing.expect(stmt == .insert);
    const cols = stmt.insert.columns.?;
    try std.testing.expectEqual(@as(usize, 2), cols.len);
    try std.testing.expectEqualStrings("a", cols[0]);
    try std.testing.expectEqualStrings("b", cols[1]);
}
