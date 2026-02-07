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
            .kw_select => ast.Statement{ .select = try self.parseSelect() },
            .kw_insert => ast.Statement{ .insert = try self.parseInsert() },
            .kw_update => ast.Statement{ .update = try self.parseUpdate() },
            .kw_delete => ast.Statement{ .delete = try self.parseDelete() },
            .kw_create => ast.Statement{ .create_table = try self.parseCreateTable() },
            .kw_drop => ast.Statement{ .drop_table = try self.parseDropTable() },
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
        const table_name = try self.expectIdentifier();

        // Optional JOIN clauses
        var joins: ?[]const ast.JoinClause = null;
        {
            var join_list: std.ArrayList(ast.JoinClause) = .empty;
            while (self.current.type == .kw_join or self.current.type == .kw_inner or self.current.type == .kw_left) {
                var join_type: ast.JoinType = .inner;
                if (self.current.type == .kw_left) {
                    join_type = .left;
                    self.advance();
                } else if (self.current.type == .kw_inner) {
                    self.advance();
                }
                try self.expect(.kw_join);
                const join_table = try self.expectIdentifier();
                try self.expect(.kw_on);
                const on_cond = try self.parseExpression();
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

        // Optional LIMIT n
        var limit: ?u64 = null;
        if (self.current.type == .kw_limit) {
            self.advance();
            if (self.current.type != .integer_literal) return ParseError.UnexpectedToken;
            limit = std.fmt.parseInt(u64, self.current.text, 10) catch return ParseError.InvalidSyntax;
            self.advance();
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
            .joins = joins,
            .where_clause = where,
            .group_by = group_by,
            .having_clause = having,
            .order_by = order_by,
            .limit = limit,
        };
    }

    // ============================================================
    // INSERT INTO table VALUES (val, val, ...)
    // ============================================================
    fn parseInsert(self: *Self) ParseError!ast.Insert {
        try self.expect(.kw_insert);
        try self.expect(.kw_into);
        const table_name = try self.expectIdentifier();
        try self.expect(.kw_values);
        try self.expect(.left_paren);

        const alloc = self.arena();
        var vals: std.ArrayList(ast.LiteralValue) = .empty;
        vals.append(alloc, try self.parseLiteral()) catch return ParseError.OutOfMemory;
        while (self.current.type == .comma) {
            self.advance();
            vals.append(alloc, try self.parseLiteral()) catch return ParseError.OutOfMemory;
        }

        try self.expect(.right_paren);

        return .{
            .table_name = table_name,
            .values = vals.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
        };
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

    fn parseSelectColumn(self: *Self) ParseError!ast.SelectColumn {
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

            var column: ?[]const u8 = null;
            if (self.current.type == .op_star) {
                // COUNT(*)
                self.advance();
            } else {
                column = try self.expectIdentifier();
            }

            try self.expect(.right_paren);
            return .{ .aggregate = .{ .func = func, .column = column } };
        }

        const first = try self.expectIdentifier();
        // Handle table.column — preserve as qualified reference
        if (self.current.type == .dot) {
            self.advance();
            return .{ .qualified = .{ .table = first, .column = try self.expectIdentifier() } };
        }
        return .{ .named = first };
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
        const value = try self.parseLiteral();
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
        cols.append(alloc, try self.parseColumnDef()) catch return ParseError.OutOfMemory;
        while (self.current.type == .comma) {
            self.advance();
            cols.append(alloc, try self.parseColumnDef()) catch return ParseError.OutOfMemory;
        }

        try self.expect(.right_paren);

        return .{
            .table_name = table_name,
            .columns = cols.toOwnedSlice(alloc) catch return ParseError.OutOfMemory,
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
        if (self.current.type == .kw_primary) {
            self.advance();
            try self.expect(.kw_key);
            is_primary_key = true;
        }

        return .{
            .name = name,
            .data_type = data_type,
            .max_length = max_length,
            .nullable = !is_primary_key,
            .is_primary_key = is_primary_key,
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
        const left = try self.parsePrimary();
        const alloc = self.arena();

        // BETWEEN low AND high
        if (self.current.type == .kw_between) {
            self.advance();
            const low = try self.parsePrimary();
            try self.expect(.kw_and);
            const high = try self.parsePrimary();
            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .between_expr = .{ .value = left, .low = low, .high = high } };
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
            const right = try self.parsePrimary();
            const expr = try alloc.create(ast.Expression);
            expr.* = .{ .comparison = .{ .left = left, .op = comp_op, .right = right } };
            return expr;
        }

        return left;
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
            else => return ParseError.UnexpectedToken,
        }
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

test "parse INSERT" {
    var parser = Parser.init(std.testing.allocator, "INSERT INTO users VALUES (1, 'alice', 'alice@example.com')");
    defer parser.deinit();

    const stmt = try parser.parse();
    const ins = stmt.insert;

    try std.testing.expectEqualStrings("users", ins.table_name);
    try std.testing.expectEqual(@as(usize, 3), ins.values.len);
    try std.testing.expectEqual(@as(i64, 1), ins.values[0].integer);
    try std.testing.expectEqualStrings("alice", ins.values[1].string);
    try std.testing.expectEqualStrings("alice@example.com", ins.values[2].string);
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
        try std.testing.expectEqualStrings("Bob", upd.assignments[0].value.string);
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
        const on_cond = join_list[0].on_condition;
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
