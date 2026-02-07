const std = @import("std");

pub const TokenType = enum {
    // Keywords
    kw_select,
    kw_from,
    kw_where,
    kw_insert,
    kw_into,
    kw_values,
    kw_delete,
    kw_update,
    kw_set,
    kw_create,
    kw_drop,
    kw_table,
    kw_and,
    kw_or,
    kw_not,
    kw_null,
    kw_true,
    kw_false,
    kw_primary,
    kw_key,

    // Query modifiers
    kw_order,
    kw_by,
    kw_asc,
    kw_desc,
    kw_limit,

    // Aggregate functions
    kw_count,
    kw_sum,
    kw_avg,
    kw_min,
    kw_max,

    // GROUP BY / HAVING
    kw_group,
    kw_having,

    // JOIN
    kw_join,
    kw_inner,
    kw_left,
    kw_on,

    // Aliases and modifiers
    kw_as,
    kw_distinct,

    // SQL operators
    kw_between,
    kw_like,
    kw_in,
    kw_exists,

    // ALTER TABLE
    kw_alter,
    kw_add,
    kw_column,

    // Transaction keywords
    kw_begin,
    kw_commit,
    kw_rollback,

    // Data types
    kw_int,
    kw_integer,
    kw_bigint,
    kw_float,
    kw_boolean,
    kw_varchar,
    kw_text,

    // Identifiers and literals
    identifier,
    integer_literal,
    float_literal,
    string_literal,

    // Operators
    op_eq, // =
    op_neq, // != or <>
    op_lt, // <
    op_gt, // >
    op_lte, // <=
    op_gte, // >=
    op_plus, // +
    op_minus, // -
    op_star, // *
    op_slash, // /

    // Punctuation
    left_paren, // (
    right_paren, // )
    comma, // ,
    semicolon, // ;
    dot, // .

    // Special
    eof,
    invalid,
};

pub const Token = struct {
    type: TokenType,
    text: []const u8,
    position: usize,
};

const keywords = std.StaticStringMap(TokenType).initComptime(.{
    .{ "select", .kw_select },
    .{ "from", .kw_from },
    .{ "where", .kw_where },
    .{ "insert", .kw_insert },
    .{ "into", .kw_into },
    .{ "values", .kw_values },
    .{ "delete", .kw_delete },
    .{ "update", .kw_update },
    .{ "set", .kw_set },
    .{ "create", .kw_create },
    .{ "drop", .kw_drop },
    .{ "table", .kw_table },
    .{ "and", .kw_and },
    .{ "or", .kw_or },
    .{ "not", .kw_not },
    .{ "null", .kw_null },
    .{ "true", .kw_true },
    .{ "false", .kw_false },
    .{ "primary", .kw_primary },
    .{ "key", .kw_key },
    .{ "order", .kw_order },
    .{ "by", .kw_by },
    .{ "asc", .kw_asc },
    .{ "desc", .kw_desc },
    .{ "limit", .kw_limit },
    .{ "count", .kw_count },
    .{ "sum", .kw_sum },
    .{ "avg", .kw_avg },
    .{ "min", .kw_min },
    .{ "max", .kw_max },
    .{ "group", .kw_group },
    .{ "having", .kw_having },
    .{ "join", .kw_join },
    .{ "inner", .kw_inner },
    .{ "left", .kw_left },
    .{ "on", .kw_on },
    .{ "add", .kw_add },
    .{ "alter", .kw_alter },
    .{ "as", .kw_as },
    .{ "between", .kw_between },
    .{ "column", .kw_column },
    .{ "distinct", .kw_distinct },
    .{ "exists", .kw_exists },
    .{ "in", .kw_in },
    .{ "like", .kw_like },
    .{ "begin", .kw_begin },
    .{ "commit", .kw_commit },
    .{ "rollback", .kw_rollback },
    .{ "int", .kw_int },
    .{ "integer", .kw_integer },
    .{ "bigint", .kw_bigint },
    .{ "float", .kw_float },
    .{ "boolean", .kw_boolean },
    .{ "varchar", .kw_varchar },
    .{ "text", .kw_text },
});

pub const Lexer = struct {
    input: []const u8,
    pos: usize,
    // Scratch buffer for lowercasing keywords
    lower_buf: [64]u8,

    const Self = @This();

    pub fn init(input: []const u8) Self {
        return .{
            .input = input,
            .pos = 0,
            .lower_buf = undefined,
        };
    }

    pub fn nextToken(self: *Self) Token {
        self.skipWhitespace();

        if (self.pos >= self.input.len) {
            return .{ .type = .eof, .text = "", .position = self.pos };
        }

        const start = self.pos;
        const c = self.input[self.pos];

        // Single-character tokens
        switch (c) {
            '(' => {
                self.pos += 1;
                return .{ .type = .left_paren, .text = "(", .position = start };
            },
            ')' => {
                self.pos += 1;
                return .{ .type = .right_paren, .text = ")", .position = start };
            },
            ',' => {
                self.pos += 1;
                return .{ .type = .comma, .text = ",", .position = start };
            },
            ';' => {
                self.pos += 1;
                return .{ .type = .semicolon, .text = ";", .position = start };
            },
            '.' => {
                self.pos += 1;
                return .{ .type = .dot, .text = ".", .position = start };
            },
            '+' => {
                self.pos += 1;
                return .{ .type = .op_plus, .text = "+", .position = start };
            },
            '-' => {
                self.pos += 1;
                return .{ .type = .op_minus, .text = "-", .position = start };
            },
            '*' => {
                self.pos += 1;
                return .{ .type = .op_star, .text = "*", .position = start };
            },
            '/' => {
                self.pos += 1;
                return .{ .type = .op_slash, .text = "/", .position = start };
            },
            '=' => {
                self.pos += 1;
                return .{ .type = .op_eq, .text = "=", .position = start };
            },
            '<' => {
                self.pos += 1;
                if (self.pos < self.input.len) {
                    if (self.input[self.pos] == '=') {
                        self.pos += 1;
                        return .{ .type = .op_lte, .text = "<=", .position = start };
                    }
                    if (self.input[self.pos] == '>') {
                        self.pos += 1;
                        return .{ .type = .op_neq, .text = "<>", .position = start };
                    }
                }
                return .{ .type = .op_lt, .text = "<", .position = start };
            },
            '>' => {
                self.pos += 1;
                if (self.pos < self.input.len and self.input[self.pos] == '=') {
                    self.pos += 1;
                    return .{ .type = .op_gte, .text = ">=", .position = start };
                }
                return .{ .type = .op_gt, .text = ">", .position = start };
            },
            '!' => {
                self.pos += 1;
                if (self.pos < self.input.len and self.input[self.pos] == '=') {
                    self.pos += 1;
                    return .{ .type = .op_neq, .text = "!=", .position = start };
                }
                return .{ .type = .invalid, .text = self.input[start..self.pos], .position = start };
            },
            '\'' => return self.readString(),
            else => {},
        }

        // Numbers
        if (std.ascii.isDigit(c)) {
            return self.readNumber();
        }

        // Identifiers and keywords
        if (std.ascii.isAlphabetic(c) or c == '_') {
            return self.readIdentifier();
        }

        // Unknown character
        self.pos += 1;
        return .{ .type = .invalid, .text = self.input[start..self.pos], .position = start };
    }

    fn skipWhitespace(self: *Self) void {
        while (self.pos < self.input.len and std.ascii.isWhitespace(self.input[self.pos])) {
            self.pos += 1;
        }
    }

    fn readString(self: *Self) Token {
        const start = self.pos;
        self.pos += 1; // skip opening quote

        while (self.pos < self.input.len) {
            if (self.input[self.pos] == '\'') {
                // Check for escaped quote ('')
                if (self.pos + 1 < self.input.len and self.input[self.pos + 1] == '\'') {
                    self.pos += 2;
                    continue;
                }
                self.pos += 1; // skip closing quote
                // Return the content without quotes
                return .{
                    .type = .string_literal,
                    .text = self.input[start + 1 .. self.pos - 1],
                    .position = start,
                };
            }
            self.pos += 1;
        }

        // Unterminated string
        return .{ .type = .invalid, .text = self.input[start..self.pos], .position = start };
    }

    fn readNumber(self: *Self) Token {
        const start = self.pos;
        var is_float = false;

        while (self.pos < self.input.len and std.ascii.isDigit(self.input[self.pos])) {
            self.pos += 1;
        }

        // Check for decimal point
        if (self.pos < self.input.len and self.input[self.pos] == '.') {
            is_float = true;
            self.pos += 1;
            while (self.pos < self.input.len and std.ascii.isDigit(self.input[self.pos])) {
                self.pos += 1;
            }
        }

        return .{
            .type = if (is_float) .float_literal else .integer_literal,
            .text = self.input[start..self.pos],
            .position = start,
        };
    }

    fn readIdentifier(self: *Self) Token {
        const start = self.pos;

        while (self.pos < self.input.len) {
            const ch = self.input[self.pos];
            if (std.ascii.isAlphanumeric(ch) or ch == '_') {
                self.pos += 1;
            } else {
                break;
            }
        }

        const text = self.input[start..self.pos];

        // Check if it's a keyword (case-insensitive)
        const len = @min(text.len, self.lower_buf.len);
        for (text[0..len], 0..) |ch, i| {
            self.lower_buf[i] = std.ascii.toLower(ch);
        }
        const lower = self.lower_buf[0..len];

        if (keywords.get(lower)) |kw| {
            return .{ .type = kw, .text = text, .position = start };
        }

        return .{ .type = .identifier, .text = text, .position = start };
    }

    /// Peek at the next token without consuming it
    pub fn peek(self: *Self) Token {
        const saved_pos = self.pos;
        const tok = self.nextToken();
        self.pos = saved_pos;
        return tok;
    }
};

// Tests

test "lexer basic tokens" {
    var lexer = Lexer.init("SELECT * FROM users;");

    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.kw_select, t1.type);
    try std.testing.expectEqualStrings("SELECT", t1.text);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.op_star, t2.type);

    const t3 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.kw_from, t3.type);

    const t4 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, t4.type);
    try std.testing.expectEqualStrings("users", t4.text);

    const t5 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.semicolon, t5.type);

    const t6 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.eof, t6.type);
}

test "lexer numbers and strings" {
    var lexer = Lexer.init("42 3.14 'hello world'");

    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.integer_literal, t1.type);
    try std.testing.expectEqualStrings("42", t1.text);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.float_literal, t2.type);
    try std.testing.expectEqualStrings("3.14", t2.text);

    const t3 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.string_literal, t3.type);
    try std.testing.expectEqualStrings("hello world", t3.text);
}

test "lexer operators" {
    var lexer = Lexer.init("= != <> < > <= >=");

    try std.testing.expectEqual(TokenType.op_eq, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.op_neq, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.op_neq, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.op_lt, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.op_gt, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.op_lte, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.op_gte, lexer.nextToken().type);
}

test "lexer create table" {
    var lexer = Lexer.init("CREATE TABLE users (id INT, name VARCHAR(255))");

    try std.testing.expectEqual(TokenType.kw_create, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.kw_table, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.left_paren, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.kw_int, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.comma, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.kw_varchar, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.left_paren, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.integer_literal, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.right_paren, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.right_paren, lexer.nextToken().type);
}

test "lexer case insensitive keywords" {
    var lexer = Lexer.init("select FROM Where");

    try std.testing.expectEqual(TokenType.kw_select, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.kw_from, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.kw_where, lexer.nextToken().type);
}

test "lexer transaction keywords" {
    var lexer = Lexer.init("BEGIN; COMMIT; ROLLBACK;");

    try std.testing.expectEqual(TokenType.kw_begin, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.semicolon, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.kw_commit, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.semicolon, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.kw_rollback, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.semicolon, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "lexer unterminated string" {
    var lexer = Lexer.init("'hello");
    const tok = lexer.nextToken();
    try std.testing.expectEqual(TokenType.invalid, tok.type);
}

test "lexer escaped quotes in string" {
    var lexer = Lexer.init("'it''s a test'");
    const tok = lexer.nextToken();
    try std.testing.expectEqual(TokenType.string_literal, tok.type);
    // Text includes the escaped quote as raw ''
    try std.testing.expectEqualStrings("it''s a test", tok.text);
}

test "lexer underscore identifier" {
    var lexer = Lexer.init("_col_name _123");
    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, t1.type);
    try std.testing.expectEqualStrings("_col_name", t1.text);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, t2.type);
    try std.testing.expectEqualStrings("_123", t2.text);
}

test "lexer empty input" {
    var lexer = Lexer.init("");
    const tok = lexer.nextToken();
    try std.testing.expectEqual(TokenType.eof, tok.type);
}

test "lexer whitespace only" {
    var lexer = Lexer.init("   \t\n  ");
    const tok = lexer.nextToken();
    try std.testing.expectEqual(TokenType.eof, tok.type);
}

test "lexer unknown character" {
    var lexer = Lexer.init("@");
    const tok = lexer.nextToken();
    try std.testing.expectEqual(TokenType.invalid, tok.type);
}

test "lexer bang without equals" {
    var lexer = Lexer.init("! 42");
    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.invalid, t1.type);
    try std.testing.expectEqualStrings("!", t1.text);

    // Should still parse the 42
    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.integer_literal, t2.type);
}

test "lexer dot operator" {
    var lexer = Lexer.init("users.id");
    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, t1.type);
    try std.testing.expectEqualStrings("users", t1.text);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.dot, t2.type);

    const t3 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, t3.type);
    try std.testing.expectEqualStrings("id", t3.text);
}

test "lexer negative number as minus then integer" {
    var lexer = Lexer.init("-42");
    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.op_minus, t1.type);
    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.integer_literal, t2.type);
    try std.testing.expectEqualStrings("42", t2.text);
}
