const std = @import("std");

// Storage layer
pub const storage = struct {
    pub const page = @import("storage/page.zig");
    pub const disk_manager = @import("storage/disk_manager.zig");
    pub const buffer_pool = @import("storage/buffer_pool.zig");
    pub const wal = @import("storage/wal.zig");
    pub const tuple = @import("storage/tuple.zig");
    pub const table = @import("storage/table.zig");
    pub const catalog = @import("storage/catalog.zig");
};

// Index layer
pub const index = struct {
    pub const btree_page = @import("index/btree_page.zig");
    pub const btree = @import("index/btree.zig");
};

// Parser layer
pub const parser = struct {
    pub const lexer = @import("parser/lexer.zig");
    pub const ast_mod = @import("parser/ast.zig");
    pub const parser_mod = @import("parser/parser.zig");
};

// Executor layer
pub const executor_layer = struct {
    pub const executor_mod = @import("executor/executor.zig");
};

// Network layer
pub const network = struct {
    pub const protocol = @import("network/protocol.zig");
    pub const msquic = @import("network/msquic.zig");
    pub const server = @import("network/server.zig");
    pub const client = @import("network/client.zig");
};

const DiskManager = storage.disk_manager.DiskManager;
const BufferPool = storage.buffer_pool.BufferPool;
const Catalog = storage.catalog.Catalog;
const Executor = executor_layer.executor_mod.Executor;
const Server = network.server.Server;
const Client = network.client.Client;
const protocol = network.protocol;

const DB_FILE = "graphenedb.dat";
const BUFFER_POOL_SIZE = 1024;
const DEFAULT_PORT = 4567;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Set up stdout writer
    var write_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.Writer.init(std.Io.File.stdout(), io, &write_buf);
    const out = &stdout.interface;

    // Parse CLI arguments
    const args = try init.minimal.args.toSlice(allocator);

    // Skip argv[0] (program name)
    const cli_args = if (args.len > 1) args[1..] else args[0..0];

    if (cli_args.len > 0 and std.mem.eql(u8, cli_args[0], "serve")) {
        const port = if (cli_args.len > 1) std.fmt.parseInt(u16, cli_args[1], 10) catch DEFAULT_PORT else DEFAULT_PORT;
        try runServer(allocator, out, port);
    } else if (cli_args.len > 0 and std.mem.eql(u8, cli_args[0], "connect")) {
        var host: [*:0]const u8 = "localhost";
        var port: u16 = DEFAULT_PORT;

        if (cli_args.len > 1) {
            const arg: []const u8 = cli_args[1];
            if (std.mem.lastIndexOfScalar(u8, arg, ':')) |colon| {
                if (colon > 0) {
                    // The arg slice from toSlice is sentinel-terminated per element,
                    // but we need to create a new null-terminated host string
                    const host_slice = arg[0..colon];
                    const host_z = try allocator.dupeZ(u8, host_slice);
                    host = host_z;
                }
                port = std.fmt.parseInt(u16, arg[colon + 1 ..], 10) catch DEFAULT_PORT;
            } else {
                // Just a host, no port — the arg is already sentinel-terminated
                host = cli_args[1];
            }
        }

        // Set up stdin reader for client REPL
        var read_buf: [4096]u8 = undefined;
        var stdin = std.Io.File.Reader.initStreaming(std.Io.File.stdin(), io, &read_buf);
        const in = &stdin.interface;

        try runClient(allocator, out, in, host, port);
    } else {
        // Default: local REPL
        var read_buf: [4096]u8 = undefined;
        var stdin = std.Io.File.Reader.initStreaming(std.Io.File.stdin(), io, &read_buf);
        const in = &stdin.interface;

        try runRepl(allocator, out, in);
    }
}

// ── Server mode ──────────────────────────────────────────────────────

fn runServer(allocator: std.mem.Allocator, out: *std.Io.Writer, port: u16) !void {
    // Initialize storage engine
    var dm = DiskManager.init(allocator, DB_FILE);
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(allocator, &dm, BUFFER_POOL_SIZE);
    defer bp.deinit();

    var catalog = try Catalog.init(allocator, &bp);
    defer catalog.deinit();

    var exec = Executor.init(allocator, &catalog);

    var srv = try Server.init(allocator, &exec, null, null);
    defer srv.deinit();

    try srv.listen(port);

    try out.print("GrapheneDB v0.1.0\n", .{});
    try out.print("QUIC server listening on port {d}\n", .{port});
    try out.print("Press Ctrl+C to stop.\n", .{});
    try out.flush();

    // Block forever (msquic handles connections in background threads)
    var stop_event: std.Thread.ResetEvent = .unset;
    stop_event.wait();
}

// ── Client mode ──────────────────────────────────────────────────────

fn runClient(
    allocator: std.mem.Allocator,
    out: *std.Io.Writer,
    in: *std.Io.Reader,
    host: [*:0]const u8,
    port: u16,
) !void {
    var cli = try Client.init(allocator);
    defer cli.deinit();

    try out.print("Connecting to {s}:{d}...\n", .{ std.mem.span(host), port });
    try out.flush();

    cli.connect(host, port) catch |err| {
        try out.print("Connection failed: {s}\n", .{@errorName(err)});
        try out.flush();
        return;
    };

    try out.print("GrapheneDB v0.1.0 (connected)\n", .{});
    try out.print("Type SQL statements, or \\q to quit.\n\n", .{});

    while (true) {
        try out.print("graphene> ", .{});
        try out.flush();

        const line = in.takeDelimiter('\n') catch break orelse break;
        const trimmed = std.mem.trimEnd(u8, line, "\r");

        if (trimmed.len == 0) continue;

        if (trimmed[0] == '\\') {
            if (std.mem.eql(u8, trimmed, "\\q") or std.mem.eql(u8, trimmed, "\\quit")) {
                try out.print("Goodbye!\n", .{});
                try out.flush();
                break;
            }
            try out.print("Unknown command: {s}\n", .{trimmed});
            try out.flush();
            continue;
        }

        // Send query over QUIC
        const response = cli.query(trimmed) catch |err| {
            try out.print("ERROR: {s}\n", .{@errorName(err)});
            try out.flush();
            continue;
        };

        switch (response.msg_type) {
            .ok_message => {
                try out.print("{s}\n", .{response.payload});
            },
            .ok_row_count => {
                if (response.payload.len >= 8) {
                    const count = std.mem.readInt(u64, response.payload[0..8], .big);
                    if (count == 1) {
                        try out.print("OK, 1 row affected\n", .{});
                    } else {
                        try out.print("OK, {d} rows affected\n", .{count});
                    }
                }
            },
            .result_set => {
                var rs = protocol.parseResultSet(response.payload, allocator) catch {
                    try out.print("ERROR: failed to parse result set\n", .{});
                    try out.flush();
                    continue;
                };
                defer rs.deinit(allocator);
                try printResultSetTable(out, rs.columns, rs.rows);
            },
            .error_response => {
                try out.print("ERROR: {s}\n", .{response.payload});
            },
            else => {
                try out.print("Unexpected response type\n", .{});
            },
        }
        try out.flush();
    }
}

// ── Local REPL mode ──────────────────────────────────────────────────

fn runRepl(allocator: std.mem.Allocator, out: *std.Io.Writer, in: *std.Io.Reader) !void {
    // Initialize storage engine
    var dm = DiskManager.init(allocator, DB_FILE);
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(allocator, &dm, BUFFER_POOL_SIZE);
    defer bp.deinit();

    var catalog = try Catalog.init(allocator, &bp);
    defer catalog.deinit();

    var exec = Executor.init(allocator, &catalog);

    // Banner
    try out.print("GrapheneDB v0.1.0\n", .{});
    try out.print("Type SQL statements, or \\q to quit.\n\n", .{});

    // REPL loop
    while (true) {
        try out.print("graphene> ", .{});
        try out.flush();

        const line = in.takeDelimiter('\n') catch break orelse break;
        const trimmed = std.mem.trimEnd(u8, line, "\r");

        if (trimmed.len == 0) continue;

        // Meta-commands
        if (trimmed[0] == '\\') {
            if (std.mem.eql(u8, trimmed, "\\q") or std.mem.eql(u8, trimmed, "\\quit")) {
                try out.print("Goodbye!\n", .{});
                try out.flush();
                break;
            }
            if (std.mem.eql(u8, trimmed, "\\dt")) {
                try listTables(&exec, out);
                continue;
            }
            try out.print("Unknown command: {s}\n", .{trimmed});
            try out.flush();
            continue;
        }

        // Execute SQL
        const result = exec.execute(trimmed) catch |err| {
            try out.print("ERROR: {s}\n", .{@errorName(err)});
            try out.flush();
            continue;
        };
        defer exec.freeResult(result);

        switch (result) {
            .message => |msg| {
                try out.print("{s}\n", .{msg});
            },
            .row_count => |count| {
                if (count == 1) {
                    try out.print("OK, 1 row affected\n", .{});
                } else {
                    try out.print("OK, {d} rows affected\n", .{count});
                }
            },
            .rows => |r| {
                try printResultTable(out, r.columns, r.rows);
            },
        }
        try out.flush();
    }
}

// ── Table formatting (local REPL) ────────────────────────────────────

fn printResultTable(
    out: *std.Io.Writer,
    columns: [][]const u8,
    rows: []const executor_layer.executor_mod.ResultRow,
) !void {
    if (columns.len == 0) return;

    const widths = try std.heap.page_allocator.alloc(usize, columns.len);
    defer std.heap.page_allocator.free(widths);

    for (columns, 0..) |col, i| {
        widths[i] = col.len;
    }
    for (rows) |row| {
        for (row.values, 0..) |val, i| {
            if (val.len > widths[i]) widths[i] = val.len;
        }
    }

    try printSeparator(out, widths);
    try out.print("|", .{});
    for (columns, 0..) |col, i| {
        try out.print(" ", .{});
        try out.print("{s}", .{col});
        try printPadding(out, widths[i] - col.len);
        try out.print(" |", .{});
    }
    try out.print("\n", .{});
    try printSeparator(out, widths);

    for (rows) |row| {
        try out.print("|", .{});
        for (row.values, 0..) |val, i| {
            try out.print(" ", .{});
            try out.print("{s}", .{val});
            try printPadding(out, widths[i] - val.len);
            try out.print(" |", .{});
        }
        try out.print("\n", .{});
    }

    try printSeparator(out, widths);
    if (rows.len == 1) {
        try out.print("(1 row)\n", .{});
    } else {
        try out.print("({d} rows)\n", .{rows.len});
    }
}

// ── Table formatting (network client) ────────────────────────────────

fn printResultSetTable(
    out: *std.Io.Writer,
    columns: [][]const u8,
    rows: []const []const []const u8,
) !void {
    if (columns.len == 0) return;

    const widths = try std.heap.page_allocator.alloc(usize, columns.len);
    defer std.heap.page_allocator.free(widths);

    for (columns, 0..) |col, i| {
        widths[i] = col.len;
    }
    for (rows) |row| {
        for (row, 0..) |val, i| {
            if (val.len > widths[i]) widths[i] = val.len;
        }
    }

    try printSeparator(out, widths);
    try out.print("|", .{});
    for (columns, 0..) |col, i| {
        try out.print(" ", .{});
        try out.print("{s}", .{col});
        try printPadding(out, widths[i] - col.len);
        try out.print(" |", .{});
    }
    try out.print("\n", .{});
    try printSeparator(out, widths);

    for (rows) |row| {
        try out.print("|", .{});
        for (row, 0..) |val, i| {
            try out.print(" ", .{});
            try out.print("{s}", .{val});
            try printPadding(out, widths[i] - val.len);
            try out.print(" |", .{});
        }
        try out.print("\n", .{});
    }

    try printSeparator(out, widths);
    if (rows.len == 1) {
        try out.print("(1 row)\n", .{});
    } else {
        try out.print("({d} rows)\n", .{rows.len});
    }
}

fn printSeparator(out: *std.Io.Writer, widths: []const usize) !void {
    try out.print("+", .{});
    for (widths) |w| {
        var j: usize = 0;
        while (j < w + 2) : (j += 1) {
            try out.print("-", .{});
        }
        try out.print("+", .{});
    }
    try out.print("\n", .{});
}

fn printPadding(out: *std.Io.Writer, count: usize) !void {
    var i: usize = 0;
    while (i < count) : (i += 1) {
        try out.print(" ", .{});
    }
}

fn listTables(exec: *Executor, out: *std.Io.Writer) !void {
    const result = exec.catalog.listTables() catch {
        try out.print("ERROR: could not list tables\n", .{});
        try out.flush();
        return;
    };
    defer exec.catalog.freeTableList(result);

    try out.print("Tables:\n", .{});
    for (result) |name| {
        try out.print("  {s}\n", .{name});
    }
    try out.flush();
}

test {
    // Import all modules for testing
    _ = storage.page;
    _ = storage.disk_manager;
    _ = storage.buffer_pool;
    _ = storage.wal;
    _ = storage.tuple;
    _ = storage.table;
    _ = storage.catalog;
    _ = index.btree_page;
    _ = index.btree;
    _ = parser.lexer;
    _ = parser.ast_mod;
    _ = parser.parser_mod;
    _ = executor_layer.executor_mod;
    _ = network.protocol;
    _ = network.msquic;
    _ = network.server;
    _ = network.client;
}
