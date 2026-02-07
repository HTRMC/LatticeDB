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
    pub const mvcc = @import("storage/mvcc.zig");
    pub const undo_log = @import("storage/undo_log.zig");
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

// Planner layer
pub const planner = struct {
    pub const plan = @import("planner/plan.zig");
    pub const planner_mod = @import("planner/planner.zig");
};

// Network layer
pub const network = struct {
    pub const protocol = @import("network/protocol.zig");
    pub const msquic = @import("network/msquic.zig");
    pub const server = @import("network/server.zig");
    pub const client = @import("network/client.zig");
    pub const tls = @import("network/tls.zig");
};

const DiskManager = storage.disk_manager.DiskManager;
const BufferPool = storage.buffer_pool.BufferPool;
const Catalog = storage.catalog.Catalog;
const TransactionManager = storage.mvcc.TransactionManager;
const UndoLog = storage.undo_log.UndoLog;
const Executor = executor_layer.executor_mod.Executor;
const Server = network.server.Server;
const Client = network.client.Client;
const protocol = network.protocol;
const tls = network.tls;

const DB_FILE = "graphenedb.dat";
const BUFFER_POOL_SIZE = 1024;
const DEFAULT_PORT = 4567;
const DATA_DIR = ".graphenedb";
const CERT_FILE = DATA_DIR ++ "/server.p12";
const CERT_PASSWORD = "graphenedb"; // PKCS12 password for on-disk storage
const KNOWN_SERVERS_FILE = DATA_DIR ++ "/known_servers";

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Set up stdout writer
    var write_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.Writer.init(std.Io.File.stdout(), io, &write_buf);
    const out = &stdout.interface;

    // Parse CLI arguments (use arena so all arg allocations are freed together)
    var args_arena: std.heap.ArenaAllocator = .init(allocator);
    defer args_arena.deinit();
    const args = try init.minimal.args.toSlice(args_arena.allocator());

    // Skip argv[0] (program name)
    const cli_args = if (args.len > 1) args[1..] else args[0..0];

    if (cli_args.len > 0 and std.mem.eql(u8, cli_args[0], "serve")) {
        var port: u16 = DEFAULT_PORT;
        var cert_hash: ?[20]u8 = null;
        var cert_file_arg: ?[*:0]const u8 = null;
        var key_file_arg: ?[*:0]const u8 = null;
        var tls_mode: enum { ephemeral, persistent, manual } = .ephemeral;

        var i: usize = 1;
        while (i < cli_args.len) : (i += 1) {
            const arg: []const u8 = cli_args[i];
            if (std.mem.eql(u8, arg, "--cert-hash") and i + 1 < cli_args.len) {
                i += 1;
                cert_hash = parseHexHash(cli_args[i]) orelse {
                    try out.print("Invalid cert hash: expected 40 hex characters (SHA-1 thumbprint)\n", .{});
                    try out.flush();
                    return;
                };
                tls_mode = .manual;
            } else if (std.mem.eql(u8, arg, "--cert") and i + 1 < cli_args.len) {
                i += 1;
                cert_file_arg = cli_args[i];
                tls_mode = .manual;
            } else if (std.mem.eql(u8, arg, "--key") and i + 1 < cli_args.len) {
                i += 1;
                key_file_arg = cli_args[i];
                tls_mode = .manual;
            } else if (std.mem.eql(u8, arg, "--tls") and i + 1 < cli_args.len) {
                i += 1;
                const mode_str: []const u8 = cli_args[i];
                if (std.mem.eql(u8, mode_str, "persistent")) {
                    tls_mode = .persistent;
                } else if (std.mem.eql(u8, mode_str, "ephemeral")) {
                    tls_mode = .ephemeral;
                } else {
                    try out.print("Invalid --tls mode: {s} (use 'ephemeral' or 'persistent')\n", .{mode_str});
                    try out.flush();
                    return;
                }
            } else {
                port = std.fmt.parseInt(u16, arg, 10) catch DEFAULT_PORT;
            }
        }

        var cert_bundle: ?tls.CertBundle = null;
        defer if (cert_bundle) |*cb| cb.deinit(allocator);

        const tls_cert: Server.TlsCert = blk: {
            // Manual cert flags take priority
            if (cert_hash) |hash| break :blk .{ .hash = hash };
            if (cert_file_arg != null and key_file_arg != null)
                break :blk .{ .files = .{ .cert = cert_file_arg.?, .key = key_file_arg.? } };
            if (tls_mode == .manual) {
                try out.print("Manual TLS mode requires --cert-hash or --cert + --key flags.\n", .{});
                try out.flush();
                return;
            }

            // Auto-generate certificate
            if (tls_mode == .persistent) {
                // Try to load existing cert from disk
                if (loadCertFile(allocator, io)) |der| {
                    cert_bundle = tls.loadFromBytes(allocator, der, CERT_PASSWORD) catch |err| {
                        try out.print("Failed to parse saved certificate: {s}\n", .{@errorName(err)});
                        try out.flush();
                        allocator.free(der);
                        return;
                    };
                    allocator.free(der);
                    try out.print("Loaded certificate from {s}\n", .{CERT_FILE});
                    try out.flush();
                } else {
                    // Generate new cert and save
                    cert_bundle = tls.generateSelfSigned(allocator, "localhost", CERT_PASSWORD) catch |err| {
                        try out.print("Failed to generate certificate: {s}\n", .{@errorName(err)});
                        try out.flush();
                        return;
                    };

                    // Create data directory and save
                    saveCertFile(cert_bundle.?.pkcs12_der, io) catch |err| {
                        try out.print("Failed to save certificate: {s}\n", .{@errorName(err)});
                        try out.flush();
                        return;
                    };
                    try out.print("Generated certificate saved to {s}\n", .{CERT_FILE});
                    try out.flush();
                }
            } else {
                // Ephemeral: generate fresh cert in memory
                cert_bundle = tls.generateSelfSigned(allocator, "localhost", null) catch |err| {
                    try out.print("Failed to generate certificate: {s}\n", .{@errorName(err)});
                    try out.flush();
                    return;
                };
            }

            // On Windows, use certificate_context (Schannel). On Linux, use PKCS12.
            const cb = &cert_bundle.?;
            if (cb.cert_context) |ctx| {
                break :blk .{ .context = ctx };
            } else {
                const password: ?[*:0]const u8 = if (tls_mode == .persistent) CERT_PASSWORD else null;
                break :blk .{ .pkcs12 = .{ .der = cb.pkcs12_der, .password = password } };
            }
        };

        // Print fingerprint
        if (cert_bundle) |cb| {
            var fp_buf: [95]u8 = undefined;
            const fp_str = tls.formatFingerprint(cb.fingerprint, &fp_buf);
            try out.print("Fingerprint: {s}\n", .{fp_str});
            try out.flush();
        }

        try runServer(allocator, out, port, tls_cert);
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

        try runClient(allocator, out, in, io, host, port);
    } else {
        // Default: local REPL
        var read_buf: [4096]u8 = undefined;
        var stdin = std.Io.File.Reader.initStreaming(std.Io.File.stdin(), io, &read_buf);
        const in = &stdin.interface;

        try runRepl(allocator, out, in);
    }
}

// ── Server mode ──────────────────────────────────────────────────────

fn runServer(allocator: std.mem.Allocator, out: *std.Io.Writer, port: u16, tls_cert: Server.TlsCert) !void {
    // Initialize storage engine
    var dm = DiskManager.init(allocator, DB_FILE);
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(allocator, &dm, BUFFER_POOL_SIZE);
    defer bp.deinit();

    var catalog = try Catalog.init(allocator, &bp);
    defer catalog.deinit();

    var wal = storage.wal.Wal.init(allocator, "graphenedb.wal");
    try wal.open();
    defer wal.deinit();

    var txn_manager = TransactionManager.initWithWal(allocator, &wal);
    defer txn_manager.deinit();

    var undo_log = UndoLog.init(allocator);
    defer undo_log.deinit();

    var srv = try Server.init(allocator, &catalog, &txn_manager, &undo_log, tls_cert);
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
    io: Io,
    host: [*:0]const u8,
    port: u16,
) !void {
    var cli = try Client.init(allocator);
    defer cli.deinit();

    const host_span = std.mem.span(host);

    // Look up known fingerprint for TOFU pinning
    const host_port = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ host_span, port });
    defer allocator.free(host_port);
    cli.expected_fingerprint = lookupKnownFingerprint(allocator, io, host_port);

    try out.print("Connecting to {s}...\n", .{host_port});
    try out.flush();

    cli.connect(host, port) catch |err| {
        // Check if rejection was due to fingerprint mismatch
        if (cli.pin_result == .mismatch) {
            try out.print("CONNECTION REFUSED: Server fingerprint has changed!\n", .{});
            if (cli.server_fingerprint) |fp| {
                var fp_buf: [95]u8 = undefined;
                const fp_str = tls.formatFingerprint(fp, &fp_buf);
                try out.print("  New fingerprint: {s}\n", .{fp_str});
            }
            if (cli.expected_fingerprint) |fp| {
                var fp_buf: [95]u8 = undefined;
                const fp_str = tls.formatFingerprint(fp, &fp_buf);
                try out.print("  Expected:        {s}\n", .{fp_str});
            }
            try out.print("If you trust this server, delete its entry from {s}\n", .{KNOWN_SERVERS_FILE});
            try out.flush();
            return;
        }
        try out.print("Connection failed: {s}\n", .{@errorName(err)});
        try out.flush();
        return;
    };

    // Show pinning result
    switch (cli.pin_result) {
        .new_host => {
            if (cli.server_fingerprint) |fp| {
                var fp_buf: [95]u8 = undefined;
                const fp_str = tls.formatFingerprint(fp, &fp_buf);
                try out.print("New server fingerprint: {s}\n", .{fp_str});
                // Save for future connections (TOFU)
                saveKnownFingerprint(allocator, host_port, fp, io) catch |err| {
                    try out.print("Warning: could not save fingerprint: {s}\n", .{@errorName(err)});
                };
                try out.print("Fingerprint saved to {s}\n", .{KNOWN_SERVERS_FILE});
            }
        },
        .trusted => {
            try out.print("Server fingerprint verified.\n", .{});
        },
        else => {},
    }

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

    var wal = storage.wal.Wal.init(allocator, "graphenedb.wal");
    try wal.open();
    defer wal.deinit();

    var txn_manager = TransactionManager.initWithWal(allocator, &wal);
    defer txn_manager.deinit();

    var undo_log = UndoLog.init(allocator);
    defer undo_log.deinit();

    var exec = Executor.initWithMvcc(allocator, &catalog, &txn_manager, &undo_log);

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

fn parseHexHash(hex: []const u8) ?[20]u8 {
    if (hex.len != 40) return null;
    var result: [20]u8 = undefined;
    for (0..20) |i| {
        const hi = hexVal(hex[i * 2]) orelse return null;
        const lo = hexVal(hex[i * 2 + 1]) orelse return null;
        result[i] = (@as(u8, hi) << 4) | lo;
    }
    return result;
}

fn hexVal(c: u8) ?u4 {
    if (c >= '0' and c <= '9') return @intCast(c - '0');
    if (c >= 'a' and c <= 'f') return @intCast(c - 'a' + 10);
    if (c >= 'A' and c <= 'F') return @intCast(c - 'A' + 10);
    return null;
}

// ── Certificate file I/O ─────────────────────────────────────────────

const Io = std.Io;
const Dir = Io.Dir;

/// Try to load the cert file, returns null if not found.
fn loadCertFile(allocator: std.mem.Allocator, io: Io) ?[]u8 {
    const file = Dir.openFile(.cwd(), io, CERT_FILE, .{ .mode = .read_only }) catch return null;
    defer file.close(io);
    const stat = file.stat(io) catch return null;
    if (stat.size == 0) return null;
    const buf = allocator.alloc(u8, stat.size) catch return null;
    const bytes_read = file.readPositionalAll(io, buf, 0) catch {
        allocator.free(buf);
        return null;
    };
    if (bytes_read != stat.size) {
        allocator.free(buf);
        return null;
    }
    return buf;
}

/// Save cert file to disk, creating the data directory if needed.
fn saveCertFile(der: []const u8, io: Io) !void {
    // Create data directory (ignore if already exists)
    Dir.createDir(.cwd(), io, DATA_DIR, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    const file = try Dir.createFile(.cwd(), io, CERT_FILE, .{ .truncate = true });
    defer file.close(io);
    try file.writePositionalAll(io, der, 0);
}

/// Look up a known fingerprint for a host:port from the known_servers file.
fn lookupKnownFingerprint(allocator: std.mem.Allocator, io: Io, host_port: []const u8) ?[32]u8 {
    const content = loadFileContent(allocator, io, KNOWN_SERVERS_FILE) orelse return null;
    defer allocator.free(content);

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trimEnd(u8, line, "\r");
        if (trimmed.len == 0) continue;
        if (std.mem.indexOfScalar(u8, trimmed, ' ')) |space_idx| {
            const entry_host = trimmed[0..space_idx];
            const entry_fp_hex = trimmed[space_idx + 1 ..];
            if (std.mem.eql(u8, entry_host, host_port)) {
                return parseHexFingerprint(entry_fp_hex);
            }
        }
    }
    return null;
}

/// Save a fingerprint for a host:port to the known_servers file.
/// Updates the entry if the host:port already exists.
fn saveKnownFingerprint(allocator: std.mem.Allocator, host_port: []const u8, fp: [32]u8, io: Io) !void {
    Dir.createDir(.cwd(), io, DATA_DIR, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    // Format fingerprint as 64 hex chars
    const hex_chars = "0123456789ABCDEF";
    var hex_buf: [64]u8 = undefined;
    for (fp, 0..) |byte, idx| {
        hex_buf[idx * 2] = hex_chars[byte >> 4];
        hex_buf[idx * 2 + 1] = hex_chars[byte & 0x0f];
    }

    // Build new file content, replacing any existing entry for this host:port
    var content = std.ArrayListUnmanaged(u8).empty;
    defer content.deinit(allocator);

    if (loadFileContent(allocator, io, KNOWN_SERVERS_FILE)) |existing| {
        defer allocator.free(existing);
        var lines = std.mem.splitScalar(u8, existing, '\n');
        while (lines.next()) |line| {
            const trimmed = std.mem.trimEnd(u8, line, "\r");
            if (trimmed.len == 0) continue;
            // Skip existing entry for this host:port (will be replaced)
            if (std.mem.indexOfScalar(u8, trimmed, ' ')) |space_idx| {
                if (std.mem.eql(u8, trimmed[0..space_idx], host_port)) continue;
            }
            try content.appendSlice(allocator, trimmed);
            try content.append(allocator, '\n');
        }
    }

    // Append new entry
    try content.appendSlice(allocator, host_port);
    try content.append(allocator, ' ');
    try content.appendSlice(allocator, &hex_buf);
    try content.append(allocator, '\n');

    // Write file
    const file = try Dir.createFile(.cwd(), io, KNOWN_SERVERS_FILE, .{ .truncate = true });
    defer file.close(io);
    try file.writePositionalAll(io, content.items, 0);
}

fn loadFileContent(allocator: std.mem.Allocator, io: Io, path: []const u8) ?[]u8 {
    const file = Dir.openFile(.cwd(), io, path, .{ .mode = .read_only }) catch return null;
    defer file.close(io);
    const stat = file.stat(io) catch return null;
    if (stat.size == 0) return null;
    const buf = allocator.alloc(u8, stat.size) catch return null;
    const n = file.readPositionalAll(io, buf, 0) catch {
        allocator.free(buf);
        return null;
    };
    if (n == 0) {
        allocator.free(buf);
        return null;
    }
    return buf[0..n];
}

fn parseHexFingerprint(hex: []const u8) ?[32]u8 {
    if (hex.len != 64) return null;
    var result: [32]u8 = undefined;
    for (0..32) |i| {
        const hi = hexVal(hex[i * 2]) orelse return null;
        const lo = hexVal(hex[i * 2 + 1]) orelse return null;
        result[i] = (@as(u8, hi) << 4) | lo;
    }
    return result;
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
    _ = storage.mvcc;
    _ = storage.undo_log;
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
    _ = network.tls;
}
