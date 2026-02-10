const std = @import("std");
const engine = @import("engine");

const Executor = engine.executor.Executor;
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;

// ── Configuration ──────────────────────────────────────────────────────

pub const BenchmarkConfig = struct {
    name: []const u8,
    warmup_iters: u32 = 3,
    measure_iters: u32 = 10,
    scale_factor: u32 = 1,
};

pub const BenchmarkResult = struct {
    name: []const u8,
    total_ns: u64,
    min_ns: u64,
    max_ns: u64,
    mean_ns: u64,
    median_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    ops_count: u64,
    ops_per_sec: f64,
};

pub const BenchContext = struct {
    allocator: std.mem.Allocator,
    exec: *Executor,
    scale_factor: u32,
    io: Io,
};

pub const BenchmarkFn = *const fn (ctx: *BenchContext) void;

// ── Runner ─────────────────────────────────────────────────────────────

pub fn runBenchmark(config: BenchmarkConfig, ctx: *BenchContext, func: BenchmarkFn) BenchmarkResult {
    // Warmup
    for (0..config.warmup_iters) |_| {
        func(ctx);
    }

    // Measure
    var latencies: [1000]u64 = undefined;
    const iters: u64 = @min(config.measure_iters, 1000);

    for (0..iters) |i| {
        const start = Io.Clock.Timestamp.now(ctx.io, .awake);
        func(ctx);
        const elapsed = start.untilNow(ctx.io);
        latencies[i] = @intCast(elapsed.raw.nanoseconds);
    }

    // Sort for percentile calculations
    std.mem.sort(u64, latencies[0..iters], {}, std.sort.asc(u64));

    var total: u64 = 0;
    for (latencies[0..iters]) |l| total += l;

    const mean = total / iters;
    const median = latencies[iters / 2];
    const p95 = latencies[@min(iters - 1, iters * 95 / 100)];
    const p99 = latencies[@min(iters - 1, iters * 99 / 100)];
    const min_ns = latencies[0];
    const max_ns = latencies[iters - 1];

    const mean_secs = @as(f64, @floatFromInt(mean)) / 1_000_000_000.0;
    const ops_per_sec = if (mean_secs > 0) 1.0 / mean_secs else 0;

    return .{
        .name = config.name,
        .total_ns = total,
        .min_ns = min_ns,
        .max_ns = max_ns,
        .mean_ns = mean,
        .median_ns = median,
        .p95_ns = p95,
        .p99_ns = p99,
        .ops_count = iters,
        .ops_per_sec = ops_per_sec,
    };
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Execute SQL, free result immediately. Panics on error.
pub fn execSQL(exec: *Executor, sql: []const u8) void {
    const result = exec.execute(sql) catch |err| {
        std.debug.print("BENCH SQL ERROR: {s}\nSQL: {s}\n", .{ @errorName(err), sql });
        @panic("benchmark SQL failed");
    };
    exec.freeResult(result);
}

/// Execute SQL and return the row count from SELECT. Panics on error.
pub fn execSQLRowCount(exec: *Executor, sql: []const u8) u64 {
    const result = exec.execute(sql) catch |err| {
        std.debug.print("BENCH SQL ERROR: {s}\nSQL: {s}\n", .{ @errorName(err), sql });
        @panic("benchmark SQL failed");
    };
    defer exec.freeResult(result);
    return switch (result) {
        .rows => |r| r.rows.len,
        .row_count => |c| c,
        .message => 0,
    };
}

// ── CSV Output (Io API) ────────────────────────────────────────────────

pub fn writeCSV(io: Io, path: []const u8, results: []const BenchmarkResult) !void {
    const file = try Dir.createFile(.cwd(), io, path, .{});
    defer file.close(io);

    var write_buf: [4096]u8 = undefined;
    var writer = File.Writer.init(file, io, &write_buf);
    const w = &writer.interface;

    // Header
    try w.print("benchmark,total_ns,min_ns,max_ns,mean_ns,median_ns,p95_ns,p99_ns,ops_count,ops_per_sec\n", .{});

    // Rows
    for (results) |r| {
        try w.print("{s},{d},{d},{d},{d},{d},{d},{d},{d},{d:.2}\n", .{
            r.name,
            r.total_ns,
            r.min_ns,
            r.max_ns,
            r.mean_ns,
            r.median_ns,
            r.p95_ns,
            r.p99_ns,
            r.ops_count,
            r.ops_per_sec,
        });
    }

    try w.flush();
}

// ── Console Summary ────────────────────────────────────────────────────

pub fn printSummary(results: []const BenchmarkResult) void {
    std.debug.print("\n{s}\n", .{"=" ** 96});
    std.debug.print(" {s:<30} {s:>12} {s:>12} {s:>12} {s:>12} {s:>12}\n", .{
        "Benchmark", "Mean", "Median", "P95", "Min", "Ops/sec",
    });
    std.debug.print("{s}\n", .{"-" ** 96});

    for (results) |r| {
        std.debug.print(" {s:<30} {s:>12} {s:>12} {s:>12} {s:>12} {d:>12.1}\n", .{
            r.name,
            fmtNs(r.mean_ns),
            fmtNs(r.median_ns),
            fmtNs(r.p95_ns),
            fmtNs(r.min_ns),
            r.ops_per_sec,
        });
    }

    std.debug.print("{s}\n\n", .{"=" ** 96});
}

fn fmtNs(ns: u64) [12]u8 {
    var buf: [12]u8 = .{' '} ** 12;
    if (ns >= 1_000_000_000) {
        _ = std.fmt.bufPrint(&buf, "{d:.2}s", .{@as(f64, @floatFromInt(ns)) / 1_000_000_000.0}) catch {};
    } else if (ns >= 1_000_000) {
        _ = std.fmt.bufPrint(&buf, "{d:.2}ms", .{@as(f64, @floatFromInt(ns)) / 1_000_000.0}) catch {};
    } else if (ns >= 1_000) {
        _ = std.fmt.bufPrint(&buf, "{d:.2}us", .{@as(f64, @floatFromInt(ns)) / 1_000.0}) catch {};
    } else {
        _ = std.fmt.bufPrint(&buf, "{d}ns", .{ns}) catch {};
    }
    return buf;
}
