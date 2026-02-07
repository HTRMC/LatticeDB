const std = @import("std");
const bench_runner = @import("bench_runner.zig");

const BenchContext = bench_runner.BenchContext;
const BenchmarkResult = bench_runner.BenchmarkResult;
const execSQL = bench_runner.execSQL;

pub fn run(ctx: *BenchContext, results: *std.ArrayListUnmanaged(BenchmarkResult)) void {
    const scale = ctx.scale_factor;

    // ── Setup: table with data ─────────────────────────────────────────
    execSQL(ctx.exec, "CREATE TABLE bench_data (id INT, val INT, category INT, label VARCHAR(100), amount FLOAT)");

    var buf: [256]u8 = undefined;
    const n = @as(u32, 1000) * scale;
    for (0..n) |i| {
        const id: u32 = @intCast(i);
        const val = (id *% 7 +% 13) % 1000;
        const category = id % 10;
        const amount_int = (id *% 3 +% 7) % 500;
        const sql = std.fmt.bufPrint(&buf,
            "INSERT INTO bench_data VALUES ({d}, {d}, {d}, 'item_{d}', {d}.0)",
            .{ id, val, category, id, amount_int },
        ) catch @panic("buf overflow");
        execSQL(ctx.exec, sql);
    }

    // ── create_index ───────────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "create_index",
        .warmup_iters = 0,
        .measure_iters = 1,
        .scale_factor = scale,
    }, ctx, createIndex)) catch @panic("OOM");

    // Drop and re-create for unique test
    execSQL(ctx.exec, "DROP INDEX idx_bench_id");

    // ── unique_index ───────────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "unique_index",
        .warmup_iters = 0,
        .measure_iters = 1,
        .scale_factor = scale,
    }, ctx, uniqueIndex)) catch @panic("OOM");

    // ── index_speedup (point queries: no index vs with index) ──────────
    // First measure without index on val
    const no_idx = bench_runner.runBenchmark(.{
        .name = "point_query_no_index",
        .warmup_iters = 1,
        .measure_iters = 5,
        .scale_factor = scale,
    }, ctx, pointQueryVal);

    // Create index on val
    execSQL(ctx.exec, "CREATE INDEX idx_bench_val ON bench_data (val)");

    const with_idx = bench_runner.runBenchmark(.{
        .name = "point_query_with_index",
        .warmup_iters = 1,
        .measure_iters = 5,
        .scale_factor = scale,
    }, ctx, pointQueryVal);

    results.append(ctx.allocator, no_idx) catch @panic("OOM");
    results.append(ctx.allocator, with_idx) catch @panic("OOM");

    // Report speedup ratio
    const speedup = if (with_idx.mean_ns > 0)
        @as(f64, @floatFromInt(no_idx.mean_ns)) / @as(f64, @floatFromInt(with_idx.mean_ns))
    else
        0;
    std.debug.print("  Index speedup ratio: {d:.2}x\n", .{speedup});
}

// ── Benchmark functions ────────────────────────────────────────────────

fn createIndex(ctx: *BenchContext) void {
    execSQL(ctx.exec, "CREATE INDEX idx_bench_id ON bench_data (id)");
}

fn uniqueIndex(ctx: *BenchContext) void {
    execSQL(ctx.exec, "CREATE UNIQUE INDEX idx_bench_id_unique ON bench_data (id)");
}

fn pointQueryVal(ctx: *BenchContext) void {
    const n = @as(u32, 100) * ctx.scale_factor;
    var buf: [128]u8 = undefined;
    for (0..n) |i| {
        const val = (@as(u32, @intCast(i)) *% 13 +% 7) % 1000;
        const sql = std.fmt.bufPrint(&buf,
            "SELECT * FROM bench_data WHERE val = {d}",
            .{val},
        ) catch @panic("buf overflow");
        _ = bench_runner.execSQLRowCount(ctx.exec, sql);
    }
}
