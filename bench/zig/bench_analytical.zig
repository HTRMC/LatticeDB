const std = @import("std");
const bench_runner = @import("bench_runner.zig");

const BenchContext = bench_runner.BenchContext;
const BenchmarkResult = bench_runner.BenchmarkResult;
const execSQL = bench_runner.execSQL;

pub fn run(ctx: *BenchContext, results: *std.ArrayListUnmanaged(BenchmarkResult)) void {
    const scale = ctx.scale_factor;

    // ── Setup: tables with data ────────────────────────────────────────
    execSQL(ctx.exec, "CREATE TABLE bench_data (id INT, val INT, category INT, label VARCHAR(100), amount FLOAT)");
    execSQL(ctx.exec, "CREATE TABLE bench_lookup (id INT, name VARCHAR(50), region INT)");

    // Populate bench_data
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

    // Populate bench_lookup (10 * scale rows)
    const lookup_n = @as(u32, 10) * scale;
    for (0..lookup_n) |i| {
        const id: u32 = @intCast(i);
        const region = id % 3;
        const sql = std.fmt.bufPrint(&buf,
            "INSERT INTO bench_lookup VALUES ({d}, 'lookup_{d}', {d})",
            .{ id, id, region },
        ) catch @panic("buf overflow");
        execSQL(ctx.exec, sql);
    }

    // ── Benchmarks ─────────────────────────────────────────────────────

    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "group_by_count",
        .warmup_iters = 0,
        .measure_iters = 1,
        .scale_factor = scale,
    }, ctx, groupByCount)) catch @panic("OOM");

    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "group_by_sum_avg",
        .scale_factor = scale,
    }, ctx, groupBySumAvg)) catch @panic("OOM");

    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "join",
        .scale_factor = scale,
    }, ctx, joinBench)) catch @panic("OOM");

    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "left_join",
        .scale_factor = scale,
    }, ctx, leftJoinBench)) catch @panic("OOM");

    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "order_by_limit",
        .scale_factor = scale,
    }, ctx, orderByLimit)) catch @panic("OOM");

    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "min_max",
        .scale_factor = scale,
    }, ctx, minMax)) catch @panic("OOM");
}

// ── Benchmark functions ────────────────────────────────────────────────

fn groupByCount(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT category, COUNT(*) FROM bench_data GROUP BY category");
}

fn groupBySumAvg(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT category, SUM(amount), AVG(val) FROM bench_data GROUP BY category");
}

fn joinBench(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT bench_data.id, bench_lookup.name FROM bench_data JOIN bench_lookup ON bench_data.category = bench_lookup.id");
}

fn leftJoinBench(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT bench_data.id, bench_lookup.name FROM bench_data LEFT JOIN bench_lookup ON bench_data.category = bench_lookup.id");
}

fn orderByLimit(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT * FROM bench_data ORDER BY val LIMIT 100");
}

fn minMax(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT MIN(val), MAX(val) FROM bench_data");
}
