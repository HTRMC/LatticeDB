const std = @import("std");
const bench_runner = @import("bench_runner.zig");

const BenchContext = bench_runner.BenchContext;
const BenchmarkResult = bench_runner.BenchmarkResult;
const execSQL = bench_runner.execSQL;

pub fn run(ctx: *BenchContext, results: *std.ArrayListUnmanaged(BenchmarkResult)) void {
    const scale = ctx.scale_factor;

    // ── Setup ──────────────────────────────────────────────────────────
    execSQL(ctx.exec, "CREATE TABLE bench_txn (id INT, counter INT)");

    // ── txn_throughput ─────────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "txn_commit_throughput",
        .warmup_iters = 1,
        .measure_iters = 5,
        .scale_factor = scale,
    }, ctx, txnThroughput)) catch @panic("OOM");

    // ── rollback_overhead ──────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "txn_rollback_overhead",
        .warmup_iters = 1,
        .measure_iters = 5,
        .scale_factor = scale,
    }, ctx, rollbackOverhead)) catch @panic("OOM");

    // ── mixed_readwrite ────────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "mixed_readwrite",
        .warmup_iters = 1,
        .measure_iters = 5,
        .scale_factor = scale,
    }, ctx, mixedReadWrite)) catch @panic("OOM");
}

// ── Benchmark functions ────────────────────────────────────────────────

fn txnThroughput(ctx: *BenchContext) void {
    const n = @as(u32, 100) * ctx.scale_factor;
    var buf: [128]u8 = undefined;
    for (0..n) |i| {
        execSQL(ctx.exec, "BEGIN");
        const sql = std.fmt.bufPrint(&buf,
            "INSERT INTO bench_txn VALUES ({d}, {d})",
            .{ @as(u32, @intCast(i)) + 10000, @as(u32, @intCast(i)) },
        ) catch @panic("buf overflow");
        execSQL(ctx.exec, sql);
        execSQL(ctx.exec, "COMMIT");
    }
}

fn rollbackOverhead(ctx: *BenchContext) void {
    const n = @as(u32, 100) * ctx.scale_factor;
    var buf: [128]u8 = undefined;
    for (0..n) |i| {
        execSQL(ctx.exec, "BEGIN");
        const sql = std.fmt.bufPrint(&buf,
            "INSERT INTO bench_txn VALUES ({d}, {d})",
            .{ @as(u32, @intCast(i)) + 90000, @as(u32, @intCast(i)) },
        ) catch @panic("buf overflow");
        execSQL(ctx.exec, sql);
        execSQL(ctx.exec, "ROLLBACK");
    }
}

fn mixedReadWrite(ctx: *BenchContext) void {
    const n = @as(u32, 100) * ctx.scale_factor;
    var buf: [128]u8 = undefined;
    for (0..n) |i| {
        // Insert
        const insert_sql = std.fmt.bufPrint(&buf,
            "INSERT INTO bench_txn VALUES ({d}, {d})",
            .{ @as(u32, @intCast(i)) + 50000, @as(u32, @intCast(i)) },
        ) catch @panic("buf overflow");
        execSQL(ctx.exec, insert_sql);

        // Read random row
        const read_id = (@as(u32, @intCast(i)) *% 7 +% 3) % (@as(u32, @intCast(i)) + 1) + 50000;
        const read_sql = std.fmt.bufPrint(&buf,
            "SELECT * FROM bench_txn WHERE id = {d}",
            .{read_id},
        ) catch @panic("buf overflow");
        _ = bench_runner.execSQLRowCount(ctx.exec, read_sql);
    }
}
