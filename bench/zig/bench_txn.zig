const std = @import("std");
const bench_runner = @import("bench_runner.zig");
const engine = @import("engine");
const ast = engine.parser.ast;

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

    var stmt = ctx.exec.prepare("INSERT INTO bench_txn VALUES ($1, $2)") catch @panic("prepare failed");
    defer stmt.deinit();

    for (0..n) |i| {
        execSQL(ctx.exec, "BEGIN");
        const params = [_]ast.LiteralValue{
            .{ .integer = @as(i64, @intCast(i)) + 10000 },
            .{ .integer = @intCast(i) },
        };
        bench_runner.execPreparedSQL(ctx.exec, &stmt, &params);
        execSQL(ctx.exec, "COMMIT");
    }
}

fn rollbackOverhead(ctx: *BenchContext) void {
    const n = @as(u32, 100) * ctx.scale_factor;

    var stmt = ctx.exec.prepare("INSERT INTO bench_txn VALUES ($1, $2)") catch @panic("prepare failed");
    defer stmt.deinit();

    for (0..n) |i| {
        execSQL(ctx.exec, "BEGIN");
        const params = [_]ast.LiteralValue{
            .{ .integer = @as(i64, @intCast(i)) + 90000 },
            .{ .integer = @intCast(i) },
        };
        bench_runner.execPreparedSQL(ctx.exec, &stmt, &params);
        execSQL(ctx.exec, "ROLLBACK");
    }
}

fn mixedReadWrite(ctx: *BenchContext) void {
    const n = @as(u32, 100) * ctx.scale_factor;

    var insert_stmt = ctx.exec.prepare("INSERT INTO bench_txn VALUES ($1, $2)") catch @panic("prepare failed");
    defer insert_stmt.deinit();

    var select_stmt = ctx.exec.prepare("SELECT * FROM bench_txn WHERE id = $1") catch @panic("prepare failed");
    defer select_stmt.deinit();

    for (0..n) |i| {
        // Insert
        const insert_params = [_]ast.LiteralValue{
            .{ .integer = @as(i64, @intCast(i)) + 50000 },
            .{ .integer = @intCast(i) },
        };
        bench_runner.execPreparedSQL(ctx.exec, &insert_stmt, &insert_params);

        // Read random row
        const read_id = (@as(u32, @intCast(i)) *% 7 +% 3) % (@as(u32, @intCast(i)) + 1) + 50000;
        const select_params = [_]ast.LiteralValue{
            .{ .integer = @intCast(read_id) },
        };
        _ = bench_runner.execPreparedSQLRowCount(ctx.exec, &select_stmt, &select_params);
    }
}
