const std = @import("std");
const bench_runner = @import("bench_runner.zig");
const engine = @import("engine");
const ast = engine.parser.ast;

const BenchContext = bench_runner.BenchContext;
const BenchmarkResult = bench_runner.BenchmarkResult;
const execSQL = bench_runner.execSQL;

pub fn run(ctx: *BenchContext, results: *std.ArrayListUnmanaged(BenchmarkResult)) void {
    const scale = ctx.scale_factor;

    // ── Setup: CREATE TABLE ────────────────────────────────────────────
    execSQL(ctx.exec, "CREATE TABLE bench_data (id INT, val INT, category INT, label VARCHAR(100), amount FLOAT)");

    // ── bulk_insert ────────────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "bulk_insert",
        .warmup_iters = 0,
        .measure_iters = 1,
        .scale_factor = scale,
    }, ctx, bulkInsert)) catch @panic("OOM");

    // ── point_query_seqscan ────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "point_query_seqscan",
        .scale_factor = scale,
    }, ctx, pointQuerySeqScan)) catch @panic("OOM");

    // ── full_scan ──────────────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "full_scan",
        .scale_factor = scale,
    }, ctx, fullScan)) catch @panic("OOM");

    // ── range_query_seqscan ────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "range_query_seqscan",
        .scale_factor = scale,
    }, ctx, rangeQuerySeqScan)) catch @panic("OOM");

    // ── CREATE INDEX for indexed benchmarks ────────────────────────────
    execSQL(ctx.exec, "CREATE INDEX idx_bench_id ON bench_data (id)");
    execSQL(ctx.exec, "CREATE INDEX idx_bench_val ON bench_data (val)");

    // ── point_query_indexscan ──────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "point_query_indexscan",
        .scale_factor = scale,
    }, ctx, pointQueryIndexScan)) catch @panic("OOM");

    // ── range_query_indexscan ──────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "range_query_indexscan",
        .scale_factor = scale,
    }, ctx, rangeQueryIndexScan)) catch @panic("OOM");

    // ── update ─────────────────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "update",
        .warmup_iters = 0,
        .measure_iters = 1,
        .scale_factor = scale,
    }, ctx, updateBench)) catch @panic("OOM");

    // ── delete ─────────────────────────────────────────────────────────
    results.append(ctx.allocator, bench_runner.runBenchmark(.{
        .name = "delete",
        .warmup_iters = 0,
        .measure_iters = 1,
        .scale_factor = scale,
    }, ctx, deleteBench)) catch @panic("OOM");
}

// ── Benchmark functions ────────────────────────────────────────────────

fn bulkInsert(ctx: *BenchContext) void {
    const n = @as(u32, 1000) * ctx.scale_factor;

    var stmt = ctx.exec.prepare("INSERT INTO bench_data VALUES ($1, $2, $3, $4, $5)") catch @panic("prepare failed");
    defer stmt.deinit();

    var label_buf: [32]u8 = undefined;
    for (0..n) |i| {
        const id: u32 = @intCast(i);
        const val = (id *% 7 +% 13) % 1000;
        const category = id % 10;
        const amount_int = (id *% 3 +% 7) % 500;
        const label = std.fmt.bufPrint(&label_buf, "item_{d}", .{id}) catch @panic("buf overflow");

        const params = [_]ast.LiteralValue{
            .{ .integer = @intCast(id) },
            .{ .integer = @intCast(val) },
            .{ .integer = @intCast(category) },
            .{ .string = label },
            .{ .float = @floatFromInt(amount_int) },
        };
        bench_runner.execPreparedSQL(ctx.exec, &stmt, &params);
    }
}

fn pointQuerySeqScan(ctx: *BenchContext) void {
    const n = @as(u32, 100) * ctx.scale_factor;
    var buf: [128]u8 = undefined;
    for (0..n) |i| {
        const id = (@as(u32, @intCast(i)) *% 17 +% 5) % (@as(u32, 1000) * ctx.scale_factor);
        const sql = std.fmt.bufPrint(&buf,
            "SELECT * FROM bench_data WHERE id = {d}",
            .{id},
        ) catch @panic("buf overflow");
        _ = bench_runner.execSQLRowCount(ctx.exec, sql);
    }
}

fn pointQueryIndexScan(ctx: *BenchContext) void {
    const n = @as(u32, 100) * ctx.scale_factor;
    var buf: [128]u8 = undefined;
    for (0..n) |i| {
        const id = (@as(u32, @intCast(i)) *% 17 +% 5) % (@as(u32, 1000) * ctx.scale_factor);
        const sql = std.fmt.bufPrint(&buf,
            "SELECT * FROM bench_data WHERE id = {d}",
            .{id},
        ) catch @panic("buf overflow");
        _ = bench_runner.execSQLRowCount(ctx.exec, sql);
    }
}

fn fullScan(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT * FROM bench_data");
}

fn rangeQuerySeqScan(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT * FROM bench_data WHERE val BETWEEN 100 AND 200");
}

fn rangeQueryIndexScan(ctx: *BenchContext) void {
    _ = bench_runner.execSQLRowCount(ctx.exec, "SELECT * FROM bench_data WHERE val BETWEEN 100 AND 200");
}

fn updateBench(ctx: *BenchContext) void {
    execSQL(ctx.exec, "UPDATE bench_data SET val = 999 WHERE category = 5");
}

fn deleteBench(ctx: *BenchContext) void {
    execSQL(ctx.exec, "DELETE FROM bench_data WHERE category = 9");
}
