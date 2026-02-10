const std = @import("std");
const engine = @import("engine");

const bench_runner = @import("bench_runner.zig");
const bench_crud = @import("bench_crud.zig");
const bench_analytical = @import("bench_analytical.zig");
const bench_index = @import("bench_index.zig");
const bench_txn = @import("bench_txn.zig");

const DiskManager = engine.storage.disk_manager.DiskManager;
const BufferPool = engine.storage.buffer_pool.BufferPool;
const AllocManager = engine.storage.alloc_map.AllocManager;
const Catalog = engine.storage.catalog.Catalog;
const Wal = engine.storage.wal.Wal;
const TransactionManager = engine.storage.mvcc.TransactionManager;
const UndoLog = engine.storage.undo_log.UndoLog;
const Executor = engine.executor.Executor;
const BenchmarkResult = bench_runner.BenchmarkResult;
const BenchContext = bench_runner.BenchContext;

const Io = std.Io;
const Dir = Io.Dir;

const DB_FILE = "bench_graphene.dat";
const WAL_DIR = "bench_graphene_wal";
const BUFFER_POOL_SIZE = 4096;

const Suite = enum { crud, analytical, index, txn, all };

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Parse CLI args
    var scale: u32 = 1;
    var suite: Suite = .all;
    var output_path: []const u8 = "bench/results/graphenedb_results.csv";

    var args_arena: std.heap.ArenaAllocator = .init(allocator);
    defer args_arena.deinit();
    const args = try init.minimal.args.toSlice(args_arena.allocator());
    const cli_args = if (args.len > 1) args[1..] else args[0..0];

    var i: usize = 0;
    while (i < cli_args.len) : (i += 1) {
        const arg: []const u8 = cli_args[i];
        if (std.mem.eql(u8, arg, "--scale") and i + 1 < cli_args.len) {
            i += 1;
            scale = std.fmt.parseInt(u32, cli_args[i], 10) catch 1;
        } else if (std.mem.eql(u8, arg, "--suite") and i + 1 < cli_args.len) {
            i += 1;
            suite = std.meta.stringToEnum(Suite, cli_args[i]) orelse .all;
        } else if (std.mem.eql(u8, arg, "--output") and i + 1 < cli_args.len) {
            i += 1;
            output_path = cli_args[i];
        }
    }

    std.debug.print("\nGrapheneDB Benchmark Suite\n", .{});
    std.debug.print("  Scale factor: {d}\n", .{scale});
    std.debug.print("  Suite: {s}\n", .{@tagName(suite)});
    std.debug.print("  Output: {s}\n\n", .{output_path});

    var all_results: std.ArrayListUnmanaged(BenchmarkResult) = .empty;
    defer all_results.deinit(allocator);

    if (suite == .crud or suite == .all) {
        std.debug.print("── CRUD Suite ──\n", .{});
        runSuiteIsolated(allocator, io, scale, &all_results, bench_crud.run);
    }
    if (suite == .analytical or suite == .all) {
        std.debug.print("── Analytical Suite ──\n", .{});
        runSuiteIsolated(allocator, io, scale, &all_results, bench_analytical.run);
    }
    if (suite == .index or suite == .all) {
        std.debug.print("── Index Suite ──\n", .{});
        runSuiteIsolated(allocator, io, scale, &all_results, bench_index.run);
    }
    if (suite == .txn or suite == .all) {
        std.debug.print("── Transaction Suite ──\n", .{});
        runSuiteIsolated(allocator, io, scale, &all_results, bench_txn.run);
    }

    bench_runner.printSummary(all_results.items);

    bench_runner.writeCSV(io, output_path, all_results.items) catch |err| {
        std.debug.print("Failed to write CSV: {s}\n", .{@errorName(err)});
    };
    std.debug.print("Results written to {s}\n", .{output_path});
}

fn runSuiteIsolated(
    allocator: std.mem.Allocator,
    io: Io,
    scale: u32,
    results: *std.ArrayListUnmanaged(BenchmarkResult),
    suite_fn: *const fn (*BenchContext, *std.ArrayListUnmanaged(BenchmarkResult)) void,
) void {
    // Clean up previous files
    Dir.deleteFile(.cwd(), io, DB_FILE) catch {};

    // Initialize engine stack
    var dm = DiskManager.init(allocator, DB_FILE);
    dm.open() catch @panic("failed to open db");
    defer dm.close();

    var bp = BufferPool.init(allocator, &dm, BUFFER_POOL_SIZE) catch @panic("failed to init buffer pool");
    defer bp.deinit();

    var am = AllocManager.init(&bp, &dm);
    am.initializeFile() catch @panic("failed to init alloc manager");

    var catalog = Catalog.init(allocator, &bp, &am) catch @panic("failed to init catalog");
    defer catalog.deinit();

    var wal = Wal.init(allocator, WAL_DIR);
    wal.open() catch @panic("failed to open WAL");
    defer wal.deinit();

    var txn_manager = TransactionManager.initWithWal(allocator, &wal);
    defer txn_manager.deinit();

    var undo_log = UndoLog.init(allocator);
    defer undo_log.deinit();

    var exec = Executor.initWithMvcc(allocator, &catalog, &txn_manager, &undo_log);

    var ctx = BenchContext{
        .allocator = allocator,
        .exec = &exec,
        .scale_factor = scale,
        .io = io,
    };

    suite_fn(&ctx, results);
}
