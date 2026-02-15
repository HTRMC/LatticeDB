const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const msquic_path = b.option([]const u8, "msquic-path", "Path to msquic native directory (e.g. deps/msquic/build/native)");
    const openssl_path = b.option([]const u8, "openssl-path", "Path to OpenSSL install directory (e.g. vcpkg installed/x64-windows)");

    const exe = b.addExecutable(.{
        .name = "GrapheneDB",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    linkDeps(b, exe, msquic_path, openssl_path);

    exe.root_module.addWin32ResourceFile(.{
        .file = b.path("src/resources.rc"),
    });

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    linkDeps(b, exe_tests, msquic_path, openssl_path);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&b.addRunArtifact(exe_tests).step);

    // ── Engine tests (no msquic/openssl needed) ───────────────────────
    const engine_test_mod = b.createModule(.{
        .root_source_file = b.path("src/test_engine.zig"),
        .target = target,
        .optimize = optimize,
    });

    const engine_tests = b.addTest(.{
        .root_module = engine_test_mod,
    });

    const engine_test_step = b.step("engine-test", "Run engine tests (no msquic needed)");
    engine_test_step.dependOn(&b.addRunArtifact(engine_tests).step);

    // ── Benchmark executable (no msquic/openssl needed) ──────────────
    // Always build benchmarks with ReleaseFast for meaningful results
    // (SIMD vectorization, no safety checks, full compiler optimizations)
    const bench_optimize = if (optimize != .Debug) optimize else .ReleaseFast;

    const engine_mod = b.createModule(.{
        .root_source_file = b.path("src/engine.zig"),
        .target = target,
        .optimize = bench_optimize,
    });

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/zig/bench_main.zig"),
        .target = target,
        .optimize = bench_optimize,
    });
    bench_mod.addImport("engine", engine_mod);

    const bench_exe = b.addExecutable(.{
        .name = "graphene-bench",
        .root_module = bench_mod,
    });

    const bench_step = b.step("bench", "Run benchmarks");
    const bench_run = b.addRunArtifact(bench_exe);
    bench_step.dependOn(&bench_run.step);

    if (b.args) |a| {
        bench_run.addArgs(a);
    }
}

fn linkDeps(b: *std.Build, compile: *std.Build.Step.Compile, msquic_path: ?[]const u8, openssl_path: ?[]const u8) void {
    const arch_str = switch (compile.rootModuleTarget().cpu.arch) {
        .x86_64 => "x64",
        .aarch64 => "arm64",
        .x86 => "x86",
        else => "x64",
    };

    // msquic
    if (msquic_path) |base| {
        const lib_dir = std.fmt.allocPrint(b.allocator, "{s}/lib/{s}", .{ base, arch_str }) catch @panic("OOM");
        const bin_dir = std.fmt.allocPrint(b.allocator, "{s}/bin/{s}", .{ base, arch_str }) catch @panic("OOM");

        compile.root_module.addLibraryPath(.{ .cwd_relative = lib_dir });
        compile.root_module.addLibraryPath(.{ .cwd_relative = bin_dir });
    }
    compile.root_module.linkSystemLibrary("msquic", .{});

    // OpenSSL (libssl + libcrypto)
    // Provide .lib files directly AND add lib path for extern "libcrypto" auto-linking
    if (openssl_path) |base| {
        const ssl_lib_dir = std.fmt.allocPrint(b.allocator, "{s}/lib", .{base}) catch @panic("OOM");
        const ssl_bin_dir = std.fmt.allocPrint(b.allocator, "{s}/bin", .{base}) catch @panic("OOM");

        compile.root_module.addLibraryPath(.{ .cwd_relative = ssl_lib_dir });
        compile.root_module.addLibraryPath(.{ .cwd_relative = ssl_bin_dir });
    }
    compile.root_module.linkSystemLibrary("crypto", .{});
}
