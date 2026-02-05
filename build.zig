const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const msquic_path = b.option([]const u8, "msquic-path", "Path to msquic native directory (e.g. deps/msquic/build/native)");

    const exe = b.addExecutable(.{
        .name = "GrapheneDB",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    linkMsQuic(b, exe, msquic_path);

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

    linkMsQuic(b, exe_tests, msquic_path);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&b.addRunArtifact(exe_tests).step);
}

fn linkMsQuic(b: *std.Build, compile: *std.Build.Step.Compile, msquic_path: ?[]const u8) void {
    if (msquic_path) |base| {
        const arch_str = switch (compile.rootModuleTarget().cpu.arch) {
            .x86_64 => "x64",
            .aarch64 => "arm64",
            .x86 => "x86",
            else => "x64",
        };

        const lib_dir = std.fmt.allocPrint(b.allocator, "{s}/lib/{s}", .{ base, arch_str }) catch @panic("OOM");
        const bin_dir = std.fmt.allocPrint(b.allocator, "{s}/bin/{s}", .{ base, arch_str }) catch @panic("OOM");

        compile.root_module.addLibraryPath(.{ .cwd_relative = lib_dir });
        compile.root_module.addLibraryPath(.{ .cwd_relative = bin_dir });
    }

    compile.root_module.linkSystemLibrary("msquic", .{});
}
