// Test entry point for the engine (no msquic/openssl dependencies).
// Imports all modules that contain tests so zig's test runner discovers them.

comptime {
    _ = @import("executor/executor.zig");
    _ = @import("executor/vec_filter.zig");
    _ = @import("executor/vec_agg.zig");
    _ = @import("executor/vec_scan.zig");
    _ = @import("executor/vec_exec.zig");
    _ = @import("executor/vector.zig");
    _ = @import("storage/mvcc.zig");
    _ = @import("storage/tuple.zig");
    _ = @import("storage/page.zig");
    _ = @import("storage/buffer_pool.zig");
}
