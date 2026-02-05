const std = @import("std");

// Storage layer
pub const storage = struct {
    pub const page = @import("storage/page.zig");
    pub const disk_manager = @import("storage/disk_manager.zig");
    pub const buffer_pool = @import("storage/buffer_pool.zig");
    pub const wal = @import("storage/wal.zig");
};

pub fn main() void {
    std.debug.print("Hello, LatticeDB!\n", .{});
}

test {
    // Import all modules for testing
    _ = storage.page;
    _ = storage.disk_manager;
    _ = storage.buffer_pool;
    _ = storage.wal;
}

