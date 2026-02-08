const std = @import("std");
const page_mod = @import("page.zig");
const alloc_map_mod = @import("alloc_map.zig");

const PageId = page_mod.PageId;
const AllocManager = alloc_map_mod.AllocManager;

/// Predefined filegroup IDs
pub const PRIMARY_FILEGROUP: u16 = 0;
pub const DEFAULT_FILEGROUP: u16 = 0; // Initially same as primary

/// A logical grouping of data files.
/// Currently single-file, designed for future multi-file proportional fill.
pub const Filegroup = struct {
    name: []const u8,
    filegroup_id: u16,
    /// File IDs in this filegroup (currently always [0])
    file_ids: []const u16,
    alloc_manager: *AllocManager,

    const Self = @This();

    /// Create the primary filegroup (single file, file_id=0).
    pub fn primary(alloc_manager: *AllocManager) Self {
        return .{
            .name = "PRIMARY",
            .filegroup_id = PRIMARY_FILEGROUP,
            .file_ids = &.{0},
            .alloc_manager = alloc_manager,
        };
    }

    /// Allocate an extent from this filegroup.
    /// With a single file, just delegates to AllocManager.
    /// Future: proportional fill picks the file with most free space.
    pub fn allocateExtent(self: *Self) !PageId {
        return try self.alloc_manager.allocateExtent();
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "Filegroup primary creation" {
    // Just test struct construction — no I/O needed
    const disk_manager_mod = @import("disk_manager.zig");
    const buffer_pool_mod = @import("buffer_pool.zig");
    const DiskManager = disk_manager_mod.DiskManager;
    const BufferPool = buffer_pool_mod.BufferPool;

    const test_file = "test_filegroup.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 32);
    defer bp.deinit();

    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    var fg = Filegroup.primary(&am);
    try std.testing.expectEqualStrings("PRIMARY", fg.name);
    try std.testing.expectEqual(@as(u16, 0), fg.filegroup_id);
    try std.testing.expectEqual(@as(usize, 1), fg.file_ids.len);

    // Should be able to allocate an extent
    const ext = try fg.allocateExtent();
    try std.testing.expect(ext > 0);
}
