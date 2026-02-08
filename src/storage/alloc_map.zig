const std = @import("std");
const page_mod = @import("page.zig");
const file_header = @import("file_header.zig");
const buffer_pool_mod = @import("buffer_pool.zig");
const disk_manager_mod = @import("disk_manager.zig");

const PAGE_SIZE = page_mod.PAGE_SIZE;
const PageId = page_mod.PageId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const BufferPool = buffer_pool_mod.BufferPool;
const DiskManager = disk_manager_mod.DiskManager;
const FileHeaderPage = file_header.FileHeaderPage;

pub const AllocError = error{
    NoFreeExtents,
    NoFreePages,
    InvalidPage,
    BufferPoolError,
    DiskError,
    OutOfMemory,
    CorruptedFile,
};

/// Extent size in pages (8 pages = 64KB)
pub const EXTENT_SIZE: u32 = 8;

// ── PFS (Page Free Space) ────────────────────────────────────────────

/// PFS byte layout:
///   bit 0:   allocated (1=yes)
///   bit 1-3: free space category (0=empty, 1=1-50%, 2=51-80%, 3=81-95%, 4=96-100%)
///   bit 4-5: page type (0=data, 1=index, 2=overflow, 3=reserved)
///   bit 6:   IAM page flag
///   bit 7:   mixed extent flag
pub const PfsEntry = packed struct {
    allocated: bool,
    free_space: u3,
    page_type: u2,
    iam_page: bool,
    mixed_extent: bool,
};

/// Header at the start of each PFS page
const PFS_HEADER_SIZE: usize = 4; // 4-byte header (page_id marker)

/// Number of pages tracked by a single PFS page
pub const PFS_ENTRIES_PER_PAGE: usize = PAGE_SIZE - PFS_HEADER_SIZE;

/// PFS page operations — work directly on a page buffer
pub const Pfs = struct {
    /// Initialize a PFS page with all entries unallocated.
    pub fn initPage(buf: *[PAGE_SIZE]u8) void {
        @memset(buf, 0);
        // Write PFS marker in header
        buf[0] = 'P';
        buf[1] = 'F';
        buf[2] = 'S';
        buf[3] = 0;
    }

    /// Get the PFS entry for a page (relative to this PFS page's range).
    pub fn getEntry(buf: *const [PAGE_SIZE]u8, page_offset: usize) PfsEntry {
        if (page_offset >= PFS_ENTRIES_PER_PAGE) return @bitCast(@as(u8, 0));
        return @bitCast(buf[PFS_HEADER_SIZE + page_offset]);
    }

    /// Set the PFS entry for a page.
    pub fn setEntry(buf: *[PAGE_SIZE]u8, page_offset: usize, entry: PfsEntry) void {
        if (page_offset >= PFS_ENTRIES_PER_PAGE) return;
        buf[PFS_HEADER_SIZE + page_offset] = @bitCast(entry);
    }

    /// Mark a page as allocated.
    pub fn markAllocated(buf: *[PAGE_SIZE]u8, page_offset: usize) void {
        if (page_offset >= PFS_ENTRIES_PER_PAGE) return;
        var entry: PfsEntry = @bitCast(buf[PFS_HEADER_SIZE + page_offset]);
        entry.allocated = true;
        buf[PFS_HEADER_SIZE + page_offset] = @bitCast(entry);
    }

    /// Mark a page as free.
    pub fn markFree(buf: *[PAGE_SIZE]u8, page_offset: usize) void {
        if (page_offset >= PFS_ENTRIES_PER_PAGE) return;
        buf[PFS_HEADER_SIZE + page_offset] = 0;
    }

    /// Find a free (unallocated) page within a range.
    pub fn findFreePage(buf: *const [PAGE_SIZE]u8, start: usize, end: usize) ?usize {
        const actual_end = @min(end, PFS_ENTRIES_PER_PAGE);
        for (start..actual_end) |i| {
            const entry: PfsEntry = @bitCast(buf[PFS_HEADER_SIZE + i]);
            if (!entry.allocated) return i;
        }
        return null;
    }
};

// ── GAM (Global Allocation Map) ──────────────────────────────────────

/// Header at the start of each GAM/SGAM page
const GAM_HEADER_SIZE: usize = 4;

/// Number of extents tracked by one GAM page (1 bit per extent)
pub const GAM_EXTENTS_PER_PAGE: usize = (PAGE_SIZE - GAM_HEADER_SIZE) * 8;

/// GAM page operations. Bit=1 means extent is FREE, Bit=0 means ALLOCATED.
pub const Gam = struct {
    /// Initialize a GAM page (all extents free = all bits set to 1).
    pub fn initPage(buf: *[PAGE_SIZE]u8) void {
        @memset(buf, 0);
        buf[0] = 'G';
        buf[1] = 'A';
        buf[2] = 'M';
        buf[3] = 0;
        // Set all extent bits to 1 (free)
        @memset(buf[GAM_HEADER_SIZE..], 0xFF);
    }

    /// Check if an extent is free.
    pub fn isExtentFree(buf: *const [PAGE_SIZE]u8, extent_id: usize) bool {
        if (extent_id >= GAM_EXTENTS_PER_PAGE) return false;
        const byte_idx = GAM_HEADER_SIZE + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        return (buf[byte_idx] & (@as(u8, 1) << bit_idx)) != 0;
    }

    /// Mark an extent as allocated (bit = 0).
    pub fn allocateExtent(buf: *[PAGE_SIZE]u8, extent_id: usize) void {
        if (extent_id >= GAM_EXTENTS_PER_PAGE) return;
        const byte_idx = GAM_HEADER_SIZE + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        buf[byte_idx] &= ~(@as(u8, 1) << bit_idx);
    }

    /// Mark an extent as free (bit = 1).
    pub fn freeExtent(buf: *[PAGE_SIZE]u8, extent_id: usize) void {
        if (extent_id >= GAM_EXTENTS_PER_PAGE) return;
        const byte_idx = GAM_HEADER_SIZE + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        buf[byte_idx] |= (@as(u8, 1) << bit_idx);
    }

    /// Find the first free extent starting from `start_extent`.
    pub fn findFreeExtent(buf: *const [PAGE_SIZE]u8, start_extent: usize) ?usize {
        var i = start_extent;
        while (i < GAM_EXTENTS_PER_PAGE) : (i += 1) {
            if (isExtentFree(buf, i)) return i;
        }
        return null;
    }
};

// ── SGAM (Shared Global Allocation Map) ──────────────────────────────

/// SGAM page operations. Bit=1 means extent is MIXED with free pages.
pub const Sgam = struct {
    /// Initialize an SGAM page (all bits 0 = no mixed extents initially).
    pub fn initPage(buf: *[PAGE_SIZE]u8) void {
        @memset(buf, 0);
        buf[0] = 'S';
        buf[1] = 'G';
        buf[2] = 'M';
        buf[3] = 0;
    }

    /// Check if an extent is mixed with free pages.
    pub fn isMixedWithFreePages(buf: *const [PAGE_SIZE]u8, extent_id: usize) bool {
        if (extent_id >= GAM_EXTENTS_PER_PAGE) return false;
        const byte_idx = GAM_HEADER_SIZE + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        return (buf[byte_idx] & (@as(u8, 1) << bit_idx)) != 0;
    }

    /// Mark an extent as mixed with free pages.
    pub fn markMixedWithFreePages(buf: *[PAGE_SIZE]u8, extent_id: usize) void {
        if (extent_id >= GAM_EXTENTS_PER_PAGE) return;
        const byte_idx = GAM_HEADER_SIZE + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        buf[byte_idx] |= (@as(u8, 1) << bit_idx);
    }

    /// Clear the mixed flag (extent is now uniform or full).
    pub fn clearMixed(buf: *[PAGE_SIZE]u8, extent_id: usize) void {
        if (extent_id >= GAM_EXTENTS_PER_PAGE) return;
        const byte_idx = GAM_HEADER_SIZE + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        buf[byte_idx] &= ~(@as(u8, 1) << bit_idx);
    }

    /// Find a mixed extent with free pages starting from `start_extent`.
    pub fn findMixedExtent(buf: *const [PAGE_SIZE]u8, start_extent: usize) ?usize {
        var i = start_extent;
        while (i < GAM_EXTENTS_PER_PAGE) : (i += 1) {
            if (isMixedWithFreePages(buf, i)) return i;
        }
        return null;
    }
};

// ── AllocManager ─────────────────────────────────────────────────────

/// Coordinates GAM/PFS/SGAM to allocate and free pages and extents.
/// Works through the buffer pool for all page access.
pub const AllocManager = struct {
    buffer_pool: *BufferPool,
    disk_manager: *DiskManager,

    const Self = @This();

    pub fn init(buffer_pool: *BufferPool, disk_manager: *DiskManager) Self {
        return .{
            .buffer_pool = buffer_pool,
            .disk_manager = disk_manager,
        };
    }

    /// Initialize a new data file with header + system pages (PFS, GAM, SGAM, reserved).
    pub fn initializeFile(self: *Self) AllocError!void {
        // Ensure the file has at least SYSTEM_PAGES pages
        while (self.disk_manager.getNumPages() < file_header.SYSTEM_PAGES) {
            _ = self.disk_manager.allocatePage() catch return AllocError.DiskError;
        }

        // Write file header (page 0)
        {
            var pg = self.buffer_pool.fetchPage(file_header.FILE_HEADER_PAGE) catch return AllocError.BufferPoolError;
            const hdr = FileHeaderPage.init(0, file_header.SYSTEM_PAGES);
            @memset(pg.data, 0);
            @memcpy(pg.data[0..FileHeaderPage.SIZE], std.mem.asBytes(&hdr));
            self.buffer_pool.unpinPage(file_header.FILE_HEADER_PAGE, true) catch return AllocError.BufferPoolError;
        }

        // Write PFS page (page 1)
        {
            var pg = self.buffer_pool.fetchPage(file_header.PFS_PAGE) catch return AllocError.BufferPoolError;
            Pfs.initPage(pg.data);
            // Mark system pages as allocated in PFS
            for (0..file_header.SYSTEM_PAGES) |i| {
                Pfs.markAllocated(pg.data, i);
            }
            self.buffer_pool.unpinPage(file_header.PFS_PAGE, true) catch return AllocError.BufferPoolError;
        }

        // Write GAM page (page 2)
        {
            var pg = self.buffer_pool.fetchPage(file_header.GAM_PAGE) catch return AllocError.BufferPoolError;
            Gam.initPage(pg.data);
            // Mark extent 0 (pages 0-7, containing system pages) as allocated
            Gam.allocateExtent(pg.data, 0);
            self.buffer_pool.unpinPage(file_header.GAM_PAGE, true) catch return AllocError.BufferPoolError;
        }

        // Write SGAM page (page 3)
        {
            var pg = self.buffer_pool.fetchPage(file_header.SGAM_PAGE) catch return AllocError.BufferPoolError;
            Sgam.initPage(pg.data);
            // Extent 0 is mixed (system pages use some, rest are free for data)
            Sgam.markMixedWithFreePages(pg.data, 0);
            self.buffer_pool.unpinPage(file_header.SGAM_PAGE, true) catch return AllocError.BufferPoolError;
        }

        // Reserved pages 4-5 (just zero-filled, already allocated by disk_manager)
    }

    /// Allocate a uniform extent — returns the first page ID of the extent.
    /// The extent is EXTENT_SIZE contiguous pages.
    pub fn allocateExtent(self: *Self) AllocError!PageId {
        // Find a free extent in GAM
        var gam_pg = self.buffer_pool.fetchPage(file_header.GAM_PAGE) catch return AllocError.BufferPoolError;
        defer self.buffer_pool.unpinPage(file_header.GAM_PAGE, true) catch {};

        // Start searching from extent 1 (extent 0 has system pages)
        const extent_id = Gam.findFreeExtent(gam_pg.data, 1) orelse return AllocError.NoFreeExtents;

        // Mark extent as allocated in GAM
        Gam.allocateExtent(gam_pg.data, extent_id);

        // Calculate the first page ID for this extent
        const first_page: PageId = @intCast(extent_id * EXTENT_SIZE);

        // Ensure the file is large enough
        while (self.disk_manager.getNumPages() < first_page + EXTENT_SIZE) {
            _ = self.disk_manager.allocatePage() catch return AllocError.DiskError;
        }

        // Mark all pages in PFS as allocated
        {
            var pfs_pg = self.buffer_pool.fetchPage(file_header.PFS_PAGE) catch return AllocError.BufferPoolError;
            defer self.buffer_pool.unpinPage(file_header.PFS_PAGE, true) catch {};

            for (0..EXTENT_SIZE) |i| {
                Pfs.markAllocated(pfs_pg.data, first_page + i);
            }
        }

        // Update file header page count
        self.updatePageCount(first_page + EXTENT_SIZE);

        return first_page;
    }

    /// Allocate a single page from a mixed extent (for small tables).
    pub fn allocateMixedPage(self: *Self) AllocError!PageId {
        // Try to find a mixed extent with free pages via SGAM
        var sgam_pg = self.buffer_pool.fetchPage(file_header.SGAM_PAGE) catch return AllocError.BufferPoolError;

        const mixed_extent_id = Sgam.findMixedExtent(sgam_pg.data, 0);
        self.buffer_pool.unpinPage(file_header.SGAM_PAGE, false) catch {};

        if (mixed_extent_id) |ext_id| {
            // Found a mixed extent — find a free page in it via PFS
            const ext_start: usize = ext_id * EXTENT_SIZE;

            var pfs_pg = self.buffer_pool.fetchPage(file_header.PFS_PAGE) catch return AllocError.BufferPoolError;
            defer self.buffer_pool.unpinPage(file_header.PFS_PAGE, true) catch {};

            // Search for a free page within the extent's range
            if (Pfs.findFreePage(pfs_pg.data, ext_start, ext_start + EXTENT_SIZE)) |page_offset| {
                Pfs.markAllocated(pfs_pg.data, page_offset);

                // Check if the extent is now full — clear SGAM bit
                if (Pfs.findFreePage(pfs_pg.data, ext_start, ext_start + EXTENT_SIZE) == null) {
                    var sgam_pg2 = self.buffer_pool.fetchPage(file_header.SGAM_PAGE) catch return AllocError.BufferPoolError;
                    Sgam.clearMixed(sgam_pg2.data, ext_id);
                    self.buffer_pool.unpinPage(file_header.SGAM_PAGE, true) catch {};
                }

                return @intCast(page_offset);
            }
        }

        // No mixed extent available — allocate a new extent and make it mixed
        const first_page = try self.allocateExtent();
        const new_extent_id: usize = @intCast(first_page / EXTENT_SIZE);

        // Mark it as mixed in SGAM
        {
            var sgam_pg2 = self.buffer_pool.fetchPage(file_header.SGAM_PAGE) catch return AllocError.BufferPoolError;
            Sgam.markMixedWithFreePages(sgam_pg2.data, new_extent_id);
            self.buffer_pool.unpinPage(file_header.SGAM_PAGE, true) catch {};
        }

        // The first page of the extent is allocated (already marked in allocateExtent)
        // But we only want to mark the first page as used, rest stay free in PFS
        // Actually, allocateExtent marked ALL pages. Undo that for pages 1-7.
        {
            var pfs_pg = self.buffer_pool.fetchPage(file_header.PFS_PAGE) catch return AllocError.BufferPoolError;
            for (1..EXTENT_SIZE) |i| {
                Pfs.markFree(pfs_pg.data, first_page + i);
            }
            self.buffer_pool.unpinPage(file_header.PFS_PAGE, true) catch {};
        }

        return first_page;
    }

    /// Free a single page.
    pub fn freePage(self: *Self, page_id: PageId) AllocError!void {
        var pfs_pg = self.buffer_pool.fetchPage(file_header.PFS_PAGE) catch return AllocError.BufferPoolError;
        Pfs.markFree(pfs_pg.data, page_id);
        self.buffer_pool.unpinPage(file_header.PFS_PAGE, true) catch {};

        // Check if the entire extent is now free — if so, free it in GAM
        const extent_id: usize = @intCast(page_id / EXTENT_SIZE);
        const ext_start: usize = extent_id * EXTENT_SIZE;

        // Re-fetch PFS to check all pages in the extent
        var pfs_pg2 = self.buffer_pool.fetchPage(file_header.PFS_PAGE) catch return AllocError.BufferPoolError;
        defer self.buffer_pool.unpinPage(file_header.PFS_PAGE, false) catch {};

        var all_free = true;
        for (0..EXTENT_SIZE) |i| {
            const entry = Pfs.getEntry(pfs_pg2.data, ext_start + i);
            if (entry.allocated) {
                all_free = false;
                break;
            }
        }

        if (all_free and extent_id > 0) { // Don't free extent 0 (system pages)
            var gam_pg = self.buffer_pool.fetchPage(file_header.GAM_PAGE) catch return AllocError.BufferPoolError;
            Gam.freeExtent(gam_pg.data, extent_id);
            self.buffer_pool.unpinPage(file_header.GAM_PAGE, true) catch {};

            // Also clear SGAM
            var sgam_pg = self.buffer_pool.fetchPage(file_header.SGAM_PAGE) catch return AllocError.BufferPoolError;
            Sgam.clearMixed(sgam_pg.data, extent_id);
            self.buffer_pool.unpinPage(file_header.SGAM_PAGE, true) catch {};
        }
    }

    /// Free an entire extent.
    pub fn freeExtent(self: *Self, first_page: PageId) AllocError!void {
        const extent_id: usize = @intCast(first_page / EXTENT_SIZE);
        if (extent_id == 0) return; // Don't free system extent

        // Mark all pages as free in PFS
        {
            var pfs_pg = self.buffer_pool.fetchPage(file_header.PFS_PAGE) catch return AllocError.BufferPoolError;
            for (0..EXTENT_SIZE) |i| {
                Pfs.markFree(pfs_pg.data, first_page + i);
            }
            self.buffer_pool.unpinPage(file_header.PFS_PAGE, true) catch {};
        }

        // Mark extent as free in GAM
        {
            var gam_pg = self.buffer_pool.fetchPage(file_header.GAM_PAGE) catch return AllocError.BufferPoolError;
            Gam.freeExtent(gam_pg.data, extent_id);
            self.buffer_pool.unpinPage(file_header.GAM_PAGE, true) catch {};
        }

        // Clear SGAM
        {
            var sgam_pg = self.buffer_pool.fetchPage(file_header.SGAM_PAGE) catch return AllocError.BufferPoolError;
            Sgam.clearMixed(sgam_pg.data, extent_id);
            self.buffer_pool.unpinPage(file_header.SGAM_PAGE, true) catch {};
        }
    }

    /// Read the page count from the file header.
    pub fn getPageCount(self: *Self) AllocError!u32 {
        var pg = self.buffer_pool.fetchPage(file_header.FILE_HEADER_PAGE) catch return AllocError.BufferPoolError;
        defer self.buffer_pool.unpinPage(file_header.FILE_HEADER_PAGE, false) catch {};
        const hdr = FileHeaderPage.readFrom(pg.data);
        return hdr.page_count;
    }

    /// Update the page count in the file header.
    fn updatePageCount(self: *Self, new_count: u32) void {
        var pg = self.buffer_pool.fetchPage(file_header.FILE_HEADER_PAGE) catch return;
        var hdr = FileHeaderPage.readFrom(pg.data);
        if (new_count > hdr.page_count) {
            hdr.page_count = new_count;
            @memcpy(pg.data[0..FileHeaderPage.SIZE], std.mem.asBytes(&hdr));
            self.buffer_pool.unpinPage(file_header.FILE_HEADER_PAGE, true) catch {};
        } else {
            self.buffer_pool.unpinPage(file_header.FILE_HEADER_PAGE, false) catch {};
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "PFS init and mark allocated" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Pfs.initPage(&buf);

    // All entries should start unallocated
    const entry = Pfs.getEntry(&buf, 0);
    try std.testing.expect(!entry.allocated);

    // Mark page 5 as allocated
    Pfs.markAllocated(&buf, 5);
    const entry2 = Pfs.getEntry(&buf, 5);
    try std.testing.expect(entry2.allocated);

    // Other pages still unallocated
    const entry3 = Pfs.getEntry(&buf, 4);
    try std.testing.expect(!entry3.allocated);
}

test "PFS find free page" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Pfs.initPage(&buf);

    // Mark first 5 pages allocated
    for (0..5) |i| {
        Pfs.markAllocated(&buf, i);
    }

    // Should find page 5 as first free
    const free = Pfs.findFreePage(&buf, 0, 10);
    try std.testing.expectEqual(@as(?usize, 5), free);

    // Starting from 5, should find 5
    const free2 = Pfs.findFreePage(&buf, 5, 10);
    try std.testing.expectEqual(@as(?usize, 5), free2);

    // Mark 5 allocated too, should find 6
    Pfs.markAllocated(&buf, 5);
    const free3 = Pfs.findFreePage(&buf, 0, 10);
    try std.testing.expectEqual(@as(?usize, 6), free3);
}

test "PFS mark free" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Pfs.initPage(&buf);

    Pfs.markAllocated(&buf, 3);
    try std.testing.expect(Pfs.getEntry(&buf, 3).allocated);

    Pfs.markFree(&buf, 3);
    try std.testing.expect(!Pfs.getEntry(&buf, 3).allocated);
}

test "GAM init all free" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Gam.initPage(&buf);

    // All extents should start free
    try std.testing.expect(Gam.isExtentFree(&buf, 0));
    try std.testing.expect(Gam.isExtentFree(&buf, 100));
    try std.testing.expect(Gam.isExtentFree(&buf, 1000));
}

test "GAM allocate and free extent" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Gam.initPage(&buf);

    // Allocate extent 5
    Gam.allocateExtent(&buf, 5);
    try std.testing.expect(!Gam.isExtentFree(&buf, 5));
    try std.testing.expect(Gam.isExtentFree(&buf, 4));
    try std.testing.expect(Gam.isExtentFree(&buf, 6));

    // Free extent 5
    Gam.freeExtent(&buf, 5);
    try std.testing.expect(Gam.isExtentFree(&buf, 5));
}

test "GAM find free extent" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Gam.initPage(&buf);

    // Allocate extents 0-9
    for (0..10) |i| {
        Gam.allocateExtent(&buf, i);
    }

    // Should find extent 10
    const free = Gam.findFreeExtent(&buf, 0);
    try std.testing.expectEqual(@as(?usize, 10), free);

    // Starting from 10, should still find 10
    const free2 = Gam.findFreeExtent(&buf, 10);
    try std.testing.expectEqual(@as(?usize, 10), free2);
}

test "SGAM init and mark mixed" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Sgam.initPage(&buf);

    // Nothing mixed initially
    try std.testing.expect(!Sgam.isMixedWithFreePages(&buf, 0));

    // Mark extent 3 as mixed
    Sgam.markMixedWithFreePages(&buf, 3);
    try std.testing.expect(Sgam.isMixedWithFreePages(&buf, 3));
    try std.testing.expect(!Sgam.isMixedWithFreePages(&buf, 2));

    // Find it
    const found = Sgam.findMixedExtent(&buf, 0);
    try std.testing.expectEqual(@as(?usize, 3), found);

    // Clear it
    Sgam.clearMixed(&buf, 3);
    try std.testing.expect(!Sgam.isMixedWithFreePages(&buf, 3));
}

test "AllocManager initialize file" {
    const test_file = "test_alloc_init.db";
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

    // File should have at least SYSTEM_PAGES pages
    try std.testing.expect(dm.getNumPages() >= file_header.SYSTEM_PAGES);

    // Verify file header
    const page_count = try am.getPageCount();
    try std.testing.expectEqual(file_header.SYSTEM_PAGES, page_count);

    // Verify GAM: extent 0 allocated, extent 1 free
    {
        var gam_pg = try bp.fetchPage(file_header.GAM_PAGE);
        defer bp.unpinPage(file_header.GAM_PAGE, false) catch {};
        try std.testing.expect(!Gam.isExtentFree(gam_pg.data, 0));
        try std.testing.expect(Gam.isExtentFree(gam_pg.data, 1));
    }

    // Verify PFS: system pages allocated
    {
        var pfs_pg = try bp.fetchPage(file_header.PFS_PAGE);
        defer bp.unpinPage(file_header.PFS_PAGE, false) catch {};
        for (0..file_header.SYSTEM_PAGES) |i| {
            try std.testing.expect(Pfs.getEntry(pfs_pg.data, i).allocated);
        }
    }
}

test "AllocManager allocate extent" {
    const test_file = "test_alloc_extent.db";
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

    // Allocate first extent (should be extent 1 = page 8)
    const ext1 = try am.allocateExtent();
    try std.testing.expectEqual(@as(PageId, 8), ext1);

    // Allocate second extent (should be extent 2 = page 16)
    const ext2 = try am.allocateExtent();
    try std.testing.expectEqual(@as(PageId, 16), ext2);

    // Verify GAM
    {
        var gam_pg = try bp.fetchPage(file_header.GAM_PAGE);
        defer bp.unpinPage(file_header.GAM_PAGE, false) catch {};
        try std.testing.expect(!Gam.isExtentFree(gam_pg.data, 0)); // system
        try std.testing.expect(!Gam.isExtentFree(gam_pg.data, 1)); // ext1
        try std.testing.expect(!Gam.isExtentFree(gam_pg.data, 2)); // ext2
        try std.testing.expect(Gam.isExtentFree(gam_pg.data, 3)); // still free
    }
}

test "AllocManager free extent" {
    const test_file = "test_alloc_free.db";
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

    // Allocate then free
    const ext = try am.allocateExtent();
    try am.freeExtent(ext);

    // Should be free in GAM again
    {
        var gam_pg = try bp.fetchPage(file_header.GAM_PAGE);
        defer bp.unpinPage(file_header.GAM_PAGE, false) catch {};
        try std.testing.expect(Gam.isExtentFree(gam_pg.data, ext / EXTENT_SIZE));
    }
}

test "AllocManager allocate many extents" {
    const test_file = "test_alloc_many.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 64);
    defer bp.deinit();

    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    // Allocate 100 extents — well beyond the old MAX_EXTENTS=62
    var extents: [100]PageId = undefined;
    for (0..100) |i| {
        extents[i] = try am.allocateExtent();
        // Each extent should start at (i+1)*EXTENT_SIZE
        try std.testing.expectEqual(@as(PageId, @intCast((i + 1) * EXTENT_SIZE)), extents[i]);
    }
}
