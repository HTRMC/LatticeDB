const std = @import("std");
const page_mod = @import("page.zig");
const buffer_pool_mod = @import("buffer_pool.zig");

const PageId = page_mod.PageId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const PAGE_SIZE = page_mod.PAGE_SIZE;
const BufferPool = buffer_pool_mod.BufferPool;

/// IAM page header layout
pub const IamHeader = extern struct {
    /// Magic marker "IAM\x01"
    magic: u32,
    /// Table ID or index ID that owns these extents
    allocation_unit_id: u32,
    /// File ID this IAM page maps extents for
    file_id: u16,
    /// Sequence number within the IAM chain (0 = first)
    sequence_number: u16,
    /// Page ID of the next IAM page in chain (INVALID_PAGE_ID if last)
    next_iam_page: PageId,
    /// Number of single-page slots used (for mixed extent tracking)
    single_page_count: u16,
    _pad: u16,

    pub const SIZE: usize = @sizeOf(IamHeader);
};

/// Magic bytes for IAM pages
pub const IAM_MAGIC: u32 = 0x49414D01; // "IAM\x01"

/// Maximum single-page slots in an IAM page (for mixed extent pages)
pub const MAX_SINGLE_PAGE_SLOTS: usize = 8;

/// Size of the single-page slots area
const SINGLE_PAGE_AREA_SIZE: usize = MAX_SINGLE_PAGE_SLOTS * @sizeOf(PageId);

/// Offset where the extent bitmap starts
const BITMAP_OFFSET: usize = IamHeader.SIZE + SINGLE_PAGE_AREA_SIZE;

/// Number of bits (extents) tracked by one IAM page
pub const EXTENTS_PER_IAM_PAGE: usize = (PAGE_SIZE - BITMAP_OFFSET) * 8;

/// IAM page operations — work directly on page buffers.
pub const Iam = struct {
    /// Initialize a new IAM page.
    pub fn initPage(buf: *[PAGE_SIZE]u8, alloc_unit_id: u32, file_id: u16, seq: u16) void {
        @memset(buf, 0);
        var hdr = IamHeader{
            .magic = IAM_MAGIC,
            .allocation_unit_id = alloc_unit_id,
            .file_id = file_id,
            .sequence_number = seq,
            .next_iam_page = INVALID_PAGE_ID,
            .single_page_count = 0,
            ._pad = 0,
        };
        @memcpy(buf[0..IamHeader.SIZE], std.mem.asBytes(&hdr));
    }

    /// Read the header from an IAM page.
    pub fn readHeader(buf: *const [PAGE_SIZE]u8) IamHeader {
        return std.mem.bytesToValue(IamHeader, buf[0..IamHeader.SIZE]);
    }

    /// Write the header to an IAM page.
    pub fn writeHeader(buf: *[PAGE_SIZE]u8, hdr: IamHeader) void {
        @memcpy(buf[0..IamHeader.SIZE], std.mem.asBytes(&hdr));
    }

    /// Check if this IAM page owns a given extent.
    pub fn hasExtent(buf: *const [PAGE_SIZE]u8, extent_id: usize) bool {
        if (extent_id >= EXTENTS_PER_IAM_PAGE) return false;
        const byte_idx = BITMAP_OFFSET + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        return (buf[byte_idx] & (@as(u8, 1) << bit_idx)) != 0;
    }

    /// Add an extent to this IAM page's bitmap.
    pub fn addExtent(buf: *[PAGE_SIZE]u8, extent_id: usize) void {
        if (extent_id >= EXTENTS_PER_IAM_PAGE) return;
        const byte_idx = BITMAP_OFFSET + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        buf[byte_idx] |= (@as(u8, 1) << bit_idx);
    }

    /// Remove an extent from this IAM page's bitmap.
    pub fn removeExtent(buf: *[PAGE_SIZE]u8, extent_id: usize) void {
        if (extent_id >= EXTENTS_PER_IAM_PAGE) return;
        const byte_idx = BITMAP_OFFSET + extent_id / 8;
        const bit_idx: u3 = @intCast(extent_id % 8);
        buf[byte_idx] &= ~(@as(u8, 1) << bit_idx);
    }

    /// Add a single-page slot (for mixed extent pages).
    pub fn addSinglePage(buf: *[PAGE_SIZE]u8, page_id: PageId) bool {
        var hdr = readHeader(buf);
        if (hdr.single_page_count >= MAX_SINGLE_PAGE_SLOTS) return false;
        const slot_offset = IamHeader.SIZE + @as(usize, hdr.single_page_count) * @sizeOf(PageId);
        @memcpy(buf[slot_offset..][0..@sizeOf(PageId)], std.mem.asBytes(&page_id));
        hdr.single_page_count += 1;
        writeHeader(buf, hdr);
        return true;
    }

    /// Get a single-page slot by index.
    pub fn getSinglePage(buf: *const [PAGE_SIZE]u8, index: usize) ?PageId {
        const hdr = readHeader(buf);
        if (index >= hdr.single_page_count) return null;
        const slot_offset = IamHeader.SIZE + index * @sizeOf(PageId);
        return std.mem.bytesToValue(PageId, buf[slot_offset..][0..@sizeOf(PageId)]);
    }

    /// Set the next IAM page in the chain.
    pub fn setNextPage(buf: *[PAGE_SIZE]u8, next_page: PageId) void {
        var hdr = readHeader(buf);
        hdr.next_iam_page = next_page;
        writeHeader(buf, hdr);
    }

    /// Get the next IAM page in the chain.
    pub fn getNextPage(buf: *const [PAGE_SIZE]u8) PageId {
        const hdr = readHeader(buf);
        return hdr.next_iam_page;
    }
};

/// Iterator that walks an IAM chain and yields all owned extent IDs.
pub const IamChainIterator = struct {
    buffer_pool: *BufferPool,
    current_page_id: PageId,
    current_extent: usize,
    done: bool,

    pub fn init(buffer_pool: *BufferPool, first_iam_page: PageId) IamChainIterator {
        return .{
            .buffer_pool = buffer_pool,
            .current_page_id = first_iam_page,
            .current_extent = 0,
            .done = first_iam_page == INVALID_PAGE_ID,
        };
    }

    /// Returns the next owned extent ID, or null when exhausted.
    pub fn next(self: *IamChainIterator) ?usize {
        while (!self.done) {
            // Fetch the current IAM page
            var pg = self.buffer_pool.fetchPage(self.current_page_id) catch return null;
            defer self.buffer_pool.unpinPage(self.current_page_id, false) catch {};

            // Search for set bits in the bitmap starting from current_extent
            while (self.current_extent < EXTENTS_PER_IAM_PAGE) {
                const ext = self.current_extent;
                self.current_extent += 1;
                if (Iam.hasExtent(pg.data, ext)) {
                    return ext;
                }
            }

            // Move to next IAM page in chain
            const next_page = Iam.getNextPage(pg.data);
            if (next_page == INVALID_PAGE_ID) {
                self.done = true;
                return null;
            }
            self.current_page_id = next_page;
            self.current_extent = 0;
        }
        return null;
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "IAM page init and header" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Iam.initPage(&buf, 42, 0, 0);

    const hdr = Iam.readHeader(&buf);
    try std.testing.expectEqual(IAM_MAGIC, hdr.magic);
    try std.testing.expectEqual(@as(u32, 42), hdr.allocation_unit_id);
    try std.testing.expectEqual(@as(u16, 0), hdr.file_id);
    try std.testing.expectEqual(INVALID_PAGE_ID, hdr.next_iam_page);
}

test "IAM add and check extents" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Iam.initPage(&buf, 1, 0, 0);

    try std.testing.expect(!Iam.hasExtent(&buf, 5));

    Iam.addExtent(&buf, 5);
    try std.testing.expect(Iam.hasExtent(&buf, 5));
    try std.testing.expect(!Iam.hasExtent(&buf, 4));
    try std.testing.expect(!Iam.hasExtent(&buf, 6));

    Iam.addExtent(&buf, 100);
    try std.testing.expect(Iam.hasExtent(&buf, 100));

    Iam.removeExtent(&buf, 5);
    try std.testing.expect(!Iam.hasExtent(&buf, 5));
    try std.testing.expect(Iam.hasExtent(&buf, 100));
}

test "IAM single page slots" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Iam.initPage(&buf, 1, 0, 0);

    // Add single pages
    try std.testing.expect(Iam.addSinglePage(&buf, 10));
    try std.testing.expect(Iam.addSinglePage(&buf, 20));
    try std.testing.expect(Iam.addSinglePage(&buf, 30));

    // Read them back
    try std.testing.expectEqual(@as(?PageId, 10), Iam.getSinglePage(&buf, 0));
    try std.testing.expectEqual(@as(?PageId, 20), Iam.getSinglePage(&buf, 1));
    try std.testing.expectEqual(@as(?PageId, 30), Iam.getSinglePage(&buf, 2));
    try std.testing.expectEqual(@as(?PageId, null), Iam.getSinglePage(&buf, 3));

    // Header should show count = 3
    const hdr = Iam.readHeader(&buf);
    try std.testing.expectEqual(@as(u16, 3), hdr.single_page_count);
}

test "IAM chain next page" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Iam.initPage(&buf, 1, 0, 0);

    try std.testing.expectEqual(INVALID_PAGE_ID, Iam.getNextPage(&buf));

    Iam.setNextPage(&buf, 42);
    try std.testing.expectEqual(@as(PageId, 42), Iam.getNextPage(&buf));
}

test "IAM many extents" {
    var buf: [PAGE_SIZE]u8 = undefined;
    Iam.initPage(&buf, 1, 0, 0);

    // Add many extents — well beyond old MAX_EXTENTS=62
    for (0..200) |i| {
        Iam.addExtent(&buf, i);
    }

    // Verify all are set
    for (0..200) |i| {
        try std.testing.expect(Iam.hasExtent(&buf, i));
    }
    try std.testing.expect(!Iam.hasExtent(&buf, 200));
}

test "IamChainIterator single page" {
    const disk_manager_mod = @import("disk_manager.zig");
    const DiskManager = disk_manager_mod.DiskManager;

    const test_file = "test_iam_chain_iter.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 16);
    defer bp.deinit();

    // Allocate a page for the IAM
    const result = try bp.newPage();
    const iam_page_id = result.page_id;
    Iam.initPage(result.page.data, 1, 0, 0);
    Iam.addExtent(result.page.data, 3);
    Iam.addExtent(result.page.data, 7);
    Iam.addExtent(result.page.data, 15);
    try bp.unpinPage(iam_page_id, true);

    // Iterate
    var iter = IamChainIterator.init(&bp, iam_page_id);
    try std.testing.expectEqual(@as(?usize, 3), iter.next());
    try std.testing.expectEqual(@as(?usize, 7), iter.next());
    try std.testing.expectEqual(@as(?usize, 15), iter.next());
    try std.testing.expectEqual(@as(?usize, null), iter.next());
}
