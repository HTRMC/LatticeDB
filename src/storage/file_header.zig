const std = @import("std");
const page_mod = @import("page.zig");

const PAGE_SIZE = page_mod.PAGE_SIZE;
const PageId = page_mod.PageId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;

/// Magic bytes: "GDB\x01"
pub const FILE_MAGIC: u32 = 0x47444201;

/// Current file format version
pub const FILE_FORMAT_VERSION: u32 = 1;

/// Well-known page IDs within a data file
pub const FILE_HEADER_PAGE: PageId = 0;
pub const PFS_PAGE: PageId = 1;
pub const GAM_PAGE: PageId = 2;
pub const SGAM_PAGE: PageId = 3;
pub const RESERVED_PAGE_4: PageId = 4;
pub const RESERVED_PAGE_5: PageId = 5;
pub const FIRST_DATA_PAGE: PageId = 6;

/// Number of system pages reserved at the start of a data file
pub const SYSTEM_PAGES: u32 = 6;

/// File header stored at page 0.
pub const FileHeaderPage = extern struct {
    magic: u32,
    version: u32,
    file_id: u16,
    _pad: u16,
    creation_time: u64,
    page_count: u32,
    first_free_extent: u32,
    // Catalog bootstrap IDs — persisted so catalog survives restart
    catalog_tables_id: PageId,
    catalog_columns_id: PageId,
    catalog_indexes_id: PageId,
    _reserved2: u32,

    pub const SIZE: usize = @sizeOf(FileHeaderPage);

    pub fn init(file_id: u16, page_count: u32) FileHeaderPage {
        return .{
            .magic = FILE_MAGIC,
            .version = FILE_FORMAT_VERSION,
            .file_id = file_id,
            ._pad = 0,
            .creation_time = 0, // Could use timestamp but not critical
            .page_count = page_count,
            .first_free_extent = 0,
            .catalog_tables_id = INVALID_PAGE_ID,
            .catalog_columns_id = INVALID_PAGE_ID,
            .catalog_indexes_id = INVALID_PAGE_ID,
            ._reserved2 = 0,
        };
    }

    pub fn validate(self: *const FileHeaderPage) bool {
        return self.magic == FILE_MAGIC and self.version == FILE_FORMAT_VERSION;
    }

    /// Serialize header into a page buffer.
    pub fn writeTo(self: *const FileHeaderPage, buf: *[PAGE_SIZE]u8) void {
        @memset(buf, 0);
        @memcpy(buf[0..SIZE], std.mem.asBytes(self));
    }

    /// Deserialize header from a page buffer.
    pub fn readFrom(buf: *const [PAGE_SIZE]u8) FileHeaderPage {
        return std.mem.bytesToValue(FileHeaderPage, buf[0..SIZE]);
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "FileHeaderPage init and validate" {
    const hdr = FileHeaderPage.init(0, 64);
    try std.testing.expect(hdr.validate());
    try std.testing.expectEqual(FILE_MAGIC, hdr.magic);
    try std.testing.expectEqual(FILE_FORMAT_VERSION, hdr.version);
    try std.testing.expectEqual(@as(u16, 0), hdr.file_id);
    try std.testing.expectEqual(@as(u32, 64), hdr.page_count);
}

test "FileHeaderPage serialize roundtrip" {
    const hdr = FileHeaderPage.init(1, 128);
    var buf: [PAGE_SIZE]u8 = undefined;
    hdr.writeTo(&buf);

    const hdr2 = FileHeaderPage.readFrom(&buf);
    try std.testing.expect(hdr2.validate());
    try std.testing.expectEqual(@as(u16, 1), hdr2.file_id);
    try std.testing.expectEqual(@as(u32, 128), hdr2.page_count);
}

test "FileHeaderPage invalid magic" {
    var hdr = FileHeaderPage.init(0, 10);
    hdr.magic = 0xDEADBEEF;
    try std.testing.expect(!hdr.validate());
}
