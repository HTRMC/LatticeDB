const std = @import("std");

pub const PAGE_SIZE: usize = 8192; // 8KB pages

/// Page ID type - uniquely identifies a page in the database
pub const PageId = u32;

/// Invalid page ID sentinel
pub const INVALID_PAGE_ID: PageId = std.math.maxInt(PageId);

/// Slot pointer - points to a tuple within a page
pub const SlotId = u16;

/// Tuple identifier - combines page ID and slot ID
pub const TupleId = struct {
    page_id: PageId,
    slot_id: SlotId,

    pub fn invalid() TupleId {
        return .{
            .page_id = INVALID_PAGE_ID,
            .slot_id = std.math.maxInt(SlotId),
        };
    }

    pub fn isValid(self: TupleId) bool {
        return self.page_id != INVALID_PAGE_ID;
    }
};

/// Page flags
pub const PageFlags = packed struct {
    is_dirty: bool = false,
    is_leaf: bool = true, // For B+tree pages
    _padding: u6 = 0,
};

/// Page header - stored at the beginning of each page
/// Layout: [Header][Slot Array ->    <- Tuple Data]
pub const PageHeader = extern struct {
    /// Unique identifier for this page
    page_id: PageId,
    /// Log sequence number for WAL
    lsn: u64,
    /// Number of slots (tuples) in this page
    slot_count: u16,
    /// Offset to start of free space (end of slot array)
    free_space_start: u16,
    /// Offset to end of free space (start of tuple data)
    free_space_end: u16,
    /// Page flags
    flags: PageFlags,
    /// Reserved for alignment
    _reserved: u8 = 0,

    pub const SIZE: usize = @sizeOf(PageHeader);

    pub fn init(page_id: PageId) PageHeader {
        return .{
            .page_id = page_id,
            .lsn = 0,
            .slot_count = 0,
            .free_space_start = @intCast(PageHeader.SIZE),
            .free_space_end = PAGE_SIZE,
            .flags = .{},
        };
    }

    pub fn freeSpace(self: *const PageHeader) usize {
        if (self.free_space_end <= self.free_space_start) return 0;
        return self.free_space_end - self.free_space_start;
    }
};

/// Slot entry - points to tuple data within the page
pub const Slot = extern struct {
    /// Offset from page start to tuple data
    offset: u16,
    /// Length of tuple data
    length: u16,

    pub const SIZE: usize = @sizeOf(Slot);
    pub const EMPTY: Slot = .{ .offset = 0, .length = 0 };

    pub fn isEmpty(self: Slot) bool {
        return self.length == 0;
    }
};

/// A database page - fixed 8KB block
pub const Page = struct {
    data: *align(8) [PAGE_SIZE]u8,

    const Self = @This();

    /// Initialize a new page with the given ID
    pub fn init(data: *align(8) [PAGE_SIZE]u8, page_id: PageId) Self {
        const page = Self{ .data = data };
        @memset(data, 0);
        page.getHeaderMut().* = PageHeader.init(page_id);
        return page;
    }

    /// Wrap existing page data
    pub fn fromBytes(data: *align(8) [PAGE_SIZE]u8) Self {
        return Self{ .data = data };
    }

    /// Get page header (read-only)
    pub fn getHeader(self: *const Self) *const PageHeader {
        return @ptrCast(self.data);
    }

    /// Get page header (mutable)
    pub fn getHeaderMut(self: *const Self) *PageHeader {
        return @ptrCast(self.data);
    }

    /// Get the page ID
    pub fn getPageId(self: *const Self) PageId {
        return self.getHeader().page_id;
    }

    /// Get available free space
    pub fn getFreeSpace(self: *const Self) usize {
        return self.getHeader().freeSpace();
    }

    /// Get slot array (read-only)
    fn getSlots(self: *const Self) []const Slot {
        const header = self.getHeader();
        const slot_start: [*]const Slot = @ptrCast(@alignCast(self.data[PageHeader.SIZE..]));
        return slot_start[0..header.slot_count];
    }

    /// Get slot array (mutable)
    fn getSlotsMut(self: *const Self) []Slot {
        const header = self.getHeader();
        const slot_start: [*]Slot = @ptrCast(@alignCast(self.data[PageHeader.SIZE..]));
        return slot_start[0..header.slot_count];
    }

    /// Get a specific slot
    pub fn getSlot(self: *const Self, slot_id: SlotId) ?Slot {
        const slots = self.getSlots();
        if (slot_id >= slots.len) return null;
        return slots[slot_id];
    }

    /// Insert tuple data into the page
    /// Returns the slot ID if successful, null if not enough space
    pub fn insertTuple(self: *Self, tuple_data: []const u8) ?SlotId {
        const header = self.getHeaderMut();
        const required_space = tuple_data.len + Slot.SIZE;

        // Check if we have enough space
        if (header.freeSpace() < required_space) {
            return null;
        }

        // Allocate space for tuple at the end (growing backwards)
        const tuple_offset = header.free_space_end - @as(u16, @intCast(tuple_data.len));
        header.free_space_end = tuple_offset;

        // Copy tuple data
        @memcpy(self.data[tuple_offset..][0..tuple_data.len], tuple_data);

        // Add slot entry
        const slot_id = header.slot_count;
        const slot_offset = PageHeader.SIZE + (slot_id * Slot.SIZE);

        const slot_ptr: *Slot = @ptrCast(@alignCast(self.data[slot_offset..]));
        slot_ptr.* = .{
            .offset = tuple_offset,
            .length = @intCast(tuple_data.len),
        };

        header.slot_count += 1;
        header.free_space_start = @intCast(PageHeader.SIZE + (header.slot_count * Slot.SIZE));

        return slot_id;
    }

    /// Get tuple data by slot ID
    pub fn getTuple(self: *const Self, slot_id: SlotId) ?[]const u8 {
        const slot = self.getSlot(slot_id) orelse return null;
        if (slot.isEmpty()) return null;
        return self.data[slot.offset..][0..slot.length];
    }

    /// Update bytes within an existing tuple at the given offset.
    /// Used for in-place modifications like setting xmax in MVCC headers.
    /// Returns false if the slot is invalid or the write would exceed tuple bounds.
    pub fn updateTupleData(self: *Self, slot_id: SlotId, offset: u16, data: []const u8) bool {
        const slot = self.getSlot(slot_id) orelse return false;
        if (slot.isEmpty()) return false;
        if (offset + data.len > slot.length) return false;

        const dest_start = slot.offset + offset;
        @memcpy(self.data[dest_start..][0..data.len], data);
        return true;
    }

    /// Delete a tuple by slot ID (marks slot as empty, doesn't reclaim space yet)
    pub fn deleteTuple(self: *Self, slot_id: SlotId) bool {
        const header = self.getHeader();
        if (slot_id >= header.slot_count) return false;

        const slot_offset = PageHeader.SIZE + (slot_id * Slot.SIZE);
        const slot_ptr: *Slot = @ptrCast(@alignCast(self.data[slot_offset..]));
        slot_ptr.* = Slot.EMPTY;
        return true;
    }

    /// Mark page as dirty
    pub fn setDirty(self: *Self, dirty: bool) void {
        self.getHeaderMut().flags.is_dirty = dirty;
    }

    /// Check if page is dirty
    pub fn isDirty(self: *const Self) bool {
        return self.getHeader().flags.is_dirty;
    }

    /// Update the LSN (for WAL)
    pub fn setLsn(self: *Self, lsn: u64) void {
        self.getHeaderMut().lsn = lsn;
    }

    /// Get the LSN
    pub fn getLsn(self: *const Self) u64 {
        return self.getHeader().lsn;
    }

    /// Iterate over all valid tuples
    pub fn tupleIterator(self: *const Self) TupleIterator {
        return TupleIterator.init(self);
    }

    pub const TupleIterator = struct {
        page: *const Page,
        current_slot: SlotId,

        pub fn init(page: *const Page) TupleIterator {
            return .{ .page = page, .current_slot = 0 };
        }

        pub fn next(self: *TupleIterator) ?struct { slot_id: SlotId, data: []const u8 } {
            const header = self.page.getHeader();
            while (self.current_slot < header.slot_count) {
                const slot_id = self.current_slot;
                self.current_slot += 1;

                if (self.page.getTuple(slot_id)) |data| {
                    return .{ .slot_id = slot_id, .data = data };
                }
            }
            return null;
        }
    };
};

// Tests
test "page initialization" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    const page = Page.init(&data, 42);

    try std.testing.expectEqual(@as(PageId, 42), page.getPageId());
    try std.testing.expectEqual(@as(u16, 0), page.getHeader().slot_count);
    try std.testing.expect(page.getFreeSpace() > 0);
}

test "page insert and retrieve tuple" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var page = Page.init(&data, 1);

    const tuple1 = "Hello, GrapheneDB!";
    const tuple2 = "Second tuple";

    const slot1 = page.insertTuple(tuple1) orelse unreachable;
    const slot2 = page.insertTuple(tuple2) orelse unreachable;

    try std.testing.expectEqual(@as(SlotId, 0), slot1);
    try std.testing.expectEqual(@as(SlotId, 1), slot2);

    const retrieved1 = page.getTuple(slot1) orelse unreachable;
    const retrieved2 = page.getTuple(slot2) orelse unreachable;

    try std.testing.expectEqualStrings(tuple1, retrieved1);
    try std.testing.expectEqualStrings(tuple2, retrieved2);
}

test "page delete tuple" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var page = Page.init(&data, 1);

    const tuple = "Test tuple";
    const slot_id = page.insertTuple(tuple) orelse unreachable;

    try std.testing.expect(page.getTuple(slot_id) != null);
    try std.testing.expect(page.deleteTuple(slot_id));
    try std.testing.expect(page.getTuple(slot_id) == null);
}

test "page tuple iterator" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var page = Page.init(&data, 1);

    _ = page.insertTuple("tuple1") orelse unreachable;
    _ = page.insertTuple("tuple2") orelse unreachable;
    _ = page.insertTuple("tuple3") orelse unreachable;

    var iter = page.tupleIterator();
    var count: usize = 0;
    while (iter.next()) |_| {
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 3), count);
}

test "page full detection" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var page = Page.init(&data, 1);

    // Fill the page with large tuples
    var large_tuple: [512]u8 = undefined;
    @memset(&large_tuple, 'X');

    var count: usize = 0;
    while (page.insertTuple(&large_tuple)) |_| {
        count += 1;
    }

    // Should have inserted some but eventually failed
    try std.testing.expect(count > 0);
    try std.testing.expect(count < 20); // Can't fit too many 512-byte tuples in 8KB
}

test "page updateTupleData" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var pg = Page.init(&data, 1);

    // Insert a tuple with known content
    const original = [_]u8{ 0x00, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC, 0xDD };
    const slot_id = pg.insertTuple(&original) orelse unreachable;

    // Update the first 4 bytes (simulating setting xmax in a TupleHeader)
    const new_xmax = [_]u8{ 0x05, 0x00, 0x00, 0x00 };
    try std.testing.expect(pg.updateTupleData(slot_id, 0, &new_xmax));

    // Verify the update
    const retrieved = pg.getTuple(slot_id) orelse unreachable;
    try std.testing.expectEqual(@as(u8, 0x05), retrieved[0]);
    try std.testing.expectEqual(@as(u8, 0x00), retrieved[1]);
    // Original bytes after offset 4 should be unchanged
    try std.testing.expectEqual(@as(u8, 0xAA), retrieved[4]);
    try std.testing.expectEqual(@as(u8, 0xDD), retrieved[7]);

    // Out-of-bounds update should fail
    try std.testing.expect(!pg.updateTupleData(slot_id, 6, &new_xmax));

    // Invalid slot should fail
    try std.testing.expect(!pg.updateTupleData(99, 0, &new_xmax));
}

test "page insert zero-length tuple" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var page = Page.init(&data, 1);

    const slot = page.insertTuple(&.{});
    // Zero-length tuple gets a slot, but getTuple returns null (length == 0 means empty)
    try std.testing.expect(slot != null);
    try std.testing.expect(page.getTuple(slot.?) == null);
}

test "page delete invalid slot_id" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var page = Page.init(&data, 1);

    // Deleting from empty page (slot_count = 0) should return false
    try std.testing.expect(!page.deleteTuple(0));
    try std.testing.expect(!page.deleteTuple(100));

    // Insert one, then try deleting out-of-bounds
    _ = page.insertTuple("test") orelse unreachable;
    try std.testing.expect(!page.deleteTuple(1)); // only slot 0 exists
    try std.testing.expect(page.deleteTuple(0)); // this one should work
}

test "page iterator skips deleted slots" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var page = Page.init(&data, 1);

    _ = page.insertTuple("zero") orelse unreachable;
    _ = page.insertTuple("one") orelse unreachable;
    _ = page.insertTuple("two") orelse unreachable;
    _ = page.insertTuple("three") orelse unreachable;
    _ = page.insertTuple("four") orelse unreachable;

    // Delete slots 1 and 3
    try std.testing.expect(page.deleteTuple(1));
    try std.testing.expect(page.deleteTuple(3));

    // Iterator should return only 0, 2, 4
    var iter = page.tupleIterator();
    var count: usize = 0;
    var slot_ids: [5]SlotId = undefined;
    while (iter.next()) |entry| {
        slot_ids[count] = entry.slot_id;
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(SlotId, 0), slot_ids[0]);
    try std.testing.expectEqual(@as(SlotId, 2), slot_ids[1]);
    try std.testing.expectEqual(@as(SlotId, 4), slot_ids[2]);
}

test "page getTuple on non-existent slot returns null" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    const page = Page.init(&data, 1);

    // No tuples at all
    try std.testing.expect(page.getTuple(0) == null);
    try std.testing.expect(page.getTuple(65535) == null);
}

test "page updateTupleData exact boundary write" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var pg = Page.init(&data, 1);

    const original = [_]u8{ 1, 2, 3, 4 };
    const slot_id = pg.insertTuple(&original) orelse unreachable;

    // Write exactly at the end boundary (offset + data.len == slot.length)
    const new_bytes = [_]u8{ 0xFF, 0xFF };
    try std.testing.expect(pg.updateTupleData(slot_id, 2, &new_bytes));

    const retrieved = pg.getTuple(slot_id) orelse unreachable;
    try std.testing.expectEqual(@as(u8, 1), retrieved[0]);
    try std.testing.expectEqual(@as(u8, 2), retrieved[1]);
    try std.testing.expectEqual(@as(u8, 0xFF), retrieved[2]);
    try std.testing.expectEqual(@as(u8, 0xFF), retrieved[3]);

    // One byte past the end should fail
    try std.testing.expect(!pg.updateTupleData(slot_id, 3, &new_bytes));
}

test "page double delete same slot" {
    var data: [PAGE_SIZE]u8 align(8) = undefined;
    var page = Page.init(&data, 1);

    _ = page.insertTuple("test") orelse unreachable;
    try std.testing.expect(page.deleteTuple(0));
    // Second delete on already-empty slot should still return true (slot exists, just zeroed)
    try std.testing.expect(page.deleteTuple(0));
    // But tuple should still be null
    try std.testing.expect(page.getTuple(0) == null);
}
