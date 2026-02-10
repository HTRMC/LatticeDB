const std = @import("std");
const page_mod = @import("page.zig");
const disk_manager_mod = @import("disk_manager.zig");

const Page = page_mod.Page;
const PageId = page_mod.PageId;
const PAGE_SIZE = page_mod.PAGE_SIZE;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const DiskManager = disk_manager_mod.DiskManager;

pub const BufferPoolError = error{
    NoFreeFrames,
    PageNotFound,
    PagePinned,
    DiskError,
    OutOfMemory,
};

/// Frame ID type - index into the buffer pool
pub const FrameId = u32;
const INVALID_FRAME_ID: FrameId = std.math.maxInt(FrameId);

/// A frame holds a page in memory along with metadata
const Frame = struct {
    data: [PAGE_SIZE]u8 align(8),
    page_id: PageId,
    pin_count: u32,
    is_dirty: bool,

    fn init() Frame {
        return .{
            .data = undefined,
            .page_id = INVALID_PAGE_ID,
            .pin_count = 0,
            .is_dirty = false,
        };
    }

    fn reset(self: *Frame) void {
        self.page_id = INVALID_PAGE_ID;
        self.pin_count = 0;
        self.is_dirty = false;
    }
};

/// LRU Replacer - tracks unpinned frames for eviction
const LruReplacer = struct {
    /// Doubly-linked list node
    const Node = struct {
        frame_id: FrameId,
        prev: ?*Node,
        next: ?*Node,
    };

    allocator: std.mem.Allocator,
    /// Map from frame_id to node for O(1) access
    node_map: std.AutoHashMap(FrameId, *Node),
    /// Head of LRU list (least recently used)
    head: ?*Node,
    /// Tail of LRU list (most recently used)
    tail: ?*Node,

    fn init(allocator: std.mem.Allocator) LruReplacer {
        return .{
            .allocator = allocator,
            .node_map = std.AutoHashMap(FrameId, *Node).init(allocator),
            .head = null,
            .tail = null,
        };
    }

    fn deinit(self: *LruReplacer) void {
        // Free all nodes
        var it = self.node_map.valueIterator();
        while (it.next()) |node_ptr| {
            self.allocator.destroy(node_ptr.*);
        }
        self.node_map.deinit();
    }

    /// Add a frame to the replacer (called when pin_count becomes 0)
    fn add(self: *LruReplacer, frame_id: FrameId) !void {
        if (self.node_map.contains(frame_id)) {
            // Already in replacer, move to end (most recently used)
            self.touch(frame_id);
            return;
        }

        const node = try self.allocator.create(Node);
        node.* = .{
            .frame_id = frame_id,
            .prev = self.tail,
            .next = null,
        };

        if (self.tail) |t| {
            t.next = node;
        } else {
            self.head = node;
        }
        self.tail = node;

        try self.node_map.put(frame_id, node);
    }

    /// Remove a frame from the replacer (called when pin_count becomes > 0)
    fn remove(self: *LruReplacer, frame_id: FrameId) void {
        const node = self.node_map.get(frame_id) orelse return;
        self.removeNode(node);
        _ = self.node_map.remove(frame_id);
        self.allocator.destroy(node);
    }

    /// Move a frame to the end (most recently used)
    fn touch(self: *LruReplacer, frame_id: FrameId) void {
        const node = self.node_map.get(frame_id) orelse return;

        // Already at tail
        if (node == self.tail) return;

        // Remove from current position
        self.removeNode(node);

        // Add to tail
        node.prev = self.tail;
        node.next = null;
        if (self.tail) |t| {
            t.next = node;
        } else {
            self.head = node;
        }
        self.tail = node;
    }

    /// Get the least recently used frame (victim for eviction)
    fn victim(self: *LruReplacer) ?FrameId {
        const head = self.head orelse return null;
        const frame_id = head.frame_id;

        // Remove from list and map
        self.removeNode(head);
        _ = self.node_map.remove(frame_id);
        self.allocator.destroy(head);

        return frame_id;
    }

    /// Remove a node from the doubly-linked list
    fn removeNode(self: *LruReplacer, node: *Node) void {
        if (node.prev) |p| {
            p.next = node.next;
        } else {
            self.head = node.next;
        }

        if (node.next) |n| {
            n.prev = node.prev;
        } else {
            self.tail = node.prev;
        }
    }

    /// Get number of frames available for eviction
    fn size(self: *const LruReplacer) usize {
        return self.node_map.count();
    }
};

/// Buffer Pool Manager - manages in-memory page cache
pub const BufferPool = struct {
    allocator: std.mem.Allocator,
    disk_manager: *DiskManager,

    /// Array of frames
    frames: []Frame,
    /// Number of frames in the pool
    pool_size: usize,

    /// Page table: maps page_id -> frame_id
    page_table: std.AutoHashMap(PageId, FrameId),
    /// LRU replacer for eviction
    replacer: LruReplacer,
    /// Free list of frames
    free_list: std.ArrayList(FrameId),

    /// Background checkpoint state
    checkpoint_cursor: u32 = 0,

    const Self = @This();

    /// Create a new buffer pool with the specified number of frames
    pub fn init(allocator: std.mem.Allocator, disk_manager: *DiskManager, pool_size: usize) !Self {
        const frames = try allocator.alloc(Frame, pool_size);
        for (frames) |*frame| {
            frame.* = Frame.init();
        }

        var free_list: std.ArrayList(FrameId) = .empty;
        // Add all frames to free list (in reverse so we pop from 0)
        var i: FrameId = @intCast(pool_size);
        while (i > 0) {
            i -= 1;
            try free_list.append(allocator, i);
        }

        return .{
            .allocator = allocator,
            .disk_manager = disk_manager,
            .frames = frames,
            .pool_size = pool_size,
            .page_table = std.AutoHashMap(PageId, FrameId).init(allocator),
            .replacer = LruReplacer.init(allocator),
            .free_list = free_list,
        };
    }

    pub fn deinit(self: *Self) void {
        // Flush all dirty pages before cleanup
        self.flushAllPages() catch {};

        self.replacer.deinit();
        self.page_table.deinit();
        self.free_list.deinit(self.allocator);
        self.allocator.free(self.frames);
    }

    /// Fetch a page from the buffer pool
    /// If the page is not in the pool, read it from disk
    /// Returns a Page wrapper and increments pin count
    pub fn fetchPage(self: *Self, page_id: PageId) BufferPoolError!Page {
        // Check if page is already in buffer pool
        if (self.page_table.get(page_id)) |frame_id| {
            const frame = &self.frames[frame_id];
            frame.pin_count += 1;
            // Remove from replacer since it's now pinned
            self.replacer.remove(frame_id);
            return Page.fromBytes(&frame.data);
        }

        // Need to load page from disk - get a free frame
        const frame_id = try self.getFrame();
        const frame = &self.frames[frame_id];

        // Read page from disk
        self.disk_manager.readPage(page_id, &frame.data) catch {
            // Return frame to free list on error
            self.free_list.append(self.allocator, frame_id) catch {};
            return BufferPoolError.DiskError;
        };

        // Update frame metadata
        frame.page_id = page_id;
        frame.pin_count = 1;
        frame.is_dirty = false;

        // Update page table
        self.page_table.put(page_id, frame_id) catch {
            self.free_list.append(self.allocator, frame_id) catch {};
            return BufferPoolError.OutOfMemory;
        };

        return Page.fromBytes(&frame.data);
    }

    /// Create a new page in the buffer pool
    /// Allocates a new page on disk and loads it into the pool
    pub fn newPage(self: *Self) BufferPoolError!struct { page_id: PageId, page: Page } {
        // Allocate a new page on disk
        const page_id = self.disk_manager.allocatePage() catch {
            return BufferPoolError.DiskError;
        };

        // Get a free frame
        const frame_id = self.getFrame() catch |err| {
            // TODO: deallocate the page on disk
            return err;
        };

        const frame = &self.frames[frame_id];

        // Initialize the frame
        const pg = Page.init(&frame.data, page_id);
        frame.page_id = page_id;
        frame.pin_count = 1;
        frame.is_dirty = true; // New page needs to be written

        // Update page table
        self.page_table.put(page_id, frame_id) catch {
            self.free_list.append(self.allocator, frame_id) catch {};
            return BufferPoolError.OutOfMemory;
        };

        return .{ .page_id = page_id, .page = pg };
    }

    /// Unpin a page - decrements pin count
    /// If pin count reaches 0, the page becomes eligible for eviction
    pub fn unpinPage(self: *Self, page_id: PageId, is_dirty: bool) BufferPoolError!void {
        const frame_id = self.page_table.get(page_id) orelse {
            return BufferPoolError.PageNotFound;
        };

        const frame = &self.frames[frame_id];

        if (frame.pin_count == 0) {
            return; // Already unpinned
        }

        frame.pin_count -= 1;
        if (is_dirty) {
            frame.is_dirty = true;
        }

        // If pin count reaches 0, add to replacer
        if (frame.pin_count == 0) {
            self.replacer.add(frame_id) catch {
                return BufferPoolError.OutOfMemory;
            };
        }
    }

    /// Flush a specific page to disk
    pub fn flushPage(self: *Self, page_id: PageId) BufferPoolError!void {
        const frame_id = self.page_table.get(page_id) orelse {
            return BufferPoolError.PageNotFound;
        };

        const frame = &self.frames[frame_id];

        if (frame.is_dirty) {
            self.disk_manager.writePage(page_id, &frame.data) catch {
                return BufferPoolError.DiskError;
            };
            frame.is_dirty = false;
        }
    }

    /// Flush all pages to disk
    pub fn flushAllPages(self: *Self) BufferPoolError!void {
        var it = self.page_table.iterator();
        while (it.next()) |entry| {
            const frame = &self.frames[entry.value_ptr.*];
            if (frame.is_dirty) {
                self.disk_manager.writePage(entry.key_ptr.*, &frame.data) catch {
                    return BufferPoolError.DiskError;
                };
                frame.is_dirty = false;
            }
        }
        self.disk_manager.flush() catch {
            return BufferPoolError.DiskError;
        };
    }

    /// Delete a page from the buffer pool and disk
    pub fn deletePage(self: *Self, page_id: PageId) BufferPoolError!void {
        const frame_id = self.page_table.get(page_id) orelse {
            return; // Page not in pool, nothing to do
        };

        const frame = &self.frames[frame_id];

        if (frame.pin_count > 0) {
            return BufferPoolError.PagePinned;
        }

        // Remove from replacer
        self.replacer.remove(frame_id);

        // Remove from page table
        _ = self.page_table.remove(page_id);

        // Reset frame and add to free list
        frame.reset();
        self.free_list.append(self.allocator, frame_id) catch {
            return BufferPoolError.OutOfMemory;
        };
    }

    /// Get a free frame, evicting if necessary
    fn getFrame(self: *Self) BufferPoolError!FrameId {
        // Try free list first
        if (self.free_list.pop()) |frame_id| {
            return frame_id;
        }

        // Need to evict - get victim from replacer
        const victim_frame_id = self.replacer.victim() orelse {
            return BufferPoolError.NoFreeFrames;
        };

        const frame = &self.frames[victim_frame_id];

        // Write dirty page to disk before evicting
        if (frame.is_dirty) {
            self.disk_manager.writePage(frame.page_id, &frame.data) catch {
                // Put back in replacer on error
                self.replacer.add(victim_frame_id) catch {};
                return BufferPoolError.DiskError;
            };
        }

        // Remove old page from page table
        _ = self.page_table.remove(frame.page_id);

        // Reset frame
        frame.reset();

        return victim_frame_id;
    }

    /// Allocate a contiguous extent of pages on disk (no frames allocated)
    pub fn allocateExtent(self: *Self, count: u32) BufferPoolError!PageId {
        return self.disk_manager.allocateExtent(count) catch {
            return BufferPoolError.DiskError;
        };
    }

    /// Get the pin count for a page (for debugging/testing)
    pub fn getPinCount(self: *const Self, page_id: PageId) ?u32 {
        const frame_id = self.page_table.get(page_id) orelse return null;
        return self.frames[frame_id].pin_count;
    }

    /// Check if a page is dirty (for debugging/testing)
    pub fn isDirty(self: *const Self, page_id: PageId) ?bool {
        const frame_id = self.page_table.get(page_id) orelse return null;
        return self.frames[frame_id].is_dirty;
    }

    /// Get the number of pages currently in the pool
    pub fn size(self: *const Self) usize {
        return self.page_table.count();
    }

    /// Proactively flush up to `batch` dirty unpinned pages to disk.
    /// Uses a round-robin cursor so work is spread across calls.
    pub fn checkpoint(self: *Self, batch: u32) void {
        if (self.pool_size == 0) return;

        var flushed: u32 = 0;
        var scanned: u32 = 0;
        const pool: u32 = @intCast(self.pool_size);

        while (flushed < batch and scanned < pool) : (scanned += 1) {
            const idx = self.checkpoint_cursor % pool;
            self.checkpoint_cursor = idx + 1;

            const frame = &self.frames[idx];
            if (frame.is_dirty and frame.pin_count == 0 and frame.page_id != INVALID_PAGE_ID) {
                self.disk_manager.writePage(frame.page_id, &frame.data) catch continue;
                frame.is_dirty = false;
                flushed += 1;
            }
        }
    }
};

// Tests
test "buffer pool basic operations" {
    const test_file = "test_buffer_pool.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    // Create a new page
    const result = try bp.newPage();
    const page_id = result.page_id;
    var pg = result.page;

    try std.testing.expectEqual(@as(PageId, 0), page_id);
    try std.testing.expectEqual(@as(?u32, 1), bp.getPinCount(page_id));

    // Insert some data
    _ = pg.insertTuple("Hello, Buffer Pool!");

    // Unpin the page (marking it dirty)
    try bp.unpinPage(page_id, true);
    try std.testing.expectEqual(@as(?u32, 0), bp.getPinCount(page_id));
    try std.testing.expectEqual(@as(?bool, true), bp.isDirty(page_id));

    // Fetch the page again
    var pg2 = try bp.fetchPage(page_id);
    try std.testing.expectEqual(@as(?u32, 1), bp.getPinCount(page_id));

    // Verify data
    const data = pg2.getTuple(0) orelse unreachable;
    try std.testing.expectEqualStrings("Hello, Buffer Pool!", data);

    try bp.unpinPage(page_id, false);
}

test "buffer pool eviction" {
    const test_file = "test_buffer_pool_evict.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    // Small pool of 3 frames
    var bp = try BufferPool.init(std.testing.allocator, &dm, 3);
    defer bp.deinit();

    // Create 3 pages, filling the pool
    var page_ids: [3]PageId = undefined;
    for (&page_ids, 0..) |*pid, i| {
        const result = try bp.newPage();
        pid.* = result.page_id;
        var pg = result.page;

        var buf: [32]u8 = undefined;
        const data = std.fmt.bufPrint(&buf, "Page {}", .{i}) catch unreachable;
        _ = pg.insertTuple(data);

        try bp.unpinPage(pid.*, true);
    }

    try std.testing.expectEqual(@as(usize, 3), bp.size());

    // Create a 4th page - should evict the first one (LRU)
    const result4 = try bp.newPage();
    try std.testing.expectEqual(@as(usize, 3), bp.size());

    // Page 0 should have been evicted
    try std.testing.expectEqual(@as(?u32, null), bp.getPinCount(page_ids[0]));

    try bp.unpinPage(result4.page_id, true);

    // Fetch page 0 - should reload from disk
    var pg0 = try bp.fetchPage(page_ids[0]);
    const data0 = pg0.getTuple(0) orelse unreachable;
    try std.testing.expectEqualStrings("Page 0", data0);

    try bp.unpinPage(page_ids[0], false);
}

test "buffer pool persistence" {
    const test_file = "test_buffer_pool_persist.db";

    // First session - create and write
    {
        var dm = DiskManager.init(std.testing.allocator, test_file);
        try dm.open();
        defer dm.close();

        var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
        defer bp.deinit();

        const result = try bp.newPage();
        var pg = result.page;
        _ = pg.insertTuple("Persistent data via buffer pool");
        try bp.unpinPage(result.page_id, true);

        try bp.flushAllPages();
    }

    // Second session - read back
    {
        var dm = DiskManager.init(std.testing.allocator, test_file);
        defer dm.deleteFile();
        try dm.open();
        defer dm.close();

        var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
        defer bp.deinit();

        var pg = try bp.fetchPage(0);
        const data = pg.getTuple(0) orelse unreachable;
        try std.testing.expectEqualStrings("Persistent data via buffer pool", data);

        try bp.unpinPage(0, false);
    }
}

test "buffer pool pinned page not evicted" {
    const test_file = "test_buffer_pool_pin.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    // Pool of 2 frames
    var bp = try BufferPool.init(std.testing.allocator, &dm, 2);
    defer bp.deinit();

    // Create 2 pages
    const result1 = try bp.newPage();
    const result2 = try bp.newPage();

    // Unpin only page 2
    try bp.unpinPage(result2.page_id, false);

    // Create a 3rd page - should evict page 2 (not page 1 which is still pinned)
    const result3 = try bp.newPage();

    // Page 1 should still be in pool (pinned)
    try std.testing.expectEqual(@as(?u32, 1), bp.getPinCount(result1.page_id));
    // Page 2 should be evicted
    try std.testing.expectEqual(@as(?u32, null), bp.getPinCount(result2.page_id));
    // Page 3 should be in pool
    try std.testing.expectEqual(@as(?u32, 1), bp.getPinCount(result3.page_id));

    try bp.unpinPage(result1.page_id, false);
    try bp.unpinPage(result3.page_id, false);
}

test "buffer pool all pinned returns error" {
    const test_file = "test_buffer_pool_full.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    // Pool of 2 frames
    var bp = try BufferPool.init(std.testing.allocator, &dm, 2);
    defer bp.deinit();

    // Create 2 pages and keep them pinned
    const result1 = try bp.newPage();
    const result2 = try bp.newPage();

    // Try to create a 3rd page - should fail
    const result3 = bp.newPage();
    try std.testing.expectError(BufferPoolError.NoFreeFrames, result3);

    // Clean up
    try bp.unpinPage(result1.page_id, false);
    try bp.unpinPage(result2.page_id, false);
}

test "lru replacer ordering" {
    const test_file = "test_buffer_pool_lru.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    // Pool of 3 frames
    var bp = try BufferPool.init(std.testing.allocator, &dm, 3);
    defer bp.deinit();

    // Create pages 0, 1, 2
    const r0 = try bp.newPage();
    const r1 = try bp.newPage();
    const r2 = try bp.newPage();

    // Unpin in order: 0, 1, 2
    try bp.unpinPage(r0.page_id, false);
    try bp.unpinPage(r1.page_id, false);
    try bp.unpinPage(r2.page_id, false);

    // Access page 0 again (moves to end of LRU)
    _ = try bp.fetchPage(r0.page_id);
    try bp.unpinPage(r0.page_id, false);

    // Now order should be: 1, 2, 0 (LRU to MRU)
    // Creating a new page should evict page 1
    _ = try bp.newPage();

    try std.testing.expectEqual(@as(?u32, null), bp.getPinCount(r1.page_id)); // evicted
    try std.testing.expect(bp.getPinCount(r0.page_id) != null); // still in pool
    try std.testing.expect(bp.getPinCount(r2.page_id) != null); // still in pool
}

test "buffer pool double fetch increments pin count" {
    const test_file = "test_buffer_pool_double_pin.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    const result = try bp.newPage();
    const page_id = result.page_id;
    try std.testing.expectEqual(@as(?u32, 1), bp.getPinCount(page_id));

    // Fetch same page again — pin count should go to 2
    _ = try bp.fetchPage(page_id);
    try std.testing.expectEqual(@as(?u32, 2), bp.getPinCount(page_id));

    // Must unpin twice
    try bp.unpinPage(page_id, false);
    try std.testing.expectEqual(@as(?u32, 1), bp.getPinCount(page_id));

    try bp.unpinPage(page_id, false);
    try std.testing.expectEqual(@as(?u32, 0), bp.getPinCount(page_id));
}

test "buffer pool new page ids are sequential" {
    const test_file = "test_buffer_pool_seq_ids.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    const r0 = try bp.newPage();
    const r1 = try bp.newPage();
    const r2 = try bp.newPage();

    try std.testing.expectEqual(@as(PageId, 0), r0.page_id);
    try std.testing.expectEqual(@as(PageId, 1), r1.page_id);
    try std.testing.expectEqual(@as(PageId, 2), r2.page_id);

    try bp.unpinPage(r0.page_id, false);
    try bp.unpinPage(r1.page_id, false);
    try bp.unpinPage(r2.page_id, false);
}

test "buffer pool dirty flag survives re-fetch" {
    const test_file = "test_buffer_pool_dirty_refetch.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    const result = try bp.newPage();
    const page_id = result.page_id;

    // Unpin as dirty
    try bp.unpinPage(page_id, true);
    try std.testing.expectEqual(@as(?bool, true), bp.isDirty(page_id));

    // Re-fetch — dirty flag should still be true
    _ = try bp.fetchPage(page_id);
    try std.testing.expectEqual(@as(?bool, true), bp.isDirty(page_id));

    try bp.unpinPage(page_id, false);
}

test "buffer pool single frame pool" {
    const test_file = "test_buffer_pool_single.db";

    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();

    // Minimum viable pool: 1 frame
    var bp = try BufferPool.init(std.testing.allocator, &dm, 1);
    defer bp.deinit();

    const r0 = try bp.newPage();
    var pg0 = r0.page;
    _ = pg0.insertTuple("data-on-page-0");
    try bp.unpinPage(r0.page_id, true);

    // Creating a second page should evict the first
    const r1 = try bp.newPage();
    try std.testing.expectEqual(@as(?u32, null), bp.getPinCount(r0.page_id));
    try bp.unpinPage(r1.page_id, false);

    // Re-fetch page 0 (evicts page 1, loads from disk)
    var pg0_refetch = try bp.fetchPage(r0.page_id);
    const refetched_data = pg0_refetch.getTuple(0) orelse unreachable;
    try std.testing.expectEqualStrings("data-on-page-0", refetched_data);
    try bp.unpinPage(r0.page_id, false);
}

test "buffer pool flush non-dirty page is no-op" {
    const test_file = "test_bp_flush_clean.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    const r = try bp.newPage();
    var pg = r.page;
    _ = pg.insertTuple("data");
    // Unpin as NOT dirty
    try bp.unpinPage(r.page_id, false);

    // Flush should be a no-op (page not dirty)
    try bp.flushPage(r.page_id);
    // No error means success
}

test "buffer pool deletePage frees frame" {
    const test_file = "test_bp_delete_free.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 2);
    defer bp.deinit();

    const r0 = try bp.newPage();
    try bp.unpinPage(r0.page_id, false);

    const r1 = try bp.newPage();
    try bp.unpinPage(r1.page_id, false);

    // Delete page 0 — frees a frame
    try bp.deletePage(r0.page_id);

    // Pin count should be null (not in pool)
    try std.testing.expectEqual(@as(?u32, null), bp.getPinCount(r0.page_id));

    // Should be able to create a new page (frame is free)
    const r2 = try bp.newPage();
    try bp.unpinPage(r2.page_id, false);
}

test "buffer pool flushAllPages on clean pool" {
    const test_file = "test_bp_flush_all_clean.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    // No pages at all — flushAllPages should be a no-op
    try bp.flushAllPages();

    // Create pages but unpin as clean
    const r = try bp.newPage();
    try bp.unpinPage(r.page_id, false);
    try bp.flushAllPages();
}

test "buffer pool flushPage unknown page" {
    const test_file = "test_bp_flush_unknown.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    // Flushing a page not in the pool should return PageNotFound
    try std.testing.expectError(BufferPoolError.PageNotFound, bp.flushPage(999));
}

test "buffer pool checkpoint flushes dirty unpinned pages" {
    const test_file = "test_bp_checkpoint.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    // Create 3 pages, unpin as dirty
    var pids: [3]PageId = undefined;
    for (&pids) |*pid| {
        const r = try bp.newPage();
        pid.* = r.page_id;
        try bp.unpinPage(r.page_id, true);
    }

    // All should be dirty
    for (pids) |pid| {
        try std.testing.expectEqual(@as(?bool, true), bp.isDirty(pid));
    }

    // Checkpoint with batch=2 — should flush at most 2
    bp.checkpoint(2);
    var clean_count: u32 = 0;
    for (pids) |pid| {
        if (bp.isDirty(pid) == false) clean_count += 1;
    }
    try std.testing.expect(clean_count >= 2);

    // Another checkpoint pass should flush the remaining
    bp.checkpoint(2);
    for (pids) |pid| {
        try std.testing.expectEqual(@as(?bool, false), bp.isDirty(pid));
    }
}

test "buffer pool checkpoint skips pinned pages" {
    const test_file = "test_bp_checkpoint_pin.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 10);
    defer bp.deinit();

    // Create page and keep it pinned (dirty)
    const r = try bp.newPage();
    // newPage sets is_dirty = true and pin_count = 1

    // Checkpoint should NOT flush the pinned page
    bp.checkpoint(10);
    try std.testing.expectEqual(@as(?bool, true), bp.isDirty(r.page_id));

    // Unpin and checkpoint again — now it should flush
    try bp.unpinPage(r.page_id, true);
    bp.checkpoint(10);
    try std.testing.expectEqual(@as(?bool, false), bp.isDirty(r.page_id));
}
