const std = @import("std");
const page_mod = @import("../storage/page.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const btree_page = @import("btree_page.zig");

const PageId = page_mod.PageId;
const TupleId = page_mod.TupleId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const PAGE_SIZE = page_mod.PAGE_SIZE;
const BufferPool = buffer_pool_mod.BufferPool;
const Key = btree_page.Key;
const LeafNode = btree_page.LeafNode;
const InternalNode = btree_page.InternalNode;
const NodeType = btree_page.NodeType;
const LeafEntry = btree_page.LeafEntry;

pub const BTreeError = error{
    BufferPoolError,
    DuplicateKey,
    KeyNotFound,
    TreeCorrupted,
};

/// B+Tree index
/// Provides O(log n) search, insert, and delete on integer keys.
/// All data resides in leaf nodes; internal nodes guide the search.
pub const BTree = struct {
    buffer_pool: *BufferPool,
    root_page_id: PageId,

    const Self = @This();

    /// Create a new B+Tree index. Allocates a root leaf page.
    pub fn create(buffer_pool: *BufferPool) BTreeError!Self {
        const result = buffer_pool.newPage() catch {
            return BTreeError.BufferPoolError;
        };

        // Initialize as an empty leaf node (root starts as a leaf)
        _ = LeafNode.init(result.page.data, result.page_id);

        buffer_pool.unpinPage(result.page_id, true) catch {};

        return .{
            .buffer_pool = buffer_pool,
            .root_page_id = result.page_id,
        };
    }

    /// Open an existing B+Tree by its root page ID
    pub fn open(buffer_pool: *BufferPool, root_page_id: PageId) Self {
        return .{
            .buffer_pool = buffer_pool,
            .root_page_id = root_page_id,
        };
    }

    /// Search for a key. Returns the TupleId if found.
    pub fn search(self: *Self, key: Key) BTreeError!?TupleId {
        const leaf_page_id = try self.findLeaf(key);

        const page = self.buffer_pool.fetchPage(leaf_page_id) catch {
            return BTreeError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(leaf_page_id, false) catch {};

        const leaf = LeafNode.fromPage(page.data);
        const result = leaf.findKey(key);

        if (result.found) {
            const entry = leaf.getEntry(result.index).?;
            return entry.tid();
        }
        return null;
    }

    /// Insert a key-TupleId pair into the index.
    pub fn insert(self: *Self, key: Key, tid: TupleId) BTreeError!void {
        const leaf_page_id = try self.findLeaf(key);

        var page = self.buffer_pool.fetchPage(leaf_page_id) catch {
            return BTreeError.BufferPoolError;
        };

        var leaf = LeafNode.fromPage(page.data);

        // Check for duplicate
        const find_result = leaf.findKey(key);
        if (find_result.found) {
            self.buffer_pool.unpinPage(leaf_page_id, false) catch {};
            return BTreeError.DuplicateKey;
        }

        // Try to insert into the leaf
        if (leaf.insert(key, tid)) {
            self.buffer_pool.unpinPage(leaf_page_id, true) catch {};
            return;
        }

        // Leaf is full - need to split
        try self.splitLeafAndInsert(leaf_page_id, leaf, key, tid);
    }

    /// Delete a key from the index. Returns true if found and deleted.
    pub fn delete(self: *Self, key: Key) BTreeError!bool {
        const leaf_page_id = try self.findLeaf(key);

        var page = self.buffer_pool.fetchPage(leaf_page_id) catch {
            return BTreeError.BufferPoolError;
        };

        const leaf = LeafNode.fromPage(page.data);
        const result = leaf.delete(key);

        self.buffer_pool.unpinPage(leaf_page_id, result) catch {};
        return result;
    }

    /// Range scan - returns an iterator over entries with keys in [start_key, end_key].
    pub fn rangeScan(self: *Self, start_key: Key, end_key: Key) BTreeError!RangeScanIterator {
        const leaf_page_id = try self.findLeaf(start_key);
        return .{
            .btree = self,
            .current_page_id = leaf_page_id,
            .current_index = 0,
            .start_key = start_key,
            .end_key = end_key,
            .started = false,
        };
    }

    pub const RangeScanIterator = struct {
        btree: *Self,
        current_page_id: PageId,
        current_index: u16,
        start_key: Key,
        end_key: Key,
        started: bool,

        /// Returns the next key-TupleId pair in range, or null when done.
        pub fn next(self: *RangeScanIterator) BTreeError!?struct { key: Key, tid: TupleId } {
            while (self.current_page_id != INVALID_PAGE_ID) {
                const page = self.btree.buffer_pool.fetchPage(self.current_page_id) catch {
                    return BTreeError.BufferPoolError;
                };

                const leaf = LeafNode.fromPage(page.data);
                const count = leaf.keyCount();

                // On first access to a new page, find starting position
                if (!self.started) {
                    const result = leaf.findKey(self.start_key);
                    self.current_index = result.index;
                    self.started = true;
                }

                while (self.current_index < count) {
                    const entry = leaf.getEntry(self.current_index).?;
                    self.current_index += 1;

                    if (entry.key > self.end_key) {
                        self.btree.buffer_pool.unpinPage(self.current_page_id, false) catch {};
                        self.current_page_id = INVALID_PAGE_ID;
                        return null;
                    }

                    if (entry.key >= self.start_key) {
                        self.btree.buffer_pool.unpinPage(self.current_page_id, false) catch {};
                        return .{ .key = entry.key, .tid = entry.tid() };
                    }
                }

                // Move to next leaf
                const next_leaf = leaf.nextLeaf();
                self.btree.buffer_pool.unpinPage(self.current_page_id, false) catch {};
                self.current_page_id = next_leaf;
                self.current_index = 0;
                self.started = true; // already positioned
            }

            return null;
        }
    };

    // ============================================================
    // Internal helpers
    // ============================================================

    /// Traverse from root to the leaf node that would contain the given key.
    fn findLeaf(self: *Self, key: Key) BTreeError!PageId {
        var page_id = self.root_page_id;

        while (true) {
            const page = self.buffer_pool.fetchPage(page_id) catch {
                return BTreeError.BufferPoolError;
            };

            const node_type = btree_page.getNodeType(page.data);

            if (node_type == .leaf) {
                self.buffer_pool.unpinPage(page_id, false) catch {};
                return page_id;
            }

            // Internal node - find the child to descend into
            const internal = InternalNode.fromPage(page.data);
            const child_id = internal.findChild(key);
            self.buffer_pool.unpinPage(page_id, false) catch {};
            page_id = child_id;
        }
    }

    /// Split a full leaf and insert the new key-tid pair.
    fn splitLeafAndInsert(
        self: *Self,
        leaf_page_id: PageId,
        leaf: LeafNode,
        key: Key,
        tid: TupleId,
    ) BTreeError!void {
        // Allocate a new page for the right sibling
        const new_result = self.buffer_pool.newPage() catch {
            self.buffer_pool.unpinPage(leaf_page_id, false) catch {};
            return BTreeError.BufferPoolError;
        };
        const new_page_id = new_result.page_id;
        const new_leaf = LeafNode.init(new_result.page.data, new_page_id);

        // Split the leaf: left keeps lower half, right gets upper half
        const split_key = leaf.splitInto(new_leaf);

        // Update prev pointer of old next leaf
        const old_next = new_leaf.nextLeaf();
        if (old_next != INVALID_PAGE_ID) {
            const next_page = self.buffer_pool.fetchPage(old_next) catch {
                self.buffer_pool.unpinPage(new_page_id, true) catch {};
                self.buffer_pool.unpinPage(leaf_page_id, true) catch {};
                return BTreeError.BufferPoolError;
            };
            const next_leaf = LeafNode.fromPage(next_page.data);
            next_leaf.setPrevLeaf(new_page_id);
            self.buffer_pool.unpinPage(old_next, true) catch {};
        }

        // Insert the new key into the correct sibling
        if (key < split_key) {
            _ = leaf.insert(key, tid);
        } else {
            _ = new_leaf.insert(key, tid);
        }

        // Copy parent from old leaf
        new_leaf.headerMut().parent = leaf.header().parent;

        self.buffer_pool.unpinPage(new_page_id, true) catch {};
        self.buffer_pool.unpinPage(leaf_page_id, true) catch {};

        // Insert the split key into the parent
        try self.insertIntoParent(leaf_page_id, split_key, new_page_id);
    }

    /// Insert a key and right child into the parent of left_child.
    /// If no parent exists (left_child is root), create a new root.
    fn insertIntoParent(
        self: *Self,
        left_child_id: PageId,
        key: Key,
        right_child_id: PageId,
    ) BTreeError!void {
        // Re-fetch the left child to read its parent pointer
        const left_page = self.buffer_pool.fetchPage(left_child_id) catch {
            return BTreeError.BufferPoolError;
        };
        const parent_id = blk: {
            const hdr: *const btree_page.NodeHeader = @ptrCast(@alignCast(left_page.data));
            break :blk hdr.parent;
        };
        self.buffer_pool.unpinPage(left_child_id, false) catch {};

        if (parent_id == INVALID_PAGE_ID) {
            // Left child is the root - create a new root
            try self.createNewRoot(left_child_id, key, right_child_id);
            return;
        }

        // Fetch the parent internal node
        var parent_page = self.buffer_pool.fetchPage(parent_id) catch {
            return BTreeError.BufferPoolError;
        };

        var parent = InternalNode.fromPage(parent_page.data);

        if (parent.insertKeyChild(key, right_child_id)) {
            // Successfully inserted into parent
            self.buffer_pool.unpinPage(parent_id, true) catch {};
            // Update the right child's parent pointer
            try self.setParent(right_child_id, parent_id);
            return;
        }

        // Parent is full - split the internal node
        try self.splitInternalAndInsert(parent_id, parent, key, right_child_id);
    }

    /// Create a new root node when the old root splits.
    fn createNewRoot(
        self: *Self,
        left_child_id: PageId,
        key: Key,
        right_child_id: PageId,
    ) BTreeError!void {
        const result = self.buffer_pool.newPage() catch {
            return BTreeError.BufferPoolError;
        };
        const new_root_id = result.page_id;

        const root = InternalNode.init(result.page.data, new_root_id);
        root.setFirstChild(left_child_id);
        _ = root.insertKeyChild(key, right_child_id);

        self.buffer_pool.unpinPage(new_root_id, true) catch {};

        // Update children's parent pointers
        try self.setParent(left_child_id, new_root_id);
        try self.setParent(right_child_id, new_root_id);

        self.root_page_id = new_root_id;
    }

    /// Split a full internal node and insert a key + right child.
    fn splitInternalAndInsert(
        self: *Self,
        node_page_id: PageId,
        node: InternalNode,
        key: Key,
        right_child_id: PageId,
    ) BTreeError!void {
        // Allocate a new page for the right sibling
        const new_result = self.buffer_pool.newPage() catch {
            self.buffer_pool.unpinPage(node_page_id, false) catch {};
            return BTreeError.BufferPoolError;
        };
        const new_page_id = new_result.page_id;
        const new_node = InternalNode.init(new_result.page.data, new_page_id);

        // Split: left keeps lower half, median goes up, right gets upper half
        const median_key = node.splitInto(new_node);

        // Determine where to insert the new key
        if (key < median_key) {
            _ = node.insertKeyChild(key, right_child_id);
            try self.setParent(right_child_id, node_page_id);
        } else {
            _ = new_node.insertKeyChild(key, right_child_id);
            try self.setParent(right_child_id, new_page_id);
        }

        // Update parent pointers for all children of new_node
        try self.updateChildrenParent(new_node, new_page_id);

        // Copy parent from old node
        new_node.headerMut().parent = node.header().parent;

        self.buffer_pool.unpinPage(new_page_id, true) catch {};
        self.buffer_pool.unpinPage(node_page_id, true) catch {};

        // Insert median into grandparent
        try self.insertIntoParent(node_page_id, median_key, new_page_id);
    }

    /// Update the parent pointer of all children in an internal node.
    fn updateChildrenParent(self: *Self, node: InternalNode, parent_id: PageId) BTreeError!void {
        // Update first child
        try self.setParent(node.firstChild(), parent_id);

        // Update all children referenced in entries
        const count = node.keyCount();
        var i: u16 = 0;
        while (i < count) : (i += 1) {
            const entry = node.entryPtr(i);
            try self.setParent(entry.child, parent_id);
        }
    }

    /// Set the parent pointer of a node.
    fn setParent(self: *Self, child_id: PageId, parent_id: PageId) BTreeError!void {
        const page = self.buffer_pool.fetchPage(child_id) catch {
            return BTreeError.BufferPoolError;
        };
        const hdr: *btree_page.NodeHeader = @ptrCast(@alignCast(page.data));
        hdr.parent = parent_id;
        self.buffer_pool.unpinPage(child_id, true) catch {};
    }
};

// ============================================================
// Tests
// ============================================================

const disk_manager_mod = @import("../storage/disk_manager.zig");
const DiskManager = disk_manager_mod.DiskManager;

test "btree create and search empty" {
    const test_file = "test_btree_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    // Search in empty tree should return null
    const result = try btree.search(42);
    try std.testing.expect(result == null);
}

test "btree insert and search" {
    const test_file = "test_btree_basic.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    // Insert some key-value pairs
    try btree.insert(10, .{ .page_id = 1, .slot_id = 0 });
    try btree.insert(20, .{ .page_id = 1, .slot_id = 1 });
    try btree.insert(5, .{ .page_id = 1, .slot_id = 2 });
    try btree.insert(15, .{ .page_id = 2, .slot_id = 0 });

    // Search for each key
    const r1 = (try btree.search(10)).?;
    try std.testing.expectEqual(@as(PageId, 1), r1.page_id);
    try std.testing.expectEqual(@as(u16, 0), r1.slot_id);

    const r2 = (try btree.search(20)).?;
    try std.testing.expectEqual(@as(PageId, 1), r2.page_id);
    try std.testing.expectEqual(@as(u16, 1), r2.slot_id);

    const r3 = (try btree.search(5)).?;
    try std.testing.expectEqual(@as(PageId, 1), r3.page_id);
    try std.testing.expectEqual(@as(u16, 2), r3.slot_id);

    // Search for non-existing key
    const r4 = try btree.search(99);
    try std.testing.expect(r4 == null);
}

test "btree duplicate key" {
    const test_file = "test_btree_dup.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    try btree.insert(10, .{ .page_id = 1, .slot_id = 0 });
    const result = btree.insert(10, .{ .page_id = 2, .slot_id = 0 });
    try std.testing.expectError(BTreeError.DuplicateKey, result);
}

test "btree delete" {
    const test_file = "test_btree_delete.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    try btree.insert(10, .{ .page_id = 1, .slot_id = 0 });
    try btree.insert(20, .{ .page_id = 1, .slot_id = 1 });
    try btree.insert(30, .{ .page_id = 1, .slot_id = 2 });

    // Delete existing key
    const deleted = try btree.delete(20);
    try std.testing.expect(deleted);

    // Should no longer be found
    const result = try btree.search(20);
    try std.testing.expect(result == null);

    // Others should still be there
    try std.testing.expect((try btree.search(10)) != null);
    try std.testing.expect((try btree.search(30)) != null);

    // Delete non-existing key
    const not_deleted = try btree.delete(99);
    try std.testing.expect(!not_deleted);
}

test "btree leaf split" {
    const test_file = "test_btree_split.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 100);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    // Insert enough keys to force at least one leaf split
    // LEAF_MAX_ENTRIES is ~680, so we insert more than that
    const count: i32 = 1000;
    var i: i32 = 0;
    while (i < count) : (i += 1) {
        try btree.insert(i, .{ .page_id = @intCast(i), .slot_id = 0 });
    }

    // Verify all keys can be found
    i = 0;
    while (i < count) : (i += 1) {
        const result = try btree.search(i);
        try std.testing.expect(result != null);
        try std.testing.expectEqual(@as(PageId, @intCast(i)), result.?.page_id);
    }

    // Root should now be an internal node (since we had splits)
    const root_page = bp.fetchPage(btree.root_page_id) catch unreachable;
    defer bp.unpinPage(btree.root_page_id, false) catch {};
    const root_type = btree_page.getNodeType(root_page.data);
    try std.testing.expectEqual(NodeType.internal, root_type);
}

test "btree range scan" {
    const test_file = "test_btree_range.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    // Insert keys 0, 10, 20, ..., 90
    var i: i32 = 0;
    while (i < 10) : (i += 1) {
        try btree.insert(i * 10, .{ .page_id = @intCast(i), .slot_id = 0 });
    }

    // Range scan [15, 55] should return 20, 30, 40, 50
    var iter = try btree.rangeScan(15, 55);
    var results: [10]Key = undefined;
    var count: usize = 0;

    while (try iter.next()) |entry| {
        results[count] = entry.key;
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 4), count);
    try std.testing.expectEqual(@as(Key, 20), results[0]);
    try std.testing.expectEqual(@as(Key, 30), results[1]);
    try std.testing.expectEqual(@as(Key, 40), results[2]);
    try std.testing.expectEqual(@as(Key, 50), results[3]);
}

test "btree large insert with random order" {
    const test_file = "test_btree_large.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 200);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    // Insert keys in a scattered pattern (not sequential) to test various split scenarios
    const count: i32 = 2000;
    var i: i32 = 0;
    while (i < count) : (i += 1) {
        // Spread keys out: use a simple hash-like pattern
        const key: i32 = @mod(i * 997, count);
        btree.insert(key, .{ .page_id = @intCast(i), .slot_id = 0 }) catch |err| {
            // Skip duplicates from the modular arithmetic
            if (err == BTreeError.DuplicateKey) continue;
            return err;
        };
    }

    // Verify all unique keys can be found
    i = 0;
    var found_count: i32 = 0;
    while (i < count) : (i += 1) {
        if ((try btree.search(i)) != null) {
            found_count += 1;
        }
    }

    // All keys in [0, count) should be reachable since 997 is coprime with 2000
    try std.testing.expectEqual(count, found_count);
}

test "btree delete from empty tree" {
    const test_file = "test_btree_del_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    // Delete from empty tree should return false (not crash)
    const deleted = try btree.delete(42);
    try std.testing.expect(!deleted);
}

test "btree min and max key values" {
    const test_file = "test_btree_minmax.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    const min_key: i32 = std.math.minInt(i32);
    const max_key: i32 = std.math.maxInt(i32);

    try btree.insert(min_key, .{ .page_id = 1, .slot_id = 0 });
    try btree.insert(max_key, .{ .page_id = 2, .slot_id = 0 });
    try btree.insert(0, .{ .page_id = 3, .slot_id = 0 });

    const r_min = (try btree.search(min_key)).?;
    try std.testing.expectEqual(@as(PageId, 1), r_min.page_id);

    const r_max = (try btree.search(max_key)).?;
    try std.testing.expectEqual(@as(PageId, 2), r_max.page_id);

    const r_zero = (try btree.search(0)).?;
    try std.testing.expectEqual(@as(PageId, 3), r_zero.page_id);
}

test "btree range scan no results" {
    const test_file = "test_btree_range_empty.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    // Insert keys 100, 200, 300
    try btree.insert(100, .{ .page_id = 1, .slot_id = 0 });
    try btree.insert(200, .{ .page_id = 2, .slot_id = 0 });
    try btree.insert(300, .{ .page_id = 3, .slot_id = 0 });

    // Range scan [50, 99] — no keys in range
    var iter = try btree.rangeScan(50, 99);
    var count: usize = 0;
    while (try iter.next()) |_| {
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "btree insert delete then reinsert same key" {
    const test_file = "test_btree_reinsert.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    try btree.insert(42, .{ .page_id = 1, .slot_id = 0 });
    try std.testing.expect((try btree.search(42)) != null);

    const deleted = try btree.delete(42);
    try std.testing.expect(deleted);
    try std.testing.expect((try btree.search(42)) == null);

    // Reinsert same key with different value
    try btree.insert(42, .{ .page_id = 9, .slot_id = 5 });
    const result = (try btree.search(42)).?;
    try std.testing.expectEqual(@as(PageId, 9), result.page_id);
    try std.testing.expectEqual(@as(u16, 5), result.slot_id);
}

test "btree range scan entire range" {
    const test_file = "test_btree_range_all.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer dm.deleteFile();
    try dm.open();
    defer dm.close();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 50);
    defer bp.deinit();

    var btree = try BTree.create(&bp);

    // Insert 50 keys
    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        try btree.insert(i, .{ .page_id = @intCast(i), .slot_id = 0 });
    }

    // Range scan [0, 49] should return all 50
    var iter = try btree.rangeScan(0, 49);
    var count: usize = 0;
    while (try iter.next()) |_| {
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 50), count);
}
