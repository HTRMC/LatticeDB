const std = @import("std");
const page_mod = @import("../storage/page.zig");

const PageId = page_mod.PageId;
const TupleId = page_mod.TupleId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const PAGE_SIZE = page_mod.PAGE_SIZE;

/// B+Tree key type - currently only supports i32 keys.
/// This can be extended to support composite/variable-length keys later.
pub const Key = i32;

/// Sentinel key values
pub const MIN_KEY: Key = std.math.minInt(Key);
pub const MAX_KEY: Key = std.math.maxInt(Key);

/// Node type identifier
pub const NodeType = enum(u8) {
    internal = 0,
    leaf = 1,
};

/// Header for all B+Tree nodes, stored at the start of the page.
pub const NodeHeader = extern struct {
    /// Type of this node (internal or leaf)
    node_type: NodeType,
    /// Reserved for alignment
    _pad: u8 = 0,
    /// Number of keys stored in this node
    key_count: u16,
    /// Page ID of this node
    page_id: PageId,
    /// Page ID of the parent node (INVALID_PAGE_ID for root)
    parent: PageId,

    pub const SIZE: usize = @sizeOf(NodeHeader);
};

// ============================================================
// Leaf Node
// ============================================================
// Layout: [NodeHeader][next_leaf: PageId][prev_leaf: PageId][LeafEntry * N]
//
// Each LeafEntry: [key: Key][page_id: PageId][slot_id: SlotId][_pad: u16]
// ============================================================

/// A key-value pair stored in a leaf node
pub const LeafEntry = extern struct {
    key: Key,
    page_id: PageId,
    slot_id: u16,
    _pad: u16 = 0,

    pub const SIZE: usize = @sizeOf(LeafEntry);

    pub fn tid(self: LeafEntry) TupleId {
        return .{ .page_id = self.page_id, .slot_id = self.slot_id };
    }
};

/// Extra header fields for leaf nodes (after NodeHeader)
pub const LeafExtra = extern struct {
    next_leaf: PageId,
    prev_leaf: PageId,

    pub const SIZE: usize = @sizeOf(LeafExtra);
};

const LEAF_HEADER_SIZE = NodeHeader.SIZE + LeafExtra.SIZE;

/// Maximum number of entries in a leaf node
pub const LEAF_MAX_ENTRIES: usize = (PAGE_SIZE - LEAF_HEADER_SIZE) / LeafEntry.SIZE;

/// A view into a leaf node page
pub const LeafNode = struct {
    data: *align(8) [PAGE_SIZE]u8,

    const Self = @This();

    /// Initialize a new leaf node
    pub fn init(data: *align(8) [PAGE_SIZE]u8, page_id: PageId) Self {
        @memset(data, 0);
        const node = Self{ .data = data };
        node.headerMut().* = .{
            .node_type = .leaf,
            .key_count = 0,
            .page_id = page_id,
            .parent = INVALID_PAGE_ID,
        };
        node.leafExtraMut().* = .{
            .next_leaf = INVALID_PAGE_ID,
            .prev_leaf = INVALID_PAGE_ID,
        };
        return node;
    }

    /// Wrap existing page data as a leaf node
    pub fn fromPage(data: *align(8) [PAGE_SIZE]u8) Self {
        return .{ .data = data };
    }

    pub fn header(self: Self) *const NodeHeader {
        return @ptrCast(@alignCast(self.data));
    }

    pub fn headerMut(self: Self) *NodeHeader {
        return @ptrCast(@alignCast(self.data));
    }

    pub fn leafExtra(self: Self) *const LeafExtra {
        return @ptrCast(@alignCast(self.data[NodeHeader.SIZE..][0..LeafExtra.SIZE]));
    }

    fn leafExtraMut(self: Self) *LeafExtra {
        return @ptrCast(@alignCast(self.data[NodeHeader.SIZE..][0..LeafExtra.SIZE]));
    }

    pub fn keyCount(self: Self) u16 {
        return self.header().key_count;
    }

    pub fn nextLeaf(self: Self) PageId {
        return self.leafExtra().next_leaf;
    }

    pub fn prevLeaf(self: Self) PageId {
        return self.leafExtra().prev_leaf;
    }

    pub fn setNextLeaf(self: Self, pid: PageId) void {
        self.leafExtraMut().next_leaf = pid;
    }

    pub fn setPrevLeaf(self: Self, pid: PageId) void {
        self.leafExtraMut().prev_leaf = pid;
    }

    /// Get the entries array
    fn entries(self: Self) []LeafEntry {
        const count = self.keyCount();
        const start = LEAF_HEADER_SIZE;
        const bytes = self.data[start..][0 .. count * LeafEntry.SIZE];
        const ptr: [*]LeafEntry = @ptrCast(@alignCast(bytes.ptr));
        return ptr[0..count];
    }

    /// Get a mutable pointer to the entry at index i
    pub fn entryPtr(self: Self, i: usize) *LeafEntry {
        const start = LEAF_HEADER_SIZE + i * LeafEntry.SIZE;
        return @ptrCast(@alignCast(self.data[start..][0..LeafEntry.SIZE]));
    }

    /// Get entry at index
    pub fn getEntry(self: Self, index: u16) ?LeafEntry {
        if (index >= self.keyCount()) return null;
        return self.entryPtr(index).*;
    }

    /// Get key at index
    pub fn getKey(self: Self, index: u16) ?Key {
        const entry = self.getEntry(index) orelse return null;
        return entry.key;
    }

    /// Binary search for a key. Returns the index where it should be inserted.
    /// If the key exists, returns its index.
    pub fn findKey(self: Self, key: Key) struct { index: u16, found: bool } {
        const ents = self.entries();
        var lo: u16 = 0;
        var hi: u16 = self.keyCount();

        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (ents[mid].key < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        const found = lo < self.keyCount() and ents[lo].key == key;
        return .{ .index = lo, .found = found };
    }

    /// Insert a key-TupleId pair. Returns false if the node is full.
    pub fn insert(self: Self, key: Key, tid: TupleId) bool {
        const count = self.keyCount();
        if (count >= LEAF_MAX_ENTRIES) return false;

        const result = self.findKey(key);
        const pos = result.index;

        // Shift entries right to make room
        if (pos < count) {
            const src_start = LEAF_HEADER_SIZE + pos * LeafEntry.SIZE;
            const dst_start = LEAF_HEADER_SIZE + (pos + 1) * LeafEntry.SIZE;
            const len = (count - pos) * LeafEntry.SIZE;
            std.mem.copyBackwards(u8, self.data[dst_start..][0..len], self.data[src_start..][0..len]);
        }

        // Write the new entry
        self.entryPtr(pos).* = .{
            .key = key,
            .page_id = tid.page_id,
            .slot_id = tid.slot_id,
        };

        self.headerMut().key_count = count + 1;
        return true;
    }

    /// Delete a key. Returns true if found and deleted.
    pub fn delete(self: Self, key: Key) bool {
        const result = self.findKey(key);
        if (!result.found) return false;

        const pos = result.index;
        const count = self.keyCount();

        // Shift entries left
        if (pos + 1 < count) {
            const src_start = LEAF_HEADER_SIZE + (pos + 1) * LeafEntry.SIZE;
            const dst_start = LEAF_HEADER_SIZE + pos * LeafEntry.SIZE;
            const len = (count - pos - 1) * LeafEntry.SIZE;
            @memcpy(self.data[dst_start..][0..len], self.data[src_start..][0..len]);
        }

        self.headerMut().key_count = count - 1;
        return true;
    }

    /// Is this node full?
    pub fn isFull(self: Self) bool {
        return self.keyCount() >= LEAF_MAX_ENTRIES;
    }

    /// Is this node less than half full?
    pub fn isUnderflow(self: Self) bool {
        return self.keyCount() < LEAF_MAX_ENTRIES / 2;
    }

    /// Split this leaf node. Moves the upper half of entries into `new_leaf`.
    /// Returns the first key of the new (right) leaf (the "split key" to push up).
    pub fn splitInto(self: Self, new_leaf: Self) Key {
        const count = self.keyCount();
        const mid = count / 2;
        const move_count = count - mid;

        // Copy upper half to new leaf
        const src_start = LEAF_HEADER_SIZE + mid * LeafEntry.SIZE;
        const dst_start = LEAF_HEADER_SIZE;
        const len = move_count * LeafEntry.SIZE;
        @memcpy(new_leaf.data[dst_start..][0..len], self.data[src_start..][0..len]);

        new_leaf.headerMut().key_count = @intCast(move_count);
        self.headerMut().key_count = @intCast(mid);

        // Maintain leaf chain
        const old_next = self.nextLeaf();
        new_leaf.setNextLeaf(old_next);
        new_leaf.setPrevLeaf(self.header().page_id);
        self.setNextLeaf(new_leaf.header().page_id);

        // Return the first key of the new leaf
        return new_leaf.entryPtr(0).key;
    }
};

// ============================================================
// Internal Node
// ============================================================
// Layout: [NodeHeader][child_0: PageId][Key0][child_1: PageId][Key1][child_2]...[KeyN-1][child_N]
//
// For N keys, there are N+1 children.
// child_i contains keys < key_i, child_{i+1} contains keys >= key_i
//
// We store: [NodeHeader][first_child: PageId][InternalEntry * N]
// where InternalEntry = [key: Key][child: PageId]
// ============================================================

/// An entry in an internal node: a key and the child to its right
pub const InternalEntry = extern struct {
    key: Key,
    child: PageId,

    pub const SIZE: usize = @sizeOf(InternalEntry);
};

const INTERNAL_HEADER_SIZE = NodeHeader.SIZE + @sizeOf(PageId); // header + first child pointer

/// Maximum number of keys in an internal node
pub const INTERNAL_MAX_KEYS: usize = (PAGE_SIZE - INTERNAL_HEADER_SIZE) / InternalEntry.SIZE;

/// A view into an internal node page
pub const InternalNode = struct {
    data: *align(8) [PAGE_SIZE]u8,

    const Self = @This();

    /// Initialize a new internal node
    pub fn init(data: *align(8) [PAGE_SIZE]u8, page_id: PageId) Self {
        @memset(data, 0);
        const node = Self{ .data = data };
        node.headerMut().* = .{
            .node_type = .internal,
            .key_count = 0,
            .page_id = page_id,
            .parent = INVALID_PAGE_ID,
        };
        node.setFirstChild(INVALID_PAGE_ID);
        return node;
    }

    /// Wrap existing page data as an internal node
    pub fn fromPage(data: *align(8) [PAGE_SIZE]u8) Self {
        return .{ .data = data };
    }

    pub fn header(self: Self) *const NodeHeader {
        return @ptrCast(@alignCast(self.data));
    }

    pub fn headerMut(self: Self) *NodeHeader {
        return @ptrCast(@alignCast(self.data));
    }

    pub fn keyCount(self: Self) u16 {
        return self.header().key_count;
    }

    /// Get/set the first (leftmost) child pointer
    pub fn firstChild(self: Self) PageId {
        const ptr: *const PageId = @ptrCast(@alignCast(self.data[NodeHeader.SIZE..][0..@sizeOf(PageId)]));
        return ptr.*;
    }

    pub fn setFirstChild(self: Self, pid: PageId) void {
        const ptr: *PageId = @ptrCast(@alignCast(self.data[NodeHeader.SIZE..][0..@sizeOf(PageId)]));
        ptr.* = pid;
    }

    /// Get a pointer to entry at index i (the key-child pairs after first_child)
    pub fn entryPtr(self: Self, i: usize) *InternalEntry {
        const start = INTERNAL_HEADER_SIZE + i * InternalEntry.SIZE;
        return @ptrCast(@alignCast(self.data[start..][0..InternalEntry.SIZE]));
    }

    /// Get the entries array
    fn entries(self: Self) []InternalEntry {
        const count = self.keyCount();
        const start = INTERNAL_HEADER_SIZE;
        const bytes = self.data[start..][0 .. count * InternalEntry.SIZE];
        const ptr: [*]InternalEntry = @ptrCast(@alignCast(bytes.ptr));
        return ptr[0..count];
    }

    /// Get key at index
    pub fn getKey(self: Self, index: u16) ?Key {
        if (index >= self.keyCount()) return null;
        return self.entryPtr(index).key;
    }

    /// Get the child pointer for a given key.
    /// Returns the PageId of the child that could contain the key.
    pub fn findChild(self: Self, key: Key) PageId {
        const ents = self.entries();
        // Binary search for the correct child
        var lo: u16 = 0;
        var hi: u16 = self.keyCount();

        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (ents[mid].key <= key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo is the index of the first key > search key
        // The child to follow is at position lo:
        // - if lo == 0, use first_child
        // - otherwise, use entries[lo-1].child
        if (lo == 0) {
            return self.firstChild();
        }
        return ents[lo - 1].child;
    }

    /// Insert a key and right child pointer at the correct position.
    /// The key separates left_child (already in tree) and right_child (new).
    /// Returns false if full.
    pub fn insertKeyChild(self: Self, key: Key, right_child: PageId) bool {
        const count = self.keyCount();
        if (count >= INTERNAL_MAX_KEYS) return false;

        // Find insertion point
        const ents = self.entries();
        var pos: u16 = 0;
        while (pos < count and ents[pos].key < key) : (pos += 1) {}

        // Shift entries right
        if (pos < count) {
            const src_start = INTERNAL_HEADER_SIZE + pos * InternalEntry.SIZE;
            const dst_start = INTERNAL_HEADER_SIZE + (pos + 1) * InternalEntry.SIZE;
            const len = (count - pos) * InternalEntry.SIZE;
            std.mem.copyBackwards(u8, self.data[dst_start..][0..len], self.data[src_start..][0..len]);
        }

        // Write new entry
        self.entryPtr(pos).* = .{
            .key = key,
            .child = right_child,
        };

        self.headerMut().key_count = count + 1;
        return true;
    }

    /// Is this node full?
    pub fn isFull(self: Self) bool {
        return self.keyCount() >= INTERNAL_MAX_KEYS;
    }

    /// Split this internal node. Moves the upper half into `new_node`.
    /// Returns the "median key" that should be pushed up to the parent.
    /// After split: self has keys [0..mid-1], new_node has keys [mid+1..count-1],
    /// and key[mid] is returned to be inserted in the parent.
    pub fn splitInto(self: Self, new_node: Self) Key {
        const count = self.keyCount();
        const mid: u16 = count / 2;

        // The median key goes up to the parent
        const median_key = self.entryPtr(mid).key;

        // new_node's first child is the right child of the median entry
        new_node.setFirstChild(self.entryPtr(mid).child);

        // Copy keys [mid+1..count-1] to new_node
        const move_count = count - mid - 1;
        if (move_count > 0) {
            const src_start = INTERNAL_HEADER_SIZE + (mid + 1) * InternalEntry.SIZE;
            const dst_start = INTERNAL_HEADER_SIZE;
            const len = move_count * InternalEntry.SIZE;
            @memcpy(new_node.data[dst_start..][0..len], self.data[src_start..][0..len]);
        }

        new_node.headerMut().key_count = @intCast(move_count);
        self.headerMut().key_count = mid;

        return median_key;
    }
};

/// Determine the node type by reading the header
pub fn getNodeType(data: *align(8) [PAGE_SIZE]u8) NodeType {
    const hdr: *const NodeHeader = @ptrCast(@alignCast(data));
    return hdr.node_type;
}

// ============================================================
// Tests
// ============================================================

test "leaf node insert and search" {
    var buf: [PAGE_SIZE]u8 align(8) = undefined;
    const leaf = LeafNode.init(&buf, 0);

    try std.testing.expectEqual(@as(u16, 0), leaf.keyCount());

    // Insert some keys
    try std.testing.expect(leaf.insert(10, .{ .page_id = 1, .slot_id = 0 }));
    try std.testing.expect(leaf.insert(5, .{ .page_id = 1, .slot_id = 1 }));
    try std.testing.expect(leaf.insert(20, .{ .page_id = 1, .slot_id = 2 }));
    try std.testing.expect(leaf.insert(15, .{ .page_id = 1, .slot_id = 3 }));

    try std.testing.expectEqual(@as(u16, 4), leaf.keyCount());

    // Keys should be sorted
    try std.testing.expectEqual(@as(Key, 5), leaf.getKey(0).?);
    try std.testing.expectEqual(@as(Key, 10), leaf.getKey(1).?);
    try std.testing.expectEqual(@as(Key, 15), leaf.getKey(2).?);
    try std.testing.expectEqual(@as(Key, 20), leaf.getKey(3).?);

    // Search for existing key
    const r1 = leaf.findKey(10);
    try std.testing.expect(r1.found);
    try std.testing.expectEqual(@as(u16, 1), r1.index);

    // Search for non-existing key
    const r2 = leaf.findKey(12);
    try std.testing.expect(!r2.found);
    try std.testing.expectEqual(@as(u16, 2), r2.index); // would insert before 15

    // Verify TupleId
    const entry = leaf.getEntry(1).?;
    try std.testing.expectEqual(@as(PageId, 1), entry.page_id);
    try std.testing.expectEqual(@as(u16, 0), entry.slot_id);
}

test "leaf node delete" {
    var buf: [PAGE_SIZE]u8 align(8) = undefined;
    const leaf = LeafNode.init(&buf, 0);

    _ = leaf.insert(10, .{ .page_id = 1, .slot_id = 0 });
    _ = leaf.insert(20, .{ .page_id = 1, .slot_id = 1 });
    _ = leaf.insert(30, .{ .page_id = 1, .slot_id = 2 });

    try std.testing.expectEqual(@as(u16, 3), leaf.keyCount());

    // Delete middle key
    try std.testing.expect(leaf.delete(20));
    try std.testing.expectEqual(@as(u16, 2), leaf.keyCount());
    try std.testing.expectEqual(@as(Key, 10), leaf.getKey(0).?);
    try std.testing.expectEqual(@as(Key, 30), leaf.getKey(1).?);

    // Delete non-existing key
    try std.testing.expect(!leaf.delete(99));
    try std.testing.expectEqual(@as(u16, 2), leaf.keyCount());
}

test "leaf node split" {
    var buf1: [PAGE_SIZE]u8 align(8) = undefined;
    var buf2: [PAGE_SIZE]u8 align(8) = undefined;
    const leaf1 = LeafNode.init(&buf1, 0);
    const leaf2 = LeafNode.init(&buf2, 1);

    // Insert entries
    var i: Key = 0;
    while (i < 20) : (i += 1) {
        try std.testing.expect(leaf1.insert(i * 2, .{ .page_id = 0, .slot_id = @intCast(i) }));
    }

    try std.testing.expectEqual(@as(u16, 20), leaf1.keyCount());

    // Split
    const split_key = leaf1.splitInto(leaf2);

    // Left should have first half, right should have second half
    try std.testing.expectEqual(@as(u16, 10), leaf1.keyCount());
    try std.testing.expectEqual(@as(u16, 10), leaf2.keyCount());

    // Split key is first key of right node
    try std.testing.expectEqual(leaf2.getKey(0).?, split_key);

    // All keys in left < split_key
    const left_last = leaf1.getKey(leaf1.keyCount() - 1).?;
    try std.testing.expect(left_last < split_key);

    // Leaf chain maintained
    try std.testing.expectEqual(@as(PageId, 1), leaf1.nextLeaf());
    try std.testing.expectEqual(@as(PageId, 0), leaf2.prevLeaf());
}

test "internal node find child" {
    var buf: [PAGE_SIZE]u8 align(8) = undefined;
    const node = InternalNode.init(&buf, 0);

    // Set up: first_child=10, keys=[20, 40, 60], children=[11, 12, 13]
    // So: child_10 has keys < 20, child_11 has 20..39, child_12 has 40..59, child_13 has >= 60
    node.setFirstChild(10);
    try std.testing.expect(node.insertKeyChild(20, 11));
    try std.testing.expect(node.insertKeyChild(40, 12));
    try std.testing.expect(node.insertKeyChild(60, 13));

    try std.testing.expectEqual(@as(u16, 3), node.keyCount());

    // Find child for various keys
    try std.testing.expectEqual(@as(PageId, 10), node.findChild(5));
    try std.testing.expectEqual(@as(PageId, 10), node.findChild(19));
    try std.testing.expectEqual(@as(PageId, 11), node.findChild(20));
    try std.testing.expectEqual(@as(PageId, 11), node.findChild(30));
    try std.testing.expectEqual(@as(PageId, 12), node.findChild(40));
    try std.testing.expectEqual(@as(PageId, 12), node.findChild(50));
    try std.testing.expectEqual(@as(PageId, 13), node.findChild(60));
    try std.testing.expectEqual(@as(PageId, 13), node.findChild(100));
}

test "internal node split" {
    var buf1: [PAGE_SIZE]u8 align(8) = undefined;
    var buf2: [PAGE_SIZE]u8 align(8) = undefined;
    const node1 = InternalNode.init(&buf1, 0);
    const node2 = InternalNode.init(&buf2, 1);

    node1.setFirstChild(100);
    var i: Key = 1;
    while (i <= 20) : (i += 1) {
        try std.testing.expect(node1.insertKeyChild(i * 10, @as(PageId, @intCast(100 + i))));
    }

    try std.testing.expectEqual(@as(u16, 20), node1.keyCount());

    // Split
    const median = node1.splitInto(node2);

    // node1 should have keys [0..mid-1], node2 should have keys [mid+1..19]
    // median should be key[mid]
    try std.testing.expectEqual(@as(u16, 10), node1.keyCount());
    try std.testing.expectEqual(@as(u16, 9), node2.keyCount());

    // Median is 110 (key at index 10)
    try std.testing.expectEqual(@as(Key, 110), median);

    // First key of node1 should be 10
    try std.testing.expectEqual(@as(Key, 10), node1.getKey(0).?);
    // Last key of node1 should be 100
    try std.testing.expectEqual(@as(Key, 100), node1.getKey(9).?);

    // First key of node2 should be 120
    try std.testing.expectEqual(@as(Key, 120), node2.getKey(0).?);
    // Last key of node2 should be 200
    try std.testing.expectEqual(@as(Key, 200), node2.getKey(8).?);
}

test "leaf capacity" {
    // Verify the capacity calculation is reasonable
    try std.testing.expect(LEAF_MAX_ENTRIES > 100);
    try std.testing.expect(INTERNAL_MAX_KEYS > 100);
}
