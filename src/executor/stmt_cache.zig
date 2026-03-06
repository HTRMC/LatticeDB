const std = @import("std");
const ast = @import("../parser/ast.zig");
const parser_mod = @import("../parser/parser.zig");

const Parser = parser_mod.Parser;

/// LRU prepared statement cache.
/// Caches parsed AST trees keyed by SQL string to avoid repeated parsing.
pub const StmtCache = struct {
    allocator: std.mem.Allocator,
    /// Cached entries keyed by SQL hash
    entries: std.AutoHashMapUnmanaged(u64, *Entry),
    /// LRU order: most recently used at the end
    lru_head: ?*Entry,
    lru_tail: ?*Entry,
    /// Current number of entries
    count: usize,
    /// Maximum cache size
    max_size: usize,
    /// Cache statistics
    hits: u64,
    misses: u64,

    const Self = @This();

    pub const Entry = struct {
        sql_hash: u64,
        arena_state: std.heap.ArenaAllocator,
        statement: ast.Statement,
        param_count: u32,
        /// Doubly-linked list pointers for LRU
        prev: ?*Entry,
        next: ?*Entry,
    };

    pub fn init(allocator: std.mem.Allocator, max_size: usize) Self {
        return .{
            .allocator = allocator,
            .entries = .empty,
            .lru_head = null,
            .lru_tail = null,
            .count = 0,
            .max_size = max_size,
            .hits = 0,
            .misses = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        var node = self.lru_head;
        while (node) |n| {
            const next = n.next;
            n.arena_state.deinit();
            self.allocator.destroy(n);
            node = next;
        }
        self.entries.deinit(self.allocator);
    }

    pub const CacheResult = struct {
        statement: ast.Statement,
        param_count: u32,
    };

    /// Look up a cached statement by SQL string.
    /// Returns the statement and param count, or null if not cached.
    pub fn get(self: *Self, sql: []const u8) ?CacheResult {
        const hash = hashSql(sql);
        if (self.entries.get(hash)) |entry| {
            self.hits += 1;
            // Move to tail (most recently used)
            self.moveToTail(entry);
            return .{
                .statement = entry.statement,
                .param_count = entry.param_count,
            };
        }
        self.misses += 1;
        return null;
    }

    /// Parse and cache a SQL statement. Returns the parsed statement.
    pub fn getOrParse(self: *Self, sql: []const u8) !CacheResult {
        // Try cache first
        if (self.get(sql)) |cached| return cached;

        // Parse the SQL
        var parser = Parser.init(self.allocator, sql);
        const stmt = parser.parse() catch {
            parser.deinit();
            return error.ParseError;
        };
        const param_count = parser.param_count;

        // Create cache entry
        const entry = self.allocator.create(Entry) catch {
            parser.deinit();
            return error.OutOfMemory;
        };
        const hash = hashSql(sql);
        entry.* = .{
            .sql_hash = hash,
            .arena_state = parser.arena_state,
            .statement = stmt,
            .param_count = param_count,
            .prev = null,
            .next = null,
        };

        // Evict if at capacity
        if (self.count >= self.max_size) {
            self.evictLru();
        }

        // Insert into cache
        self.entries.put(self.allocator, hash, entry) catch {
            entry.arena_state.deinit();
            self.allocator.destroy(entry);
            return error.OutOfMemory;
        };
        self.appendToTail(entry);
        self.count += 1;

        return .{
            .statement = entry.statement,
            .param_count = entry.param_count,
        };
    }

    /// Invalidate all cached entries (e.g., after DDL changes).
    pub fn invalidateAll(self: *Self) void {
        var node = self.lru_head;
        while (node) |n| {
            const next = n.next;
            n.arena_state.deinit();
            self.allocator.destroy(n);
            node = next;
        }
        self.entries.clearAndFree(self.allocator);
        self.lru_head = null;
        self.lru_tail = null;
        self.count = 0;
    }

    /// Get cache hit rate as a percentage.
    pub fn hitRate(self: *const Self) f64 {
        const total = self.hits + self.misses;
        if (total == 0) return 0;
        return @as(f64, @floatFromInt(self.hits)) / @as(f64, @floatFromInt(total)) * 100.0;
    }

    // ── Internal helpers ──────────────────────────────────────────

    fn hashSql(sql: []const u8) u64 {
        return std.hash.Wyhash.hash(0, sql);
    }

    fn moveToTail(self: *Self, entry: *Entry) void {
        if (entry == self.lru_tail) return; // Already at tail

        // Remove from current position
        if (entry.prev) |p| p.next = entry.next else self.lru_head = entry.next;
        if (entry.next) |n| n.prev = entry.prev;

        // Append to tail
        entry.prev = self.lru_tail;
        entry.next = null;
        if (self.lru_tail) |t| t.next = entry;
        self.lru_tail = entry;
    }

    fn appendToTail(self: *Self, entry: *Entry) void {
        entry.prev = self.lru_tail;
        entry.next = null;
        if (self.lru_tail) |t| {
            t.next = entry;
        } else {
            self.lru_head = entry;
        }
        self.lru_tail = entry;
    }

    fn evictLru(self: *Self) void {
        const victim = self.lru_head orelse return;

        // Remove from linked list
        self.lru_head = victim.next;
        if (victim.next) |n| n.prev = null else self.lru_tail = null;

        // Remove from hash map
        _ = self.entries.remove(victim.sql_hash);

        // Free resources
        victim.arena_state.deinit();
        self.allocator.destroy(victim);
        self.count -= 1;
    }
};

// ── Tests ────────────────────────────────────────────────────────

test "stmt cache basic get and parse" {
    var cache = StmtCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    // First call: miss, parse
    const r1 = try cache.getOrParse("SELECT * FROM users");
    try std.testing.expect(r1.statement == .select);
    try std.testing.expectEqual(@as(u64, 0), cache.hits);
    try std.testing.expectEqual(@as(u64, 1), cache.misses);

    // Second call: hit
    const r2 = try cache.getOrParse("SELECT * FROM users");
    try std.testing.expect(r2.statement == .select);
    try std.testing.expectEqual(@as(u64, 1), cache.hits);
    try std.testing.expectEqual(@as(u64, 1), cache.misses);
}

test "stmt cache eviction" {
    var cache = StmtCache.init(std.testing.allocator, 2);
    defer cache.deinit();

    _ = try cache.getOrParse("SELECT * FROM a");
    _ = try cache.getOrParse("SELECT * FROM b");
    try std.testing.expectEqual(@as(usize, 2), cache.count);

    // This should evict "a" (LRU)
    _ = try cache.getOrParse("SELECT * FROM c");
    try std.testing.expectEqual(@as(usize, 2), cache.count);

    // "a" should be evicted, "b" and "c" remain
    try std.testing.expect(cache.get("SELECT * FROM a") == null);
    try std.testing.expect(cache.get("SELECT * FROM b") != null);
    try std.testing.expect(cache.get("SELECT * FROM c") != null);
}

test "stmt cache LRU order" {
    var cache = StmtCache.init(std.testing.allocator, 2);
    defer cache.deinit();

    _ = try cache.getOrParse("SELECT * FROM a");
    _ = try cache.getOrParse("SELECT * FROM b");

    // Access "a" again — makes it most recently used
    _ = try cache.getOrParse("SELECT * FROM a");

    // Insert "c" — should evict "b" (now LRU)
    _ = try cache.getOrParse("SELECT * FROM c");

    try std.testing.expect(cache.get("SELECT * FROM b") == null);
    try std.testing.expect(cache.get("SELECT * FROM a") != null);
    try std.testing.expect(cache.get("SELECT * FROM c") != null);
}

test "stmt cache invalidateAll" {
    var cache = StmtCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    _ = try cache.getOrParse("SELECT * FROM users");
    _ = try cache.getOrParse("SELECT * FROM orders");
    try std.testing.expectEqual(@as(usize, 2), cache.count);

    cache.invalidateAll();
    try std.testing.expectEqual(@as(usize, 0), cache.count);
    try std.testing.expect(cache.get("SELECT * FROM users") == null);
}

test "stmt cache hit rate" {
    var cache = StmtCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    _ = try cache.getOrParse("SELECT * FROM users");
    _ = try cache.getOrParse("SELECT * FROM users");
    _ = try cache.getOrParse("SELECT * FROM users");

    // 1 miss (first parse) + 2 hits = 66.7%
    try std.testing.expectApproxEqAbs(@as(f64, 66.66), cache.hitRate(), 0.1);
}
