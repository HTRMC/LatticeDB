const std = @import("std");
const vector_mod = @import("vector.zig");
const tuple_mod = @import("../storage/tuple.zig");
const table_mod = @import("../storage/table.zig");
const page_mod = @import("../storage/page.zig");
const mvcc_mod = @import("../storage/mvcc.zig");
const iam_mod = @import("../storage/iam.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const alloc_map_mod = @import("../storage/alloc_map.zig");
const ast = @import("../parser/ast.zig");

const DataChunk = vector_mod.DataChunk;
const ColumnVector = vector_mod.ColumnVector;
const VECTOR_SIZE = vector_mod.VECTOR_SIZE;
const Value = tuple_mod.Value;
const Schema = tuple_mod.Schema;
const ColumnType = tuple_mod.ColumnType;
const Tuple = tuple_mod.Tuple;
const Table = table_mod.Table;
const Page = page_mod.Page;
const PageId = page_mod.PageId;
const SlotId = page_mod.SlotId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const TupleHeader = mvcc_mod.TupleHeader;
const TxnId = mvcc_mod.TxnId;
const INVALID_TXN_ID = mvcc_mod.INVALID_TXN_ID;
const Transaction = mvcc_mod.Transaction;
const TransactionManager = mvcc_mod.TransactionManager;
const BufferPool = buffer_pool_mod.BufferPool;
const IamChainIterator = iam_mod.IamChainIterator;
const EXTENT_SIZE = alloc_map_mod.EXTENT_SIZE;

pub const VecScanError = error{
    BufferPoolError,
    SerializationError,
    OutOfMemory,
};

/// Max pages pinned per chunk. With VECTOR_SIZE=2048 and ~16 rows per page
/// (worst case large tuples), 128 is more than enough.
const MAX_PINNED_PAGES = 128;

/// A predicate pushed down into the scan. Evaluated on raw tuple bytes
/// before deserialization — non-matching rows are never written to the chunk.
pub const ScanPredicate = struct {
    col_idx: usize,
    col_type: ColumnType,
    op: ast.CompOp,
    literal: Value,
};

const MAX_SCAN_PREDS = 2;

/// Vectorized sequential scan that produces DataChunks from table pages.
/// Walks the IAM chain (same as Table.ScanIterator) but fills columnar batches.
pub const VecSeqScan = struct {
    buffer_pool: *BufferPool,
    schema: *const Schema,
    iam_iter: IamChainIterator,
    iam_page_id: PageId,
    table_id: PageId,
    txn_manager: ?*TransactionManager,
    txn: ?*const Transaction,

    // Scan state
    current_extent_start: PageId,
    current_page_in_extent: u32,
    current_slot: SlotId,
    has_extent: bool,
    done: bool,

    // Reusable chunk
    chunk: DataChunk,

    // Deferred string materialization
    defer_strings: bool,
    pinned_pages: [MAX_PINNED_PAGES]PageId,
    pinned_count: u8,

    // Scan-filter fusion: predicates evaluated on raw tuple bytes
    scan_preds: [MAX_SCAN_PREDS]ScanPredicate,
    scan_pred_count: u8,

    // MVCC fast path: cache last-seen xmin to avoid repeated hash lookups
    cached_xmin: TxnId,
    cached_xmin_visible: bool,

    pub fn init(
        allocator: std.mem.Allocator,
        table: *Table,
        txn: ?*const Transaction,
    ) VecScanError!VecSeqScan {
        const meta = table.readMeta() catch return VecScanError.BufferPoolError;
        const chunk = DataChunk.initFromSchema(allocator, table.schema) catch return VecScanError.OutOfMemory;
        return .{
            .buffer_pool = table.buffer_pool,
            .schema = table.schema,
            .iam_iter = IamChainIterator.init(table.buffer_pool, meta.iam_page_id),
            .iam_page_id = meta.iam_page_id,
            .table_id = table.table_id,
            .txn_manager = table.txn_manager,
            .txn = txn,
            .current_extent_start = 0,
            .current_page_in_extent = 0,
            .current_slot = 0,
            .has_extent = false,
            .done = false,
            .chunk = chunk,
            .defer_strings = false,
            .pinned_pages = undefined,
            .pinned_count = 0,
            .scan_preds = undefined,
            .scan_pred_count = 0,
            .cached_xmin = INVALID_TXN_ID,
            .cached_xmin_visible = false,
        };
    }

    /// Add a predicate to be evaluated during scan (before deserialization).
    /// Returns true if the predicate was accepted, false if scan fusion isn't possible.
    pub fn addScanPredicate(self: *VecSeqScan, pred: ScanPredicate) bool {
        if (self.scan_pred_count >= MAX_SCAN_PREDS) return false;
        // Only numeric types
        switch (pred.col_type) {
            .integer, .bigint, .float => {},
            else => return false,
        }
        self.scan_preds[self.scan_pred_count] = pred;
        self.scan_pred_count += 1;
        return true;
    }

    pub fn deinit(self: *VecSeqScan) void {
        self.unpinDeferredPages();
        self.chunk.deinit();
    }

    /// Returns the next DataChunk of up to VECTOR_SIZE rows, or null when scan is exhausted.
    pub fn next(self: *VecSeqScan) VecScanError!?*DataChunk {
        if (self.done) return null;

        // Unpin pages held from previous chunk (if deferred)
        self.unpinDeferredPages();

        self.chunk.reset();
        var row_idx: u16 = 0;

        while (row_idx < VECTOR_SIZE) {
            // Get next extent if needed
            if (!self.has_extent) {
                if (self.iam_iter.next()) |extent_id| {
                    self.current_extent_start = @intCast(extent_id * EXTENT_SIZE);
                    self.current_page_in_extent = 0;
                    self.current_slot = 0;
                    self.has_extent = true;
                } else {
                    self.done = true;
                    break;
                }
            }

            // Walk pages within current extent
            while (self.current_page_in_extent < EXTENT_SIZE and row_idx < VECTOR_SIZE) {
                const page_id = self.current_extent_start + self.current_page_in_extent;

                // Skip IAM page
                if (page_id == self.iam_page_id) {
                    self.current_page_in_extent += 1;
                    self.current_slot = 0;
                    continue;
                }

                var pg = self.buffer_pool.fetchPage(page_id) catch return VecScanError.BufferPoolError;

                const pg_header = pg.getHeader();

                // Skip uninitialized pages
                if (pg_header.free_space_end == 0) {
                    self.buffer_pool.unpinPage(page_id, false) catch {};
                    self.current_page_in_extent += 1;
                    self.current_slot = 0;
                    continue;
                }

                // Skip slot 0 on the meta page
                const start_slot: SlotId = if (page_id == self.table_id and self.current_slot == 0) 1 else self.current_slot;

                var slot = start_slot;
                while (slot < pg_header.slot_count and row_idx < VECTOR_SIZE) : (slot += 1) {
                    if (pg.getTuple(slot)) |raw| {
                        // MVCC visibility check with fast path
                        const user_data = if (self.txn != null and self.txn_manager != null) blk: {
                            if (raw.len < TupleHeader.SIZE) continue;
                            const header = std.mem.bytesToValue(TupleHeader, raw[0..TupleHeader.SIZE]);
                            const data = raw[TupleHeader.SIZE..];

                            // Fast path: live tuple (xmax==0) with cached xmin
                            if (header.xmax == 0 and header.xmin == self.cached_xmin) {
                                if (!self.cached_xmin_visible) continue;
                                break :blk data;
                            }

                            // Slow path: full visibility check
                            const vis = mvcc_mod.isVisible(&header, &self.txn.?.snapshot, self.txn_manager.?, self.txn.?.txn_id);
                            if (vis == .invisible) {
                                // Cache negative result for this xmin (if live tuple)
                                if (header.xmax == 0) {
                                    self.cached_xmin = header.xmin;
                                    self.cached_xmin_visible = false;
                                }
                                continue;
                            }

                            // Cache positive result for this xmin
                            if (header.xmax == 0) {
                                self.cached_xmin = header.xmin;
                                self.cached_xmin_visible = true;
                            }
                            break :blk data;
                        } else raw;

                        // Scan-filter fusion: check predicates on raw bytes before deserializing
                        if (self.scan_pred_count > 0) {
                            if (!self.matchesScanPredicates(user_data)) continue;
                        }

                        self.deserializeIntoChunk(user_data, row_idx) catch continue;
                        row_idx += 1;
                    }
                }

                if (self.defer_strings and self.pinned_count < MAX_PINNED_PAGES) {
                    // Keep page pinned — strings point into page data
                    self.pinned_pages[self.pinned_count] = page_id;
                    self.pinned_count += 1;
                } else {
                    self.buffer_pool.unpinPage(page_id, false) catch {};
                }

                if (row_idx >= VECTOR_SIZE) {
                    // Chunk is full — save position for next call
                    self.current_slot = slot;
                    break;
                }

                // Move to next page
                self.current_page_in_extent += 1;
                self.current_slot = 0;
            }

            if (row_idx >= VECTOR_SIZE) break;

            // Move to next extent if we finished current one
            if (self.current_page_in_extent >= EXTENT_SIZE) {
                self.has_extent = false;
            }
        }

        self.chunk.count = row_idx;
        if (row_idx == 0) return null;
        return &self.chunk;
    }

    /// Evaluate all scan predicates against raw tuple bytes.
    /// Returns true if the row matches ALL predicates (AND semantics).
    fn matchesScanPredicates(self: *const VecSeqScan, data: []const u8) bool {
        const bitmap_size = self.schema.nullBitmapSize();
        if (data.len < bitmap_size) return false;

        for (self.scan_preds[0..self.scan_pred_count]) |pred| {
            // Check null for predicate column
            const is_null = (data[pred.col_idx / 8] & (@as(u8, 1) << @as(u3, @intCast(pred.col_idx % 8)))) != 0;
            if (is_null) return false;

            // Compute byte offset to predicate column by walking preceding columns
            var offset: usize = bitmap_size;
            for (self.schema.columns[0..pred.col_idx], 0..) |col_def, ci| {
                const col_null = (data[ci / 8] & (@as(u8, 1) << @as(u3, @intCast(ci % 8)))) != 0;
                if (!col_null) {
                    switch (col_def.col_type) {
                        .boolean => offset += 1,
                        .integer => offset += 4,
                        .bigint, .float => offset += 8,
                        .varchar, .text => {
                            if (offset + 2 > data.len) return false;
                            const str_len = std.mem.bytesToValue(u16, data[offset..][0..2]);
                            offset += 2 + str_len;
                        },
                    }
                }
            }

            // Read and compare
            const matches = switch (pred.col_type) {
                .integer => blk: {
                    if (offset + 4 > data.len) break :blk false;
                    const val = std.mem.bytesToValue(i32, data[offset..][0..4]);
                    break :blk cmpI32(val, pred.literal.integer, pred.op);
                },
                .bigint => blk: {
                    if (offset + 8 > data.len) break :blk false;
                    const val = std.mem.bytesToValue(i64, data[offset..][0..8]);
                    break :blk cmpI64(val, pred.literal.bigint, pred.op);
                },
                .float => blk: {
                    if (offset + 8 > data.len) break :blk false;
                    const val = std.mem.bytesToValue(f64, data[offset..][0..8]);
                    break :blk cmpF64(val, pred.literal.float, pred.op);
                },
                else => true,
            };
            if (!matches) return false;
        }
        return true;
    }

    /// After filtering, dupe surviving row strings into the arena and unpin pages.
    /// Only needed when defer_strings is enabled.
    pub fn materializeSurvivingStrings(self: *VecSeqScan) void {
        if (self.pinned_count == 0) return;

        const chunk = &self.chunk;
        const arena_alloc = chunk.arena.allocator();

        for (self.schema.columns, 0..) |col_def, i| {
            switch (col_def.col_type) {
                .varchar, .text => {
                    if (chunk.sel) |sel| {
                        for (sel.indices[0..sel.len]) |row_idx| {
                            if (!chunk.columns[i].isNull(row_idx)) {
                                const raw = chunk.columns[i].data.bytes_ptrs[row_idx];
                                chunk.columns[i].data.bytes_ptrs[row_idx] = arena_alloc.dupe(u8, raw) catch {
                                    chunk.columns[i].setNull(row_idx);
                                    continue;
                                };
                            }
                        }
                    } else {
                        var r: u16 = 0;
                        while (r < chunk.count) : (r += 1) {
                            if (!chunk.columns[i].isNull(r)) {
                                const raw = chunk.columns[i].data.bytes_ptrs[r];
                                chunk.columns[i].data.bytes_ptrs[r] = arena_alloc.dupe(u8, raw) catch {
                                    chunk.columns[i].setNull(r);
                                    continue;
                                };
                            }
                        }
                    }
                },
                else => {},
            }
        }

        self.unpinDeferredPages();
    }

    fn unpinDeferredPages(self: *VecSeqScan) void {
        for (self.pinned_pages[0..self.pinned_count]) |pid| {
            self.buffer_pool.unpinPage(pid, false) catch {};
        }
        self.pinned_count = 0;
    }

    /// Deserialize raw tuple bytes directly into the chunk's column vectors at row_idx.
    fn deserializeIntoChunk(self: *VecSeqScan, data: []const u8, row_idx: u16) !void {
        const col_count = self.schema.columns.len;
        const bitmap_size = self.schema.nullBitmapSize();

        if (data.len < bitmap_size) {
            for (self.chunk.columns) |*col| {
                col.setNull(row_idx);
            }
            return;
        }

        var offset: usize = bitmap_size;
        for (self.schema.columns, 0..) |col_def, i| {
            const is_null = (data[i / 8] & (@as(u8, 1) << @as(u3, @intCast(i % 8)))) != 0;
            if (is_null) {
                self.chunk.columns[i].setNull(row_idx);
            } else {
                if (offset >= data.len) {
                    self.chunk.columns[i].setNull(row_idx);
                } else {
                    self.chunk.columns[i].clearNull(row_idx);
                    switch (col_def.col_type) {
                        .boolean => {
                            if (offset < data.len) {
                                self.chunk.columns[i].data.booleans[row_idx] = data[offset] != 0;
                                offset += 1;
                            }
                        },
                        .integer => {
                            if (offset + 4 <= data.len) {
                                self.chunk.columns[i].data.integers[row_idx] = std.mem.bytesToValue(i32, data[offset..][0..4]);
                                offset += 4;
                            }
                        },
                        .bigint => {
                            if (offset + 8 <= data.len) {
                                self.chunk.columns[i].data.bigints[row_idx] = std.mem.bytesToValue(i64, data[offset..][0..8]);
                                offset += 8;
                            }
                        },
                        .float => {
                            if (offset + 8 <= data.len) {
                                self.chunk.columns[i].data.floats[row_idx] = std.mem.bytesToValue(f64, data[offset..][0..8]);
                                offset += 8;
                            }
                        },
                        .varchar, .text => {
                            if (offset + 2 <= data.len) {
                                const str_len = std.mem.bytesToValue(u16, data[offset..][0..2]);
                                offset += 2;
                                if (offset + str_len <= data.len) {
                                    if (self.defer_strings) {
                                        self.chunk.columns[i].data.bytes_ptrs[row_idx] = data[offset..][0..str_len];
                                    } else {
                                        const duped = self.chunk.arena.allocator().dupe(u8, data[offset..][0..str_len]) catch return error.OutOfMemory;
                                        self.chunk.columns[i].data.bytes_ptrs[row_idx] = duped;
                                    }
                                    offset += str_len;
                                } else {
                                    self.chunk.columns[i].setNull(row_idx);
                                }
                            } else {
                                self.chunk.columns[i].setNull(row_idx);
                            }
                        },
                    }
                }
            }
        }
        _ = col_count;
    }
};

// Scalar comparison helpers for scan-filter fusion
fn cmpI32(a: i32, b: i32, op: ast.CompOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .gt => a > b,
        .lte => a <= b,
        .gte => a >= b,
    };
}

fn cmpI64(a: i64, b: i64, op: ast.CompOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .gt => a > b,
        .lte => a <= b,
        .gte => a >= b,
    };
}

fn cmpF64(a: f64, b: f64, op: ast.CompOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .gt => a > b,
        .lte => a <= b,
        .gte => a >= b,
    };
}
