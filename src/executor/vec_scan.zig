const std = @import("std");
const vector_mod = @import("vector.zig");
const tuple_mod = @import("../storage/tuple.zig");
const table_mod = @import("../storage/table.zig");
const page_mod = @import("../storage/page.zig");
const mvcc_mod = @import("../storage/mvcc.zig");
const iam_mod = @import("../storage/iam.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const alloc_map_mod = @import("../storage/alloc_map.zig");

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
        };
    }

    pub fn deinit(self: *VecSeqScan) void {
        self.chunk.deinit();
    }

    /// Returns the next DataChunk of up to VECTOR_SIZE rows, or null when scan is exhausted.
    pub fn next(self: *VecSeqScan) VecScanError!?*DataChunk {
        if (self.done) return null;

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
                        // MVCC visibility check
                        if (self.txn != null and self.txn_manager != null) {
                            if (raw.len < TupleHeader.SIZE) continue;
                            const stripped = Tuple.stripHeader(raw) catch continue;
                            const vis = mvcc_mod.isVisible(&stripped.header, &self.txn.?.snapshot, self.txn_manager.?, self.txn.?.txn_id);
                            if (vis == .invisible) continue;

                            self.deserializeIntoChunk(stripped.user_data, row_idx) catch continue;
                        } else {
                            // Legacy mode: no header
                            self.deserializeIntoChunk(raw, row_idx) catch continue;
                        }
                        row_idx += 1;
                    }
                }

                self.buffer_pool.unpinPage(page_id, false) catch {};

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

    /// Deserialize raw tuple bytes directly into the chunk's column vectors at row_idx.
    fn deserializeIntoChunk(self: *VecSeqScan, data: []const u8, row_idx: u16) !void {
        const col_count = self.schema.columns.len;
        const bitmap_size = self.schema.nullBitmapSize();

        if (data.len < bitmap_size) {
            // Short tuple: fill all columns with null
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
                    // Short tuple — column added after this tuple was written
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
                                    // Dupe string into the chunk's arena (page will be unpinned)
                                    const duped = self.chunk.arena.allocator().dupe(u8, data[offset..][0..str_len]) catch return error.OutOfMemory;
                                    self.chunk.columns[i].data.bytes_ptrs[row_idx] = duped;
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
