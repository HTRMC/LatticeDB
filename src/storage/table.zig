const std = @import("std");
const page_mod = @import("page.zig");
const buffer_pool_mod = @import("buffer_pool.zig");
const tuple_mod = @import("tuple.zig");
const mvcc_mod = @import("mvcc.zig");
const undo_log_mod = @import("undo_log.zig");
const alloc_map_mod = @import("alloc_map.zig");
const iam_mod = @import("iam.zig");

const Page = page_mod.Page;
const PageId = page_mod.PageId;
const SlotId = page_mod.SlotId;
const TupleId = page_mod.TupleId;
const INVALID_PAGE_ID = page_mod.INVALID_PAGE_ID;
const PAGE_SIZE = page_mod.PAGE_SIZE;
const BufferPool = buffer_pool_mod.BufferPool;
const Schema = tuple_mod.Schema;
const Tuple = tuple_mod.Tuple;
const Value = tuple_mod.Value;
const TupleHeader = mvcc_mod.TupleHeader;
const TxnId = mvcc_mod.TxnId;
const NO_UNDO_PTR = mvcc_mod.NO_UNDO_PTR;
const Transaction = mvcc_mod.Transaction;
const TransactionManager = mvcc_mod.TransactionManager;
const Snapshot = mvcc_mod.Snapshot;
const UndoLog = undo_log_mod.UndoLog;
const UndoRecordHeader = undo_log_mod.UndoRecordHeader;
const AllocManager = alloc_map_mod.AllocManager;
const Pfs = alloc_map_mod.Pfs;
const Iam = iam_mod.Iam;
const IamChainIterator = iam_mod.IamChainIterator;
const EXTENT_SIZE = alloc_map_mod.EXTENT_SIZE;

pub const TableError = error{
    BufferPoolError,
    TupleTooBig,
    PageFull,
    TupleNotFound,
    SerializationError,
    OutOfMemory,
    UndoLogError,
    WriteConflict,
};

/// Bytes reserved in slot 0 for table metadata
const META_RESERVED_SIZE: usize = 64;

/// Table metadata stored in slot 0 of the meta page.
/// Extent ownership is tracked via IAM page chain.
const TableMeta = extern struct {
    iam_page_id: PageId,
    _reserved: u32,
    tuple_count: u64,

    pub const SIZE: usize = @sizeOf(TableMeta); // 16
};

/// Heap-organized table with MVCC support.
/// Uses AllocManager for extent allocation and IAM pages for extent tracking.
pub const Table = struct {
    allocator: std.mem.Allocator,
    buffer_pool: *BufferPool,
    schema: *const Schema,
    /// Table ID (first page of the table's first extent)
    table_id: PageId,
    /// Transaction manager for visibility checks (null = legacy mode, all visible)
    txn_manager: ?*TransactionManager,
    /// Undo log for version chain (null = legacy mode, no undo)
    undo_log: ?*UndoLog,
    /// Allocation manager for extent-based allocation
    alloc_manager: *AllocManager,
    /// Hint: last page that had a successful insert (avoids full IAM scan)
    hint_page_id: PageId = INVALID_PAGE_ID,

    const Self = @This();

    /// Maximum tuple size that can fit in a page (with overhead)
    pub const MAX_TUPLE_SIZE = PAGE_SIZE - page_mod.PageHeader.SIZE - page_mod.Slot.SIZE - META_RESERVED_SIZE - 64;

    /// Create a new table
    pub fn create(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        alloc_manager: *AllocManager,
    ) TableError!Self {
        return createFull(allocator, buffer_pool, schema, null, null, alloc_manager);
    }

    /// Create a new table with MVCC support
    pub fn createWithMvcc(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
        alloc_manager: *AllocManager,
    ) TableError!Self {
        return createFull(allocator, buffer_pool, schema, txn_manager, undo_log, alloc_manager);
    }

    /// Create a new table with full configuration (MVCC + AllocManager)
    pub fn createFull(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
        alloc_manager: *AllocManager,
    ) TableError!Self {
        // Allocate first extent via AllocManager (8 contiguous pages)
        const first_page = alloc_manager.allocateExtent() catch return TableError.BufferPoolError;
        const table_id = first_page;
        const iam_page_id = first_page + 1;
        const extent_id: usize = @intCast(first_page / EXTENT_SIZE);

        // Initialize meta page: page 0 of the extent, slot 0 = metadata
        {
            var pg = buffer_pool.fetchPage(table_id) catch return TableError.BufferPoolError;
            pg = Page.init(pg.data, table_id);

            var meta_blob: [META_RESERVED_SIZE]u8 = [_]u8{0} ** META_RESERVED_SIZE;
            const meta = TableMeta{
                .iam_page_id = iam_page_id,
                ._reserved = 0,
                .tuple_count = 0,
            };
            @memcpy(meta_blob[0..TableMeta.SIZE], std.mem.asBytes(&meta));
            _ = pg.insertTuple(&meta_blob);

            buffer_pool.unpinPage(table_id, true) catch {};
        }

        // Initialize IAM page: page 1 of the extent, tracks owned extents
        {
            var pg = buffer_pool.fetchPage(iam_page_id) catch return TableError.BufferPoolError;
            Iam.initPage(pg.data, table_id, 0, 0);
            Iam.addExtent(pg.data, extent_id);
            buffer_pool.unpinPage(iam_page_id, true) catch {};
        }

        // Initialize data pages 2-7 to prevent stale buffer pool data
        for (2..EXTENT_SIZE) |offset| {
            const data_page_id: PageId = first_page + @as(PageId, @intCast(offset));
            var pg = buffer_pool.fetchPage(data_page_id) catch return TableError.BufferPoolError;
            pg = Page.init(pg.data, data_page_id);
            buffer_pool.unpinPage(data_page_id, true) catch {};
        }

        // Set PFS entries for all pages in this extent
        {
            const file_header_mod = @import("file_header.zig");
            var pfs_pg = buffer_pool.fetchPage(file_header_mod.PFS_PAGE) catch return TableError.BufferPoolError;
            // Meta page: compute actual free space category
            {
                var meta_pg = buffer_pool.fetchPage(table_id) catch {
                    buffer_pool.unpinPage(file_header_mod.PFS_PAGE, false) catch {};
                    return TableError.BufferPoolError;
                };
                const meta_free = meta_pg.getFreeSpace();
                buffer_pool.unpinPage(table_id, false) catch {};
                Pfs.updateFreeSpace(pfs_pg.data, table_id, Pfs.freeSpaceCategory(meta_free));
            }
            // IAM page: full from data perspective
            Pfs.setEntry(pfs_pg.data, iam_page_id, .{
                .allocated = true,
                .free_space = 4,
                .page_type = 0,
                .iam_page = true,
                .mixed_extent = false,
            });
            // Data pages 2-7: empty (category 0)
            for (2..EXTENT_SIZE) |offset| {
                const data_page_id: PageId = first_page + @as(PageId, @intCast(offset));
                Pfs.updateFreeSpace(pfs_pg.data, data_page_id, 0);
            }
            buffer_pool.unpinPage(file_header_mod.PFS_PAGE, true) catch {};
        }

        return Self{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .schema = schema,
            .table_id = table_id,
            .txn_manager = txn_manager,
            .undo_log = undo_log,
            .alloc_manager = alloc_manager,
        };
    }

    /// Open an existing table by its table ID
    pub fn open(allocator: std.mem.Allocator, buffer_pool: *BufferPool, schema: *const Schema, table_id: PageId, alloc_manager: *AllocManager) Self {
        return openFull(allocator, buffer_pool, schema, table_id, null, null, alloc_manager);
    }

    /// Open an existing table with MVCC support
    pub fn openWithMvcc(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        table_id: PageId,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
        alloc_manager: *AllocManager,
    ) Self {
        return openFull(allocator, buffer_pool, schema, table_id, txn_manager, undo_log, alloc_manager);
    }

    /// Open an existing table with full configuration
    pub fn openFull(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        schema: *const Schema,
        table_id: PageId,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
        alloc_manager: *AllocManager,
    ) Self {
        return .{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .schema = schema,
            .table_id = table_id,
            .txn_manager = txn_manager,
            .undo_log = undo_log,
            .alloc_manager = alloc_manager,
        };
    }

    /// Insert a tuple into the table.
    /// If txn is provided, the tuple gets a TupleHeader with xmin=txn.txn_id.
    /// If txn is null, falls back to legacy mode (no header).
    pub fn insertTuple(self: *Self, txn: ?*Transaction, values: []const Value) TableError!TupleId {
        var buf: [PAGE_SIZE]u8 = undefined;
        var size: usize = undefined;

        if (txn) |t| {
            // MVCC mode: serialize with TupleHeader
            const header = TupleHeader.init(t.txn_id);
            size = Tuple.serializeWithHeader(header, self.schema, values, &buf) catch {
                return TableError.SerializationError;
            };
        } else {
            // Legacy mode: no header
            size = Tuple.serialize(self.schema, values, &buf) catch {
                return TableError.SerializationError;
            };
        }

        if (size > MAX_TUPLE_SIZE) {
            return TableError.TupleTooBig;
        }

        const tuple_data = buf[0..size];

        // Fast path: try the hint page first (last page with a successful insert)
        if (self.hint_page_id != INVALID_PAGE_ID) {
            if (self.tryInsertIntoPage(self.hint_page_id, tuple_data, txn)) |tid| {
                return tid;
            } else |_| {
                self.hint_page_id = INVALID_PAGE_ID;
            }
        }

        // Read metadata to get IAM page ID
        const meta = try self.readMeta();

        // Walk IAM chain to find a page with free space (PFS-aware)
        var iam_iter = IamChainIterator.init(self.buffer_pool, meta.iam_page_id);
        while (iam_iter.next()) |extent_id| {
            const ext_start: PageId = @intCast(extent_id * EXTENT_SIZE);

            // Batch-read PFS entries for this extent
            const pfs_entries = self.alloc_manager.getExtentPfsEntries(ext_start) catch {
                return TableError.BufferPoolError;
            };

            for (0..EXTENT_SIZE) |offset| {
                const page_id: PageId = ext_start + @as(PageId, @intCast(offset));

                // Skip IAM page
                if (page_id == meta.iam_page_id) continue;

                // Skip pages marked full in PFS
                if (pfs_entries[offset].free_space == 4) continue;

                var pg = self.buffer_pool.fetchPage(page_id) catch {
                    return TableError.BufferPoolError;
                };

                // Defense-in-depth: init truly uninitialized pages
                const pg_header = pg.getHeader();
                if (pg_header.free_space_end == 0) {
                    pg = Page.init(pg.data, page_id);
                }

                const slot_id = pg.insertTuple(tuple_data);
                if (slot_id) |sid| {
                    // Update PFS with actual free space after insert
                    self.alloc_manager.updatePfsFreeSpace(page_id, pg.getFreeSpace()) catch {};
                    self.buffer_pool.unpinPage(page_id, true) catch {};
                    try self.incrementTupleCount();

                    const tid = TupleId{ .page_id = page_id, .slot_id = sid };
                    self.hint_page_id = page_id;

                    if (txn != null and self.undo_log != null) {
                        try self.writeInsertUndo(txn.?, tid);
                    }

                    return tid;
                }

                // Page is full — update PFS to category 4
                self.alloc_manager.updatePfsFreeSpace(page_id, 0) catch {};
                self.buffer_pool.unpinPage(page_id, false) catch {};
            }
        }

        // No space found — allocate a new extent
        const new_start = self.alloc_manager.allocateExtent() catch return TableError.BufferPoolError;
        const new_extent_id: usize = @intCast(new_start / EXTENT_SIZE);

        // Register new extent in IAM
        {
            var iam_pg = self.buffer_pool.fetchPage(meta.iam_page_id) catch return TableError.BufferPoolError;
            Iam.addExtent(iam_pg.data, new_extent_id);
            self.buffer_pool.unpinPage(meta.iam_page_id, true) catch {};
        }

        // Initialize ALL pages in the new extent and set PFS to empty
        for (0..EXTENT_SIZE) |offset| {
            const data_page_id: PageId = new_start + @as(PageId, @intCast(offset));
            var pg = self.buffer_pool.fetchPage(data_page_id) catch return TableError.BufferPoolError;
            pg = Page.init(pg.data, data_page_id);
            const free_space = pg.getFreeSpace();
            self.buffer_pool.unpinPage(data_page_id, true) catch {};
            self.alloc_manager.updatePfsFreeSpace(data_page_id, free_space) catch {};
        }

        // Insert into first page of new extent
        var new_pg = self.buffer_pool.fetchPage(new_start) catch {
            return TableError.BufferPoolError;
        };

        const slot_id = new_pg.insertTuple(tuple_data) orelse {
            self.buffer_pool.unpinPage(new_start, false) catch {};
            return TableError.TupleTooBig;
        };

        // Update PFS for the page we just inserted into
        self.alloc_manager.updatePfsFreeSpace(new_start, new_pg.getFreeSpace()) catch {};
        self.buffer_pool.unpinPage(new_start, true) catch {};
        try self.incrementTupleCount();

        const tid = TupleId{ .page_id = new_start, .slot_id = slot_id };
        self.hint_page_id = new_start;

        if (txn != null and self.undo_log != null) {
            try self.writeInsertUndo(txn.?, tid);
        }

        return tid;
    }

    /// Try inserting tuple_data into a specific page.
    /// Returns TupleId on success, error on failure (page full or I/O error).
    fn tryInsertIntoPage(self: *Self, page_id: PageId, tuple_data: []const u8, txn: ?*Transaction) TableError!TupleId {
        var pg = self.buffer_pool.fetchPage(page_id) catch {
            return TableError.BufferPoolError;
        };

        const pg_header = pg.getHeader();
        if (pg_header.free_space_end == 0) {
            pg = Page.init(pg.data, page_id);
        }

        const slot_id = pg.insertTuple(tuple_data) orelse {
            self.alloc_manager.updatePfsFreeSpace(page_id, 0) catch {};
            self.buffer_pool.unpinPage(page_id, false) catch {};
            return TableError.PageFull;
        };

        self.alloc_manager.updatePfsFreeSpace(page_id, pg.getFreeSpace()) catch {};
        self.buffer_pool.unpinPage(page_id, true) catch {};
        try self.incrementTupleCount();

        const tid = TupleId{ .page_id = page_id, .slot_id = slot_id };

        if (txn != null and self.undo_log != null) {
            try self.writeInsertUndo(txn.?, tid);
        }

        return tid;
    }

    /// Get a tuple by its TupleId, with optional visibility check.
    /// Caller must free the returned values slice.
    pub fn getTuple(self: *Self, tid: TupleId, snapshot: ?*const Snapshot) TableError!?[]Value {
        var pg = self.buffer_pool.fetchPage(tid.page_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(tid.page_id, false) catch {};

        const raw = pg.getTuple(tid.slot_id) orelse return null;

        // MVCC visibility check
        if (snapshot != null and self.txn_manager != null) {
            if (raw.len < TupleHeader.SIZE) return TableError.SerializationError;
            const stripped = Tuple.stripHeader(raw) catch return TableError.SerializationError;
            const vis = mvcc_mod.isVisible(&stripped.header, snapshot.?, self.txn_manager.?, snapshot.?.xmax - 1);
            if (vis == .invisible) return null;

            return Tuple.deserialize(self.allocator, self.schema, stripped.user_data) catch {
                return TableError.SerializationError;
            };
        }

        // Legacy mode: no header
        return Tuple.deserialize(self.allocator, self.schema, raw) catch {
            return TableError.SerializationError;
        };
    }

    /// Get a tuple with visibility check using the transaction's own ID and snapshot
    pub fn getTupleTxn(self: *Self, tid: TupleId, txn: *const Transaction) TableError!?[]Value {
        var pg = self.buffer_pool.fetchPage(tid.page_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(tid.page_id, false) catch {};

        const raw = pg.getTuple(tid.slot_id) orelse return null;

        if (self.txn_manager) |tm| {
            if (raw.len < TupleHeader.SIZE) return TableError.SerializationError;
            const stripped = Tuple.stripHeader(raw) catch return TableError.SerializationError;
            const vis = mvcc_mod.isVisible(&stripped.header, &txn.snapshot, tm, txn.txn_id);
            if (vis == .invisible) return null;

            return Tuple.deserialize(self.allocator, self.schema, stripped.user_data) catch {
                return TableError.SerializationError;
            };
        }

        return Tuple.deserialize(self.allocator, self.schema, raw) catch {
            return TableError.SerializationError;
        };
    }

    /// Delete a tuple by setting xmax (MVCC) or zeroing the slot (legacy).
    pub fn deleteTuple(self: *Self, txn: ?*Transaction, tid: TupleId) TableError!bool {
        var pg = self.buffer_pool.fetchPage(tid.page_id) catch {
            return TableError.BufferPoolError;
        };

        if (txn != null and self.txn_manager != null) {
            // MVCC mode: set xmax in-place
            const raw = pg.getTuple(tid.slot_id) orelse {
                self.buffer_pool.unpinPage(tid.page_id, false) catch {};
                return false;
            };
            if (raw.len < TupleHeader.SIZE) {
                self.buffer_pool.unpinPage(tid.page_id, false) catch {};
                return false;
            }

            // Write-write conflict detection: check if another txn already set xmax
            const existing_header = std.mem.bytesToValue(TupleHeader, raw[0..TupleHeader.SIZE]);
            if (existing_header.xmax != 0 and existing_header.xmax != txn.?.txn_id) {
                self.buffer_pool.unpinPage(tid.page_id, false) catch {};
                return TableError.WriteConflict;
            }

            // Write delete undo record before modifying
            if (self.undo_log != null) {
                try self.writeDeleteUndo(txn.?, tid, raw[0..TupleHeader.SIZE]);
            }

            // Set xmax to the deleting transaction's ID
            const xmax_bytes = std.mem.asBytes(&txn.?.txn_id);
            // xmax is at offset 4 in TupleHeader (after xmin)
            const updated = pg.updateTupleData(tid.slot_id, 4, xmax_bytes);

            self.buffer_pool.unpinPage(tid.page_id, updated) catch {};
            if (updated) try self.decrementTupleCount();
            return updated;
        } else {
            // Legacy mode: zero the slot
            const result = pg.deleteTuple(tid.slot_id);
            self.buffer_pool.unpinPage(tid.page_id, result) catch {};
            if (result) try self.decrementTupleCount();
            return result;
        }
    }

    /// Sequential scan with IAM-based extent iteration and optional MVCC visibility
    pub const ScanIterator = struct {
        table: *Self,
        iam_iter: IamChainIterator,
        iam_page_id: PageId,
        current_extent_start: PageId,
        current_page_in_extent: u32,
        current_slot: SlotId,
        has_extent: bool,
        /// Transaction for visibility checks (null = all visible)
        txn: ?*const Transaction,

        /// Returns the next visible tuple ID and deserialized values.
        /// Caller must free the returned values slice.
        pub fn next(self: *ScanIterator) TableError!?struct { tid: TupleId, values: []Value } {
            while (true) {
                if (!self.has_extent) {
                    if (self.iam_iter.next()) |extent_id| {
                        self.current_extent_start = @intCast(extent_id * EXTENT_SIZE);
                        self.current_page_in_extent = 0;
                        self.current_slot = 0;
                        self.has_extent = true;
                    } else {
                        return null;
                    }
                }

                while (self.current_page_in_extent < EXTENT_SIZE) {
                    const page_id = self.current_extent_start + self.current_page_in_extent;

                    // Skip IAM page
                    if (page_id == self.iam_page_id) {
                        self.current_page_in_extent += 1;
                        self.current_slot = 0;
                        continue;
                    }

                    var pg = self.table.buffer_pool.fetchPage(page_id) catch {
                        return TableError.BufferPoolError;
                    };

                    const pg_header = pg.getHeader();

                    // Skip uninitialized pages
                    if (pg_header.free_space_end == 0) {
                        self.table.buffer_pool.unpinPage(page_id, false) catch {};
                        self.current_page_in_extent += 1;
                        self.current_slot = 0;
                        continue;
                    }

                    // Skip slot 0 on the meta page (table's first page)
                    const start_slot: SlotId = if (page_id == self.table.table_id and self.current_slot == 0) 1 else self.current_slot;

                    var slot = start_slot;
                    while (slot < pg_header.slot_count) : (slot += 1) {
                        if (pg.getTuple(slot)) |raw| {
                            const tid = TupleId{ .page_id = page_id, .slot_id = slot };

                            // MVCC visibility check
                            if (self.txn != null and self.table.txn_manager != null) {
                                if (raw.len < TupleHeader.SIZE) {
                                    continue; // Skip malformed tuples
                                }
                                const stripped = Tuple.stripHeader(raw) catch continue;
                                const vis = mvcc_mod.isVisible(&stripped.header, &self.txn.?.snapshot, self.table.txn_manager.?, self.txn.?.txn_id);
                                if (vis == .invisible) continue;

                                self.current_slot = slot + 1;

                                const values = Tuple.deserialize(self.table.allocator, self.table.schema, stripped.user_data) catch {
                                    self.table.buffer_pool.unpinPage(page_id, false) catch {};
                                    return TableError.SerializationError;
                                };

                                self.table.buffer_pool.unpinPage(page_id, false) catch {};
                                return .{ .tid = tid, .values = values };
                            } else {
                                // Legacy mode: no header
                                self.current_slot = slot + 1;

                                const values = Tuple.deserialize(self.table.allocator, self.table.schema, raw) catch {
                                    self.table.buffer_pool.unpinPage(page_id, false) catch {};
                                    return TableError.SerializationError;
                                };

                                self.table.buffer_pool.unpinPage(page_id, false) catch {};
                                return .{ .tid = tid, .values = values };
                            }
                        }
                    }

                    self.table.buffer_pool.unpinPage(page_id, false) catch {};

                    // Move to next page within this extent
                    self.current_page_in_extent += 1;
                    self.current_slot = 0;
                }

                // Move to next extent
                self.has_extent = false;
            }
        }

        /// Free values returned by next()
        pub fn freeValues(self: *ScanIterator, values: []Value) void {
            self.table.allocator.free(values);
        }
    };

    /// Start a sequential scan of the table
    pub fn scan(self: *Self) TableError!ScanIterator {
        return self.scanWithTxn(null);
    }

    /// Start a sequential scan with a transaction for MVCC visibility
    pub fn scanWithTxn(self: *Self, txn: ?*const Transaction) TableError!ScanIterator {
        const meta = try self.readMeta();
        return ScanIterator{
            .table = self,
            .iam_iter = IamChainIterator.init(self.buffer_pool, meta.iam_page_id),
            .iam_page_id = meta.iam_page_id,
            .current_extent_start = 0,
            .current_page_in_extent = 0,
            .current_slot = 0,
            .has_extent = false,
            .txn = txn,
        };
    }

    // ── Undo record helpers ──────────────────────────────────────────

    fn writeInsertUndo(self: *Self, txn: *Transaction, tid: TupleId) TableError!void {
        const log = self.undo_log orelse return;
        const undo_header = UndoRecordHeader{
            .record_type = .insert,
            .slot_id = tid.slot_id,
            .txn_id = txn.txn_id,
            .prev_undo_ptr = NO_UNDO_PTR,
            .txn_prev_undo = txn.undo_chain_head,
            .table_page_id = tid.page_id,
            .data_len = 0,
        };

        const offset = log.appendRecord(undo_header, &.{}) catch {
            return TableError.UndoLogError;
        };
        txn.undo_chain_head = offset;
    }

    fn writeDeleteUndo(self: *Self, txn: *Transaction, tid: TupleId, old_header_bytes: []const u8) TableError!void {
        const log = self.undo_log orelse return;
        const undo_header = UndoRecordHeader{
            .record_type = .delete,
            .slot_id = tid.slot_id,
            .txn_id = txn.txn_id,
            .prev_undo_ptr = NO_UNDO_PTR,
            .txn_prev_undo = txn.undo_chain_head,
            .table_page_id = tid.page_id,
            .data_len = @intCast(old_header_bytes.len),
        };

        const offset = log.appendRecord(undo_header, old_header_bytes) catch {
            return TableError.UndoLogError;
        };
        txn.undo_chain_head = offset;
    }

    // ── Rollback support ─────────────────────────────────────────────

    /// Walk the transaction's undo chain and restore all changes.
    pub fn rollback(self: *Self, txn: *Transaction) TableError!void {
        const log = self.undo_log orelse return;
        var undo_ptr = txn.undo_chain_head;

        while (undo_ptr != NO_UNDO_PTR) {
            const rec = log.readRecord(undo_ptr) catch {
                return TableError.UndoLogError;
            };

            switch (rec.header.record_type) {
                .insert => {
                    // Undo insert = zero the slot + decrement count
                    var pg = self.buffer_pool.fetchPage(rec.header.table_page_id) catch {
                        return TableError.BufferPoolError;
                    };
                    _ = pg.deleteTuple(rec.header.slot_id);
                    self.buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                    try self.decrementTupleCount();
                },
                .delete => {
                    // Undo delete = restore xmax to 0 + increment count
                    var pg = self.buffer_pool.fetchPage(rec.header.table_page_id) catch {
                        return TableError.BufferPoolError;
                    };
                    const zero_xmax = std.mem.asBytes(&@as(TxnId, 0));
                    _ = pg.updateTupleData(rec.header.slot_id, 4, zero_xmax);
                    self.buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                    try self.incrementTupleCount();
                },
                .update => {
                    // Undo update = overwrite heap tuple with old data
                    var pg = self.buffer_pool.fetchPage(rec.header.table_page_id) catch {
                        return TableError.BufferPoolError;
                    };
                    _ = pg.updateTupleData(rec.header.slot_id, 0, rec.data);
                    self.buffer_pool.unpinPage(rec.header.table_page_id, true) catch {};
                },
            }

            undo_ptr = rec.header.txn_prev_undo;
        }
    }

    // ── Internal helpers ─────────────────────────────────────────────

    /// Read table metadata from slot 0
    fn readMeta(self: *Self) TableError!TableMeta {
        var pg = self.buffer_pool.fetchPage(self.table_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(self.table_id, false) catch {};

        const raw = pg.getTuple(0) orelse {
            return TableError.TupleNotFound;
        };

        if (raw.len < TableMeta.SIZE) return TableError.SerializationError;
        return std.mem.bytesToValue(TableMeta, raw[0..TableMeta.SIZE]);
    }

    /// Write table metadata to slot 0
    fn writeMeta(self: *Self, meta: TableMeta) TableError!void {
        var pg = self.buffer_pool.fetchPage(self.table_id) catch {
            return TableError.BufferPoolError;
        };
        defer self.buffer_pool.unpinPage(self.table_id, true) catch {};

        var meta_blob: [META_RESERVED_SIZE]u8 = [_]u8{0} ** META_RESERVED_SIZE;
        @memcpy(meta_blob[0..TableMeta.SIZE], std.mem.asBytes(&meta));
        _ = pg.updateTupleData(0, 0, &meta_blob);
    }

    /// Increment the tuple count in metadata
    fn incrementTupleCount(self: *Self) TableError!void {
        var meta = try self.readMeta();
        meta.tuple_count += 1;
        try self.writeMeta(meta);
    }

    /// Decrement the tuple count in metadata
    fn decrementTupleCount(self: *Self) TableError!void {
        var meta = try self.readMeta();
        if (meta.tuple_count > 0) meta.tuple_count -= 1;
        try self.writeMeta(meta);
    }

    /// Get the total number of tuples
    pub fn tupleCount(self: *Self) TableError!u64 {
        const meta = try self.readMeta();
        return meta.tuple_count;
    }

    /// Free all extents owned by this table (for DROP TABLE).
    /// Walks the IAM chain, collects extent IDs, then frees them via AllocManager.
    pub fn freeAllExtents(self: *Self) TableError!void {
        const meta = try self.readMeta();

        // Collect extent IDs first (can't free while iterating — IAM page lives in an extent)
        var extent_ids: [256]usize = undefined;
        var count: usize = 0;
        var iam_iter = IamChainIterator.init(self.buffer_pool, meta.iam_page_id);
        while (iam_iter.next()) |eid| {
            if (count < 256) {
                extent_ids[count] = eid;
                count += 1;
            }
        }

        // Free all collected extents
        for (extent_ids[0..count]) |eid| {
            const fp: PageId = @intCast(eid * EXTENT_SIZE);
            self.alloc_manager.freeExtent(fp) catch {};
        }
    }
};

// ============================================================
// Tests
// ============================================================
const disk_manager_mod = @import("disk_manager.zig");
const DiskManager = disk_manager_mod.DiskManager;

test "table create and insert" {
    const test_file = "test_table_basic.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    // Insert a row (legacy, no txn)
    const values = [_]Value{ .{ .integer = 1 }, .{ .bytes = "Alice" } };
    _ = try table.insertTuple(null, &values);

    try std.testing.expectEqual(@as(u64, 1), try table.tupleCount());
}

test "table insert and retrieve" {
    const test_file = "test_table_get.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
            .{ .name = "active", .col_type = .boolean, .max_length = 0, .nullable = true },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    const values = [_]Value{ .{ .integer = 42 }, .{ .bytes = "Bob" }, .{ .boolean = true } };
    const tid = try table.insertTuple(null, &values);

    // Retrieve (legacy, no snapshot)
    const result = try table.getTuple(tid, null) orelse unreachable;
    defer std.testing.allocator.free(result);

    try std.testing.expectEqual(@as(i32, 42), result[0].integer);
    try std.testing.expectEqualStrings("Bob", result[1].bytes);
    try std.testing.expectEqual(true, result[2].boolean);
}

test "table sequential scan" {
    const test_file = "test_table_scan.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "value", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    // Insert multiple rows
    var i: i32 = 0;
    while (i < 5) : (i += 1) {
        var name_buf: [32]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "row_{}", .{i}) catch unreachable;
        const vals = [_]Value{ .{ .integer = i }, .{ .bytes = name } };
        _ = try table.insertTuple(null, &vals);
    }

    try std.testing.expectEqual(@as(u64, 5), try table.tupleCount());

    // Scan all rows
    var iter = try table.scan();
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 5), count);
}

test "table delete tuple" {
    const test_file = "test_table_delete.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    const vals = [_]Value{.{ .integer = 1 }};
    const tid = try table.insertTuple(null, &vals);

    // Delete it (legacy)
    const deleted = try table.deleteTuple(null, tid);
    try std.testing.expect(deleted);

    // Should not be retrievable
    const result = try table.getTuple(tid, null);
    try std.testing.expect(result == null);
}

test "table multiple pages" {
    const test_file = "test_table_multi_page.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "data", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    // Insert rows with large data to force multiple pages
    var large_buf: [512]u8 = undefined;
    @memset(&large_buf, 'X');

    var i: i32 = 0;
    while (i < 20) : (i += 1) {
        const vals = [_]Value{ .{ .integer = i }, .{ .bytes = &large_buf } };
        _ = try table.insertTuple(null, &vals);
    }

    try std.testing.expectEqual(@as(u64, 20), try table.tupleCount());

    // Scan should return all 20 rows
    var iter = try table.scan();
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 20), count);
}

test "MVCC insert and visibility" {
    const test_file = "test_table_mvcc_insert.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // txn1 inserts a row
    const txn1 = try tm.begin();
    const vals = [_]Value{ .{ .integer = 1 }, .{ .bytes = "Alice" } };
    _ = try table.insertTuple(txn1, &vals);

    // txn1 can see its own insert
    var iter1 = try table.scanWithTxn(txn1);
    var count1: usize = 0;
    while (try iter1.next()) |row| {
        defer iter1.freeValues(row.values);
        count1 += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), count1);

    // txn2 starts — should NOT see txn1's uncommitted insert
    const txn2 = try tm.begin();
    var iter2 = try table.scanWithTxn(txn2);
    var count2: usize = 0;
    while (try iter2.next()) |row| {
        defer iter2.freeValues(row.values);
        count2 += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count2);

    // Commit txn1
    try tm.commit(txn1);

    // txn3 starts after commit — should see the row
    const txn3 = try tm.begin();
    var iter3 = try table.scanWithTxn(txn3);
    var count3: usize = 0;
    while (try iter3.next()) |row| {
        defer iter3.freeValues(row.values);
        count3 += 1;
        try std.testing.expectEqual(@as(i32, 1), row.values[0].integer);
    }
    try std.testing.expectEqual(@as(usize, 1), count3);

    try tm.commit(txn2);
    try tm.commit(txn3);
}

test "MVCC delete visibility" {
    const test_file = "test_table_mvcc_delete.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert and commit
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 42 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // Delete in txn2
    const txn2 = try tm.begin();
    _ = try table.deleteTuple(txn2, tid);

    // txn2 should NOT see the deleted row
    var iter2 = try table.scanWithTxn(txn2);
    var count2: usize = 0;
    while (try iter2.next()) |row| {
        defer iter2.freeValues(row.values);
        count2 += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count2);

    // txn3 (concurrent) should still see it (txn2 not committed)
    const txn3 = try tm.begin();
    var iter3 = try table.scanWithTxn(txn3);
    var count3: usize = 0;
    while (try iter3.next()) |row| {
        defer iter3.freeValues(row.values);
        count3 += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), count3);

    try tm.commit(txn2);
    try tm.commit(txn3);
}

test "MVCC rollback restores insert" {
    const test_file = "test_table_mvcc_rollback.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert in a transaction
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 99 }};
    _ = try table.insertTuple(txn1, &vals);

    // Rollback
    try table.rollback(txn1);
    tm.abort(txn1);

    // New transaction should see nothing
    const txn2 = try tm.begin();
    var iter = try table.scanWithTxn(txn2);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);

    try tm.commit(txn2);
}

test "MVCC rollback restores delete" {
    const test_file = "test_table_mvcc_rollback_del.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert and commit
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 42 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // Delete in txn2 then rollback
    const txn2 = try tm.begin();
    _ = try table.deleteTuple(txn2, tid);
    try table.rollback(txn2);
    tm.abort(txn2);

    // Row should still be visible
    const txn3 = try tm.begin();
    var iter = try table.scanWithTxn(txn3);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
        try std.testing.expectEqual(@as(i32, 42), row.values[0].integer);
    }
    try std.testing.expectEqual(@as(usize, 1), count);

    try tm.commit(txn3);
}

test "MVCC write-write conflict detection" {
    const test_file = "test_table_ww_conflict.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert and commit a row
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 1 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // txn2 deletes the row (sets xmax)
    const txn2 = try tm.begin();
    _ = try table.deleteTuple(txn2, tid);

    // txn3 tries to delete the same row — should get WriteConflict
    const txn3 = try tm.begin();
    const result = table.deleteTuple(txn3, tid);
    try std.testing.expectError(TableError.WriteConflict, result);

    try tm.commit(txn2);
    tm.abort(txn3);
}

test "MVCC same-txn double delete no conflict" {
    const test_file = "test_table_same_txn_delete.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert and commit a row
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 1 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // Same txn deletes twice — should not conflict
    const txn2 = try tm.begin();
    const del1 = try table.deleteTuple(txn2, tid);
    try std.testing.expect(del1);

    const del2 = try table.deleteTuple(txn2, tid);
    try std.testing.expect(del2);

    try tm.commit(txn2);
}

test "MVCC insert-delete same txn then rollback" {
    const test_file = "test_table_ins_del_rollback.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert a row, delete it, all in one txn, then rollback
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 77 }};
    const tid = try table.insertTuple(txn1, &vals);
    _ = try table.deleteTuple(txn1, tid);

    // Rollback: undo delete (restore xmax=0), undo insert (zero slot)
    try table.rollback(txn1);
    tm.abort(txn1);

    // New txn should see nothing — the row never existed
    const txn2 = try tm.begin();
    var iter = try table.scanWithTxn(txn2);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);
    try tm.commit(txn2);
}

test "MVCC insert-delete same txn commit makes row invisible" {
    const test_file = "test_table_ins_del_commit.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert + delete in same txn, then commit
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 88 }};
    const tid = try table.insertTuple(txn1, &vals);
    _ = try table.deleteTuple(txn1, tid);
    try tm.commit(txn1);

    // Next txn: xmin committed, xmax committed (same txn) → invisible
    const txn2 = try tm.begin();
    var iter = try table.scanWithTxn(txn2);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);
    try tm.commit(txn2);
}

test "MVCC scan empty table" {
    const test_file = "test_table_scan_empty.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Scan with active txn on empty table — should return nothing, not crash
    const txn = try tm.begin();
    var iter = try table.scanWithTxn(txn);
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count);
    try tm.commit(txn);
}

test "MVCC large batch insert then rollback" {
    const test_file = "test_table_batch_rollback.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 128);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert 50 rows in one txn
    const txn1 = try tm.begin();
    var i: i32 = 0;
    while (i < 50) : (i += 1) {
        const vals = [_]Value{.{ .integer = i }};
        _ = try table.insertTuple(txn1, &vals);
    }

    // Verify they're visible within the txn
    var iter1 = try table.scanWithTxn(txn1);
    var count1: usize = 0;
    while (try iter1.next()) |row| {
        defer iter1.freeValues(row.values);
        count1 += 1;
    }
    try std.testing.expectEqual(@as(usize, 50), count1);

    // Rollback all 50
    try table.rollback(txn1);
    tm.abort(txn1);

    // New txn should see nothing
    const txn2 = try tm.begin();
    var iter2 = try table.scanWithTxn(txn2);
    var count2: usize = 0;
    while (try iter2.next()) |row| {
        defer iter2.freeValues(row.values);
        count2 += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), count2);
    try tm.commit(txn2);
}

test "MVCC write conflict after first txn commits" {
    const test_file = "test_table_ww_after_commit.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert and commit a row
    const txn1 = try tm.begin();
    const vals = [_]Value{.{ .integer = 1 }};
    const tid = try table.insertTuple(txn1, &vals);
    try tm.commit(txn1);

    // txn2 deletes and commits — xmax is now set to a committed txn
    const txn2 = try tm.begin();
    _ = try table.deleteTuple(txn2, tid);
    try tm.commit(txn2);

    // txn3 tries to delete the same row — xmax is set to committed txn2
    const txn3 = try tm.begin();
    const result = table.deleteTuple(txn3, tid);
    try std.testing.expectError(TableError.WriteConflict, result);
    tm.abort(txn3);
}

test "table tupleCount after inserts and deletes" {
    const test_file = "test_table_count.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    try std.testing.expectEqual(@as(u64, 0), try table.tupleCount());

    // Insert 5 rows
    var i: i32 = 0;
    while (i < 5) : (i += 1) {
        _ = try table.insertTuple(null, &.{.{ .integer = i }});
    }
    try std.testing.expectEqual(@as(u64, 5), try table.tupleCount());
}

test "table getTuple invalid slot returns null" {
    const test_file = "test_table_invalid_tid.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    // Insert one row to have a valid page
    const tid = try table.insertTuple(null, &.{.{ .integer = 1 }});

    // Access a slot that doesn't exist on the same page
    const bad_tid = TupleId{ .page_id = tid.page_id, .slot_id = 999 };
    const result = try table.getTuple(bad_tid, null);
    try std.testing.expect(result == null);
}

test "table scan returns all rows after multiple inserts" {
    const test_file = "test_table_scan_multi.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    var i: i32 = 0;
    while (i < 10) : (i += 1) {
        _ = try table.insertTuple(null, &.{.{ .integer = i }});
    }

    // Scan and count
    var iter = try table.scan();
    var count: usize = 0;
    while (try iter.next()) |entry| {
        defer iter.freeValues(entry.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 10), count);
}

test "multi-table interleaving no cross-table corruption" {
    const test_file = "test_table_interleave.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 128);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    const schema_a = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "name", .col_type = .varchar, .max_length = 255, .nullable = false },
        },
    };
    const schema_b = Schema{
        .columns = &.{
            .{ .name = "val", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table_a = try Table.create(std.testing.allocator, &bp, &schema_a, &am);
    var table_b = try Table.create(std.testing.allocator, &bp, &schema_b, &am);

    // Interleave inserts into both tables
    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        var name_buf: [32]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "name_{}", .{i}) catch unreachable;
        _ = try table_a.insertTuple(null, &.{ .{ .integer = i }, .{ .bytes = name } });
        _ = try table_b.insertTuple(null, &.{.{ .integer = i * 10 }});
    }

    // Scan table_a — should see exactly 100 rows, no corruption
    var iter_a = try table_a.scan();
    var count_a: usize = 0;
    while (try iter_a.next()) |row| {
        defer iter_a.freeValues(row.values);
        count_a += 1;
    }
    try std.testing.expectEqual(@as(usize, 100), count_a);

    // Scan table_b — should see exactly 100 rows, no corruption
    var iter_b = try table_b.scan();
    var count_b: usize = 0;
    while (try iter_b.next()) |row| {
        defer iter_b.freeValues(row.values);
        count_b += 1;
    }
    try std.testing.expectEqual(@as(usize, 100), count_b);
}

test "multi-extent scan with large rows" {
    const test_file = "test_table_multi_extent.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 128);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "data", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    // Insert large rows to force multiple extent allocations
    var large_buf: [512]u8 = undefined;
    @memset(&large_buf, 'Y');

    var i: i32 = 0;
    while (i < 80) : (i += 1) {
        _ = try table.insertTuple(null, &.{ .{ .integer = i }, .{ .bytes = &large_buf } });
    }

    // Scan all rows — should get exactly 80
    var iter = try table.scan();
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 80), count);
    try std.testing.expectEqual(@as(u64, 80), try table.tupleCount());
}

test "three tables interleaved with large rows" {
    const test_file = "test_table_three_interleave.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 128);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "payload", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var t1 = try Table.create(std.testing.allocator, &bp, &schema, &am);
    var t2 = try Table.create(std.testing.allocator, &bp, &schema, &am);
    var t3 = try Table.create(std.testing.allocator, &bp, &schema, &am);

    var large_buf: [400]u8 = undefined;
    @memset(&large_buf, 'Z');

    // Interleave inserts across all three tables
    var i: i32 = 0;
    while (i < 30) : (i += 1) {
        _ = try t1.insertTuple(null, &.{ .{ .integer = i }, .{ .bytes = &large_buf } });
        _ = try t2.insertTuple(null, &.{ .{ .integer = i + 1000 }, .{ .bytes = &large_buf } });
        _ = try t3.insertTuple(null, &.{ .{ .integer = i + 2000 }, .{ .bytes = &large_buf } });
    }

    // Verify each table has exactly 30 rows with correct data
    inline for (.{ &t1, &t2, &t3 }, .{ @as(i32, 0), @as(i32, 1000), @as(i32, 2000) }) |tbl, base| {
        var iter = try tbl.scan();
        var count: usize = 0;
        while (try iter.next()) |row| {
            defer iter.freeValues(row.values);
            // Verify the id is in the expected range
            try std.testing.expect(row.values[0].integer >= base);
            try std.testing.expect(row.values[0].integer < base + 30);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 30), count);
    }
}

test "tuple_count decrements on delete" {
    const test_file = "test_table_count_dec.db";
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

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    // Insert 3 rows
    const tid1 = try table.insertTuple(null, &.{.{ .integer = 1 }});
    _ = try table.insertTuple(null, &.{.{ .integer = 2 }});
    _ = try table.insertTuple(null, &.{.{ .integer = 3 }});
    try std.testing.expectEqual(@as(u64, 3), try table.tupleCount());

    // Delete one
    _ = try table.deleteTuple(null, tid1);
    try std.testing.expectEqual(@as(u64, 2), try table.tupleCount());
}

test "rollback fixes tuple_count" {
    const test_file = "test_table_rollback_count.db";
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

    var tm = TransactionManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo = UndoLog.init(std.testing.allocator);
    defer undo.deinit();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.createWithMvcc(std.testing.allocator, &bp, &schema, &tm, &undo, &am);

    // Insert 3 rows and commit
    const txn1 = try tm.begin();
    _ = try table.insertTuple(txn1, &.{.{ .integer = 1 }});
    _ = try table.insertTuple(txn1, &.{.{ .integer = 2 }});
    _ = try table.insertTuple(txn1, &.{.{ .integer = 3 }});
    try tm.commit(txn1);
    try std.testing.expectEqual(@as(u64, 3), try table.tupleCount());

    // Insert 2 more then rollback
    const txn2 = try tm.begin();
    _ = try table.insertTuple(txn2, &.{.{ .integer = 4 }});
    _ = try table.insertTuple(txn2, &.{.{ .integer = 5 }});
    try std.testing.expectEqual(@as(u64, 5), try table.tupleCount());

    try table.rollback(txn2);
    tm.abort(txn2);
    try std.testing.expectEqual(@as(u64, 3), try table.tupleCount());
}

test "freeAllExtents releases to GAM" {
    const Gam = alloc_map_mod.Gam;
    const file_header = @import("file_header.zig");

    const test_file = "test_table_free_extents.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 128);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "data", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);
    const table_extent_id: usize = @intCast(table.table_id / EXTENT_SIZE);

    // Insert enough rows to force a second extent
    var large_buf: [512]u8 = undefined;
    @memset(&large_buf, 'X');
    var i: i32 = 0;
    while (i < 80) : (i += 1) {
        _ = try table.insertTuple(null, &.{ .{ .integer = i }, .{ .bytes = &large_buf } });
    }

    // Verify the table's first extent is allocated in GAM
    {
        var gam_pg = try bp.fetchPage(file_header.GAM_PAGE);
        defer bp.unpinPage(file_header.GAM_PAGE, false) catch {};
        try std.testing.expect(!Gam.isExtentFree(gam_pg.data, table_extent_id));
    }

    // Free all extents
    try table.freeAllExtents();

    // Verify the table's first extent is now free in GAM
    {
        var gam_pg = try bp.fetchPage(file_header.GAM_PAGE);
        defer bp.unpinPage(file_header.GAM_PAGE, false) catch {};
        try std.testing.expect(Gam.isExtentFree(gam_pg.data, table_extent_id));
    }
}

test "create/drop/create extent reuse no crash" {
    const test_file = "test_table_extent_reuse.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 128);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "data", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    // Create table A and insert rows
    var table_a = try Table.create(std.testing.allocator, &bp, &schema, &am);
    var i: i32 = 0;
    while (i < 10) : (i += 1) {
        var name_buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "row_a_{}", .{i}) catch unreachable;
        _ = try table_a.insertTuple(null, &.{ .{ .integer = i }, .{ .bytes = name } });
    }
    try std.testing.expectEqual(@as(u64, 10), try table_a.tupleCount());

    // Drop table A (free all its extents)
    try table_a.freeAllExtents();

    // Create table B — should reuse the freed extent
    var table_b = try Table.create(std.testing.allocator, &bp, &schema, &am);

    // Insert into B — must not crash from stale data
    i = 0;
    while (i < 10) : (i += 1) {
        var name_buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "row_b_{}", .{i}) catch unreachable;
        _ = try table_b.insertTuple(null, &.{ .{ .integer = i + 100 }, .{ .bytes = name } });
    }
    try std.testing.expectEqual(@as(u64, 10), try table_b.tupleCount());

    // Scan B and verify correct data
    var iter = try table_b.scan();
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        try std.testing.expect(row.values[0].integer >= 100);
        try std.testing.expect(row.values[0].integer < 110);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 10), count);
}

test "PFS marks full pages as category 4" {
    const file_header_mod = @import("file_header.zig");

    const test_file = "test_table_pfs_full.db";
    var dm = DiskManager.init(std.testing.allocator, test_file);
    defer {
        dm.deleteFile();
        dm.deinit();
    }
    try dm.open();
    var bp = try BufferPool.init(std.testing.allocator, &dm, 128);
    defer bp.deinit();
    var am = AllocManager.init(&bp, &dm);
    try am.initializeFile();

    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .col_type = .integer, .max_length = 0, .nullable = false },
            .{ .name = "data", .col_type = .text, .max_length = 0, .nullable = false },
        },
    };

    var table = try Table.create(std.testing.allocator, &bp, &schema, &am);

    // Insert large rows to fill pages — 512-byte data per row
    var large_buf: [512]u8 = undefined;
    @memset(&large_buf, 'X');

    // Insert enough to fill the first extent (6 data pages) and overflow into second
    var i: i32 = 0;
    while (i < 80) : (i += 1) {
        _ = try table.insertTuple(null, &.{ .{ .integer = i }, .{ .bytes = &large_buf } });
    }

    // Check PFS: data pages in first extent that are full should have category 4
    {
        var pfs_pg = try bp.fetchPage(file_header_mod.PFS_PAGE);
        defer bp.unpinPage(file_header_mod.PFS_PAGE, false) catch {};

        // Meta page (table.table_id) should not be category 4 (has few tuples)
        const meta_entry = Pfs.getEntry(pfs_pg.data, table.table_id);
        try std.testing.expect(meta_entry.allocated);

        // At least some data pages should be full (category 4)
        var full_count: usize = 0;
        const ext_start = table.table_id;
        for (2..EXTENT_SIZE) |offset| {
            const entry = Pfs.getEntry(pfs_pg.data, ext_start + offset);
            if (entry.free_space == 4) full_count += 1;
        }
        // With 512-byte rows and 8KB pages, each page fits ~14 rows
        // 80 rows across 6 data pages should fill most of them
        try std.testing.expect(full_count >= 3);
    }

    // Scan to verify all 80 rows are accessible
    var iter = try table.scan();
    var count: usize = 0;
    while (try iter.next()) |row| {
        defer iter.freeValues(row.values);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 80), count);
}
