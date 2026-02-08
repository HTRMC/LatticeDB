// Engine module — re-exports all non-network layers for use by benchmarks
// and other tools that don't need msquic/openssl.

pub const storage = struct {
    pub const page = @import("storage/page.zig");
    pub const disk_manager = @import("storage/disk_manager.zig");
    pub const buffer_pool = @import("storage/buffer_pool.zig");
    pub const wal = @import("storage/wal.zig");
    pub const tuple = @import("storage/tuple.zig");
    pub const table = @import("storage/table.zig");
    pub const catalog = @import("storage/catalog.zig");
    pub const mvcc = @import("storage/mvcc.zig");
    pub const undo_log = @import("storage/undo_log.zig");
    pub const data_dir = @import("storage/data_dir.zig");
    pub const file_header = @import("storage/file_header.zig");
    pub const alloc_map = @import("storage/alloc_map.zig");
};

pub const index = struct {
    pub const btree_page = @import("index/btree_page.zig");
    pub const btree = @import("index/btree.zig");
};

pub const parser = struct {
    pub const lexer = @import("parser/lexer.zig");
    pub const ast = @import("parser/ast.zig");
    pub const parser_mod = @import("parser/parser.zig");
};

pub const executor = @import("executor/executor.zig");

pub const planner = struct {
    pub const plan = @import("planner/plan.zig");
    pub const planner_mod = @import("planner/planner.zig");
};
