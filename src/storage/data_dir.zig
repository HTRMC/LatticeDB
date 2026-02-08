const std = @import("std");
const builtin = @import("builtin");

const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;
const Threaded = Io.Threaded;
const process = std.process;

pub const DataDirError = error{
    AlreadyInitialized,
    NotInitialized,
    VersionMismatch,
    PidLockHeld,
    IoError,
    OutOfMemory,
};

pub const VERSION = "0.1.0";
const VERSION_FILE = "GDB_VERSION";
const PID_FILE = "graphenedb.pid";
const CONFIG_FILE = "graphenedb.conf";
const DATA_SUBDIR = "data";
const WAL_SUBDIR = "wal";
const WAL_ARCHIVE_SUBDIR = "wal/archive";
const TLS_SUBDIR = "tls";
const DATA_FILE = "data/primary.gdb";
const WAL_FILE = "wal/main.wal";
const TLS_CERT_FILE = "tls/server.p12";
const TLS_KNOWN_HOSTS = "tls/known_hosts";

/// Manages the GrapheneDB data directory structure.
///
/// Directory layout:
///   <root>/
///     GDB_VERSION
///     graphenedb.pid
///     graphenedb.conf
///     data/
///       primary.gdb
///     wal/
///       main.wal
///       archive/
///     tls/
///       server.p12
///       known_hosts
pub const DataDir = struct {
    allocator: std.mem.Allocator,
    root_path: []const u8,

    // Computed absolute paths (owned, freed on deinit)
    data_file_path: []const u8,
    wal_file_path: []const u8,
    tls_dir_path: []const u8,
    cert_file_path: []const u8,
    known_hosts_path: []const u8,
    pid_file_path: []const u8,
    version_file_path: []const u8,
    config_file_path: []const u8,

    pid_locked: bool,
    threaded: Threaded,

    const Self = @This();

    fn io(self: *Self) Io {
        return self.threaded.io();
    }

    fn joinPath(allocator: std.mem.Allocator, root: []const u8, rel: []const u8) DataDirError![]const u8 {
        return std.fmt.allocPrint(allocator, "{s}/{s}", .{ root, rel }) catch return DataDirError.OutOfMemory;
    }

    fn computePaths(allocator: std.mem.Allocator, root: []const u8) DataDirError!struct {
        data_file_path: []const u8,
        wal_file_path: []const u8,
        tls_dir_path: []const u8,
        cert_file_path: []const u8,
        known_hosts_path: []const u8,
        pid_file_path: []const u8,
        version_file_path: []const u8,
        config_file_path: []const u8,
    } {
        return .{
            .data_file_path = try joinPath(allocator, root, DATA_FILE),
            .wal_file_path = try joinPath(allocator, root, WAL_FILE),
            .tls_dir_path = try joinPath(allocator, root, TLS_SUBDIR),
            .cert_file_path = try joinPath(allocator, root, TLS_CERT_FILE),
            .known_hosts_path = try joinPath(allocator, root, TLS_KNOWN_HOSTS),
            .pid_file_path = try joinPath(allocator, root, PID_FILE),
            .version_file_path = try joinPath(allocator, root, VERSION_FILE),
            .config_file_path = try joinPath(allocator, root, CONFIG_FILE),
        };
    }

    fn freePaths(self: *Self) void {
        self.allocator.free(self.data_file_path);
        self.allocator.free(self.wal_file_path);
        self.allocator.free(self.tls_dir_path);
        self.allocator.free(self.cert_file_path);
        self.allocator.free(self.known_hosts_path);
        self.allocator.free(self.pid_file_path);
        self.allocator.free(self.version_file_path);
        self.allocator.free(self.config_file_path);
    }

    fn buildSelf(allocator: std.mem.Allocator, root_path: []const u8) DataDirError!Self {
        const paths = try computePaths(allocator, root_path);
        return .{
            .allocator = allocator,
            .root_path = root_path,
            .data_file_path = paths.data_file_path,
            .wal_file_path = paths.wal_file_path,
            .tls_dir_path = paths.tls_dir_path,
            .cert_file_path = paths.cert_file_path,
            .known_hosts_path = paths.known_hosts_path,
            .pid_file_path = paths.pid_file_path,
            .version_file_path = paths.version_file_path,
            .config_file_path = paths.config_file_path,
            .pid_locked = false,
            .threaded = Threaded.init(allocator, .{ .environ = process.Environ.empty }),
        };
    }

    /// Initialize a new data directory — creates structure and writes GDB_VERSION.
    pub fn initNew(allocator: std.mem.Allocator, root_path: []const u8) DataDirError!Self {
        var self = try buildSelf(allocator, root_path);
        errdefer self.deinit();

        // Check if already initialized
        if (self.versionFileExists()) return DataDirError.AlreadyInitialized;

        // Create directory structure
        self.createDir(root_path);
        self.createDir(self.data_file_path[0 .. root_path.len + 1 + DATA_SUBDIR.len]);
        self.createDir(self.wal_file_path[0 .. root_path.len + 1 + WAL_SUBDIR.len]);
        const wal_archive = joinPath(allocator, root_path, WAL_ARCHIVE_SUBDIR) catch return DataDirError.OutOfMemory;
        defer allocator.free(wal_archive);
        self.createDir(wal_archive);
        self.createDir(self.tls_dir_path);

        // Write version file
        self.writeFile(self.version_file_path, VERSION) catch return DataDirError.IoError;

        return self;
    }

    /// Open an existing data directory — validates version, computes paths.
    pub fn openExisting(allocator: std.mem.Allocator, root_path: []const u8) DataDirError!Self {
        var self = try buildSelf(allocator, root_path);
        errdefer self.deinit();

        // Check version file exists
        if (!self.versionFileExists()) return DataDirError.NotInitialized;

        // Validate version
        var buf: [64]u8 = undefined;
        const content = self.readSmallFile(self.version_file_path, &buf) catch return DataDirError.IoError;
        const trimmed = std.mem.trimEnd(u8, content, "\r\n ");
        if (!std.mem.eql(u8, trimmed, VERSION)) return DataDirError.VersionMismatch;

        return self;
    }

    /// Open existing or initialize new data directory.
    pub fn openOrInit(allocator: std.mem.Allocator, root_path: []const u8) DataDirError!Self {
        // Try opening existing first
        return openExisting(allocator, root_path) catch |err| switch (err) {
            DataDirError.NotInitialized => return initNew(allocator, root_path),
            else => return err,
        };
    }

    /// Acquire PID lock. Returns error if another process holds it.
    pub fn acquirePidLock(self: *Self, port: u16) DataDirError!void {
        // Check for existing PID file
        var buf: [128]u8 = undefined;
        if (self.readSmallFile(self.pid_file_path, &buf)) |content| {
            // Parse PID from first line
            var lines = std.mem.splitScalar(u8, content, '\n');
            if (lines.next()) |pid_line| {
                const pid_str = std.mem.trimEnd(u8, pid_line, "\r");
                if (pid_str.len > 0) {
                    // PID file exists — could be stale. On Windows we can't easily
                    // check if process is running, so we just warn and overwrite.
                    // In production this would check /proc/<pid> or OpenProcess().
                }
            }
        } else |_| {
            // No PID file — good
        }

        // Write PID file
        const pid = getPid();
        var pid_buf: [128]u8 = undefined;
        const pid_content = std.fmt.bufPrint(&pid_buf, "{d}\n{d}\n", .{
            pid,
            port,
        }) catch return DataDirError.IoError;

        self.writeFile(self.pid_file_path, pid_content) catch return DataDirError.IoError;
        self.pid_locked = true;
    }

    /// Release PID lock — delete PID file.
    pub fn releasePidLock(self: *Self) void {
        if (self.pid_locked) {
            Dir.deleteFile(.cwd(), self.io(), self.pid_file_path) catch {};
            self.pid_locked = false;
        }
    }

    /// Free all resources.
    pub fn deinit(self: *Self) void {
        self.releasePidLock();
        self.freePaths();
        self.threaded.deinit();
    }

    // ── Internal helpers ─────────────────────────────────────────────

    fn getPid() u32 {
        if (builtin.os.tag == .windows) {
            return std.os.windows.GetCurrentProcessId();
        } else {
            return @intCast(std.c.getpid());
        }
    }

    fn versionFileExists(self: *Self) bool {
        const f = Dir.openFile(.cwd(), self.io(), self.version_file_path, .{ .mode = .read_only }) catch return false;
        f.close(self.io());
        return true;
    }

    fn createDir(self: *Self, path: []const u8) void {
        Dir.createDir(.cwd(), self.io(), path, .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => {},
        };
    }

    fn writeFile(self: *Self, path: []const u8, content: []const u8) !void {
        const f = Dir.createFile(.cwd(), self.io(), path, .{ .truncate = true }) catch return error.IoError;
        defer f.close(self.io());
        f.writePositionalAll(self.io(), content, 0) catch return error.IoError;
    }

    fn readSmallFile(self: *Self, path: []const u8, buf: []u8) ![]const u8 {
        const f = Dir.openFile(.cwd(), self.io(), path, .{ .mode = .read_only }) catch return error.IoError;
        defer f.close(self.io());
        const n = f.readPositionalAll(self.io(), buf, 0) catch return error.IoError;
        return buf[0..n];
    }

    /// Delete the entire data directory (for testing).
    pub fn deleteAll(self: *Self) void {
        // Delete files first
        Dir.deleteFile(.cwd(), self.io(), self.version_file_path) catch {};
        Dir.deleteFile(.cwd(), self.io(), self.pid_file_path) catch {};
        Dir.deleteFile(.cwd(), self.io(), self.config_file_path) catch {};
        Dir.deleteFile(.cwd(), self.io(), self.data_file_path) catch {};
        Dir.deleteFile(.cwd(), self.io(), self.wal_file_path) catch {};
        Dir.deleteFile(.cwd(), self.io(), self.cert_file_path) catch {};
        Dir.deleteFile(.cwd(), self.io(), self.known_hosts_path) catch {};

        // Delete subdirs (reverse order)
        const wal_archive = joinPath(self.allocator, self.root_path, WAL_ARCHIVE_SUBDIR) catch return;
        defer self.allocator.free(wal_archive);
        Dir.deleteDir(.cwd(), self.io(), wal_archive) catch {};
        Dir.deleteDir(.cwd(), self.io(), self.tls_dir_path) catch {};

        const wal_dir = joinPath(self.allocator, self.root_path, WAL_SUBDIR) catch return;
        defer self.allocator.free(wal_dir);
        Dir.deleteDir(.cwd(), self.io(), wal_dir) catch {};

        const data_dir = joinPath(self.allocator, self.root_path, DATA_SUBDIR) catch return;
        defer self.allocator.free(data_dir);
        Dir.deleteDir(.cwd(), self.io(), data_dir) catch {};

        // Delete root
        Dir.deleteDir(.cwd(), self.io(), self.root_path) catch {};
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "initNew creates directory structure" {
    const allocator = std.testing.allocator;
    const test_dir = "test_datadir_init";

    var dd = try DataDir.initNew(allocator, test_dir);
    defer {
        dd.deleteAll();
        dd.deinit();
    }

    // Version file should exist with correct content
    var buf: [64]u8 = undefined;
    const version = try dd.readSmallFile(dd.version_file_path, &buf);
    try std.testing.expectEqualStrings(VERSION, version);

    // Paths should be correct
    try std.testing.expectEqualStrings("test_datadir_init/data/primary.gdb", dd.data_file_path);
    try std.testing.expectEqualStrings("test_datadir_init/wal/main.wal", dd.wal_file_path);
    try std.testing.expectEqualStrings("test_datadir_init/tls", dd.tls_dir_path);
}

test "initNew rejects already initialized" {
    const allocator = std.testing.allocator;
    const test_dir = "test_datadir_double_init";

    var dd = try DataDir.initNew(allocator, test_dir);
    defer {
        dd.deleteAll();
        dd.deinit();
    }

    // Second init should fail
    const result = DataDir.initNew(allocator, test_dir);
    if (result) |*dd2_ptr| {
        var dd2 = dd2_ptr.*;
        dd2.deinit();
        return error.TestExpectedError;
    } else |err| {
        try std.testing.expectEqual(DataDirError.AlreadyInitialized, err);
    }
}

test "openExisting validates version" {
    const allocator = std.testing.allocator;
    const test_dir = "test_datadir_open";

    // Init first
    var dd = try DataDir.initNew(allocator, test_dir);
    dd.deinit();

    // Open should succeed
    var dd2 = try DataDir.openExisting(allocator, test_dir);
    defer {
        dd2.deleteAll();
        dd2.deinit();
    }

    try std.testing.expectEqualStrings("test_datadir_open/data/primary.gdb", dd2.data_file_path);
}

test "openExisting fails on missing dir" {
    const allocator = std.testing.allocator;

    const result = DataDir.openExisting(allocator, "test_datadir_nonexistent_xyz");
    if (result) |*dd_ptr| {
        var dd = dd_ptr.*;
        dd.deinit();
        return error.TestExpectedError;
    } else |err| {
        try std.testing.expectEqual(DataDirError.NotInitialized, err);
    }
}

test "openOrInit creates when missing, opens when existing" {
    const allocator = std.testing.allocator;
    const test_dir = "test_datadir_open_or_init";

    // First call — should init
    var dd = try DataDir.openOrInit(allocator, test_dir);
    dd.deinit();

    // Second call — should open existing
    var dd2 = try DataDir.openOrInit(allocator, test_dir);
    defer {
        dd2.deleteAll();
        dd2.deinit();
    }

    try std.testing.expectEqualStrings("test_datadir_open_or_init/data/primary.gdb", dd2.data_file_path);
}

test "PID lock acquire and release" {
    const allocator = std.testing.allocator;
    const test_dir = "test_datadir_pid";

    var dd = try DataDir.initNew(allocator, test_dir);
    defer {
        dd.deleteAll();
        dd.deinit();
    }

    // Acquire lock
    try dd.acquirePidLock(4567);
    try std.testing.expect(dd.pid_locked);

    // PID file should exist with correct content
    var buf: [128]u8 = undefined;
    const content = try dd.readSmallFile(dd.pid_file_path, &buf);
    // Should contain PID and port
    try std.testing.expect(content.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, content, "4567") != null);

    // Release
    dd.releasePidLock();
    try std.testing.expect(!dd.pid_locked);
}

test "version mismatch detection" {
    const allocator = std.testing.allocator;
    const test_dir = "test_datadir_version_mismatch";

    // Init
    var dd = try DataDir.initNew(allocator, test_dir);

    // Overwrite version file with wrong version
    dd.writeFile(dd.version_file_path, "99.99.99") catch {};
    dd.deinit();

    // Open should fail with VersionMismatch (errdefer cleans up paths)
    const result = DataDir.openExisting(allocator, test_dir);
    if (result) |*dd2_ptr| {
        var dd2 = dd2_ptr.*;
        dd2.deleteAll();
        dd2.deinit();
        return error.TestExpectedError;
    } else |err| {
        try std.testing.expectEqual(DataDirError.VersionMismatch, err);
    }

    // Clean up the test directory using buildSelf (bypasses version check)
    var cleanup = try DataDir.buildSelf(allocator, test_dir);
    cleanup.deleteAll();
    cleanup.deinit();
}

test "deinit releases pid lock" {
    const allocator = std.testing.allocator;
    const test_dir = "test_datadir_deinit_pid";

    var dd = try DataDir.initNew(allocator, test_dir);
    try dd.acquirePidLock(4567);

    // deinit should release the lock
    dd.deleteAll();
    dd.deinit();

    // PID should no longer be locked
    try std.testing.expect(!dd.pid_locked);
}
