const std = @import("std");
const msquic = @import("msquic.zig");
const protocol = @import("protocol.zig");
const executor_mod = @import("../executor/executor.zig");
const catalog_mod = @import("../storage/catalog.zig");
const disk_manager_mod = @import("../storage/disk_manager.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");

const Executor = executor_mod.Executor;
const Catalog = catalog_mod.Catalog;
const DiskManager = disk_manager_mod.DiskManager;
const BufferPool = buffer_pool_mod.BufferPool;

const ALPN = "graphenedb";

pub const Server = struct {
    allocator: std.mem.Allocator,
    api: *const msquic.QUIC_API_TABLE,
    registration: msquic.HQUIC,
    configuration: msquic.HQUIC,
    listener: msquic.HQUIC,

    // Database engine (shared across connections)
    executor: *Executor,
    exec_mutex: std.Thread.Mutex,

    pub fn init(
        allocator: std.mem.Allocator,
        executor: *Executor,
        cert_file: ?[*:0]const u8,
        key_file: ?[*:0]const u8,
    ) !Server {
        // Open msquic API
        var api: *const msquic.QUIC_API_TABLE = undefined;
        const open_status = msquic.MsQuicOpenVersion(msquic.QUIC_API_VERSION_2, &api);
        if (!msquic.statusSucceeded(open_status)) return error.MsQuicOpenFailed;

        // Create registration
        const reg_config = msquic.QUIC_REGISTRATION_CONFIG{
            .app_name = "GrapheneDB",
            .execution_profile = .low_latency,
        };
        var registration: msquic.HQUIC = null;
        const reg_status = api.registration_open(&reg_config, &registration);
        if (!msquic.statusSucceeded(reg_status)) {
            msquic.MsQuicClose(api);
            return error.RegistrationFailed;
        }

        // Create configuration with settings
        var alpn_buffer = msquic.QUIC_BUFFER.fromSlice(ALPN);
        var settings = std.mem.zeroes(msquic.QUIC_SETTINGS);
        settings.idle_timeout_ms = 30000;
        settings.is_set.idle_timeout_ms = true;
        settings.peer_bidi_stream_count = 128;
        settings.is_set.peer_bidi_stream_count = true;
        settings.server_resumption_level = msquic.QUIC_SERVER_RESUME_AND_ZERORTT;
        settings.is_set.server_resumption_level = true;

        var configuration: msquic.HQUIC = null;
        const cfg_status = api.configuration_open(
            registration,
            @ptrCast(&alpn_buffer),
            1,
            &settings,
            @sizeOf(msquic.QUIC_SETTINGS),
            null,
            &configuration,
        );
        if (!msquic.statusSucceeded(cfg_status)) {
            api.registration_close(registration);
            msquic.MsQuicClose(api);
            return error.ConfigurationFailed;
        }

        // Load TLS credential
        var cred_config = std.mem.zeroes(msquic.QUIC_CREDENTIAL_CONFIG);

        var cert_file_config: msquic.QUIC_CERTIFICATE_FILE = undefined;
        if (cert_file != null and key_file != null) {
            cert_file_config = .{
                .private_key_file = key_file.?,
                .certificate_file = cert_file.?,
            };
            cred_config.cred_type = .certificate_file;
            cred_config.certificate = @ptrCast(&cert_file_config);
        } else {
            // No cert provided — use self-signed (development only)
            cred_config.cred_type = .none;
            cred_config.flags.no_certificate_validation = true;
        }

        const cred_status = api.configuration_load_credential(configuration, &cred_config);
        if (!msquic.statusSucceeded(cred_status)) {
            api.configuration_close(configuration);
            api.registration_close(registration);
            msquic.MsQuicClose(api);
            return error.CredentialLoadFailed;
        }

        return .{
            .allocator = allocator,
            .api = api,
            .registration = registration,
            .configuration = configuration,
            .listener = null,
            .executor = executor,
            .exec_mutex = .{},
        };
    }

    pub fn listen(self: *Server, port: u16) !void {
        var alpn_buffer = msquic.QUIC_BUFFER.fromSlice(ALPN);

        const status = self.api.listener_open(
            self.registration,
            listenerCallback,
            @ptrCast(self),
            &self.listener,
        );
        if (!msquic.statusSucceeded(status)) return error.ListenerOpenFailed;

        var addr = msquic.QUIC_ADDR{};
        msquic.quicAddrSetFamily(&addr, msquic.QUIC_ADDRESS_FAMILY_UNSPEC);
        msquic.quicAddrSetPort(&addr, port);

        const start_status = self.api.listener_start(
            self.listener,
            @ptrCast(&alpn_buffer),
            1,
            &addr,
        );
        if (!msquic.statusSucceeded(start_status)) return error.ListenerStartFailed;
    }

    pub fn stop(self: *Server) void {
        if (self.listener != null) {
            self.api.listener_close(self.listener);
            self.listener = null;
        }
    }

    pub fn deinit(self: *Server) void {
        self.stop();
        if (self.configuration != null) self.api.configuration_close(self.configuration);
        if (self.registration != null) self.api.registration_close(self.registration);
        msquic.MsQuicClose(self.api);
    }

    /// Execute a query through the shared executor (thread-safe)
    fn executeQuery(self: *Server, sql: []const u8) ![]u8 {
        self.exec_mutex.lock();
        defer self.exec_mutex.unlock();

        const result = self.executor.execute(sql) catch |err| {
            return protocol.makeError(@errorName(err), self.allocator);
        };
        defer self.executor.freeResult(result);

        return switch (result) {
            .message => |msg| protocol.makeOkMessage(msg, self.allocator),
            .row_count => |count| protocol.makeOkRowCount(count, self.allocator),
            .rows => |r| {
                // Build row values as slices of slices
                const row_vals = self.allocator.alloc([]const []const u8, r.rows.len) catch
                    return protocol.makeError("OutOfMemory", self.allocator);
                defer self.allocator.free(row_vals);

                for (r.rows, 0..) |row, i| {
                    row_vals[i] = row.values;
                }

                return protocol.makeResultSet(r.columns, row_vals, self.allocator);
            },
        };
    }

    // ── msquic callbacks ─────────────────────────────────────────────

    fn listenerCallback(
        _: msquic.HQUIC,
        context: ?*anyopaque,
        event: *msquic.QUIC_LISTENER_EVENT,
    ) callconv(.c) msquic.QUIC_STATUS {
        const self: *Server = @ptrCast(@alignCast(context));

        switch (event.event_type) {
            .new_connection => {
                const conn = event.payload.new_connection.connection;
                self.api.set_callback_handler(
                    conn,
                    @constCast(@ptrCast(&connectionCallback)),
                    @ptrCast(self),
                );
                const status = self.api.connection_set_configuration(conn, self.configuration);
                if (!msquic.statusSucceeded(status)) {
                    return status;
                }
            },
            else => {},
        }
        return msquic.QUIC_STATUS_SUCCESS;
    }

    fn connectionCallback(
        connection: msquic.HQUIC,
        context: ?*anyopaque,
        event: *msquic.QUIC_CONNECTION_EVENT,
    ) callconv(.c) msquic.QUIC_STATUS {
        const self: *Server = @ptrCast(@alignCast(context));

        switch (event.event_type) {
            .connected => {
                // Connection established
                _ = self.api.connection_send_resumption_ticket(connection, 0, 0, null);
            },
            .peer_stream_started => {
                const stream = event.payload.peer_stream_started.stream;
                // Allocate a stream context for buffering incoming data
                const stream_ctx = self.allocator.create(StreamContext) catch {
                    return @bitCast(@as(u32, 0x80004005)); // E_FAIL
                };
                stream_ctx.* = StreamContext.init(self);

                self.api.set_callback_handler(
                    stream,
                    @constCast(@ptrCast(&streamCallback)),
                    @ptrCast(stream_ctx),
                );
            },
            .shutdown_complete => {
                self.api.connection_close(connection);
            },
            else => {},
        }
        return msquic.QUIC_STATUS_SUCCESS;
    }

    fn streamCallback(
        stream: msquic.HQUIC,
        context: ?*anyopaque,
        event: *msquic.QUIC_STREAM_EVENT,
    ) callconv(.c) msquic.QUIC_STATUS {
        const stream_ctx: *StreamContext = @ptrCast(@alignCast(context));
        const self = stream_ctx.server;

        switch (event.event_type) {
            .receive => {
                // Append incoming data to buffer
                const recv = event.payload.receive;
                var i: u32 = 0;
                while (i < recv.buffer_count) : (i += 1) {
                    const buf = recv.buffers[i];
                    stream_ctx.recv_buf.appendSlice(self.allocator, buf.toSlice()) catch {
                        return @bitCast(@as(u32, 0x80004005));
                    };
                }
            },
            .peer_send_shutdown => {
                // Client finished sending — process the request
                const data = stream_ctx.recv_buf.items;
                const response = self.processMessage(data) catch {
                    const err_resp = protocol.makeError("InternalError", self.allocator) catch {
                        return @bitCast(@as(u32, 0x80004005));
                    };
                    sendResponse(self, stream, stream_ctx, err_resp);
                    return msquic.QUIC_STATUS_SUCCESS;
                };
                sendResponse(self, stream, stream_ctx, response);
            },
            .shutdown_complete => {
                stream_ctx.deinit(self.allocator);
                self.allocator.destroy(stream_ctx);
                self.api.stream_close(stream);
            },
            else => {},
        }
        return msquic.QUIC_STATUS_SUCCESS;
    }

    fn sendResponse(self: *Server, stream: msquic.HQUIC, stream_ctx: *StreamContext, response: []u8) void {
        // Store response in stream context so it lives until SEND_COMPLETE
        stream_ctx.send_buf = response;
        var send_quic_buf = msquic.QUIC_BUFFER{
            .length = @intCast(response.len),
            .buffer = response.ptr,
        };
        _ = self.api.stream_send(
            stream,
            @ptrCast(&send_quic_buf),
            1,
            msquic.QUIC_SEND_FLAG_FIN,
            null,
        );
    }

    fn processMessage(self: *Server, data: []const u8) ![]u8 {
        const decoded = protocol.decode(data) catch {
            return protocol.makeError("InvalidMessage", self.allocator);
        };

        return switch (decoded.msg.msg_type) {
            .query => self.executeQuery(decoded.msg.payload),
            .disconnect => protocol.makeOkMessage("Goodbye", self.allocator),
            else => protocol.makeError("UnexpectedMessageType", self.allocator),
        };
    }
};

const StreamContext = struct {
    server: *Server,
    recv_buf: std.ArrayListUnmanaged(u8),
    send_buf: ?[]u8,

    fn init(server: *Server) StreamContext {
        return .{
            .server = server,
            .recv_buf = .empty,
            .send_buf = null,
        };
    }

    fn deinit(self: *StreamContext, allocator: std.mem.Allocator) void {
        self.recv_buf.deinit(allocator);
        if (self.send_buf) |buf| {
            allocator.free(buf);
        }
    }
};
