const std = @import("std");
const msquic = @import("msquic.zig");
const protocol = @import("protocol.zig");
const executor_mod = @import("../executor/executor.zig");
const catalog_mod = @import("../storage/catalog.zig");
const mvcc_mod = @import("../storage/mvcc.zig");
const undo_log_mod = @import("../storage/undo_log.zig");

const Executor = executor_mod.Executor;
const Catalog = catalog_mod.Catalog;
const TransactionManager = mvcc_mod.TransactionManager;
const UndoLog = undo_log_mod.UndoLog;

const ALPN = "graphenedb";

pub const Server = struct {
    allocator: std.mem.Allocator,
    api: *const msquic.QUIC_API_TABLE,
    registration: msquic.HQUIC,
    configuration: msquic.HQUIC,
    listener: msquic.HQUIC,

    // Shared database components (each connection gets its own Executor)
    catalog: *Catalog,
    txn_manager: ?*TransactionManager,
    undo_log: ?*UndoLog,
    exec_mutex: std.Io.Mutex,
    io: std.Io,

    pub const TlsCert = union(enum) {
        hash: [20]u8, // Windows Schannel: SHA-1 thumbprint from cert store
        files: struct { cert: [*:0]const u8, key: [*:0]const u8 }, // Linux OpenSSL: PEM paths
        pkcs12: struct { der: []const u8, password: ?[*:0]const u8 }, // OpenSSL: PKCS12 DER bytes
        context: *anyopaque, // Windows: PCCERT_CONTEXT from cert store
    };

    pub fn init(
        allocator: std.mem.Allocator,
        catalog: *Catalog,
        txn_manager: ?*TransactionManager,
        undo_log: ?*UndoLog,
        tls_cert: TlsCert,
        io: std.Io,
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
        var cert_hash_config: msquic.QUIC_CERTIFICATE_HASH = undefined;
        var cert_file_config: msquic.QUIC_CERTIFICATE_FILE = undefined;
        var cert_pkcs12_config: msquic.QUIC_CERTIFICATE_PKCS12 = undefined;

        switch (tls_cert) {
            .hash => |hash| {
                cert_hash_config = .{ .sha_hash = hash };
                cred_config.cred_type = .certificate_hash;
                cred_config.certificate = @ptrCast(&cert_hash_config);
            },
            .files => |files| {
                cert_file_config = .{
                    .private_key_file = files.key,
                    .certificate_file = files.cert,
                };
                cred_config.cred_type = .certificate_file;
                cred_config.certificate = @ptrCast(&cert_file_config);
            },
            .pkcs12 => |p12| {
                cert_pkcs12_config = .{
                    .asn1_blob = p12.der.ptr,
                    .asn1_blob_length = @intCast(p12.der.len),
                    .private_key_password = p12.password,
                };
                cred_config.cred_type = .certificate_pkcs12;
                cred_config.certificate = @ptrCast(&cert_pkcs12_config);
            },
            .context => |ctx| {
                cred_config.cred_type = .certificate_context;
                cred_config.certificate = ctx;
            },
        }

        const cred_status = api.configuration_load_credential(configuration, &cred_config);
        if (!msquic.statusSucceeded(cred_status)) {
            std.log.err("msquic credential load failed: status=0x{x}", .{@as(u32, @bitCast(cred_status))});
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
            .catalog = catalog,
            .txn_manager = txn_manager,
            .undo_log = undo_log,
            .exec_mutex = .init,
            .io = io,
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

    /// Execute a query through a connection's executor (thread-safe)
    fn executeQuery(self: *Server, conn_ctx: *ConnectionContext, sql: []const u8) ![]u8 {
        self.exec_mutex.lockUncancelable(self.io);
        defer self.exec_mutex.unlock(self.io);

        const result = conn_ctx.executor.execute(sql) catch |err| {
            return protocol.makeError(@errorName(err), self.allocator);
        };
        defer conn_ctx.executor.freeResult(result);

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

                // Each connection gets its own executor (own transaction state)
                const conn_ctx = self.allocator.create(ConnectionContext) catch {
                    return @bitCast(@as(u32, 0x80004005)); // E_FAIL
                };
                conn_ctx.* = ConnectionContext.init(self);

                self.api.set_callback_handler(
                    conn,
                    @constCast(@ptrCast(&connectionCallback)),
                    @ptrCast(conn_ctx),
                );
                const status = self.api.connection_set_configuration(conn, self.configuration);
                if (!msquic.statusSucceeded(status)) {
                    self.allocator.destroy(conn_ctx);
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
        const conn_ctx: *ConnectionContext = @ptrCast(@alignCast(context));
        const self = conn_ctx.server;

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
                stream_ctx.* = StreamContext.init(conn_ctx);

                self.api.set_callback_handler(
                    stream,
                    @constCast(@ptrCast(&streamCallback)),
                    @ptrCast(stream_ctx),
                );
            },
            .shutdown_complete => {
                // Auto-rollback any open transaction on disconnect
                self.exec_mutex.lockUncancelable(self.io);
                conn_ctx.executor.abortCurrentTxn();
                self.exec_mutex.unlock(self.io);

                self.allocator.destroy(conn_ctx);
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
        const conn_ctx = stream_ctx.conn_ctx;
        const self = conn_ctx.server;

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
                const response = self.processMessage(conn_ctx, data) catch {
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
        // Store response and QUIC_BUFFER in stream context so they live until SEND_COMPLETE
        stream_ctx.send_buf = response;
        stream_ctx.send_quic_buf = .{
            .length = @intCast(response.len),
            .buffer = response.ptr,
        };
        _ = self.api.stream_send(
            stream,
            @ptrCast(&stream_ctx.send_quic_buf),
            1,
            msquic.QUIC_SEND_FLAG_FIN,
            null,
        );
    }

    fn processMessage(self: *Server, conn_ctx: *ConnectionContext, data: []const u8) ![]u8 {
        const decoded = protocol.decode(data) catch {
            return protocol.makeError("InvalidMessage", self.allocator);
        };

        return switch (decoded.msg.msg_type) {
            .query => self.executeQuery(conn_ctx, decoded.msg.payload),
            .disconnect => protocol.makeOkMessage("Goodbye", self.allocator),
            else => protocol.makeError("UnexpectedMessageType", self.allocator),
        };
    }
};

/// Per-connection context — each connection gets its own Executor
/// with independent transaction state, sharing the server's catalog,
/// transaction manager, and undo log.
const ConnectionContext = struct {
    server: *Server,
    executor: Executor,

    fn init(server: *Server) ConnectionContext {
        return .{
            .server = server,
            .executor = Executor.initWithMvcc(
                server.allocator,
                server.catalog,
                server.txn_manager,
                server.undo_log,
            ),
        };
    }
};

const StreamContext = struct {
    conn_ctx: *ConnectionContext,
    recv_buf: std.ArrayListUnmanaged(u8),
    send_buf: ?[]u8,
    send_quic_buf: msquic.QUIC_BUFFER,

    fn init(conn_ctx: *ConnectionContext) StreamContext {
        return .{
            .conn_ctx = conn_ctx,
            .recv_buf = .empty,
            .send_buf = null,
            .send_quic_buf = undefined,
        };
    }

    fn deinit(self: *StreamContext, allocator: std.mem.Allocator) void {
        self.recv_buf.deinit(allocator);
        if (self.send_buf) |buf| {
            allocator.free(buf);
        }
    }
};
