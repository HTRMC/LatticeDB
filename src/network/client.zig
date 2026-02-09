const std = @import("std");
const msquic = @import("msquic.zig");
const protocol = @import("protocol.zig");
const tls = @import("tls.zig");

const ALPN = "graphenedb";

pub const Client = struct {
    allocator: std.mem.Allocator,
    api: *const msquic.QUIC_API_TABLE,
    registration: msquic.HQUIC,
    configuration: msquic.HQUIC,
    connection: msquic.HQUIC,

    // Synchronization for request/response
    response_event: std.Io.Event,
    response_data: std.ArrayListUnmanaged(u8),
    connected_event: std.Io.Event,
    io: std.Io,
    connect_status: ?msquic.QUIC_STATUS,

    // Certificate pinning (TOFU)
    expected_fingerprint: ?[32]u8,
    server_fingerprint: ?[32]u8,
    pin_result: PinResult,

    pub const PinResult = enum {
        none, // No cert received yet
        trusted, // Known host, fingerprint matches
        new_host, // Unknown host, TOFU accepted
        mismatch, // WARNING: fingerprint changed
    };

    pub fn init(allocator: std.mem.Allocator, io: std.Io) !Client {
        var api: *const msquic.QUIC_API_TABLE = undefined;
        const open_status = msquic.MsQuicOpenVersion(msquic.QUIC_API_VERSION_2, &api);
        if (!msquic.statusSucceeded(open_status)) return error.MsQuicOpenFailed;

        const reg_config = msquic.QUIC_REGISTRATION_CONFIG{
            .app_name = "GrapheneDB-Client",
            .execution_profile = .low_latency,
        };
        var registration: msquic.HQUIC = null;
        const reg_status = api.registration_open(&reg_config, &registration);
        if (!msquic.statusSucceeded(reg_status)) {
            msquic.MsQuicClose(api);
            return error.RegistrationFailed;
        }

        // Client configuration — no TLS cert needed, skip validation for dev
        var alpn_buffer = msquic.QUIC_BUFFER.fromSlice(ALPN);
        var settings = std.mem.zeroes(msquic.QUIC_SETTINGS);
        settings.idle_timeout_ms = 30000;
        settings.is_set.idle_timeout_ms = true;

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

        // Client credential — no cert, custom validation via TOFU pinning
        var cred_config = std.mem.zeroes(msquic.QUIC_CREDENTIAL_CONFIG);
        cred_config.cred_type = .none;
        cred_config.flags.client = true;
        cred_config.flags.no_certificate_validation = true;
        cred_config.flags.indicate_certificate_received = true;

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
            .connection = null,
            .response_event = .unset,
            .response_data = .empty,
            .connected_event = .unset,
            .io = io,
            .connect_status = null,
            .expected_fingerprint = null,
            .server_fingerprint = null,
            .pin_result = .none,
        };
    }

    pub fn connect(self: *Client, host: [*:0]const u8, port: u16) !void {
        const status = self.api.connection_open(
            self.registration,
            connectionCallback,
            @ptrCast(self),
            &self.connection,
        );
        if (!msquic.statusSucceeded(status)) return error.ConnectionOpenFailed;

        const start_status = self.api.connection_start(
            self.connection,
            self.configuration,
            msquic.QUIC_ADDRESS_FAMILY_UNSPEC,
            host,
            port,
        );
        if (!msquic.statusSucceeded(start_status)) return error.ConnectionStartFailed;

        // Wait for connection to establish
        self.connected_event.waitUncancelable(self.io);

        if (self.connect_status) |cs| {
            if (!msquic.statusSucceeded(cs)) return error.ConnectionFailed;
        }
    }

    /// Send a SQL query and wait for the response
    pub fn query(self: *Client, sql: []const u8) !protocol.Message {
        // Build query message
        const msg_buf = try protocol.makeQuery(sql, self.allocator);
        defer self.allocator.free(msg_buf);

        // Reset response state
        self.response_data.clearRetainingCapacity();
        self.response_event.reset();

        // Open a new stream for this query
        var stream: msquic.HQUIC = null;
        const open_status = self.api.stream_open(
            self.connection,
            msquic.QUIC_STREAM_OPEN_FLAG_NONE,
            streamCallback,
            @ptrCast(self),
            &stream,
        );
        if (!msquic.statusSucceeded(open_status)) return error.StreamOpenFailed;

        const start_status = self.api.stream_start(stream, msquic.QUIC_STREAM_START_FLAG_IMMEDIATE);
        if (!msquic.statusSucceeded(start_status)) {
            self.api.stream_close(stream);
            return error.StreamStartFailed;
        }

        // Send query data with FIN
        var send_buf = msquic.QUIC_BUFFER{
            .length = @intCast(msg_buf.len),
            .buffer = @constCast(msg_buf.ptr),
        };
        const send_status = self.api.stream_send(
            stream,
            @ptrCast(&send_buf),
            1,
            msquic.QUIC_SEND_FLAG_FIN,
            null,
        );
        if (!msquic.statusSucceeded(send_status)) {
            self.api.stream_close(stream);
            return error.SendFailed;
        }

        // Wait for response
        self.response_event.waitUncancelable(self.io);

        // Decode response
        const data = self.response_data.items;
        if (data.len == 0) return error.EmptyResponse;

        const decoded = try protocol.decode(data);
        return decoded.msg;
    }

    pub fn disconnect(self: *Client) void {
        if (self.connection != null) {
            self.api.connection_shutdown(
                self.connection,
                msquic.QUIC_CONNECTION_SHUTDOWN_FLAG_NONE,
                0,
            );
        }
    }

    pub fn deinit(self: *Client) void {
        self.disconnect();
        self.response_data.deinit(self.allocator);
        if (self.configuration != null) self.api.configuration_close(self.configuration);
        if (self.registration != null) self.api.registration_close(self.registration);
        msquic.MsQuicClose(self.api);
    }

    // ── msquic callbacks ─────────────────────────────────────────────

    fn connectionCallback(
        _: msquic.HQUIC,
        context: ?*anyopaque,
        event: *msquic.QUIC_CONNECTION_EVENT,
    ) callconv(.c) msquic.QUIC_STATUS {
        const self: *Client = @ptrCast(@alignCast(context));

        switch (event.event_type) {
            .connected => {
                self.connect_status = msquic.QUIC_STATUS_SUCCESS;
                self.connected_event.set(self.io);
            },
            .shutdown_initiated_by_transport => {
                self.connect_status = event.payload.shutdown_initiated_by_transport.status;
                self.connected_event.set(self.io);
                self.response_event.set(self.io); // Unblock any pending query
            },
            .shutdown_initiated_by_peer => {
                self.connected_event.set(self.io);
                self.response_event.set(self.io);
            },
            .peer_certificate_received => {
                const cert = event.payload.peer_certificate_received.certificate orelse {
                    self.pin_result = .none;
                    return msquic.QUIC_STATUS_SUCCESS;
                };

                const fp = tls.fingerprintFromPeerCert(cert) catch {
                    return @bitCast(@as(u32, 0x80004005)); // E_FAIL
                };
                self.server_fingerprint = fp;

                if (self.expected_fingerprint) |expected| {
                    if (std.mem.eql(u8, &fp, &expected)) {
                        self.pin_result = .trusted;
                        return msquic.QUIC_STATUS_SUCCESS;
                    } else {
                        self.pin_result = .mismatch;
                        return @bitCast(@as(u32, 0x80004005)); // Reject
                    }
                } else {
                    self.pin_result = .new_host;
                    return msquic.QUIC_STATUS_SUCCESS;
                }
            },
            .shutdown_complete => {},
            else => {},
        }
        return msquic.QUIC_STATUS_SUCCESS;
    }

    fn streamCallback(
        stream: msquic.HQUIC,
        context: ?*anyopaque,
        event: *msquic.QUIC_STREAM_EVENT,
    ) callconv(.c) msquic.QUIC_STATUS {
        const self: *Client = @ptrCast(@alignCast(context));

        switch (event.event_type) {
            .receive => {
                const recv = event.payload.receive;
                var i: u32 = 0;
                while (i < recv.buffer_count) : (i += 1) {
                    const buf = recv.buffers[i];
                    self.response_data.appendSlice(self.allocator, buf.toSlice()) catch {};
                }
            },
            .peer_send_shutdown => {
                // Server finished sending — response complete
                self.response_event.set(self.io);
            },
            .shutdown_complete => {
                self.response_event.set(self.io);
                self.api.stream_close(stream);
            },
            else => {},
        }
        return msquic.QUIC_STATUS_SUCCESS;
    }
};
