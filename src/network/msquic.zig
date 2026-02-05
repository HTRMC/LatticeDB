///! Hand-written Zig bindings for the msquic C API (v2).
///! Only includes types and functions needed by GrapheneDB.
///! Reference: https://github.com/microsoft/msquic/blob/main/src/inc/msquic.h

const std = @import("std");
const builtin = @import("builtin");

// ── Fundamental types ────────────────────────────────────────────────

pub const QUIC_STATUS = if (builtin.os.tag == .windows) c_long else c_uint;
pub const HQUIC = ?*opaque {};

pub fn statusSucceeded(status: QUIC_STATUS) bool {
    if (builtin.os.tag == .windows) {
        return status >= 0; // HRESULT: success if non-negative
    } else {
        return status == 0; // POSIX: 0 = success
    }
}

pub const QUIC_STATUS_SUCCESS: QUIC_STATUS = 0;
pub const QUIC_STATUS_PENDING: QUIC_STATUS = if (builtin.os.tag == .windows) 0x703E5 else 1;
pub const QUIC_STATUS_NOT_FOUND: QUIC_STATUS = if (builtin.os.tag == .windows) @bitCast(@as(u32, 0x80004005)) else 2;

// ── Buffers ──────────────────────────────────────────────────────────

pub const QUIC_BUFFER = extern struct {
    length: u32,
    buffer: [*]u8,

    pub fn fromSlice(slice: []const u8) QUIC_BUFFER {
        return .{
            .length = @intCast(slice.len),
            .buffer = @constCast(slice.ptr),
        };
    }

    pub fn toSlice(self: QUIC_BUFFER) []const u8 {
        return self.buffer[0..self.length];
    }
};

// ── Address ──────────────────────────────────────────────────────────

pub const QUIC_ADDRESS_FAMILY_UNSPEC: u16 = 0;
pub const QUIC_ADDRESS_FAMILY_INET: u16 = 2;
pub const QUIC_ADDRESS_FAMILY_INET6: u16 = if (builtin.os.tag == .windows) 23 else 10;

pub const QUIC_ADDR = extern struct {
    data: [128]u8 align(4) = std.mem.zeroes([128]u8),
};

// ── Registration ─────────────────────────────────────────────────────

pub const QUIC_EXECUTION_PROFILE = enum(c_int) {
    low_latency = 0,
    max_throughput = 1,
    scavenger = 2,
    real_time = 3,
};

pub const QUIC_REGISTRATION_CONFIG = extern struct {
    app_name: [*:0]const u8,
    execution_profile: QUIC_EXECUTION_PROFILE,
};

// ── Settings ─────────────────────────────────────────────────────────

pub const QUIC_SETTINGS = extern struct {
    is_set: IsSet = .{},
    max_bytes_per_key: u64 = 0,
    handshake_idle_timeout_ms: u64 = 0,
    idle_timeout_ms: u64 = 0,
    internal_1: u64 = 0, // MtuDiscoverySearchCompleteTimeoutUs
    tls_client_max_send_buffer: u32 = 0,
    tls_server_max_send_buffer: u32 = 0,
    stream_recv_window_default: u32 = 0,
    stream_recv_buffer_default: u32 = 0,
    conn_flow_control_window: u32 = 0,
    max_worker_queue_delay_us: u32 = 0,
    max_stateless_operations: u32 = 0,
    initial_window_packets: u32 = 0,
    send_idle_timeout_ms: u32 = 0,
    initial_rtt_ms: u32 = 0,
    max_ack_delay_ms: u32 = 0,
    disconnect_timeout_ms: u32 = 0,
    keep_alive_interval_ms: u32 = 0,
    congestion_control_algorithm: u16 = 0,
    peer_bidi_stream_count: u16 = 0,
    peer_unidi_stream_count: u16 = 0,
    max_binding_stateless_operations: u16 = 0,
    stateless_operation_expiration_ms: u16 = 0,
    minimum_mtu: u16 = 0,
    maximum_mtu: u16 = 0,
    send_buffering_enabled: u8 = 0,
    pacing_enabled: u8 = 0,
    migration_enabled: u8 = 0,
    datagram_receive_enabled: u8 = 0,
    server_resumption_level: u8 = 0,
    internal_2: u8 = 0,
    internal_3: u8 = 0,
    desired_versions_list: ?*u32 = null,
    desired_versions_list_length: u32 = 0,

    pub const IsSet = packed struct(u64) {
        max_bytes_per_key: bool = false,
        handshake_idle_timeout_ms: bool = false,
        idle_timeout_ms: bool = false,
        internal_1: bool = false,
        tls_client_max_send_buffer: bool = false,
        tls_server_max_send_buffer: bool = false,
        stream_recv_window_default: bool = false,
        stream_recv_buffer_default: bool = false,
        conn_flow_control_window: bool = false,
        max_worker_queue_delay_us: bool = false,
        max_stateless_operations: bool = false,
        initial_window_packets: bool = false,
        send_idle_timeout_ms: bool = false,
        initial_rtt_ms: bool = false,
        max_ack_delay_ms: bool = false,
        disconnect_timeout_ms: bool = false,
        keep_alive_interval_ms: bool = false,
        congestion_control_algorithm: bool = false,
        peer_bidi_stream_count: bool = false,
        peer_unidi_stream_count: bool = false,
        max_binding_stateless_operations: bool = false,
        stateless_operation_expiration_ms: bool = false,
        minimum_mtu: bool = false,
        maximum_mtu: bool = false,
        send_buffering_enabled: bool = false,
        pacing_enabled: bool = false,
        migration_enabled: bool = false,
        datagram_receive_enabled: bool = false,
        server_resumption_level: bool = false,
        internal_2: bool = false,
        internal_3: bool = false,
        desired_versions_list: bool = false,
        _padding: u32 = 0,
    };
};

// ── Credentials / TLS ────────────────────────────────────────────────

pub const QUIC_CREDENTIAL_TYPE = enum(c_int) {
    none = 0,
    certificate_hash = 1,
    certificate_hash_store = 2,
    certificate_context = 3,
    certificate_file = 4,
    certificate_file_protected = 5,
    certificate_pkcs12 = 6,
};

pub const QUIC_CREDENTIAL_FLAGS = packed struct(u32) {
    none: bool = false, // bit 0: unused/none indicator
    client: bool = false, // bit 1
    load_asynchronous: bool = false, // bit 2
    no_certificate_validation: bool = false, // bit 3
    enable_ocsp: bool = false, // bit 4
    indicate_certificate_received: bool = false, // bit 5
    defer_certificate_validation: bool = false, // bit 6
    require_client_authentication: bool = false, // bit 7
    use_tls_builtin_certificate_validation: bool = false, // bit 8
    revocation_check_end_cert: bool = false, // bit 9
    revocation_check_chain: bool = false, // bit 10
    revocation_check_chain_exclude_root: bool = false, // bit 11
    ignore_no_revocation_check: bool = false, // bit 12
    ignore_revocation_offline: bool = false, // bit 13
    set_allowed_cipher_suites: bool = false, // bit 14
    use_portable_certificates: bool = false, // bit 15
    use_supplied_credentials: bool = false, // bit 16
    use_system_mapper: bool = false, // bit 17
    cache_only_url_retrieval: bool = false, // bit 18
    revocation_check_cache_only: bool = false, // bit 19
    inproc_peer_certificate: bool = false, // bit 20
    set_ca_certificate_file: bool = false, // bit 21
    _padding: u10 = 0,
};

pub const QUIC_CERTIFICATE_FILE = extern struct {
    private_key_file: [*:0]const u8,
    certificate_file: [*:0]const u8,
};

pub const QUIC_CERTIFICATE_HASH = extern struct {
    sha_hash: [20]u8,
};

pub const QUIC_CREDENTIAL_CONFIG = extern struct {
    cred_type: QUIC_CREDENTIAL_TYPE,
    flags: QUIC_CREDENTIAL_FLAGS,
    certificate: ?*anyopaque = null, // Union: CertificateHash, CertificateFile, etc.
    principal: ?[*:0]const u8 = null,
    reserved: ?*anyopaque = null,
    async_handler: ?*anyopaque = null,
    allowed_cipher_suites: u32 = 0,
    ca_certificate_file: ?[*:0]const u8 = null,
};

// ── Events ───────────────────────────────────────────────────────────

// Listener events
pub const QUIC_LISTENER_EVENT_TYPE = enum(c_int) {
    new_connection = 0,
    stop_complete = 1,
};

pub const QUIC_LISTENER_EVENT = extern struct {
    event_type: QUIC_LISTENER_EVENT_TYPE,
    payload: extern union {
        new_connection: extern struct {
            info: ?*anyopaque, // QUIC_NEW_CONNECTION_INFO*
            connection: HQUIC,
        },
        stop_complete: extern struct {
            app_close_in_progress: u8,
        },
    },
};

// Connection events
pub const QUIC_CONNECTION_EVENT_TYPE = enum(c_int) {
    connected = 0,
    shutdown_initiated_by_transport = 1,
    shutdown_initiated_by_peer = 2,
    shutdown_complete = 3,
    local_address_changed = 4,
    peer_address_changed = 5,
    peer_stream_started = 6,
    streams_available = 7,
    peer_needs_streams = 8,
    ideal_processor_changed = 9,
    datagram_state_changed = 10,
    datagram_received = 11,
    datagram_send_state_changed = 12,
    resumed = 13,
    resumption_ticket_received = 14,
    peer_certificate_received = 15,
    reliable_reset_negotiated = 16,
};

pub const QUIC_CONNECTION_EVENT = extern struct {
    event_type: QUIC_CONNECTION_EVENT_TYPE,
    payload: extern union {
        connected: extern struct {
            session_resumed: u8,
            negotiated_alpn_length: u8,
            negotiated_alpn: [*]const u8,
        },
        shutdown_initiated_by_transport: extern struct {
            status: QUIC_STATUS,
            error_code: u64,
        },
        shutdown_initiated_by_peer: extern struct {
            error_code: u64,
        },
        shutdown_complete: extern struct {
            flags: u32,
        },
        peer_stream_started: extern struct {
            stream: HQUIC,
            flags: u32,
        },
        // Other events use raw bytes for now
        raw: [64]u8,
    },
};

// Stream events
pub const QUIC_STREAM_EVENT_TYPE = enum(c_int) {
    start_complete = 0,
    receive = 1,
    send_complete = 2,
    peer_send_shutdown = 3,
    peer_send_aborted = 4,
    peer_receive_aborted = 5,
    send_shutdown_complete = 6,
    shutdown_complete = 7,
    ideal_send_buffer_size = 8,
    peer_accepted = 9,
    cancel_on_loss = 10,
};

pub const QUIC_STREAM_EVENT = extern struct {
    event_type: QUIC_STREAM_EVENT_TYPE,
    payload: extern union {
        start_complete: extern struct {
            status: QUIC_STATUS,
            id: u64,
            peer_accepted: u8,
        },
        receive: extern struct {
            absolute_offset: u64,
            total_buffer_length: u64,
            buffers: [*]const QUIC_BUFFER,
            buffer_count: u32,
            flags: u32,
        },
        send_complete: extern struct {
            canceled: u8,
            client_context: ?*anyopaque,
        },
        shutdown_complete: extern struct {
            connection_shutdown: u8,
            flags: u32,
        },
        // Other events
        raw: [64]u8,
    },
};

// ── Callback types ───────────────────────────────────────────────────

pub const QUIC_LISTENER_CALLBACK = *const fn (HQUIC, ?*anyopaque, *QUIC_LISTENER_EVENT) callconv(.c) QUIC_STATUS;
pub const QUIC_CONNECTION_CALLBACK = *const fn (HQUIC, ?*anyopaque, *QUIC_CONNECTION_EVENT) callconv(.c) QUIC_STATUS;
pub const QUIC_STREAM_CALLBACK = *const fn (HQUIC, ?*anyopaque, *QUIC_STREAM_EVENT) callconv(.c) QUIC_STATUS;

// ── Send flags ───────────────────────────────────────────────────────

pub const QUIC_SEND_FLAG_NONE: u32 = 0;
pub const QUIC_SEND_FLAG_ALLOW_0_RTT: u32 = 0x00000001;
pub const QUIC_SEND_FLAG_START: u32 = 0x00000002;
pub const QUIC_SEND_FLAG_FIN: u32 = 0x00000004;
pub const QUIC_SEND_FLAG_DGRAM_PRIORITY: u32 = 0x00000008;

// Stream open flags
pub const QUIC_STREAM_OPEN_FLAG_NONE: u32 = 0;
pub const QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL: u32 = 0x00000001;

// Stream start flags
pub const QUIC_STREAM_START_FLAG_NONE: u32 = 0;
pub const QUIC_STREAM_START_FLAG_IMMEDIATE: u32 = 0x00000001;

// Stream shutdown flags
pub const QUIC_STREAM_SHUTDOWN_FLAG_NONE: u32 = 0;
pub const QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL: u32 = 0x00000001;
pub const QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND: u32 = 0x00000002;
pub const QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE: u32 = 0x00000004;
pub const QUIC_STREAM_SHUTDOWN_FLAG_ABORT: u32 = 0x00000006;

// Connection shutdown flags
pub const QUIC_CONNECTION_SHUTDOWN_FLAG_NONE: u32 = 0;
pub const QUIC_CONNECTION_SHUTDOWN_FLAG_SILENT: u32 = 0x00000001;

// Server resumption level
pub const QUIC_SERVER_NO_RESUME: u8 = 0;
pub const QUIC_SERVER_RESUME_ONLY: u8 = 1;
pub const QUIC_SERVER_RESUME_AND_ZERORTT: u8 = 2;

// ── API Table ────────────────────────────────────────────────────────

pub const QUIC_API_TABLE = extern struct {
    set_context: *const fn (HQUIC, ?*anyopaque) callconv(.c) void,
    get_context: *const fn (HQUIC) callconv(.c) ?*anyopaque,
    set_callback_handler: *const fn (HQUIC, ?*anyopaque, ?*anyopaque) callconv(.c) void,
    set_param: *const fn (HQUIC, u32, u32, ?*const anyopaque) callconv(.c) QUIC_STATUS,
    get_param: *const fn (HQUIC, u32, *u32, ?*anyopaque) callconv(.c) QUIC_STATUS,

    registration_open: *const fn (*const QUIC_REGISTRATION_CONFIG, *HQUIC) callconv(.c) QUIC_STATUS,
    registration_close: *const fn (HQUIC) callconv(.c) void,
    registration_shutdown: *const fn (HQUIC, u32, u64) callconv(.c) void,

    configuration_open: *const fn (HQUIC, [*]const QUIC_BUFFER, u32, ?*const QUIC_SETTINGS, u32, ?*anyopaque, *HQUIC) callconv(.c) QUIC_STATUS,
    configuration_close: *const fn (HQUIC) callconv(.c) void,
    configuration_load_credential: *const fn (HQUIC, *const QUIC_CREDENTIAL_CONFIG) callconv(.c) QUIC_STATUS,

    listener_open: *const fn (HQUIC, QUIC_LISTENER_CALLBACK, ?*anyopaque, *HQUIC) callconv(.c) QUIC_STATUS,
    listener_close: *const fn (HQUIC) callconv(.c) void,
    listener_start: *const fn (HQUIC, [*]const QUIC_BUFFER, u32, *const QUIC_ADDR) callconv(.c) QUIC_STATUS,
    listener_stop: *const fn (HQUIC) callconv(.c) void,

    connection_open: *const fn (HQUIC, QUIC_CONNECTION_CALLBACK, ?*anyopaque, *HQUIC) callconv(.c) QUIC_STATUS,
    connection_close: *const fn (HQUIC) callconv(.c) void,
    connection_shutdown: *const fn (HQUIC, u32, u64) callconv(.c) void,
    connection_start: *const fn (HQUIC, HQUIC, u16, [*:0]const u8, u16) callconv(.c) QUIC_STATUS,
    connection_set_configuration: *const fn (HQUIC, HQUIC) callconv(.c) QUIC_STATUS,
    connection_send_resumption_ticket: *const fn (HQUIC, u32, u16, ?[*]const u8) callconv(.c) QUIC_STATUS,

    stream_open: *const fn (HQUIC, u32, QUIC_STREAM_CALLBACK, ?*anyopaque, *HQUIC) callconv(.c) QUIC_STATUS,
    stream_close: *const fn (HQUIC) callconv(.c) void,
    stream_start: *const fn (HQUIC, u32) callconv(.c) QUIC_STATUS,
    stream_shutdown: *const fn (HQUIC, u32, u64) callconv(.c) QUIC_STATUS,
    stream_send: *const fn (HQUIC, [*]const QUIC_BUFFER, u32, u32, ?*anyopaque) callconv(.c) QUIC_STATUS,
    stream_receive_complete: *const fn (HQUIC, u64) callconv(.c) void,
    stream_receive_set_enabled: *const fn (HQUIC, u8) callconv(.c) QUIC_STATUS,

    datagram_send: *const fn (HQUIC, [*]const QUIC_BUFFER, u32, u32, ?*anyopaque) callconv(.c) QUIC_STATUS,
};

// ── Entry point ──────────────────────────────────────────────────────

pub const QUIC_API_VERSION_2: u32 = 2;

pub extern "msquic" fn MsQuicOpenVersion(version: u32, api: **const QUIC_API_TABLE) callconv(.c) QUIC_STATUS;
pub extern "msquic" fn MsQuicClose(api: *const QUIC_API_TABLE) callconv(.c) void;

// ── Helper: set address family and port ──────────────────────────────

pub fn quicAddrSetFamily(addr: *QUIC_ADDR, family: u16) void {
    // sockaddr_in and sockaddr_in6 both start with sa_family at offset 0
    // On Windows it's at offset 0 as well (USHORT in SOCKADDR_INET)
    const family_ptr: *align(1) u16 = @ptrCast(&addr.data[0]);
    family_ptr.* = family;
}

pub fn quicAddrSetPort(addr: *QUIC_ADDR, port: u16) void {
    // Port is at offset 2 in both sockaddr_in and sockaddr_in6, stored big-endian
    const port_ptr: *align(1) u16 = @ptrCast(&addr.data[2]);
    port_ptr.* = std.mem.nativeToBig(u16, port);
}
