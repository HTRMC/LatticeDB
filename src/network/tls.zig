///! TLS certificate generation using OpenSSL.
///! Generates self-signed certificates for QUIC server and provides
///! PKCS12 serialization for msquic's certificate_pkcs12 credential type.

const std = @import("std");

// ── OpenSSL opaque types ─────────────────────────────────────────────

const EVP_PKEY = opaque {};
const EVP_PKEY_CTX = opaque {};
const EVP_MD = opaque {};
const X509 = opaque {};
const X509_NAME = opaque {};
const ASN1_INTEGER = opaque {};
const ASN1_TIME = opaque {};
const PKCS12 = opaque {};

// ── OpenSSL OSSL_PARAM (for key generation) ──────────────────────────

const OSSL_PARAM = extern struct {
    key: ?[*:0]const u8,
    data_type: c_uint,
    data: ?*anyopaque,
    data_size: usize,
    return_size: usize,

    const UNSIGNED_INTEGER: c_uint = 2;

    fn end() OSSL_PARAM {
        return .{ .key = null, .data_type = 0, .data = null, .data_size = 0, .return_size = 0 };
    }
};

// ── Constants ────────────────────────────────────────────────────────

const V_ASN1_UTF8STRING: c_int = 12;
const X509_VERSION_3: c_long = 2; // 0-indexed: version 3 = value 2

// ── OpenSSL extern functions (libcrypto) ─────────────────────────────

// Key generation (EVP_PKEY_CTX + OSSL_PARAM, no macros needed)
extern "libcrypto" fn EVP_PKEY_CTX_new_from_name(libctx: ?*anyopaque, name: [*:0]const u8, propq: ?[*:0]const u8) callconv(.c) ?*EVP_PKEY_CTX;
extern "libcrypto" fn EVP_PKEY_keygen_init(ctx: ?*EVP_PKEY_CTX) callconv(.c) c_int;
extern "libcrypto" fn EVP_PKEY_CTX_set_params(ctx: ?*EVP_PKEY_CTX, params: [*]const OSSL_PARAM) callconv(.c) c_int;
extern "libcrypto" fn EVP_PKEY_generate(ctx: ?*EVP_PKEY_CTX, pkey: *?*EVP_PKEY) callconv(.c) c_int;
extern "libcrypto" fn EVP_PKEY_CTX_free(ctx: ?*EVP_PKEY_CTX) callconv(.c) void;
extern "libcrypto" fn EVP_PKEY_free(pkey: ?*EVP_PKEY) callconv(.c) void;

// X509 certificate
extern "libcrypto" fn X509_new() callconv(.c) ?*X509;
extern "libcrypto" fn X509_free(x: ?*X509) callconv(.c) void;
extern "libcrypto" fn X509_set_version(x: ?*X509, version: c_long) callconv(.c) c_int;
extern "libcrypto" fn X509_set_pubkey(x: ?*X509, pkey: ?*EVP_PKEY) callconv(.c) c_int;
extern "libcrypto" fn X509_get_serialNumber(x: ?*X509) callconv(.c) ?*ASN1_INTEGER;
extern "libcrypto" fn X509_set_issuer_name(x: ?*X509, name: ?*const X509_NAME) callconv(.c) c_int;
extern "libcrypto" fn X509_get_subject_name(x: ?*const X509) callconv(.c) ?*X509_NAME;
extern "libcrypto" fn X509_sign(x: ?*X509, pkey: ?*EVP_PKEY, md: ?*const EVP_MD) callconv(.c) c_int;
extern "libcrypto" fn X509_getm_notBefore(x: ?*const X509) callconv(.c) ?*ASN1_TIME;
extern "libcrypto" fn X509_getm_notAfter(x: ?*const X509) callconv(.c) ?*ASN1_TIME;
extern "libcrypto" fn X509_digest(data: ?*const X509, md_type: ?*const EVP_MD, md: [*]u8, len: *c_uint) callconv(.c) c_int;

// X509 name
extern "libcrypto" fn X509_NAME_add_entry_by_txt(name: ?*X509_NAME, field: [*:0]const u8, typ: c_int, bytes: [*]const u8, len: c_int, loc: c_int, set: c_int) callconv(.c) c_int;

// ASN1
extern "libcrypto" fn ASN1_INTEGER_set(a: ?*ASN1_INTEGER, v: c_long) callconv(.c) c_int;
extern "libcrypto" fn X509_gmtime_adj(s: ?*ASN1_TIME, adj: c_long) callconv(.c) ?*ASN1_TIME;

// Digest
extern "libcrypto" fn EVP_sha256() callconv(.c) ?*const EVP_MD;

// PKCS12
extern "libcrypto" fn PKCS12_create(
    pass: ?[*:0]const u8,
    name: ?[*:0]const u8,
    pkey: ?*EVP_PKEY,
    cert: ?*X509,
    ca: ?*anyopaque,
    nid_key: c_int,
    nid_cert: c_int,
    iter: c_int,
    mac_iter: c_int,
    keytype: c_int,
) callconv(.c) ?*PKCS12;
extern "libcrypto" fn PKCS12_free(p12: ?*PKCS12) callconv(.c) void;
extern "libcrypto" fn PKCS12_parse(p12: ?*PKCS12, pass: ?[*:0]const u8, pkey: *?*EVP_PKEY, cert: *?*X509, ca: ?*?*anyopaque) callconv(.c) c_int;
extern "libcrypto" fn i2d_PKCS12(p12: ?*const PKCS12, out: *?[*]u8) callconv(.c) c_int;
extern "libcrypto" fn d2i_PKCS12(p12: ?*?*PKCS12, input: *[*]const u8, len: c_long) callconv(.c) ?*PKCS12;

// Cleanup
extern "libcrypto" fn CRYPTO_free(ptr: ?*anyopaque, file: [*:0]const u8, line: c_int) callconv(.c) void;

fn opensslFree(ptr: ?*anyopaque) void {
    CRYPTO_free(ptr, "tls.zig", 0);
}

// ── Windows CryptoAPI (for converting PKCS12 → CERT_CONTEXT) ────────

const builtin = @import("builtin");
const is_windows = builtin.os.tag == .windows;

const HCERTSTORE = ?*anyopaque;
const PCCERT_CONTEXT = ?*anyopaque;

const CRYPT_DATA_BLOB = extern struct {
    cb_data: u32,
    pb_data: [*]const u8,
};

// PFXImportCertStore flags
const CRYPT_EXPORTABLE: u32 = 0x00000001;
const CRYPT_USER_KEYSET: u32 = 0x00001000;
const PKCS12_NO_PERSIST_KEY: u32 = 0x00008000;

extern "crypt32" fn PFXImportCertStore(
    p_pfx: *const CRYPT_DATA_BLOB,
    sz_password: ?[*:0]const u16,
    dw_flags: u32,
) callconv(.c) HCERTSTORE;

extern "crypt32" fn CertEnumCertificatesInStore(
    h_cert_store: HCERTSTORE,
    p_prev_cert_context: PCCERT_CONTEXT,
) callconv(.c) PCCERT_CONTEXT;

extern "crypt32" fn CertCloseStore(
    h_cert_store: HCERTSTORE,
    dw_flags: u32,
) callconv(.c) c_int;

// ── Windows CERT_CONTEXT (for reading peer certs from Schannel) ──────

const CERT_CONTEXT = extern struct {
    dw_cert_encoding_type: u32,
    pb_cert_encoded: [*]const u8,
    cb_cert_encoded: u32,
    p_cert_info: ?*anyopaque,
    h_cert_store: ?*anyopaque,
};

// ── Public types ─────────────────────────────────────────────────────

pub const CertBundle = struct {
    pkcs12_der: []u8,
    fingerprint: [32]u8,
    /// Windows: cert store handle (owns the cert context). Must be freed with deinit.
    cert_store: HCERTSTORE = null,
    /// Windows: PCCERT_CONTEXT pointer for msquic certificate_context.
    cert_context: PCCERT_CONTEXT = null,

    pub fn deinit(self: *CertBundle, allocator: std.mem.Allocator) void {
        if (is_windows) {
            if (self.cert_store != null) {
                _ = CertCloseStore(self.cert_store, 0);
                self.cert_store = null;
                self.cert_context = null;
            }
        }
        allocator.free(self.pkcs12_der);
    }
};

// ── Public API ───────────────────────────────────────────────────────

/// Generate a self-signed certificate (RSA 2048, SHA-256, 365 day validity).
/// Returns PKCS12 DER bytes (for msquic) and the SHA-256 fingerprint.
pub fn generateSelfSigned(
    allocator: std.mem.Allocator,
    common_name: [*:0]const u8,
    password: ?[*:0]const u8,
) !CertBundle {
    // 1. Generate RSA 2048 key pair
    const pkey = try generateRsaKey();
    defer EVP_PKEY_free(pkey);

    // 2. Create X509 certificate
    const cert = try createSelfSignedCert(pkey, common_name);
    defer X509_free(cert);

    // 3. Compute SHA-256 fingerprint
    var fingerprint: [32]u8 = undefined;
    var fp_len: c_uint = 32;
    if (X509_digest(cert, EVP_sha256(), &fingerprint, &fp_len) != 1) {
        return error.FingerprintFailed;
    }

    // 4. Create PKCS12 bundle
    const p12 = PKCS12_create(
        password,
        "GrapheneDB",
        pkey,
        cert,
        null, // no CA chain
        0, // default key NID
        0, // default cert NID
        0, // default iterations
        0, // default MAC iterations
        0, // default key type
    ) orelse return error.Pkcs12CreateFailed;
    defer PKCS12_free(p12);

    // 5. Serialize PKCS12 to DER
    const pkcs12_der = try serializePkcs12(allocator, p12);

    var bundle = CertBundle{
        .pkcs12_der = pkcs12_der,
        .fingerprint = fingerprint,
    };

    // 6. On Windows, import into cert store for msquic certificate_context
    if (is_windows) {
        try importPkcs12ToCertStore(&bundle, password);
    }

    return bundle;
}

/// Load PKCS12 from raw DER bytes and compute the fingerprint.
pub fn loadFromBytes(
    allocator: std.mem.Allocator,
    der: []const u8,
    password: ?[*:0]const u8,
) !CertBundle {
    // Parse PKCS12 to extract cert for fingerprinting
    var input_ptr: [*]const u8 = der.ptr;
    const p12 = d2i_PKCS12(null, &input_ptr, @intCast(der.len)) orelse return error.Pkcs12ParseFailed;
    defer PKCS12_free(p12);

    var pkey: ?*EVP_PKEY = null;
    var cert: ?*X509 = null;
    if (PKCS12_parse(p12, password, &pkey, &cert, null) != 1) {
        return error.Pkcs12ParseFailed;
    }
    defer EVP_PKEY_free(pkey);
    defer X509_free(cert);

    // Compute fingerprint
    var fingerprint: [32]u8 = undefined;
    var fp_len: c_uint = 32;
    if (X509_digest(cert, EVP_sha256(), &fingerprint, &fp_len) != 1) {
        return error.FingerprintFailed;
    }

    // Copy DER to owned slice
    const owned_der = try allocator.dupe(u8, der);

    var bundle = CertBundle{
        .pkcs12_der = owned_der,
        .fingerprint = fingerprint,
    };

    // On Windows, import into cert store for msquic certificate_context
    if (is_windows) {
        try importPkcs12ToCertStore(&bundle, password);
    }

    return bundle;
}

/// Compute SHA-256 fingerprint of a certificate from raw PKCS12 DER bytes.
pub fn fingerprintFromDer(der: []const u8, password: ?[*:0]const u8) ![32]u8 {
    var input_ptr: [*]const u8 = der.ptr;
    const p12 = d2i_PKCS12(null, &input_ptr, @intCast(der.len)) orelse return error.Pkcs12ParseFailed;
    defer PKCS12_free(p12);

    var pkey: ?*EVP_PKEY = null;
    var cert: ?*X509 = null;
    if (PKCS12_parse(p12, password, &pkey, &cert, null) != 1) {
        return error.Pkcs12ParseFailed;
    }
    defer EVP_PKEY_free(pkey);
    defer X509_free(cert);

    var fingerprint: [32]u8 = undefined;
    var fp_len: c_uint = 32;
    if (X509_digest(cert, EVP_sha256(), &fingerprint, &fp_len) != 1) {
        return error.FingerprintFailed;
    }
    return fingerprint;
}

/// Compute SHA-256 fingerprint from an OpenSSL X509* pointer (for client pinning).
pub fn fingerprintFromX509(cert_ptr: *anyopaque) ![32]u8 {
    const cert: ?*const X509 = @ptrCast(@alignCast(cert_ptr));
    var fingerprint: [32]u8 = undefined;
    var fp_len: c_uint = 32;
    if (X509_digest(cert, EVP_sha256(), &fingerprint, &fp_len) != 1) {
        return error.FingerprintFailed;
    }
    return fingerprint;
}

/// Format a 32-byte fingerprint as "AA:BB:CC:..." hex string.
pub fn formatFingerprint(fp: [32]u8, buf: *[95]u8) []u8 {
    const hex = "0123456789ABCDEF";
    var i: usize = 0;
    for (fp, 0..) |byte, idx| {
        buf[i] = hex[byte >> 4];
        buf[i + 1] = hex[byte & 0x0f];
        i += 2;
        if (idx < 31) {
            buf[i] = ':';
            i += 1;
        }
    }
    return buf[0..i];
}

/// Compute SHA-256 fingerprint from a peer certificate received in msquic callback.
/// On Windows (Schannel): cert_ptr is PCCERT_CONTEXT.
/// On Linux (OpenSSL): cert_ptr is X509*.
pub fn fingerprintFromPeerCert(cert_ptr: *anyopaque) ![32]u8 {
    if (is_windows) {
        const cert_ctx: *const CERT_CONTEXT = @ptrCast(@alignCast(cert_ptr));
        const der = cert_ctx.pb_cert_encoded[0..cert_ctx.cb_cert_encoded];
        const Sha256 = std.crypto.hash.sha2.Sha256;
        var fp: [Sha256.digest_length]u8 = undefined;
        Sha256.hash(der, &fp, .{});
        return fp;
    } else {
        return fingerprintFromX509(cert_ptr);
    }
}

// ── Internal helpers ─────────────────────────────────────────────────

fn generateRsaKey() !?*EVP_PKEY {
    const ctx = EVP_PKEY_CTX_new_from_name(null, "RSA", null) orelse return error.KeyGenFailed;
    defer EVP_PKEY_CTX_free(ctx);

    if (EVP_PKEY_keygen_init(ctx) <= 0) return error.KeyGenFailed;

    // Set RSA key size to 2048 bits via OSSL_PARAM
    var bits: c_uint = 2048;
    const params = [_]OSSL_PARAM{
        .{
            .key = "bits",
            .data_type = OSSL_PARAM.UNSIGNED_INTEGER,
            .data = @ptrCast(&bits),
            .data_size = @sizeOf(c_uint),
            .return_size = 0,
        },
        OSSL_PARAM.end(),
    };
    if (EVP_PKEY_CTX_set_params(ctx, &params) <= 0) return error.KeyGenFailed;

    var pkey: ?*EVP_PKEY = null;
    if (EVP_PKEY_generate(ctx, &pkey) <= 0) return error.KeyGenFailed;

    return pkey;
}

fn createSelfSignedCert(pkey: ?*EVP_PKEY, common_name: [*:0]const u8) !?*X509 {
    const cert = X509_new() orelse return error.CertCreateFailed;
    errdefer X509_free(cert);

    // Version 3
    if (X509_set_version(cert, X509_VERSION_3) != 1) return error.CertCreateFailed;

    // Serial number = 1
    const serial = X509_get_serialNumber(cert) orelse return error.CertCreateFailed;
    if (ASN1_INTEGER_set(serial, 1) != 1) return error.CertCreateFailed;

    // Validity: now to +365 days
    const not_before = X509_getm_notBefore(cert) orelse return error.CertCreateFailed;
    if (X509_gmtime_adj(not_before, 0) == null) return error.CertCreateFailed;
    const not_after = X509_getm_notAfter(cert) orelse return error.CertCreateFailed;
    if (X509_gmtime_adj(not_after, 365 * 24 * 3600) == null) return error.CertCreateFailed;

    // Subject name: CN=<common_name>, O=GrapheneDB
    const name = X509_get_subject_name(cert) orelse return error.CertCreateFailed;
    if (X509_NAME_add_entry_by_txt(name, "CN", V_ASN1_UTF8STRING, @ptrCast(common_name), -1, -1, 0) != 1)
        return error.CertCreateFailed;
    if (X509_NAME_add_entry_by_txt(name, "O", V_ASN1_UTF8STRING, "GrapheneDB", -1, -1, 0) != 1)
        return error.CertCreateFailed;

    // Self-signed: issuer = subject
    if (X509_set_issuer_name(cert, name) != 1) return error.CertCreateFailed;

    // Set public key
    if (X509_set_pubkey(cert, pkey) != 1) return error.CertCreateFailed;

    // Sign with SHA-256
    if (X509_sign(cert, pkey, EVP_sha256()) == 0) return error.CertCreateFailed;

    return cert;
}

/// Import PKCS12 DER into a Windows in-memory cert store, populating cert_store and cert_context.
fn importPkcs12ToCertStore(bundle: *CertBundle, password: ?[*:0]const u8) !void {
    // Convert password to UTF-16 for Windows API
    // When password is null, pass empty wide string L"" (not NULL pointer).
    // OpenSSL's PKCS12_create(NULL) uses empty password for MAC, and Windows
    // PFXImportCertStore treats NULL differently from L"".
    var wide_buf: [128]u16 = undefined;
    if (password) |pw| {
        const pw_slice = std.mem.span(pw);
        var i: usize = 0;
        for (pw_slice) |c| {
            wide_buf[i] = c;
            i += 1;
        }
        wide_buf[i] = 0;
    } else {
        wide_buf[0] = 0; // Empty string L""
    }
    const wide_password: [*:0]const u16 = @ptrCast(&wide_buf);

    var pfx_blob = CRYPT_DATA_BLOB{
        .cb_data = @intCast(bundle.pkcs12_der.len),
        .pb_data = bundle.pkcs12_der.ptr,
    };

    const store = PFXImportCertStore(
        &pfx_blob,
        wide_password,
        CRYPT_EXPORTABLE | CRYPT_USER_KEYSET,
    );
    if (store == null) return error.CertStoreImportFailed;

    // Get the first (and only) certificate from the store
    const cert_ctx = CertEnumCertificatesInStore(store, null);
    if (cert_ctx == null) {
        _ = CertCloseStore(store, 0);
        return error.CertStoreImportFailed;
    }

    bundle.cert_store = store;
    bundle.cert_context = cert_ctx;
}

fn serializePkcs12(allocator: std.mem.Allocator, p12: ?*const PKCS12) ![]u8 {
    // First call: get size
    var out_ptr: ?[*]u8 = null;
    const len = i2d_PKCS12(p12, &out_ptr);
    if (len <= 0) return error.Pkcs12SerializeFailed;

    const ssl_buf = out_ptr orelse return error.Pkcs12SerializeFailed;

    // Copy to Zig-managed memory
    const result = try allocator.alloc(u8, @intCast(len));
    @memcpy(result, ssl_buf[0..@intCast(len)]);

    // Free OpenSSL-allocated buffer
    opensslFree(@ptrCast(ssl_buf));

    return result;
}

// ── Tests ────────────────────────────────────────────────────────────

test "generate self-signed certificate" {
    const allocator = std.testing.allocator;
    var bundle = try generateSelfSigned(allocator, "localhost", null);
    defer bundle.deinit(allocator);

    // PKCS12 DER should be non-empty
    try std.testing.expect(bundle.pkcs12_der.len > 0);

    // Fingerprint should be non-zero
    var all_zero = true;
    for (bundle.fingerprint) |b| {
        if (b != 0) {
            all_zero = false;
            break;
        }
    }
    try std.testing.expect(!all_zero);
}

test "generate with password" {
    const allocator = std.testing.allocator;
    var bundle = try generateSelfSigned(allocator, "localhost", "testpass");
    defer bundle.deinit(allocator);

    try std.testing.expect(bundle.pkcs12_der.len > 0);
}

test "fingerprint from DER round-trip" {
    const allocator = std.testing.allocator;
    var bundle = try generateSelfSigned(allocator, "localhost", null);
    defer bundle.deinit(allocator);

    // Re-extract fingerprint from the DER bytes
    const fp2 = try fingerprintFromDer(bundle.pkcs12_der, null);
    try std.testing.expectEqualSlices(u8, &bundle.fingerprint, &fp2);
}

test "format fingerprint" {
    const fp = [_]u8{ 0xAA, 0xBB, 0xCC } ++ [_]u8{0} ** 29;
    var buf: [95]u8 = undefined;
    const formatted = formatFingerprint(fp, &buf);

    // Should start with "AA:BB:CC:00:..."
    try std.testing.expect(std.mem.startsWith(u8, formatted, "AA:BB:CC:00"));
    // 32 hex pairs + 31 colons = 95 chars
    try std.testing.expectEqual(@as(usize, 95), formatted.len);
}

test "two certificates have different fingerprints" {
    const allocator = std.testing.allocator;
    var bundle1 = try generateSelfSigned(allocator, "localhost", null);
    defer bundle1.deinit(allocator);
    var bundle2 = try generateSelfSigned(allocator, "localhost", null);
    defer bundle2.deinit(allocator);

    // Different keys → different fingerprints
    try std.testing.expect(!std.mem.eql(u8, &bundle1.fingerprint, &bundle2.fingerprint));
}
