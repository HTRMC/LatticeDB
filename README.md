# GrapheneDB

A database built from scratch in Zig with QUIC networking via msquic.

## Build

**Windows**:
```
zig build -Dmsquic-path=deps/msquic/build/native
zig build test -Dmsquic-path=deps/msquic/build/native
```

**Linux**:
```bash
zig build
zig build test
```

## Usage

```
GrapheneDB                      # local REPL
GrapheneDB serve [options]      # start QUIC server (default port: 4567)
GrapheneDB connect [host:port]  # connect to server (default: localhost:4567)
```

### Server options

QUIC requires TLS, so the server needs a certificate.

**Windows** (Schannel — uses Windows certificate store):
```powershell
# Generate a self-signed certificate (one-time setup)
New-SelfSignedCertificate -DnsName localhost -CertStoreLocation cert:\CurrentUser\My -FriendlyName "GrapheneDB Dev"

# Start server with the thumbprint from the output
GrapheneDB serve --cert-hash THUMBPRINT
GrapheneDB serve 4567 --cert-hash THUMBPRINT  # custom port
```

**Linux** (OpenSSL — uses PEM files):
```bash
# Generate cert and key (one-time setup)
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj '/CN=localhost'

# Start server
GrapheneDB serve --cert cert.pem --key key.pem
GrapheneDB serve 4567 --cert cert.pem --key key.pem  # custom port
```

## msquic Setup

GrapheneDB uses [msquic](https://github.com/microsoft/msquic) for QUIC transport.

**Windows** — download the NuGet package and extract:
```powershell
Invoke-WebRequest -Uri 'https://www.nuget.org/api/v2/package/Microsoft.Native.Quic.MsQuic.Schannel' -OutFile deps/msquic.zip
# Extract to deps/msquic/ (it's a zip file)
```

**Linux**:
```bash
# Add Microsoft's repo (libmsquic is not in Ubuntu's default repos)
curl -s https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/$(lsb_release -rs)/prod $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/microsoft.list
sudo apt update

# Install msquic and OpenSSL dev headers
sudo apt install libmsquic libssl-dev

# Create the linker symlink (the package doesn't include one)
sudo ln -s /usr/lib/x86_64-linux-gnu/libmsquic.so.2 /usr/lib/x86_64-linux-gnu/libmsquic.so

zig build  # no -Dmsquic-path needed, uses system lib
```

## Why msquic

QUIC requires a TLS 1.3 implementation, congestion control, loss recovery, and stream multiplexing. Building that from scratch would be a larger project than the database itself. msquic is MIT-licensed, cross-platform (Windows + Linux), and has a clean C API.

### Why hand-written Zig bindings

msquic's C header (`msquic.h`) doesn't work with Zig's `@cImport` / `translate-c`:

- **SAL annotations** — `_In_`, `_Out_`, `_Field_size_` are Microsoft-specific macros that confuse the translator
- **Unnamed unions** — the event structs (`QUIC_CONNECTION_EVENT`, `QUIC_STREAM_EVENT`) use anonymous unions that `translate-c` mishandles
- **Bitfield structs** — `QUIC_SETTINGS` packs 45+ boolean flags into a `uint64_t` bitfield, which becomes an opaque type after translation
- **Windows SDK types** — on Windows, `HRESULT`, `SOCKADDR_INET`, and `__cdecl` pull in Windows headers that have known Zig compatibility issues

Hand-writing ~300 lines of Zig extern declarations was more reliable than fighting the translator across platforms. The msquic API table pattern (a struct of ~30 function pointers) maps naturally to Zig's `extern struct`.

### Why `-Dmsquic-path` instead of hardcoded paths

Build-time path construction in Zig's build system has a subtlety: `std.fmt.comptimePrint` requires all arguments to be Zig comptime constants, but the target architecture from `compile.rootModuleTarget().cpu.arch` is a build-time value, not a Zig comptime value. Using `std.fmt.allocPrint(b.allocator, ...)` with a build option keeps it portable and lets each developer point to their own msquic installation.

### Why event-driven callbacks

msquic is not a socket library. There's no `accept()` or `recv()` to call. It runs its own internal thread pool and invokes your C callbacks when events happen (new connection, data received, send complete). This means:

- Callbacks are `callconv(.c)` function pointers passed to msquic at registration time
- Shared state like the query executor needs a mutex since callbacks run on msquic's threads, not ours
- The client bridges async-to-sync using `std.Thread.ResetEvent` — the main thread waits, the callback signals

### Why one stream per query

QUIC supports multiplexed streams on a single connection. We use the simplest model: client opens a bidirectional stream, sends one query with FIN, server responds with the result and FIN. This is synchronous from the client's perspective and avoids the complexity of request/response multiplexing in v1.
