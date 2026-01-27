#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPILER_DIR="$PROJECT_DIR/compiler"
ZIG_DIR="$COMPILER_DIR/zig"
ZIG_EXE="$ZIG_DIR/zig"
ZIGVERSION_FILE="$PROJECT_DIR/.zigversion"

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux";;
        Darwin*) echo "macos";;
        *)       echo "unknown";;
    esac
}

# Read version from .zigversion file
get_version() {
    if [ ! -f "$ZIGVERSION_FILE" ]; then
        echo "Error: .zigversion file not found" >&2
        exit 1
    fi
    cat "$ZIGVERSION_FILE" | tr -d '[:space:]'
}

# Download and extract Zig
download_zig() {
    local version="$1"
    local os="$2"
    local arch="x86_64"
    local ext="tar.xz"
    local url="https://ziglang.org/builds/zig-${arch}-${os}-${version}.${ext}"
    local archive_name="zig-${arch}-${os}-${version}"
    local temp_file="$COMPILER_DIR/zig-download.tar.xz"

    echo "Downloading Zig ${version} for ${os}..."
    echo "URL: $url"

    # Create compiler directory
    mkdir -p "$COMPILER_DIR"

    # Download (skip if already downloaded)
    if [ -f "$temp_file" ]; then
        echo "Archive already downloaded, skipping download..."
    else
        if command -v curl &> /dev/null; then
            curl -L -o "$temp_file" "$url"
        elif command -v wget &> /dev/null; then
            wget -O "$temp_file" "$url"
        else
            echo "Error: Neither curl nor wget is available" >&2
            exit 1
        fi
    fi

    echo "Extracting..."

    # Remove existing zig directory if present
    rm -rf "$ZIG_DIR"

    # Extract
    tar -xf "$temp_file" -C "$COMPILER_DIR"

    # Rename extracted folder to 'zig'
    mv "$COMPILER_DIR/$archive_name" "$ZIG_DIR"

    # Clean up
    rm -f "$temp_file"

    echo "Zig installed successfully to $ZIG_DIR"
}

# Main logic
main() {
    local os=$(detect_os)

    if [ "$os" = "unknown" ]; then
        echo "Error: Unsupported operating system" >&2
        exit 1
    fi

    local version=$(get_version)
    echo "Zig version from .zigversion: $version"

    # Check if zig executable exists
    if [ -x "$ZIG_EXE" ]; then
        echo "Zig already installed at $ZIG_EXE"
        exit 0
    fi

    echo "Zig not found. Downloading..."
    download_zig "$version" "$os"
}

main "$@"
