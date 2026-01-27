#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ZIG_EXE="$PROJECT_DIR/compiler/zig/zig"

# Ensure zig is installed
if [ ! -x "$ZIG_EXE" ]; then
    echo "Zig not found. Running install script..."
    "$SCRIPT_DIR/install.sh"
fi

# Run zig with arguments, default to "build run"
if [ $# -eq 0 ]; then
    "$ZIG_EXE" build run
else
    "$ZIG_EXE" "$@"
fi
