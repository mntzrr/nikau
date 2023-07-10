#!/bin/sh

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $SCRIPT_DIR

if [ -z "$1" ]; then
    echo "Syntax: $0 <server-host>"
    exit 1
fi

# allow passthrough of LOG_LEVEL env to client:
cargo build && sudo -E ./target/debug/nikau client $1
