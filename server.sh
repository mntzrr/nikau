#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $SCRIPT_DIR

# allow passthrough of LOG_LEVEL env to server:
cargo build && sudo -E ./target/debug/nikau server $@
