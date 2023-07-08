#!/bin/sh

# allow passthrough of LOG_LEVEL env to client:
cargo build && sudo -E ./target/debug/nikau client 127.0.0.1
