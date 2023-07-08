#!/bin/sh

# allow passthrough of LOG_LEVEL env to server:
cargo build && sudo -E ./target/debug/nikau server
