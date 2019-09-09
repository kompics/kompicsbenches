#!/bin/bash
set -ev
cd shared_rust
cargo test --verbose
cd ..
cd kompact
cargo test --verbose
cd ..
cd actix
cargo test --verbose
cd ..
