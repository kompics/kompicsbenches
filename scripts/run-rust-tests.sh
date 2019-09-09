#!/bin/bash
set -ev
cd shared_rust
cargo test --verbose
cd ..
cd kompact
cargo test --verbose --features travis_ci
cd ..
cd actix
cargo test --verbose
cd ..
