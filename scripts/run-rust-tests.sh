#!/bin/bash
set -ev
cd shared_rust
cargo test --verbose -- --test-threads=1
cd ..
cd kompact
cargo test --verbose --features travis_ci -- --test-threads=1
cd ..
cd actix
cargo test --verbose -- --test-threads=1
cd ..
cd riker
cargo test --verbose -- --test-threads=1
cd ..
