#!/bin/bash
set -ev
cd shared_rust
exec cargo test --verbose
cd ..
cd kompact
exec cargo test --verbose
cd ..
cd actix
exec cargo test --verbose
cd ..
