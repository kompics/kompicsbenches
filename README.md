MPP -- The Message-passing Perfomance Suite
===========================================
[![Build Status](https://travis-ci.com/kompics/kompicsbenches.svg?branch=master)](https://travis-ci.com/kompics/kompicsbenches)

MPP is a cross-language benchmark suite for message-passing frameworks.

Currently Supported Frameworks
------------------------------

### Rust

- [Kompact](https://crates.io/crates/kompact)
- [Actix](https://crates.io/crates/actix)

### Scala/Java

- [Kompics](http://kompics.sics.se) (Scala and Java)
- [Akka](http://akka.io) (Scala only, but both Typed and "normal" Actors)

### Erlang
Release OTP 22


Requirements
------------

- Rust nightly
- JDK (any version should do)
- SBT
- Ammonite
- Erlang OTP
- protobuf 3.6 (in particular protoc)

See [OS Specific Dependencies](OS_Specific_Dependencies.md) for more details.
