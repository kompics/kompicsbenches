OS Specific Dependencies
========================

In order to run the experiments in this repository, you must have the following packages installed.

Rust
----
You need a nightly version of **Rust** and **Cargo**.

Best to use [Rustup](https://rustup.rs/) to get stuff installed.

- `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
`
- `rustup install nightly`
- `rustup default nightly`

If you want to make changes, you also need **Rustfmt** to properly format code before submitting a PR. You can install with `rustup component add rustfmt`. Run it on a project with `cargo fmt`.

Java
----
Any **JDK** should do, but usually **Oracle JDK** behaves in the most expected manner. For actual experiments, it may be wise to run on **JDK 8** instead of the newer ones, as many libraris rely on unsafe functions to be available for performance.

SBT
---
Both Scala and Java code is built with [SBT](https://www.scala-sbt.org/index.html) for convenience. Most distributions/OSs let you install that via their package management system.

Ammonite
--------
All the top level scripts are written in **Scala** using [Ammonite](https://ammonite.io/).

On *Linux* you can install it with:
```
sudo sh -c '(echo "#!/usr/bin/env sh" && curl -L https://github.com/lihaoyi/Ammonite/releases/download/1.6.7/2.12-1.6.7) > /usr/local/bin/amm && chmod +x /usr/local/bin/amm' && amm
```

On *MacOS* you can just do `brew install ammonite-repl` (assuming you have [Homebrew](https://brew.sh/)).

Protobuffers
------------
Much of the management-level communication of the testing framework relies on [Protocol Buffers](https://developers.google.com/protocol-buffers/) and [gRPC](https://grpc.io/).
While each implementations pulls in their own **gRPC** dependencies, you will need `protoc` (with version >= 3.6) on your `PATH`.

On *MacOS* once again homebrew provides an easy install path via `brew install protobuf`.

On *Linux* you have to find the right binaries for your architecture or simply build it from source.
Assuming you are on `x86_64` something like [this](https://github.com/kompics/kompact/blob/master/travis_install_protobuf.sh) may work.
