extern crate protoc_rust_grpc;

fn main() {
    protoc_rust_grpc::run(protoc_rust_grpc::Args {
        out_dir: "src/kompact_benchmark",
        includes: &["../proto"],
        input: &["../proto/messages.proto", "../proto/benchmarks.proto"],
        rust_protobuf: true, // also generate protobuf messages, not just services
        ..Default::default()
    }).expect("protoc-rust-grpc");
}
