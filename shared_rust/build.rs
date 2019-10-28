use protoc_rust_grpc;

fn main() {
    protoc_rust_grpc::run(protoc_rust_grpc::Args {
        out_dir: "src/kompics_benchmarks",
        includes: &["../proto"],
        input: &[
            "../proto/messages.proto",
            "../proto/benchmarks.proto",
            "../proto/distributed.proto",
        ],
        rust_protobuf: true, // also generate protobuf messages, not just services
        ..Default::default()
    })
    .expect("protoc-rust-grpc");
}
