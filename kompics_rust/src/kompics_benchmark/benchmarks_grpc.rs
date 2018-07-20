// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]


// interface

pub trait BenchmarkRunner {
    fn ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::PingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;
}

// client

pub struct BenchmarkRunnerClient {
    grpc_client: ::grpc::Client,
    method_PingPong: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::PingPongRequest, super::messages::TestResult>>,
}

impl BenchmarkRunnerClient {
    pub fn with_client(grpc_client: ::grpc::Client) -> Self {
        BenchmarkRunnerClient {
            grpc_client: grpc_client,
            method_PingPong: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/PingPong".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
        }
    }

    pub fn new_plain(host: &str, port: u16, conf: ::grpc::ClientConf) -> ::grpc::Result<Self> {
        ::grpc::Client::new_plain(host, port, conf).map(|c| {
            BenchmarkRunnerClient::with_client(c)
        })
    }
    pub fn new_tls<C : ::tls_api::TlsConnector>(host: &str, port: u16, conf: ::grpc::ClientConf) -> ::grpc::Result<Self> {
        ::grpc::Client::new_tls::<C>(host, port, conf).map(|c| {
            BenchmarkRunnerClient::with_client(c)
        })
    }
}

impl BenchmarkRunner for BenchmarkRunnerClient {
    fn ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::PingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_PingPong.clone())
    }
}

// server

pub struct BenchmarkRunnerServer;


impl BenchmarkRunnerServer {
    pub fn new_service_def<H : BenchmarkRunner + 'static + Sync + Send + 'static>(handler: H) -> ::grpc::rt::ServerServiceDefinition {
        let handler_arc = ::std::sync::Arc::new(handler);
        ::grpc::rt::ServerServiceDefinition::new("/kompics.benchmarks.BenchmarkRunner",
            vec![
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/PingPong".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.ping_pong(o, p))
                    },
                ),
            ],
        )
    }
}
