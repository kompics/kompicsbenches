// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

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

pub trait BenchmarkMaster {
    fn check_in(&self, o: ::grpc::RequestOptions, p: super::distributed::ClientInfo) -> ::grpc::SingleResponse<super::distributed::CheckinResponse>;
}

// client

pub struct BenchmarkMasterClient {
    grpc_client: ::std::sync::Arc<::grpc::Client>,
    method_CheckIn: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::distributed::ClientInfo, super::distributed::CheckinResponse>>,
}

impl ::grpc::ClientStub for BenchmarkMasterClient {
    fn with_client(grpc_client: ::std::sync::Arc<::grpc::Client>) -> Self {
        BenchmarkMasterClient {
            grpc_client: grpc_client,
            method_CheckIn: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkMaster/CheckIn".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
        }
    }
}

impl BenchmarkMaster for BenchmarkMasterClient {
    fn check_in(&self, o: ::grpc::RequestOptions, p: super::distributed::ClientInfo) -> ::grpc::SingleResponse<super::distributed::CheckinResponse> {
        self.grpc_client.call_unary(o, p, self.method_CheckIn.clone())
    }
}

// server

pub struct BenchmarkMasterServer;


impl BenchmarkMasterServer {
    pub fn new_service_def<H : BenchmarkMaster + 'static + Sync + Send + 'static>(handler: H) -> ::grpc::rt::ServerServiceDefinition {
        let handler_arc = ::std::sync::Arc::new(handler);
        ::grpc::rt::ServerServiceDefinition::new("/kompics.benchmarks.BenchmarkMaster",
            vec![
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkMaster/CheckIn".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.check_in(o, p))
                    },
                ),
            ],
        )
    }
}

// interface

pub trait BenchmarkClient {
    fn setup(&self, o: ::grpc::RequestOptions, p: super::distributed::SetupConfig) -> ::grpc::SingleResponse<super::distributed::SetupResponse>;

    fn cleanup(&self, o: ::grpc::RequestOptions, p: super::distributed::CleanupInfo) -> ::grpc::SingleResponse<super::distributed::CleanupResponse>;

    fn shutdown(&self, o: ::grpc::RequestOptions, p: super::messages::ShutdownRequest) -> ::grpc::SingleResponse<super::messages::ShutdownAck>;
}

// client

pub struct BenchmarkClientClient {
    grpc_client: ::std::sync::Arc<::grpc::Client>,
    method_Setup: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::distributed::SetupConfig, super::distributed::SetupResponse>>,
    method_Cleanup: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::distributed::CleanupInfo, super::distributed::CleanupResponse>>,
    method_Shutdown: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::messages::ShutdownRequest, super::messages::ShutdownAck>>,
}

impl ::grpc::ClientStub for BenchmarkClientClient {
    fn with_client(grpc_client: ::std::sync::Arc<::grpc::Client>) -> Self {
        BenchmarkClientClient {
            grpc_client: grpc_client,
            method_Setup: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkClient/Setup".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_Cleanup: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkClient/Cleanup".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_Shutdown: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkClient/Shutdown".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
        }
    }
}

impl BenchmarkClient for BenchmarkClientClient {
    fn setup(&self, o: ::grpc::RequestOptions, p: super::distributed::SetupConfig) -> ::grpc::SingleResponse<super::distributed::SetupResponse> {
        self.grpc_client.call_unary(o, p, self.method_Setup.clone())
    }

    fn cleanup(&self, o: ::grpc::RequestOptions, p: super::distributed::CleanupInfo) -> ::grpc::SingleResponse<super::distributed::CleanupResponse> {
        self.grpc_client.call_unary(o, p, self.method_Cleanup.clone())
    }

    fn shutdown(&self, o: ::grpc::RequestOptions, p: super::messages::ShutdownRequest) -> ::grpc::SingleResponse<super::messages::ShutdownAck> {
        self.grpc_client.call_unary(o, p, self.method_Shutdown.clone())
    }
}

// server

pub struct BenchmarkClientServer;


impl BenchmarkClientServer {
    pub fn new_service_def<H : BenchmarkClient + 'static + Sync + Send + 'static>(handler: H) -> ::grpc::rt::ServerServiceDefinition {
        let handler_arc = ::std::sync::Arc::new(handler);
        ::grpc::rt::ServerServiceDefinition::new("/kompics.benchmarks.BenchmarkClient",
            vec![
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkClient/Setup".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.setup(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkClient/Cleanup".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.cleanup(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkClient/Shutdown".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.shutdown(o, p))
                    },
                ),
            ],
        )
    }
}
