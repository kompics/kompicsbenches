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

pub trait BenchmarkRunner {
    fn ready(&self, o: ::grpc::RequestOptions, p: super::messages::ReadyRequest) -> ::grpc::SingleResponse<super::messages::ReadyResponse>;

    fn shutdown(&self, o: ::grpc::RequestOptions, p: super::messages::ShutdownRequest) -> ::grpc::SingleResponse<super::messages::ShutdownAck>;

    fn ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::PingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn net_ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::PingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn throughput_ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::ThroughputPingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn net_throughput_ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::ThroughputPingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn atomic_register(&self, o: ::grpc::RequestOptions, p: super::benchmarks::AtomicRegisterRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn streaming_windows(&self, o: ::grpc::RequestOptions, p: super::benchmarks::StreamingWindowsRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn fibonacci(&self, o: ::grpc::RequestOptions, p: super::benchmarks::FibonacciRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn chameneos(&self, o: ::grpc::RequestOptions, p: super::benchmarks::ChameneosRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn all_pairs_shortest_path(&self, o: ::grpc::RequestOptions, p: super::benchmarks::APSPRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn atomic_broadcast(&self, o: ::grpc::RequestOptions, p: super::benchmarks::AtomicBroadcastRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;

    fn sized_throughput(&self, o: ::grpc::RequestOptions, p: super::benchmarks::SizedThroughputRequest) -> ::grpc::SingleResponse<super::messages::TestResult>;
}

// client

pub struct BenchmarkRunnerClient {
    grpc_client: ::std::sync::Arc<::grpc::Client>,
    method_Ready: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::messages::ReadyRequest, super::messages::ReadyResponse>>,
    method_Shutdown: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::messages::ShutdownRequest, super::messages::ShutdownAck>>,
    method_PingPong: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::PingPongRequest, super::messages::TestResult>>,
    method_NetPingPong: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::PingPongRequest, super::messages::TestResult>>,
    method_ThroughputPingPong: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::ThroughputPingPongRequest, super::messages::TestResult>>,
    method_NetThroughputPingPong: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::ThroughputPingPongRequest, super::messages::TestResult>>,
    method_AtomicRegister: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::AtomicRegisterRequest, super::messages::TestResult>>,
    method_StreamingWindows: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::StreamingWindowsRequest, super::messages::TestResult>>,
    method_Fibonacci: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::FibonacciRequest, super::messages::TestResult>>,
    method_Chameneos: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::ChameneosRequest, super::messages::TestResult>>,
    method_AllPairsShortestPath: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::APSPRequest, super::messages::TestResult>>,
    method_AtomicBroadcast: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::AtomicBroadcastRequest, super::messages::TestResult>>,
    method_SizedThroughput: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::benchmarks::SizedThroughputRequest, super::messages::TestResult>>,
}

impl ::grpc::ClientStub for BenchmarkRunnerClient {
    fn with_client(grpc_client: ::std::sync::Arc<::grpc::Client>) -> Self {
        BenchmarkRunnerClient {
            grpc_client: grpc_client,
            method_Ready: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/Ready".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_Shutdown: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/Shutdown".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_PingPong: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/PingPong".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_NetPingPong: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/NetPingPong".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_ThroughputPingPong: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/ThroughputPingPong".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_NetThroughputPingPong: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/NetThroughputPingPong".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_AtomicRegister: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/AtomicRegister".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_StreamingWindows: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/StreamingWindows".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_Fibonacci: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/Fibonacci".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_Chameneos: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/Chameneos".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_AllPairsShortestPath: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/AllPairsShortestPath".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_AtomicBroadcast: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/AtomicBroadcast".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_SizedThroughput: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/kompics.benchmarks.BenchmarkRunner/SizedThroughput".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
        }
    }
}

impl BenchmarkRunner for BenchmarkRunnerClient {
    fn ready(&self, o: ::grpc::RequestOptions, p: super::messages::ReadyRequest) -> ::grpc::SingleResponse<super::messages::ReadyResponse> {
        self.grpc_client.call_unary(o, p, self.method_Ready.clone())
    }

    fn shutdown(&self, o: ::grpc::RequestOptions, p: super::messages::ShutdownRequest) -> ::grpc::SingleResponse<super::messages::ShutdownAck> {
        self.grpc_client.call_unary(o, p, self.method_Shutdown.clone())
    }

    fn ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::PingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_PingPong.clone())
    }

    fn net_ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::PingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_NetPingPong.clone())
    }

    fn throughput_ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::ThroughputPingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_ThroughputPingPong.clone())
    }

    fn net_throughput_ping_pong(&self, o: ::grpc::RequestOptions, p: super::benchmarks::ThroughputPingPongRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_NetThroughputPingPong.clone())
    }

    fn atomic_register(&self, o: ::grpc::RequestOptions, p: super::benchmarks::AtomicRegisterRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_AtomicRegister.clone())
    }

    fn streaming_windows(&self, o: ::grpc::RequestOptions, p: super::benchmarks::StreamingWindowsRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_StreamingWindows.clone())
    }

    fn fibonacci(&self, o: ::grpc::RequestOptions, p: super::benchmarks::FibonacciRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_Fibonacci.clone())
    }

    fn chameneos(&self, o: ::grpc::RequestOptions, p: super::benchmarks::ChameneosRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_Chameneos.clone())
    }

    fn all_pairs_shortest_path(&self, o: ::grpc::RequestOptions, p: super::benchmarks::APSPRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_AllPairsShortestPath.clone())
    }

    fn atomic_broadcast(&self, o: ::grpc::RequestOptions, p: super::benchmarks::AtomicBroadcastRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_AtomicBroadcast.clone())
    }

    fn sized_throughput(&self, o: ::grpc::RequestOptions, p: super::benchmarks::SizedThroughputRequest) -> ::grpc::SingleResponse<super::messages::TestResult> {
        self.grpc_client.call_unary(o, p, self.method_SizedThroughput.clone())
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
                        name: "/kompics.benchmarks.BenchmarkRunner/Ready".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.ready(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/Shutdown".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.shutdown(o, p))
                    },
                ),
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
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/NetPingPong".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.net_ping_pong(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/ThroughputPingPong".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.throughput_ping_pong(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/NetThroughputPingPong".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.net_throughput_ping_pong(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/AtomicRegister".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.atomic_register(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/StreamingWindows".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.streaming_windows(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/Fibonacci".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.fibonacci(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/Chameneos".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.chameneos(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/AllPairsShortestPath".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.all_pairs_shortest_path(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/AtomicBroadcast".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.atomic_broadcast(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/kompics.benchmarks.BenchmarkRunner/SizedThroughput".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.sized_throughput(o, p))
                    },
                ),
            ],
        )
    }
}
