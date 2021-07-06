use crate::{
    benchmark::*,
    kompics_benchmarks::{distributed, distributed_grpc, messages},
};
use crossbeam::channel as cbchannel;
use futures::{future, sync::oneshot, Future};
use grpc::ClientStubExt;
use retry::{delay::Fixed, retry, OperationResult};
#[allow(unused_imports)]
use slog::{crit, debug, error, info, o, warn, Drain, Logger};
use std::{
    fmt,
    net::IpAddr,
    process,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

const MAX_ATTEMPTS: usize = 5;

pub fn run(
    service_address: IpAddr,
    service_port: u16,
    master_address: IpAddr,
    master_port: u16,
    benchmarks: Box<dyn BenchmarkFactory>,
    logger: Logger,
) -> () {
    let (command_sender, command_receiver) = cbchannel::unbounded();
    let mut inst = BenchmarkClient::new(
        logger.new(
            o!("service-address" => format!("{}",service_address), "service-port" => service_port, "ty" => "BenchmarkClient"),
        ),
        benchmarks,
        service_address.clone(),
        service_port,
        master_address,
        master_port,
        command_receiver,
    );

    // Client HANDLER

    let client_server_result = retry(Fixed::from_millis(500).take(10), || {
        let client_handler = ClientHandler::new(
            logger.new(o!("service-address" => format!("{}",service_address), "service-port" => service_port, "ty" => "ClientHandler")),
            command_sender.clone(),
        );
        let client_address = format!("{}:{}", service_address, service_port);
        match std::net::TcpListener::bind(client_address.clone()) {
            // FIXME workaround for httbis panic on bound socket in 0.7.0
            Ok(l) => {
                drop(l);
                let mut serverb = grpc::ServerBuilder::new_plain();
                let res: OperationResult<grpc::Server, String> =
                    match serverb.http.set_addr(client_address.clone()) {
                        Ok(_) => {
                            let service_def =
                                distributed_grpc::BenchmarkClientServer::new_service_def(
                                    client_handler,
                                );
                            serverb.add_service(service_def);
                            match serverb.build() {
                                Ok(server) => OperationResult::Ok(server),
                                Err(e) => OperationResult::Retry(format!(
                                    "Could not start client on {}: {}.",
                                    client_address, e
                                )),
                            }
                        },
                        Err(e) => OperationResult::Err(format!(
                            "Could not read client address {}: {}",
                            client_address, e
                        )),
                    };
                res
            },
            Err(e) => OperationResult::Retry(format!(
                "Could not bind to client address {}: {}",
                client_address, e
            )),
        }
    });

    let client_server = client_server_result.expect("client server");

    info!(logger, "ClientServer running on {}", client_server.local_addr());

    inst.start()
}

enum ClientCommand {
    Setup(distributed::SetupConfig, oneshot::Sender<distributed::SetupResponse>),
    Cleanup(distributed::CleanupInfo, oneshot::Sender<distributed::CleanupResponse>),
    Shutdown,
}
impl ClientCommand {
    fn from_setup(
        sc: distributed::SetupConfig,
    ) -> (ClientCommand, oneshot::Receiver<distributed::SetupResponse>) {
        let (p, f) = oneshot::channel();
        let cmd = ClientCommand::Setup(sc, p);
        (cmd, f)
    }

    fn from_cleanup(
        sc: distributed::CleanupInfo,
    ) -> (ClientCommand, oneshot::Receiver<distributed::CleanupResponse>) {
        let (p, f) = oneshot::channel();
        let cmd = ClientCommand::Cleanup(sc, p);
        (cmd, f)
    }
}

struct BenchmarkClient {
    logger:           Logger,
    state:            StateHolder,
    benchmarks:       Box<dyn BenchmarkFactory>,
    service_address:  IpAddr,
    service_port:     u16,
    master_address:   IpAddr,
    master_port:      u16,
    checkin_attempts: usize,
    command_queue:    cbchannel::Receiver<ClientCommand>,
}

impl BenchmarkClient {
    fn new(
        logger: Logger,
        benchmarks: Box<dyn BenchmarkFactory>,
        service_address: IpAddr,
        service_port: u16,
        master_address: IpAddr,
        master_port: u16,
        command_queue: cbchannel::Receiver<ClientCommand>,
    ) -> BenchmarkClient {
        BenchmarkClient {
            logger,
            state: StateHolder::init(),
            benchmarks,
            service_address,
            service_port,
            master_address,
            master_port,
            checkin_attempts: 0,
            command_queue,
        }
    }

    #[allow(dead_code)]
    fn state(&self) -> StateHolder { self.state.clone() }

    fn start(&mut self) -> () {
        info!(self.logger, "Starting...");
        while self.state.matches(State::CheckingIn) {
            let f = self.checkin();
            match f.wait() {
                Ok((_resp, stub)) => {
                    info!(self.logger, "Connected to master!");
                    self.state.cas(State::CheckingIn, State::Ready).expect("Was already ready?!?");
                    drop(stub);
                },
                Err(e) => {
                    warn!(self.logger, "Could not connect to master: {:?}", e);
                    if self.checkin_attempts < MAX_ATTEMPTS {
                        info!(self.logger, "Retrying connection...");
                        thread::sleep(Duration::from_millis(500));
                    } else {
                        error!(self.logger, "Giving up on Master and shutting down.");
                        process::exit(1);
                    }
                },
            }
        }
        loop {
            if self.state.matches(State::Stopped) {
                return;
            }
            let cmd = self.command_queue.recv().expect("Queue to ClientService broke!");
            self.state.with_state(|state| {
                match cmd {
                    ClientCommand::Setup(mut sc, promise) => {
                        if *state != State::Ready {
                            *state = State::Ready; // Clearly Check-In succeeded, even if the RPC was faulty
                        }
                        let test_label = sc.take_label();
                        info!(self.logger, "Getting benchmark by label {}", test_label);
                        let b_res = self.benchmarks.by_label(&test_label);
                        let client_data_res: Result<String, BenchmarkError> = b_res
                            .map_err(|e| BenchmarkError::NotImplemented(e))
                            .and_then(|b| match b {
                                AbstractBench::Local(_lb) => Err(BenchmarkError::InvalidTest(
                                    format!("Test {} is local!", test_label),
                                )),
                                AbstractBench::Distributed(db) => {
                                    let mut active_bench = ActiveBench::new(db);
                                    let client_data_res = active_bench.setup(sc);
                                    client_data_res.map(|client_data| {
                                        active_bench.prepare();
                                        *state = State::Running(active_bench);
                                        info!(self.logger, "{} is set up.", test_label);
                                        client_data
                                    })
                                },
                            });
                        match client_data_res {
                            Ok(client_data) => {
                                let mut sr = distributed::SetupResponse::new();
                                sr.set_success(true);
                                sr.set_data(client_data);
                                promise.send(sr).expect("Promise channel was broken!")
                            },
                            Err(e) => {
                                let error_msg = format!("{:?}", e);
                                error!(
                                    self.logger,
                                    "Setup for test {} was not successful: {}",
                                    test_label,
                                    error_msg
                                );
                                let mut sr = distributed::SetupResponse::new();
                                sr.set_success(false);
                                sr.set_data(error_msg);
                                promise.send(sr).expect("Promise channel was broken!");
                            },
                        }
                    },
                    ClientCommand::Cleanup(ci, promise) => match state {
                        State::Running(active_bench) => {
                            let test_label = active_bench.label();
                            debug!(self.logger, "Cleaning active bench.");
                            if ci.get_field_final() {
                                active_bench.cleanup(true);
                                *state = State::Ready;
                                info!(self.logger, "{} is cleaned.", test_label);
                            } else {
                                active_bench.cleanup(false);
                                active_bench.prepare();
                            }
                            promise
                                .send(distributed::CleanupResponse::new())
                                .expect("Promise channel was broken!");
                        },
                        _ => panic!("Invalid state for Cleanup message!"),
                    },
                    ClientCommand::Shutdown => {
                        info!(self.logger, "Shutting down...");
                        *state = State::Stopped;
                        thread::sleep(Duration::from_millis(500)); // give it some time to send the response
                    },
                }
            })
        }
    }

    fn checkin(
        &mut self,
    ) -> impl Future<
        Item = (distributed::CheckinResponse, distributed_grpc::BenchmarkMasterClient),
        Error = ::grpc::Error,
    > + '_ {
        self.checkin_attempts += 1;
        info!(self.logger, "Check-In connection attempt #{}...", self.checkin_attempts);
        let master_addr_string = format!("{}", self.master_address);
        let stub_res = distributed_grpc::BenchmarkMasterClient::new_plain(
            &master_addr_string,
            self.master_port,
            Default::default(),
        );
        let stub_f = future::result(stub_res);
        //.expect(&format!("Could not connect to master {:?}:{:?}", self.master_address, self.master_port));
        stub_f.and_then(move |stub| {
            info!(self.logger, "Connected to Master, checking in...");
            let mut ci = distributed::ClientInfo::new();
            let service_addr_string = format!("{}", self.service_address);
            ci.set_address(service_addr_string);
            ci.set_port(self.service_port as u32);
            let res = distributed_grpc::BenchmarkMaster::check_in(
                &stub,
                ::grpc::RequestOptions::default(),
                ci,
            )
            .drop_metadata();
            res.map(|r| (r, stub))
        })
    }
}

struct ClientHandler {
    logger:        Logger,
    command_queue: cbchannel::Sender<ClientCommand>,
}

impl ClientHandler {
    fn new(logger: Logger, command_queue: cbchannel::Sender<ClientCommand>) -> ClientHandler {
        ClientHandler { logger, command_queue }
    }
}

impl distributed_grpc::BenchmarkClient for ClientHandler {
    fn setup(
        &self,
        _o: ::grpc::RequestOptions,
        p: distributed::SetupConfig,
    ) -> ::grpc::SingleResponse<distributed::SetupResponse> {
        let (cmd, f) = ClientCommand::from_setup(p);
        self.command_queue.send(cmd).expect("Command channel broke!");
        grpc::SingleResponse::no_metadata(f.map_err(|c| c.into()))
    }

    fn cleanup(
        &self,
        _o: ::grpc::RequestOptions,
        p: distributed::CleanupInfo,
    ) -> ::grpc::SingleResponse<distributed::CleanupResponse> {
        let (cmd, f) = ClientCommand::from_cleanup(p);
        self.command_queue.send(cmd).expect("Command channel broke!");
        grpc::SingleResponse::no_metadata(f.map_err(|c| c.into()))
    }

    fn shutdown(
        &self,
        _o: ::grpc::RequestOptions,
        p: messages::ShutdownRequest,
    ) -> ::grpc::SingleResponse<messages::ShutdownAck> {
        info!(self.logger, "Got shutdown request: {:?}", p);
        if p.force {
            crate::force_shutdown();
        } else {
            self.command_queue.send(ClientCommand::Shutdown).expect("Command channel broke!");
        }
        grpc::SingleResponse::completed(messages::ShutdownAck::new())
    }
}

#[derive(Clone)]
struct StateHolder(Arc<Mutex<State>>);

impl StateHolder {
    fn init() -> StateHolder { StateHolder(Arc::new(Mutex::new(State::CheckingIn))) }

    #[allow(dead_code)]
    fn assign(&self, v: State) -> () {
        let mut state = self.0.lock().unwrap();
        *state = v;
    }

    fn cas(&self, old_value: State, new_value: State) -> Result<(), StateError> {
        let mut state = self.0.lock().unwrap();
        if *state == old_value {
            *state = new_value;
            Ok(())
        } else {
            Err(StateError::InvalidTransition {
                from:     format!("{:?}", *state),
                to:       format!("{:?}", new_value),
                expected: format!("{:?}", old_value),
            })
        }
    }

    fn matches(&self, v: State) -> bool {
        let state = self.0.lock().unwrap();
        *state == v
    }

    fn with_state<F>(&self, f: F) -> ()
    where F: FnOnce(&mut State) -> () {
        let mut state = self.0.lock().unwrap();
        f(&mut state)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    CheckingIn,
    Ready,
    Running(ActiveBench),
    Stopped,
}

#[derive(Debug, Clone)]
enum StateError {
    InvalidTransition { from: String, to: String, expected: String }, // to avoid clone on State
}

struct ActiveBench {
    b:        Box<dyn AbstractDistributedBenchmark>,
    instance: Box<dyn AbstractBenchmarkClient>,
}
impl ActiveBench {
    fn new(b: Box<dyn AbstractDistributedBenchmark>) -> ActiveBench {
        let instance = b.new_client();
        ActiveBench { b, instance }
    }

    fn setup(&mut self, sc: distributed::SetupConfig) -> Result<String, BenchmarkError> {
        let client_data_res = self.instance.setup(sc.data.into());
        client_data_res.map(|client_data| client_data.into())
    }

    fn prepare(&mut self) -> () { self.instance.prepare_iteration(); }

    fn cleanup(&mut self, last_iteration: bool) -> () {
        self.instance.cleanup_iteration(last_iteration);
    }

    fn label(&self) -> &'static str { self.b.label() }
}
impl fmt::Debug for ActiveBench {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActiveBench({})", self.b.label())
    }
}
impl PartialEq for ActiveBench {
    fn eq(&self, other: &ActiveBench) -> bool { self.b.label() == other.b.label() }
}
impl Eq for ActiveBench {}
