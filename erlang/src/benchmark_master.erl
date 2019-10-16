-module(benchmark_master).

-behaviour(gen_statem).

-export([start_link/1, start_runner/1]).
-export([check_in/1,get_state/0, shutdown_all/1, request_bench/1, new_bench_request/2]).
-export([init/1, callback_mode/0, handle_event/4]).

-define(GLOBAl_NAME, {global, benchmark_master}).
-define(TIMEOUT, 1000).

-record(client_entry, {node :: node()}).
-type client_entry() :: #client_entry{}.

-record(state, {
	wait_for :: integer(),
	clients = [] :: [client_entry()]}).
-type state() :: #state{}.

-type master_state() :: 'INIT' | 'READY' | 'SETUP' | 'RUN' | 'CLEANUP' | 'FINISHED' | 'STOPPED'.

-record(bench_request, {
	bench_module :: module(),
	params :: term(),
	complete :: futures:promise()}).

-opaque bench_request() :: #bench_request{}.


-export_type([master_state/0, bench_request/0]).

%%%%% Public Type Constructors %%%%%

-spec new_bench_request(BenchModule :: module(), Params :: term()) -> {ok, bench_request(), futures:future()}.
new_bench_request(BenchModule, Params) ->
	{Future, Promise} = futures:promise_pair(),
	Request = #bench_request{bench_module = BenchModule, params = Params, complete = Promise},
	{ok, Request, Future}.

%%%%% Public API %%%%%

-spec start_link(Args :: any()) ->
	{ok, Pid :: pid()} |
	ignore |
	{error, Error :: {already_started, Pid :: pid()} | term()}.
start_link(Args) ->
    gen_statem:start_link(?GLOBAl_NAME, ?MODULE, Args, []).

-spec start_runner(RunnerAddr :: socket_addr:ip()) -> ok.
start_runner(RunnerAddr) ->
	RunnerPort = socket_addr:port(RunnerAddr),
	{ok, _Server} = grpc:start_server(
		runner_server,
		tcp,
		RunnerPort,
		#{'BenchmarkRunner' => #{handler => benchmarks_server_master}}),
	io:fwrite("Benchmark server is running on TCP port=~b.~n",[RunnerPort]),
	ok.

-spec check_in(ClientInfo :: distributed_native:'ClientInfo'()) ->
	distributed_native:'CheckinResponse'() |
	{error, Reason :: term()}.
check_in(ClientInfo) ->
    try gen_statem:call(?GLOBAl_NAME, {'CheckIn', ClientInfo}, ?TIMEOUT) of
    	{'CheckinResponse', Content} ->
    		Content;
    	Other ->
    		{error, Other}
    catch
    	Error -> {error, Error}
    end.

-spec get_state() ->
	benchmark_master:master_state() |
	{error, Reason :: term()}.
get_state() ->
    try gen_statem:call(?GLOBAl_NAME, master_state, ?TIMEOUT) of
    	{master_state, Content} ->
    		Content;
    	Other ->
    		{error, Other}
    catch
    	Error ->
    		{error, Error}
    end.

-spec shutdown_all(Request :: distributed_native:'ShutdownRequest'()) ->
	ok |
	{error, Reason :: term()}.
shutdown_all(Request) ->
	try gen_statem:call(?GLOBAl_NAME, {'Shutdown', Request}, ?TIMEOUT*10) of
    	ok ->
    		ok;
    	Other ->
    		{error, Other}
    catch
    	Error ->
    		{error, Error}
    end.

-spec request_bench(BenchRequest :: bench_request()) -> ok.
request_bench(BenchRequest) ->
	gen_statem:cast(?GLOBAl_NAME, {bench, BenchRequest}).

%%%%% gen_statem impl %%%%%

% -spec callback_mode() ->
% 	gen_statem:callback_mode() |
% 	[ gen_statem:callback_mode() | gen_statem:state_enter() ].
callback_mode() ->
	handle_event_function.

-spec init(Args :: any()) -> {ok, State :: master_state(), Data :: state()}.
init(Args) ->
	case Args of
		#{wait_for := WaitFor} when is_integer(WaitFor) andalso (WaitFor >= 0) ->
			Data = #state{wait_for = WaitFor},
    		{ok, 'INIT', Data};
    	#{wait_for := WaitFor} ->
    		{stop, {invalid_args, WaitFor}};
    	_ ->
    		{stop, {invalid_args, Args}}
    end.

handle_event({call, From}, EventContent, State, Data) ->
	case EventContent of
		{'CheckIn', ClientInfo} ->
			do_check_in(ClientInfo, From, State, Data);
		master_state ->
			{keep_state_and_data, {reply, From, {master_state, State}}};
		{'Shutdown', Request} ->
			do_shutdown(Request, From, State, Data);
		Other ->
			io:fwrite("Got unexpected call with content ~p.~n", [Other]),
			keep_state_and_data
	end;
handle_event(cast, EventContent, State, Data) ->
	case EventContent of
		{bench, BenchRequest} when State == 'READY' ->
			do_bench(BenchRequest, State, Data);
		{bench, _} ->
			{keep_state_and_data, [postpone]};
		Other ->
			io:fwrite("Got unexpected cast with content ~p.~n", [Other]),
			keep_state_and_data
	end;
handle_event(EventType, EventContent, _State, _Data) ->
	io:fwrite("Got unexpected event type ~p with content ~p.~n", [EventType, EventContent]),
	keep_state_and_data.

%%%%% Event Handlers %%%%%

% -spec do_check_in(ClientInfo :: 'ClientInfo'(), From :: from(), State :: master_state(), Data :: state()) ->
% 	{next_state, master_state(), state(), {reply, pid(), {'CheckinResponse', distributed:'CheckinResponse'()}}} |
% 	{keep_state, state(), {reply, pid(), {'CheckinResponse', distributed:'CheckinResponse'()}}} |
% 	{keep_state_and_data, {reply, pid(), {'CheckinResponse', distributed:'CheckinResponse'()}}}.
-spec do_check_in(ClientInfo :: distributed_native:'ClientInfo'(), From :: gen_statem_ext:from(), State :: master_state(), Data :: state()) ->
	gen_statem:event_handler_result(master_state()).
do_check_in(ClientInfo, From, State, Data) ->
	io:fwrite("Got check in from ~w with args ~p.~n", [From, ClientInfo]),
	NewData = Data#state{clients = [#client_entry{node = ClientInfo} | Data#state.clients]},
	NumCheckIns = length(NewData#state.clients),
	Reply = {reply, From, distributed_native:new_checkin_reponse_msg()},
	case State of
		'INIT' when NumCheckIns == NewData#state.wait_for ->
			io:fwrite("Got all ~b Check-Ins: Ready!", [NewData#state.wait_for]),
			{next_state, 'READY', NewData, Reply}; % In Scala we replay all the queued events here, but Erlang should do this automatically after a state change
		'INIT' ->
			io:fwrite("Got ~b/~b Check-Ins.", [NumCheckIns, NewData#state.wait_for]),
			{keep_state, NewData, Reply};
		_ ->
			io:fwrite("Ignoring late Check-In in state: ~w.~n", [State]),
			{keep_state_and_data, Reply}
	end.

% -spec do_shutdown(Request :: distributed:'ShutdownRequest'(), From :: from(), State :: master_state(), Data :: state()) ->
% 	{stop_and_reply, normal, {reply, pid(), {'ShutdownAck', distributed:'ShutdownAck'()}}}.
-spec do_shutdown(Request :: distributed_native:'ShutdownRequest'(), From :: gen_statem_ext:from(), State :: master_state(), Data :: state()) ->
	gen_statem:event_handler_result(master_state()).
do_shutdown(Request, From, _State, Data) ->
	io:fwrite("Got shutdown request ~p.~n", [Request]),
	case benchmark_client:shutdown_all(get_clients(Data), Request) of
		ok ->
			io:fwrite("All clients shut down successfully!~n");
		{error, Reason} ->
			io:fwrite("Some clients failed to shut down in time: ~p.~n", [Reason])
	end,
	Reply = {reply, From, distributed_native:new_shutdown_ack_msg()},
	{stop_and_reply, normal, Reply}.

-spec do_bench(Request :: bench_request(), State :: master_state(), Data :: state()) ->
	gen_statem:event_handler_result(master_state()).
do_bench(Request, _State, Data) ->
	io:fwrite("Got bench request ~p.~n", [Request]),
	case bench_helpers:benchmark_type(Request#bench_request.bench_module) of
		local ->
			run_local_benchmark(Request);
		distributed ->
			run_distributed_benchmark(Request, Data);
		none ->
			complete_benchmark(Request, {error, not_a_benchmark})
	end,
	keep_state_and_data.

-spec complete_benchmark(Request :: bench_request(), Response :: {ok, [float()]} | {error, term()}) -> ok.
complete_benchmark(Request, Response) ->
	futures:complete(Request#bench_request.complete, Response).

-spec run_local_benchmark(Request :: bench_request()) -> ok.
run_local_benchmark(Request) ->
	io:fwrite("Starting local benchmark ~w with params ~p.~n", [Request#bench_request.bench_module, Request#bench_request.params]),
	Result = benchmark_runner:run(Request#bench_request.bench_module, Request#bench_request.params),
	io:fwrite("Finished local benchmark ~w with result: ~p.~n", [Request#bench_request.bench_module, Result]),
	complete_benchmark(Request, Result),
	ok.

-spec run_distributed_benchmark(Request :: bench_request(), Data :: state()) -> ok.
run_distributed_benchmark(Request, Data) ->
	io:fwrite("Starting distributed benchmark ~w with params ~p.~n", [Request#bench_request.bench_module, Request#bench_request.params]),
	try run_distributed_benchmark_caught(Request, Data) of
		{ok, ResultData} ->
			io:fwrite("Benchmark ~w completed with ~b runs.~n", [Request#bench_request.bench_module, length(ResultData)]),
			complete_benchmark(Request, {ok, ResultData});
		{error, Reason} ->
			io:fwrite("Benchmark ~w failed with reason: ~p.~n", [Request#bench_request.bench_module, Reason]),
			complete_benchmark(Request, {error, Reason})
	catch
		Error ->
			io:fwrite("Benchmark ~w failed with error: ~p.~n", [Request#bench_request.bench_module, Error]),
			complete_benchmark(Request, {error, Error})
	end.

-spec run_distributed_benchmark_caught(Request :: bench_request(), Data :: state()) ->
	{ok, ResultData :: [float()]} |
	{error, Reason :: term()}.
run_distributed_benchmark_caught(Request, Data) ->
	Benchmark = Request#bench_request.bench_module,
	ConfigMsg = Request#bench_request.params,
	case Benchmark:msg_to_master_conf(ConfigMsg) of
		{ok, MasterConfig} ->
			EmptyMaster = Benchmark:new_master(),
			NumClients = length(Data#state.clients),
			Meta = distributed_benchmark:meta_create(NumClients),
			case Benchmark:master_setup(EmptyMaster, MasterConfig, Meta) of
				{ok, SetupMaster, ClientConf} ->
					SetupConf = distributed_native:new_setup_config(Benchmark, ClientConf),
					ClientDataRes = benchmark_client:setup_all(get_clients(Data), SetupConf),
					io:fwrite("Got client data response:~p.~n", [ClientDataRes]),
					case ClientDataRes of
						{ok, ClientData} ->
							case loop_distributed_iteration(get_clients(Data), Benchmark, SetupMaster, ClientData, []) of
								{ok, _CleanedMaster, ResultData} ->
									io:fwrite("Finished ~w iterations.~n", [Benchmark]),
									{ok, ResultData};
								{error, Reason} ->
									{error, Reason}
							end;
						{error, Reason} ->
							{error, Reason}
					end;

				{error, Reason} -> {error, Reason}
			end;
		Other ->
			{error, Other}
	end.

-spec loop_distributed_iteration(Clients :: [node()], Benchmark :: module(), Master, ClientData :: [term()], ResultData :: [float()]) ->
	{ok, Master, ResultData :: [float()]} |
	{error, Reason :: term()}.
loop_distributed_iteration(Clients, Benchmark, SetupMaster, ClientData, ResultData) ->
	NRuns = length(ResultData),
	io:fwrite("Preparing run ~b.~n", [NRuns]),
	{ok, PreparedMaster} = Benchmark:master_prepare_iteration(SetupMaster, ClientData),
	io:fwrite("Starting run ~b.~n", [NRuns]),
	{Time, RunMaster} = measure(Benchmark, PreparedMaster),
	io:fwrite("Finished run ~b in ~w ms.~n", [NRuns, Time]),
	NewResultData = [Time | ResultData],
	NewNRuns = NRuns + 1,
	MAX_RUNS = benchmark_runner:max_runs(),
	if
		NewNRuns < MAX_RUNS ->
			LastIteration = (NewNRuns >= benchmark_runner:min_runs()) andalso (statistics:rse(NewResultData) =< benchmark_runner:rse_target()),
			io:fwrite("LastIteration=~w.~n", [LastIteration]),
			{ok, CleanupMaster} = Benchmark:master_cleanup_iteration(RunMaster, LastIteration, Time),
			case benchmark_client:cleanup_all(Clients, distributed_native:new_cleanup_info(LastIteration)) of
				ok when LastIteration == false ->
					loop_distributed_iteration(Clients, Benchmark, CleanupMaster, ClientData, NewResultData);
				ok when LastIteration == true ->
					{ok, CleanupMaster, NewResultData};
				Other ->
					Other
			end;
		true ->
			% still got to clean up, but results don't matter
			_ = Benchmark:master_cleanup_iteration(RunMaster, true, Time),
			_ = benchmark_client:cleanup_all(Clients, distributed_native:new_cleanup_info(true)),
			RSE = statistics:rse(NewResultData),
			{error, {exceeded_max_runs, RSE}}
	end.

-spec get_clients(Data :: state()) -> [node()].
get_clients(Data) ->
	lists:map(fun(Client) -> Client#client_entry.node end,Data#state.clients).

-spec measure(Benchmark :: module(), Instance) -> {float(), Instance}.
measure(Benchmark,Instance) ->
	{Time, {ok, RunInstance}} = timer:tc(Benchmark,master_run_iteration,[Instance]),
	TimeMillis = Time / 1000.0,
	{TimeMillis, RunInstance}.
