-module(benchmark_client).

-behaviour(gen_statem).

-export([start_link/0]).
-export([setup/2, setup_all/2, cleanup/2, cleanup_all/2, shutdown/2, shutdown_all/2]).
-export([init/1, callback_mode/0, handle_event/4]).

-define(LOCAL_NAME, {local, benchmark_client}).
-define(GLOBAL_NAME(Node), {benchmark_client, Node}).
-define(SIMPLE_NAME, benchmark_client).
-define(TIMEOUT, 1000).
-define(MAX_ATTEMPTS, 5).
-define(MOD(Instance), (Instance#bench_instance.bench_module)).

-record(bench_instance, {bench_module :: module(), instance :: term()}).
-type bench_instance() :: #bench_instance{}.

-type client_state() :: 
	{'CHECKING_IN', Attempts :: integer()} |
	'READY' |
	{'RUNNING', Instance :: bench_instance()} |
	'STOPPED'.

%%%%% Public API %%%%%

-spec start_link() -> 
	{ok, Pid :: pid()} | 
	ignore | 
	{error, Error :: {already_started, Pid :: pid()} | term()}.
start_link() ->
    gen_statem:start_link(?LOCAL_NAME, ?MODULE, [], []).

-spec setup(Node :: node(), Config :: distributed_native:'SetupConfig'()) -> 
	distributed_native:'SetupResponse'() |
	{error, Reason :: term()}.
setup(Node, Config) ->
	try gen_statem:call(?GLOBAL_NAME(Node), {'Setup', Config}, ?TIMEOUT) of
		{'SetupResponse', Content} ->
			Content;
		Other ->
			{error, Other}
	catch
		Error ->
			{error, Error}
	end.

-spec setup_all(Nodes :: [node()], Config :: distributed_native:'SetupConfig'()) -> 
	{ok, [ClientData :: term()]} |
	{error, Reason :: term()}.
setup_all(Nodes, Config) ->
	try gen_statem_ext:multi_call(Nodes, ?SIMPLE_NAME, {'Setup', Config}, ?TIMEOUT*10) of
		{Replies, []} ->
			Fun = fun(Acc, Entry) ->
				{_Node, Response} = Entry,
				case Response of
					{'SetupResponse', #{success := true, data := Data}} ->
						{ok, [Data | Acc]};
					{'SetupResponse', #{success := false, data := _Reason}} ->
						fail;
					_Other ->
						fail
				end
			end,
			case lists_ext:fail_fast_foldl(Fun, [], Replies) of
				{ok, Data} ->
					{ok, Data};
				{fail, Value} ->
					{error, {invalid_client_data, Value}}
			end;
		{_Replies, BadNodes} ->
			{error, {bad_nodes, BadNodes}}
	catch
		Error ->
			{error, Error}
	end.

-spec cleanup(Node :: node(), Info :: distributed_native:'CleanupInfo'()) -> 
	distributed_native:'CleanupResponse'() |
	{error, Reason :: term()}.
cleanup(Node, Info) ->
	try gen_statem:call(?GLOBAL_NAME(Node), {'Cleanup', Info}, ?TIMEOUT) of
		{'CleanupResponse', Content} ->
			Content;
		Other ->
			{error, Other}
	catch
		Error ->
			{error, Error}
	end.

-spec cleanup_all(Nodes :: [node()], Info :: distributed_native:'CleanupInfo'()) -> 
	ok |
	{error, Reason :: term()}.
cleanup_all(Nodes, Info) ->
	try gen_statem_ext:multi_call(Nodes, ?SIMPLE_NAME, {'Cleanup', Info}, ?TIMEOUT*10) of
		{Replies, []} ->
			Fun = fun(_Acc, Entry) ->
				{_Node, Response} = Entry,
				case Response of
					{'CleanupResponse', #{}} ->
						{ok, none};
					_Other ->
						fail
				end
			end,
			case lists_ext:fail_fast_foldl(Fun, none, Replies) of
				{ok, none} ->
					ok;
				{fail, Value} ->
					{error, {invalid_client_data, Value}}
			end;
		{_Replies, BadNodes} ->
			{error, {bad_nodes, BadNodes}}
	catch
		Error ->
			{error, Error}
	end.

-spec shutdown(Node :: node(), Request :: distributed_native:'ShutdownRequest'()) -> 
	distributed_native:'ShutdownAck'() |
	{error, Reason :: term()}.
shutdown(Node, Request) ->
	try gen_statem:call(?GLOBAL_NAME(Node), {'Shutdown', Request}, ?TIMEOUT) of
		{'ShutdownAck', Content} ->
			Content;
		Other ->
			{error, Other}
	catch
		Error ->
			{error, Error}
	end.

-spec shutdown_all(Nodes :: [node()], Info :: distributed_native:'ShutdownRequest'()) -> 
	ok |
	{error, Reason :: term()}.
shutdown_all(Nodes, Info) ->
	try gen_statem_ext:multi_call(Nodes, ?SIMPLE_NAME, {'Shutdown', Info}, ?TIMEOUT*10) of
		{Replies, []} ->
			Fun = fun(_Acc, Entry) ->
				{_Node, Response} = Entry,
				case Response of
					{'ShutdownAck', #{}} ->
						{ok, none};
					_Other ->
						fail
				end
			end,
			case lists_ext:fail_fast_foldl(Fun, none, Replies) of
				{ok, none} ->
					ok;
				{fail, Value} ->
					{error, {invalid_client_data, Value}}
			end;
		{_Replies, BadNodes} ->
			{error, {bad_nodes, BadNodes}}
	catch
		Error ->
			{error, Error}
	end.

%%%%% gen_statem impl %%%%%

% -spec callback_mode() -> 
% 	gen_statem:callback_mode() | 
% 	[ gen_statem:callback_mode() | gen_statem:state_enter() ].
callback_mode() ->
	handle_event_function.

-spec init(Args :: any()) -> {ok, State :: client_state(), 'undefined', Action :: gen_statem:action()}.
init(_Args) ->
	{ok, {'CHECKING_IN', 0}, undefined, {timeout, 1000, 'CheckIn'}}.

handle_event({call, From}, EventContent, State, _Data) ->
	case EventContent of
		{'Setup', SetupConfig} when State == 'READY' ->
			do_setup(SetupConfig, From);
		{'Setup', _} ->
			io:fwrite("Got Setup request, but wasn't ready (state=~w)! Ignoring.~n", [State]),
			keep_state_and_data;
		{'Cleanup', CleanupInfo} ->
			case State of
				{'RUNNING', Instance} ->
					do_cleanup(Instance, CleanupInfo, From);
				_ ->
					io:fwrite("Invalid cleanup call in state=~w.~n",[State]),
					{stop, illegal_transition}
			end;
		{'Shutdown', Request} ->
			do_shutdown(Request, From);
		Other ->
			io:fwrite("Got unexpected call with content ~p.~n", [Other]),
			keep_state_and_data
	end;
handle_event(timeout, EventContent, State, _Data) ->
	case EventContent of
		'CheckIn' ->
			case State of
				{'CHECKING_IN', Attempts} when Attempts =< ?MAX_ATTEMPTS ->
					do_check_in(Attempts);
				{'CHECKING_IN', _Attempts} ->
					io:fwrite("Maximum number of Check-In attempts exceeded! Taking down VM.~n"),
					init:stop(1);
				_ ->
					io:fwrite("Ignoring CheckIn timeout in state ~w.~n",[State]),
					keep_state_and_data
			end;
		Other ->
			io:fwrite("Got unexpected timeout with content ~p.~n", [Other]),
			keep_state_and_data
	end;
handle_event(EventType, EventContent, _State, _Data) ->
	io:fwrite("Got unexpected event type ~p with content ~p.~n", [EventType, EventContent]),
	keep_state_and_data.

%%%%% Event Handlers %%%%%

do_check_in(Attempts) ->
	case benchmark_master:check_in(node()) of
		#{} ->
			io:fwrite("Check-In successful!~n"),
			{next_state, 'READY', undefined};
		{error, Reason} ->
			io:fwrite("Check-In failed:~p.~n", [Reason]),
			{next_state, {'CHECKING_IN', Attempts + 1}, undefined, {timeout, 1000, 'CheckIn'}}
	end.

-spec do_setup(SetupConfig :: distributed:'SetupConfig'(), From :: gen_statem_ext:from()) -> 
	gen_statem:event_handler_result(client_state()).
do_setup(SetupConfig, From) ->
	io:fwrite("Got setup request:~p.~n",[SetupConfig]),
	case SetupConfig of
		#{label := BenchmarkModule, data := Config} ->
			case bench_helpers:benchmark_type(BenchmarkModule) of
				distributed ->
					Client = BenchmarkModule:new_client(),
					{ok, SetupClient, Data} = BenchmarkModule:client_setup(Client, Config),
					SetupResponse = distributed_native:new_setup_response_msg(true, Data),
					Reply = {reply, From, SetupResponse},
					gen_statem_ext:reply_multi(Reply),
					{ok, PreparedClient} = BenchmarkModule:client_prepare_iteration(SetupClient),
					Instance = #bench_instance{bench_module = BenchmarkModule, instance = PreparedClient},
					io:fwrite("Benchmark ~w is set up.~n", [BenchmarkModule]),
					{next_state, {'RUNNING', Instance}, undefined};
				Other ->
					io:fwrite("Invalid benchmark module ~w of type ~w.~n", [BenchmarkModule, Other]),
					SetupResponse = distributed_native:new_setup_response_msg(false, {invalid_bench, BenchmarkModule}),
					Reply = {reply, From, SetupResponse},
					gen_statem_ext:reply_multi(Reply),
					keep_state_and_data
			end;
		_Other ->
			io:fwrite("Invalid setup config ~p.~n", [SetupConfig]),
			SetupResponse = distributed_native:new_setup_response_msg(false, {invalid_config, SetupConfig}),
			Reply = {reply, From, SetupResponse},
			gen_statem_ext:reply_multi(Reply),
			keep_state_and_data
	end.

-spec do_cleanup(Instance :: bench_instance(), CleanupInfo :: distributed:'CleanupInfo'(), From :: gen_statem_ext:from()) -> 
	gen_statem:event_handler_result(client_state()).
do_cleanup(Instance, CleanupInfo, From) ->
	io:fwrite("Got cleanup request:~p.~n",[CleanupInfo]),
	case CleanupInfo of
		#{final := true} ->
			{ok, _} = ?MOD(Instance):client_cleanup_iteration(Instance#bench_instance.instance, true),
			Reply = {reply, From, distributed_native:new_cleanup_response_msg()},
			{next_state, 'READY', undefined, Reply};
		#{final := false} ->
			{ok, CleanedClientInstance} = ?MOD(Instance):client_cleanup_iteration(Instance#bench_instance.instance, false),
			{ok, PreparedClientInstance} = ?MOD(Instance):client_prepare_iteration(CleanedClientInstance),
			Reply = {reply, From, distributed_native:new_cleanup_response_msg()},
			gen_statem_ext:reply_multi(Reply),
			NewInstance = Instance#bench_instance{instance = PreparedClientInstance},
			{next_state, {'RUNNING', NewInstance}, undefined}
	end.

-spec do_shutdown(Request :: distributed:'ShutdownRequest'(), From :: gen_statem_ext:from()) -> 
	gen_statem:event_handler_result(client_state()).
do_shutdown(Request, From) ->
	io:fwrite("Got shutdown request ~p.~n", [Request]),
	Reply = {reply, From, distributed_native:new_shutdown_ack_msg()},
	gen_statem_ext:reply_multi(Reply),
	{stop, normal}.
