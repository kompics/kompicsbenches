-module(fibonacci_bench).

-behaviour(local_benchmark).

-export([
	new_instance/0,
	msg_to_conf/1,
	setup/2,
	prepare_iteration/1,
	run_iteration/1,
	cleanup_iteration/3
	]).

-record(conf, {fib_number :: integer()}).

-type conf() :: #conf{}.

-record(state, {
	fib_number = -1 :: integer(), 
	fib :: pid() | 'undefined'}).

-type instance() :: #state{}.

-spec new_instance() -> instance().
new_instance() -> 
	#state{}.

-spec msg_to_conf(Msg :: term()) -> {ok, conf()} | {error, string()}.
msg_to_conf(Msg) ->
	case Msg of
		#{fib_number := Fnum} when is_integer(Fnum) andalso (Fnum > 0) ->
			{ok, #conf{fib_number = Fnum}};
		#{fib_number := Fnum} ->
			{error, io_lib:fwrite("Invalid config parameter:~p", [Fnum])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p", [Msg])}
	end.


-spec setup(Instance :: instance(), Conf :: conf()) -> {ok, instance()}.
setup(Instance, Conf) ->
	NewInstance = Instance#state{fib_number = Conf#conf.fib_number},
	process_flag(trap_exit, true),
	{ok, NewInstance}.

-spec prepare_iteration(Instance :: instance()) -> {ok, instance()}.
prepare_iteration(Instance) -> 
	Self = self(),
	Fib= spawn_link(fun() -> fibonacci(Self) end),
	NewInstance = Instance#state{fib = Fib},
	{ok, NewInstance}.

-spec run_iteration(Instance :: instance()) -> {ok, instance()}.
run_iteration(Instance) ->
	Instance#state.fib ! new_fib_request(Instance#state.fib_number),
	receive
		{fib_response, _} ->
			{ok, Instance};
		X -> 
			io:fwrite("Got unexpected message during iteration: ~p!~n",[X]),
			throw(X)
	end.

-spec cleanup_iteration(Instance :: instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, instance()}.
cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
	{ok, _} = bench_helpers:await_exit(Instance#state.fib),
	NewInstance = Instance#state{fib = undefined},
	{ok, NewInstance}.


%%%%%% Messages %%%%%%
-type fib_request() :: {fib_request, integer()}.
-spec new_fib_request(N :: integer()) -> fib_request().
new_fib_request(N) ->
	{fib_request, N}.
-type fib_response() :: {fib_response, integer()}.
-spec new_fib_response(Value :: integer()) -> fib_response().
new_fib_response(Value) ->
	{fib_response, Value}.

%%%%%% Fibonacci %%%%%%
-spec fibonacci(Return :: pid()) -> ok.
fibonacci(Return) ->
	receive
		{fib_request, N} ->
			{ok, Value} = calc_fibonacci(N),
			Return ! new_fib_response(Value),
			ok; % stop here
		X ->
			io:fwrite("Fibonacci got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

-spec calc_fibonacci(N :: integer()) -> {ok, integer()}.
calc_fibonacci(N) when N =< 2 ->
	{ok, 1};
calc_fibonacci(N) ->
	Self = self(),
	F1 = spawn_link(fun() -> fibonacci(Self) end),
	F1 ! new_fib_request(N - 1),
	F2 = spawn_link(fun() -> fibonacci(Self) end),
	F2 ! new_fib_request(N - 2),
	receive
		{fib_response, Value1} ->
			receive
				{fib_response, Value2} ->
					{ok, Value1 + Value2};
				X ->
					io:fwrite("Fibonacci got unexpected message: ~p!~n",[X]),
					throw(X) % don't accept weird stuff
			end;
		X ->
			io:fwrite("Fibonacci got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

%%%% TESTS %%%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

fibonacci_test() ->
	FibR = #{
		fib_number => 15
	},
	{ok, Result} = benchmarks_server:await_benchmark_result(?MODULE, FibR, "Fibonacci"),
	true = test_result:is_success(Result) orelse test_result:is_rse_failure(Result).

-endif.
