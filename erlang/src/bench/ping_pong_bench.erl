-module(ping_pong_bench).

-behaviour(local_benchmark).

-export([
	new_instance/0,
	msg_to_conf/1,
	setup/2,
	prepare_iteration/1,
	run_iteration/1,
	cleanup_iteration/3
	]).

-record(conf, {number_of_messages :: integer()}).

-type conf() :: #conf{}.

-record(state, {
	num = 0 :: integer(), 
	pinger :: pid() | 'undefined', 
	ponger :: pid() | 'undefined'}).

-type instance() :: #state{}.

-spec new_instance() -> instance().
new_instance() -> 
	#state{}.

-spec msg_to_conf(Msg :: term()) -> {ok, conf()} | {error, string()}.
msg_to_conf(Msg) ->
	case Msg of
		#{number_of_messages := Nom} when is_integer(Nom) andalso (Nom > 0) ->
			{ok, #conf{number_of_messages = Nom}};
		#{number_of_messages := Nom} ->
			{error, io_lib:fwrite("Invalid config parameter:~p", [Nom])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p", [Msg])}
	end.


-spec setup(Instance :: instance(), Conf :: conf()) -> {ok, instance()}.
setup(Instance, Conf) ->
	NewInstance = Instance#state{num = Conf#conf.number_of_messages},
	process_flag(trap_exit, true),
	{ok, NewInstance}.

-spec prepare_iteration(Instance :: instance()) -> {ok, instance()}.
prepare_iteration(Instance) -> 
	Ponger = spawn_link(fun ponger/0),
	Self = self(),
	Pinger = spawn_link(fun() -> pinger(Ponger, Instance#state.num, Self) end),
	NewInstance = Instance#state{pinger = Pinger, ponger = Ponger},
	{ok, NewInstance}.

-spec run_iteration(Instance :: instance()) -> {ok, instance()}.
run_iteration(Instance) ->
	Instance#state.pinger ! start,
	receive
		ok ->
			{ok, Instance};
		X -> 
			io:fwrite("Got unexpected message during iteration: ~p!~n",[X]),
			throw(X)
	end.

-spec cleanup_iteration(Instance :: instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, instance()}.
cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
	Instance#state.ponger ! stop,
	ok = bench_helpers:await_exit_all([Instance#state.pinger, Instance#state.ponger]),
	NewInstance = Instance#state{pinger = undefined, ponger = undefined},
	{ok, NewInstance}.


%%%%%% Pinger %%%%%%
-spec pinger(Ponger :: pid(), MsgCount :: integer(), Return :: pid()) -> ok.
pinger(Ponger, MsgCount, Return) ->
	receive
		start ->
			pinger_run(Ponger, MsgCount - 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

-spec pinger_run(Ponger :: pid(), MsgCount :: integer(), Return :: pid()) -> ok.
pinger_run(_Ponger, MsgCount, Return) when MsgCount =< 0 ->
	Return ! ok,
	io:fwrite("Pinger is done!~n");
pinger_run(Ponger, MsgCount, Return) ->
	%io:fwrite("Pinger sending ping#~b!~n",[MsgCount]),
	Ponger ! {ping, self()},
	receive
		pong ->
			pinger_run(Ponger, MsgCount - 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

%%%%%% Ponger %%%%%%
-spec ponger() -> ok.
ponger() ->
	receive
		stop ->
			ok;
		{ping, Pinger} ->
			Pinger ! pong,
			ponger();
		X ->
			io:fwrite("Ponger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.


%%%% TESTS %%%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

ping_pong_test() ->
	PPR = #{number_of_messages => 100},
	{ok, Result} = benchmarks_server:await_benchmark_result(?MODULE, PPR, "PingPong"),
	true = test_result:is_success(Result) orelse test_result:is_rse_failure(Result).

-endif.
