<<<<<<< HEAD
-module(throughput_ping_pong_bench).

-behaviour(local_benchmark).

-export([
	new_instance/0,
	msg_to_conf/1,
	setup/2,
	prepare_iteration/1,
	run_iteration/1,
	cleanup_iteration/3
	]).

-record(conf, {
	num_msgs = 0 :: integer(),
	num_pairs = 0 :: integer(),
	pipeline = 1 :: integer(),
	static_only = true :: boolean()}).

-type conf() :: #conf{}.

-record(state, {
	config = #conf{} :: conf(),
	pingers :: [pid()] | 'undefined', 
	pongers :: [pid()] | 'undefined'}).

-type instance() :: #state{}.

-spec get_num_msgs(State :: instance()) -> integer().
get_num_msgs(State) ->
	State#state.config#conf.num_msgs.

-spec get_num_pairs(State :: instance()) -> integer().
get_num_pairs(State) ->
	State#state.config#conf.num_pairs.

-spec get_pipeline(State :: instance()) -> integer().
get_pipeline(State) ->
	State#state.config#conf.pipeline.

-spec is_static_only(State :: instance()) -> boolean().
is_static_only(State) ->
	State#state.config#conf.static_only.

-spec new_instance() -> instance().
new_instance() -> 
	#state{}.

-spec msg_to_conf(Msg :: term()) -> {ok, conf()} | {error, string()}.
msg_to_conf(Msg) ->
	case Msg of
		#{	messages_per_pair := NumMsgs,
        	pipeline_size := Pipeline,
        	parallelism := NumPairs,
        	static_only := StaticOnly
        } when 
        	is_integer(NumMsgs) andalso (NumMsgs > 0),
        	is_integer(Pipeline) andalso (Pipeline > 0),
        	is_integer(NumPairs) andalso (NumPairs > 0),
        	is_boolean(StaticOnly) ->
			{ok, #conf{num_msgs = NumMsgs, num_pairs = NumPairs, pipeline = Pipeline, static_only = StaticOnly}};
		#{	messages_per_pair := _NumMsgs,
        	pipeline_size := _Pipeline,
        	parallelism := _NumPairs,
        	static_only := _StaticOnly
        } ->
			{error, io_lib:fwrite("Invalid config parameters:~p.~n", [Msg])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p.~n", [Msg])}
	end.


-spec setup(Instance :: instance(), Conf :: conf()) -> {ok, instance()}.
setup(Instance, Conf) ->
	NewInstance = Instance#state{config = Conf},
	process_flag(trap_exit, true),
	{ok, NewInstance}.

-spec prepare_iteration(Instance :: instance()) -> {ok, instance()}.
prepare_iteration(Instance) ->
	%io:fwrite("Preparing iteration.~n"),
	Range = lists:seq(1, get_num_pairs(Instance)),
	StaticOnly = is_static_only(Instance),
	PongerFun = case StaticOnly of
		true ->
			(fun static_ponger/0);
		false ->
			(fun ponger/0)
	end,
	Pongers = lists:map(fun(_Index) -> spawn_link(PongerFun) end, Range),
	Self = self(),
	PingerFun = case StaticOnly of
		true ->
			fun(Ponger) -> 
				fun() -> 
					static_pinger(Ponger, get_num_msgs(Instance), get_pipeline(Instance), Self)
				end 
			end;
		false ->
			fun(Ponger) -> 
				fun() -> 
					pinger(Ponger, get_num_msgs(Instance), get_pipeline(Instance), Self)
				end 
			end
	end,
	Pingers = lists:map(fun(Ponger) -> spawn_link(PingerFun(Ponger)) end, Pongers),
	NewInstance = Instance#state{pingers = Pingers, pongers = Pongers},
	{ok, NewInstance}.

-spec run_iteration(Instance :: instance()) -> {ok, instance()}.
run_iteration(Instance) ->
	lists:foreach(fun(Pinger) -> Pinger ! start end, Instance#state.pingers),
	ok = bench_helpers:await_all(Instance#state.pingers, ok),
	{ok, Instance}.

-spec cleanup_iteration(Instance :: instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, instance()}.
cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
	%io:fwrite("Cleaning iteration.~n Stopping pongers.~n"),
	lists:foreach(fun(Ponger) -> Ponger ! stop end, Instance#state.pongers),
	%io:fwrite("Waiting for all children to die.~n"),
	ok = bench_helpers:await_exit_all(lists:append(Instance#state.pingers, Instance#state.pongers)),
	%io:fwrite("All children died.~n"),
	NewInstance = Instance#state{pingers = undefined, pongers = undefined},
	{ok, NewInstance}.


%%%%%% Static Pinger %%%%%%
-spec static_pinger(Ponger :: pid(), MsgCount :: integer(), Pipeline :: integer(), Return :: pid()) -> ok.
static_pinger(Ponger, MsgCount, Pipeline, Return) ->
	receive
		start ->
			%io:fwrite("Starting static pinger ~p.~n", [self()]),
			static_pinger_pipe(Ponger, MsgCount, 0, 0, Pipeline, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

-spec static_pinger_pipe(Ponger :: pid(), MsgCount :: integer(), SentCount :: integer(), RecvCount :: integer(), Pipeline :: integer(), Return :: pid()) -> ok.
static_pinger_pipe(Ponger, MsgCount, SentCount, RecvCount, Pipeline, Return) 
	when (SentCount < Pipeline) andalso (SentCount < MsgCount) ->
	Ponger ! {ping, self()},
	static_pinger_pipe(Ponger, MsgCount, SentCount + 1, RecvCount, Pipeline, Return);
static_pinger_pipe(Ponger, MsgCount, SentCount, RecvCount, _Pipeline, Return) ->
	static_pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return).

-spec static_pinger_run(Ponger :: pid(), MsgCount :: integer(), SentCount :: integer(), RecvCount :: integer(), Return :: pid()) -> ok.
static_pinger_run(_Ponger, MsgCount, _SentCount, RecvCount, Return) when RecvCount >= MsgCount ->
	Return ! {ok, self()},
	%io:fwrite("Finished static pinger ~p.~n", [self()]),
	ok;
static_pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return) when SentCount >= MsgCount ->
	receive
		pong ->
			static_pinger_run(Ponger, MsgCount, SentCount, RecvCount + 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end;
static_pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return) ->
	Ponger ! {ping, self()},
	receive
		pong ->
			static_pinger_run(Ponger, MsgCount, SentCount + 1, RecvCount + 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

%%%%%% Pinger %%%%%%
-spec pinger(Ponger :: pid(), MsgCount :: integer(), Pipeline :: integer(), Return :: pid()) -> ok.
pinger(Ponger, MsgCount, Pipeline, Return) ->
	receive
		start ->
			%io:fwrite("Starting pinger ~p.~n", [self()]),
			pinger_pipe(Ponger, MsgCount, 0, 0, Pipeline, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

-spec pinger_pipe(Ponger :: pid(), MsgCount :: integer(), SentCount :: integer(), RecvCount :: integer(), Pipeline :: integer(), Return :: pid()) -> ok.
pinger_pipe(Ponger, MsgCount, SentCount, RecvCount, Pipeline, Return) 
	when (SentCount < Pipeline) andalso (SentCount < MsgCount) ->
	Ponger ! {ping, SentCount, self()},
	pinger_pipe(Ponger, MsgCount, SentCount + 1, RecvCount, Pipeline, Return);
pinger_pipe(Ponger, MsgCount, SentCount, RecvCount, _Pipeline, Return) ->
	pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return).

-spec pinger_run(Ponger :: pid(), MsgCount :: integer(), SentCount :: integer(), RecvCount :: integer(), Return :: pid()) -> ok.
pinger_run(_Ponger, MsgCount, _SentCount, RecvCount, Return) when RecvCount >= MsgCount ->
	Return ! {ok, self()},
	%io:fwrite("Finished pinger ~p.~n", [self()]),
	ok;
pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return) when SentCount >= MsgCount ->
	receive
		{pong, _Index} ->
			pinger_run(Ponger, MsgCount, SentCount, RecvCount + 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end;
pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return) ->
	Ponger ! {ping, SentCount, self()},
	receive
		{pong, _Index} ->
			pinger_run(Ponger, MsgCount, SentCount + 1, RecvCount + 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

%%%%%% Static Ponger %%%%%%
-spec static_ponger() -> ok.
static_ponger() ->
	receive
		stop ->
			%io:fwrite("Stopping static ponger ~w.~n", [self()]),
			ok;
		{ping, Pinger} ->
			Pinger ! pong,
			static_ponger();
		X ->
			io:fwrite("Ponger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

%%%%%% Ponger %%%%%%
-spec ponger() -> ok.
ponger() ->
	receive
		stop ->
			%io:fwrite("Stopping ponger ~w.~n", [self()]),
			ok;
		{ping, Index, Pinger} ->
			Pinger ! {pong, Index},
			ponger();
		X ->
			io:fwrite("Ponger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.
=======
-module(throughput_ping_pong_bench).

-behaviour(local_benchmark).

-export([
	new_instance/0,
	msg_to_conf/1,
	setup/2,
	prepare_iteration/1,
	run_iteration/1,
	cleanup_iteration/3
	]).

-record(conf, {
	num_msgs = 0 :: integer(),
	num_pairs = 0 :: integer(),
	pipeline = 1 :: integer(),
	static_only = true :: boolean()}).

-type conf() :: #conf{}.

-record(state, {
	config = #conf{} :: conf(),
	pingers :: [pid()] | 'undefined', 
	pongers :: [pid()] | 'undefined'}).

-type instance() :: #state{}.

-spec get_num_msgs(State :: instance()) -> integer().
get_num_msgs(State) ->
	State#state.config#conf.num_msgs.

-spec get_num_pairs(State :: instance()) -> integer().
get_num_pairs(State) ->
	State#state.config#conf.num_pairs.

-spec get_pipeline(State :: instance()) -> integer().
get_pipeline(State) ->
	State#state.config#conf.pipeline.

-spec is_static_only(State :: instance()) -> boolean().
is_static_only(State) ->
	State#state.config#conf.static_only.

-spec new_instance() -> instance().
new_instance() -> 
	#state{}.

-spec msg_to_conf(Msg :: term()) -> {ok, conf()} | {error, string()}.
msg_to_conf(Msg) ->
	case Msg of
		#{	messages_per_pair := NumMsgs,
        	pipeline_size := Pipeline,
        	parallelism := NumPairs,
        	static_only := StaticOnly
        } when 
        	is_integer(NumMsgs) andalso (NumMsgs > 0),
        	is_integer(Pipeline) andalso (Pipeline > 0),
        	is_integer(NumPairs) andalso (NumPairs > 0),
        	is_boolean(StaticOnly) ->
			{ok, #conf{num_msgs = NumMsgs, num_pairs = NumPairs, pipeline = Pipeline, static_only = StaticOnly}};
		#{	messages_per_pair := _NumMsgs,
        	pipeline_size := _Pipeline,
        	parallelism := _NumPairs,
        	static_only := _StaticOnly
        } ->
			{error, io_lib:fwrite("Invalid config parameters:~p.~n", [Msg])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p.~n", [Msg])}
	end.


-spec setup(Instance :: instance(), Conf :: conf()) -> {ok, instance()}.
setup(Instance, Conf) ->
	NewInstance = Instance#state{config = Conf},
	process_flag(trap_exit, true),
	{ok, NewInstance}.

-spec prepare_iteration(Instance :: instance()) -> {ok, instance()}.
prepare_iteration(Instance) ->
	%io:fwrite("Preparing iteration.~n"),
	Range = lists:seq(1, get_num_pairs(Instance)),
	StaticOnly = is_static_only(Instance),
	PongerFun = case StaticOnly of
		true ->
			(fun static_ponger/0);
		false ->
			(fun ponger/0)
	end,
	Pongers = lists:map(fun(_Index) -> spawn_link(PongerFun) end, Range),
	Self = self(),
	PingerFun = case StaticOnly of
		true ->
			fun(Ponger) -> 
				fun() -> 
					static_pinger(Ponger, get_num_msgs(Instance), get_pipeline(Instance), Self)
				end 
			end;
		false ->
			fun(Ponger) -> 
				fun() -> 
					pinger(Ponger, get_num_msgs(Instance), get_pipeline(Instance), Self)
				end 
			end
	end,
	Pingers = lists:map(fun(Ponger) -> spawn_link(PingerFun(Ponger)) end, Pongers),
	NewInstance = Instance#state{pingers = Pingers, pongers = Pongers},
	{ok, NewInstance}.

-spec run_iteration(Instance :: instance()) -> {ok, instance()}.
run_iteration(Instance) ->
	lists:foreach(fun(Pinger) -> Pinger ! start end, Instance#state.pingers),
	ok = bench_helpers:await_all(Instance#state.pingers, ok),
	{ok, Instance}.

-spec cleanup_iteration(Instance :: instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, instance()}.
cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
	%io:fwrite("Cleaning iteration.~n Stopping pongers.~n"),
	lists:foreach(fun(Ponger) -> Ponger ! stop end, Instance#state.pongers),
	%io:fwrite("Waiting for all children to die.~n"),
	ok = bench_helpers:await_exit_all(lists:append(Instance#state.pingers, Instance#state.pongers)),
	%io:fwrite("All children died.~n"),
	NewInstance = Instance#state{pingers = undefined, pongers = undefined},
	{ok, NewInstance}.


%%%%%% Static Pinger %%%%%%
-spec static_pinger(Ponger :: pid(), MsgCount :: integer(), Pipeline :: integer(), Return :: pid()) -> ok.
static_pinger(Ponger, MsgCount, Pipeline, Return) ->
	receive
		start ->
			%io:fwrite("Starting static pinger ~p.~n", [self()]),
			static_pinger_pipe(Ponger, MsgCount, 0, 0, Pipeline, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

-spec static_pinger_pipe(Ponger :: pid(), MsgCount :: integer(), SentCount :: integer(), RecvCount :: integer(), Pipeline :: integer(), Return :: pid()) -> ok.
static_pinger_pipe(Ponger, MsgCount, SentCount, RecvCount, Pipeline, Return) 
	when (SentCount < Pipeline) andalso (SentCount < MsgCount) ->
	Ponger ! {ping, self()},
	static_pinger_pipe(Ponger, MsgCount, SentCount + 1, RecvCount, Pipeline, Return);
static_pinger_pipe(Ponger, MsgCount, SentCount, RecvCount, _Pipeline, Return) ->
	static_pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return).

-spec static_pinger_run(Ponger :: pid(), MsgCount :: integer(), SentCount :: integer(), RecvCount :: integer(), Return :: pid()) -> ok.
static_pinger_run(_Ponger, MsgCount, _SentCount, RecvCount, Return) when RecvCount >= MsgCount ->
	Return ! {ok, self()},
	%io:fwrite("Finished static pinger ~p.~n", [self()]),
	ok;
static_pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return) when SentCount >= MsgCount ->
	receive
		pong ->
			static_pinger_run(Ponger, MsgCount, SentCount, RecvCount + 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end;
static_pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return) ->
	Ponger ! {ping, self()},
	receive
		pong ->
			static_pinger_run(Ponger, MsgCount, SentCount + 1, RecvCount + 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

%%%%%% Pinger %%%%%%
-spec pinger(Ponger :: pid(), MsgCount :: integer(), Pipeline :: integer(), Return :: pid()) -> ok.
pinger(Ponger, MsgCount, Pipeline, Return) ->
	receive
		start ->
			%io:fwrite("Starting pinger ~p.~n", [self()]),
			pinger_pipe(Ponger, MsgCount, 0, 0, Pipeline, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

-spec pinger_pipe(Ponger :: pid(), MsgCount :: integer(), SentCount :: integer(), RecvCount :: integer(), Pipeline :: integer(), Return :: pid()) -> ok.
pinger_pipe(Ponger, MsgCount, SentCount, RecvCount, Pipeline, Return) 
	when (SentCount < Pipeline) andalso (SentCount < MsgCount) ->
	Ponger ! {ping, SentCount, self()},
	pinger_pipe(Ponger, MsgCount, SentCount + 1, RecvCount, Pipeline, Return);
pinger_pipe(Ponger, MsgCount, SentCount, RecvCount, _Pipeline, Return) ->
	pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return).

-spec pinger_run(Ponger :: pid(), MsgCount :: integer(), SentCount :: integer(), RecvCount :: integer(), Return :: pid()) -> ok.
pinger_run(_Ponger, MsgCount, _SentCount, RecvCount, Return) when RecvCount >= MsgCount ->
	Return ! {ok, self()},
	%io:fwrite("Finished pinger ~p.~n", [self()]),
	ok;
pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return) when SentCount >= MsgCount ->
	receive
		{pong, _Index} ->
			pinger_run(Ponger, MsgCount, SentCount, RecvCount + 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end;
pinger_run(Ponger, MsgCount, SentCount, RecvCount, Return) ->
	Ponger ! {ping, SentCount, self()},
	receive
		{pong, _Index} ->
			pinger_run(Ponger, MsgCount, SentCount + 1, RecvCount + 1, Return);
		X ->
			io:fwrite("Pinger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

%%%%%% Static Ponger %%%%%%
-spec static_ponger() -> ok.
static_ponger() ->
	receive
		stop ->
			%io:fwrite("Stopping static ponger ~w.~n", [self()]),
			ok;
		{ping, Pinger} ->
			Pinger ! pong,
			static_ponger();
		X ->
			io:fwrite("Ponger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.

%%%%%% Ponger %%%%%%
-spec ponger() -> ok.
ponger() ->
	receive
		stop ->
			%io:fwrite("Stopping ponger ~w.~n", [self()]),
			ok;
		{ping, Index, Pinger} ->
			Pinger ! {pong, Index},
			ponger();
		X ->
			io:fwrite("Ponger got unexpected message: ~p!~n",[X]),
			throw(X) % don't accept weird stuff
	end.


%%%% TESTS %%%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

static_throughput_ping_pong_test() ->
	TPPR = #{
		messages_per_pair => 100,
		parallelism => 2,
		pipeline_size => 20,
		static_only => true
	},
	{ok, Result} = benchmarks_server:await_benchmark_result(?MODULE, TPPR, "ThroughputPingPong (Static)"),
	true = test_result:is_success(Result) orelse test_result:is_rse_failure(Result).

gc_throughput_ping_pong_test() ->
	TPPR = #{
		messages_per_pair => 100,
		parallelism => 2,
		pipeline_size => 20,
		static_only => false
	},
	{ok, Result} = benchmarks_server:await_benchmark_result(?MODULE, TPPR, "ThroughputPingPong (GC)"),
	true = test_result:is_success(Result) orelse test_result:is_rse_failure(Result).

-endif.
>>>>>>> c92c44604e519dd0b6cc96f7ef122b9ca6b9cde1
