<<<<<<< HEAD
-module(net_throughput_ping_pong_bench).

-behaviour(distributed_benchmark).

-export([
	msg_to_master_conf/1,
	new_master/0,
	new_client/0,
	master_setup/3,
	master_prepare_iteration/2,
	master_run_iteration/1,
	master_cleanup_iteration/3,
	client_setup/2,
	client_prepare_iteration/1,
	client_cleanup_iteration/2
	]).

-record(master_conf, {
	num_msgs = 0 :: integer(),
	num_pairs = 0 :: integer(),
	pipeline = 1 :: integer(),
	static_only = true :: boolean()}).
-type master_conf() :: #master_conf{}.
-record(client_conf, {
	num_pairs = 0 :: integer(),
	static_only = true :: boolean()}).
-type client_conf() :: #client_conf{}.
-type client_data() :: [pid()].

-record(master_state, {
	config = #master_conf{} :: master_conf(),
	pingers :: [pid()] | 'undefined',
	pongers :: [pid()] | 'undefined'}).

-type master_instance() :: #master_state{}.

-record(client_state, {
	config = #client_conf{} :: client_conf(),
	pongers :: [pid()] | 'undefined'}).

-type client_instance() :: #client_state{}.

-spec get_num_msgs(State :: master_instance()) -> integer().
get_num_msgs(State) ->
	State#master_state.config#master_conf.num_msgs.

-spec get_num_pairs(State :: master_instance() | client_instance()) -> integer().
get_num_pairs(State) ->
	case State of
		#master_state{ config = Conf } ->
			Conf#master_conf.num_pairs;
		#client_state{ config = Conf } ->
			Conf#client_conf.num_pairs
	end.

-spec get_pipeline(State :: master_instance()) -> integer().
get_pipeline(State) ->
	State#master_state.config#master_conf.pipeline.

-spec is_static_only(State :: master_instance() | client_instance()) -> boolean().
is_static_only(State) ->
	case State of
		#master_state{ config = Conf } ->
			Conf#master_conf.static_only;
		#client_state{ config = Conf } ->
			Conf#client_conf.static_only
	end.

-spec msg_to_master_conf(Msg :: term()) ->
	{ok, MasterConf :: master_conf()} |
	{error, Reason :: string()}.
msg_to_master_conf(Msg) ->
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
			{ok, #master_conf{num_msgs = NumMsgs, num_pairs = NumPairs, pipeline = Pipeline, static_only = StaticOnly}};
		#{	messages_per_pair := _NumMsgs,
        	pipeline_size := _Pipeline,
        	parallelism := _NumPairs,
        	static_only := _StaticOnly
        } ->
			{error, io_lib:fwrite("Invalid config parameters:~p.~n", [Msg])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p.~n", [Msg])}
	end.

-spec new_master() -> MasterInstance :: master_instance().
new_master() ->
	#master_state{}.

-spec new_client() -> ClientInstance :: client_instance().
new_client() ->
	#client_state{}.

%%%% On Master Instance %%%%%

-spec master_setup(Instance :: master_instance(), Conf :: master_conf(), Meta :: distributed_benchmark:deployment_metadata()) ->
	{ok, Newnstance :: master_instance(), ClientConf :: client_conf()}.
master_setup(Instance, Conf, _Meta) ->
	NewInstance = Instance#master_state{config = Conf},
	process_flag(trap_exit, true),
	ClientConf = #client_conf{num_pairs = get_num_pairs(NewInstance), static_only = is_static_only(NewInstance) },
	{ok, NewInstance, ClientConf}.

-spec master_prepare_iteration(Instance :: master_instance(), ClientData :: [client_data()]) ->
	{ok, NewInstance :: master_instance()}.
master_prepare_iteration(Instance, ClientData) ->
	[Pongers| _Rest] = ClientData,
	Self = self(),
	StaticOnly = is_static_only(Instance),
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
	NewInstance = Instance#master_state{pingers = Pingers, pongers = Pongers},
	{ok, NewInstance}.

-spec master_run_iteration(Instance :: master_instance()) ->
	{ok, NewInstance :: master_instance()}.
master_run_iteration(Instance) ->
	lists:foreach(fun(Pinger) -> Pinger ! start end, Instance#master_state.pingers),
	ok = bench_helpers:await_all(Instance#master_state.pingers, ok),
	{ok, Instance}.

-spec master_cleanup_iteration(Instance :: master_instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, NewInstance :: master_instance()}.
master_cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
	ok = bench_helpers:await_exit_all(Instance#master_state.pingers),
	NewInstance = Instance#master_state{pingers = undefined, pongers = undefined},
	{ok, NewInstance}.

%%%% On Client Instance %%%%%

-spec client_setup(Instance :: client_instance(), Conf :: client_conf()) ->
	{ok, NewInstance :: client_instance(), ClientData :: client_data()}.
client_setup(Instance, Conf) ->
	ConfInstance = Instance#client_state{config = Conf},
	process_flag(trap_exit, true),
	Range = lists:seq(1, get_num_pairs(ConfInstance)),
	StaticOnly = is_static_only(ConfInstance),
	PongerFun = case StaticOnly of
		true ->
			(fun static_ponger/0);
		false ->
			(fun ponger/0)
	end,
	Pongers = lists:map(fun(_Index) -> spawn_link(PongerFun) end, Range),
	NewInstance = ConfInstance#client_state{pongers = Pongers},
	{ok, NewInstance, Pongers}.

-spec client_prepare_iteration(Instance :: client_instance()) ->
	{ok, NewInstance :: client_instance()}.
client_prepare_iteration(Instance) ->
	io:fwrite("Preparing ponger iteration.~n"),
	{ok, Instance}.

-spec client_cleanup_iteration(Instance :: client_instance(), LastIteration :: boolean()) ->
	{ok, NewInstance :: client_instance()}.
client_cleanup_iteration(Instance, LastIteration) ->
	io:fwrite("Cleaning up ponger side.~n"),
	case LastIteration of
		true ->
			lists:foreach(fun(Ponger) -> Ponger ! stop end, Instance#client_state.pongers),
			ok = bench_helpers:await_exit_all(Instance#client_state.pongers),
			NewInstance = Instance#client_state{pongers = undefined},
			{ok, NewInstance};
		_ ->
			{ok, Instance}
	end.

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
-module(net_throughput_ping_pong_bench).

-behaviour(distributed_benchmark).

-export([
	msg_to_master_conf/1,
	new_master/0,
	new_client/0,
	master_setup/3,
	master_prepare_iteration/2,
	master_run_iteration/1,
	master_cleanup_iteration/3,
	client_setup/2,
	client_prepare_iteration/1,
	client_cleanup_iteration/2
	]).

-record(master_conf, {
	num_msgs = 0 :: integer(),
	num_pairs = 0 :: integer(),
	pipeline = 1 :: integer(),
	static_only = true :: boolean()}).
-type master_conf() :: #master_conf{}.
-record(client_conf, {
	num_pairs = 0 :: integer(),
	static_only = true :: boolean()}).
-type client_conf() :: #client_conf{}.
-type client_data() :: [pid()].

-record(master_state, {
	config = #master_conf{} :: master_conf(),
	pingers :: [pid()] | 'undefined',
	pongers :: [pid()] | 'undefined'}).

-type master_instance() :: #master_state{}.

-record(client_state, {
	config = #client_conf{} :: client_conf(),
	pongers :: [pid()] | 'undefined'}).

-type client_instance() :: #client_state{}.

-spec get_num_msgs(State :: master_instance()) -> integer().
get_num_msgs(State) ->
	State#master_state.config#master_conf.num_msgs.

-spec get_num_pairs(State :: master_instance() | client_instance()) -> integer().
get_num_pairs(State) ->
	case State of
		#master_state{ config = Conf } ->
			Conf#master_conf.num_pairs;
		#client_state{ config = Conf } ->
			Conf#client_conf.num_pairs
	end.

-spec get_pipeline(State :: master_instance()) -> integer().
get_pipeline(State) ->
	State#master_state.config#master_conf.pipeline.

-spec is_static_only(State :: master_instance() | client_instance()) -> boolean().
is_static_only(State) ->
	case State of
		#master_state{ config = Conf } ->
			Conf#master_conf.static_only;
		#client_state{ config = Conf } ->
			Conf#client_conf.static_only
	end.

-spec msg_to_master_conf(Msg :: term()) ->
	{ok, MasterConf :: master_conf()} |
	{error, Reason :: string()}.
msg_to_master_conf(Msg) ->
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
			{ok, #master_conf{num_msgs = NumMsgs, num_pairs = NumPairs, pipeline = Pipeline, static_only = StaticOnly}};
		#{	messages_per_pair := _NumMsgs,
        	pipeline_size := _Pipeline,
        	parallelism := _NumPairs,
        	static_only := _StaticOnly
        } ->
			{error, io_lib:fwrite("Invalid config parameters:~p.~n", [Msg])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p.~n", [Msg])}
	end.

-spec new_master() -> MasterInstance :: master_instance().
new_master() ->
	#master_state{}.

-spec new_client() -> ClientInstance :: client_instance().
new_client() ->
	#client_state{}.

%%%% On Master Instance %%%%%

-spec master_setup(Instance :: master_instance(), Conf :: master_conf(), Meta :: distributed_benchmark:deployment_metadata()) ->
	{ok, Newnstance :: master_instance(), ClientConf :: client_conf()}.
master_setup(Instance, Conf, _Meta) ->
	NewInstance = Instance#master_state{config = Conf},
	process_flag(trap_exit, true),
	ClientConf = #client_conf{num_pairs = get_num_pairs(NewInstance), static_only = is_static_only(NewInstance) },
	{ok, NewInstance, ClientConf}.

-spec master_prepare_iteration(Instance :: master_instance(), ClientData :: [client_data()]) ->
	{ok, NewInstance :: master_instance()}.
master_prepare_iteration(Instance, ClientData) ->
	[Pongers| _Rest] = ClientData,
	Self = self(),
	StaticOnly = is_static_only(Instance),
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
	NewInstance = Instance#master_state{pingers = Pingers, pongers = Pongers},
	{ok, NewInstance}.

-spec master_run_iteration(Instance :: master_instance()) ->
	{ok, NewInstance :: master_instance()}.
master_run_iteration(Instance) ->
	lists:foreach(fun(Pinger) -> Pinger ! start end, Instance#master_state.pingers),
	ok = bench_helpers:await_all(Instance#master_state.pingers, ok),
	{ok, Instance}.

-spec master_cleanup_iteration(Instance :: master_instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, NewInstance :: master_instance()}.
master_cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
	ok = bench_helpers:await_exit_all(Instance#master_state.pingers),
	NewInstance = Instance#master_state{pingers = undefined, pongers = undefined},
	{ok, NewInstance}.

%%%% On Client Instance %%%%%

-spec client_setup(Instance :: client_instance(), Conf :: client_conf()) ->
	{ok, NewInstance :: client_instance(), ClientData :: client_data()}.
client_setup(Instance, Conf) ->
	ConfInstance = Instance#client_state{config = Conf},
	process_flag(trap_exit, true),
	Range = lists:seq(1, get_num_pairs(ConfInstance)),
	StaticOnly = is_static_only(ConfInstance),
	PongerFun = case StaticOnly of
		true ->
			(fun static_ponger/0);
		false ->
			(fun ponger/0)
	end,
	Pongers = lists:map(fun(_Index) -> spawn_link(PongerFun) end, Range),
	NewInstance = ConfInstance#client_state{pongers = Pongers},
	{ok, NewInstance, Pongers}.

-spec client_prepare_iteration(Instance :: client_instance()) ->
	{ok, NewInstance :: client_instance()}.
client_prepare_iteration(Instance) ->
	io:fwrite("Preparing ponger iteration.~n"),
	{ok, Instance}.

-spec client_cleanup_iteration(Instance :: client_instance(), LastIteration :: boolean()) ->
	{ok, NewInstance :: client_instance()}.
client_cleanup_iteration(Instance, LastIteration) ->
	io:fwrite("Cleaning up ponger side.~n"),
	case LastIteration of
		true ->
			lists:foreach(fun(Ponger) -> Ponger ! stop end, Instance#client_state.pongers),
			ok = bench_helpers:await_exit_all(Instance#client_state.pongers),
			NewInstance = Instance#client_state{pongers = undefined},
			{ok, NewInstance};
		_ ->
			{ok, Instance}
	end.

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
>>>>>>> c92c44604e519dd0b6cc96f7ef122b9ca6b9cde1
