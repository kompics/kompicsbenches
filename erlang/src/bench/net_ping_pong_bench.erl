-module(net_ping_pong_bench).

-behaviour(distributed_benchmark).

-export([
	msg_to_master_conf/1,
	new_master/0,
	new_client/0,
	master_setup/2,
	master_prepare_iteration/2,
	master_run_iteration/1,
	master_cleanup_iteration/3,
	client_setup/2,
	client_prepare_iteration/1,
	client_cleanup_iteration/2
	]).

-record(master_conf, {number_of_messages :: integer()}).
-type master_conf() :: #master_conf{}.
-type client_conf() :: 'undefined'.
-type client_data() :: pid().

-record(master_state, {
	num = 0 :: integer(), 
	pinger :: pid() | 'undefined', 
	ponger :: pid() | 'undefined'}).

-type master_instance() :: #master_state{}.

-record(client_state, { 
	ponger :: pid() | 'undefined'}).

-type client_instance() :: #client_state{}.

-spec msg_to_master_conf(Msg :: term()) ->
	{ok, MasterConf :: master_conf()} |
	{error, Reason :: string()}.
msg_to_master_conf(Msg) ->
	case Msg of
		#{number_of_messages := Nom} when is_integer(Nom) andalso (Nom > 0) ->
			{ok, #master_conf{number_of_messages = Nom}};
		#{number_of_messages := Nom} ->
			{error, io_lib:fwrite("Invalid config parameter:~p", [Nom])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p", [Msg])}
	end.

-spec new_master() -> MasterInstance :: master_instance().
new_master() ->
	#master_state{}.

-spec new_client() -> ClientInstance :: client_instance().
new_client() ->
	#client_state{}.

%%%% On Master Instance %%%%%

-spec master_setup(Instance :: master_instance(), Conf :: master_conf()) -> 
	{ok, Newnstance :: master_instance(), ClientConf :: client_conf()}.
master_setup(Instance, Conf) ->
	NewInstance = Instance#master_state{num = Conf#master_conf.number_of_messages},
	process_flag(trap_exit, true),
	{ok, NewInstance, undefined}.

-spec master_prepare_iteration(Instance :: master_instance(), ClientData :: [client_data()]) ->
	{ok, NewInstance :: master_instance()}.
master_prepare_iteration(Instance, ClientData) ->
	[Ponger| _Rest] = ClientData,
	Self = self(),
	Pinger = spawn_link(fun() -> pinger(Ponger, Instance#master_state.num, Self) end),
	Newnstance = Instance#master_state{pinger = Pinger, ponger = Ponger},
	{ok, Newnstance}.

-spec master_run_iteration(Instance :: master_instance()) ->
	{ok, NewInstance :: master_instance()}.
master_run_iteration(Instance) ->
	Instance#master_state.pinger ! start,
	receive
		ok ->
			{ok, Instance};
		X -> 
			io:fwrite("Got unexpected message during iteration: ~p!~n",[X]),
			throw(X)
	end.

-spec master_cleanup_iteration(Instance :: master_instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, NewInstance :: master_instance()}.
master_cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
	{ok, _} = bench_helpers:await_exit(Instance#master_state.pinger),
	NewInstance = Instance#master_state{pinger = undefined, ponger = undefined},
	{ok, NewInstance}.

%%%% On Client Instance %%%%%

-spec client_setup(Instance :: client_instance(), Conf :: client_conf()) ->
	{ok, NewInstance :: client_instance(), ClientData :: client_data()}.
client_setup(Instance, _Conf) ->
	process_flag(trap_exit, true),
	Ponger = spawn_link(fun ponger/0),
	NewInstance = Instance#client_state{ponger = Ponger},
	{ok, NewInstance, Ponger}.

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
			Ponger = Instance#client_state.ponger,
			Ponger ! stop,
			{ok, _} = bench_helpers:await_exit(Ponger),
			NewInstance = Instance#client_state{ponger = undefined},
			{ok, NewInstance};
		_ ->
			{ok, Instance}
	end.

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
