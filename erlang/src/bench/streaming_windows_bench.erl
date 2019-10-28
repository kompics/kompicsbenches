-module(streaming_windows_bench).

-include_lib("kernel/include/logger.hrl").

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

% for testing
-export([
	source_fun/1,
	windower_fun/4,
	sink_fun/3]).

-record(master_conf, {
	num_partitions = 0 :: integer(),
	batch_size = 0 :: integer(),
	window_size = duration:zero() :: duration:duration(),
	num_windows = 0 :: integer(),
	amplification = 0 :: integer()}).
-type master_conf() :: #master_conf{}.
-record(client_conf, {
	batch_size = 0 :: integer(),
	window_size = duration:zero() :: duration:duration(),
	amplification = 0 :: integer(),
	upstream_actors :: [pid()]}).
-type client_conf() :: #client_conf{}.
-type client_data() :: [pid()].

-record(master_state, {
	config = #master_conf{} :: master_conf(), 
	sources :: [pid()] | 'undefined', 
	sinks :: [pid()] | 'undefined'}).

-type master_instance() :: #master_state{}.

-record(client_state, { 
	config :: client_conf() | 'undefined',
	windowers :: [pid()] | 'undefined'}).

-type client_instance() :: #client_state{}.

%%%% Extractors %%%%
-spec get_num_partitions(State :: master_instance()) -> integer().
get_num_partitions(State) ->
	State#master_state.config#master_conf.num_partitions.

-spec get_batch_size(State :: master_instance() | client_instance()) -> integer().
get_batch_size(State) ->
	case State of
		#master_state{config = Conf} -> 
			Conf#master_conf.batch_size;
		#client_state{config = Conf} ->
			Conf#client_conf.batch_size
	end.

-spec get_window_size(State :: master_instance() | client_instance()) -> duration:duration().
get_window_size(State) ->
	case State of
		#master_state{config = Conf} -> 
			Conf#master_conf.window_size;
		#client_state{config = Conf} ->
			Conf#client_conf.window_size
	end.

-spec get_num_windows(State :: master_instance()) -> integer().
get_num_windows(State) ->
	State#master_state.config#master_conf.num_windows.

-spec get_amplification(State :: master_instance() | client_instance()) -> integer().
get_amplification(State) ->
	case State of
		#master_state{config = Conf} -> 
			Conf#master_conf.amplification;
		#client_state{config = Conf} ->
			Conf#client_conf.amplification
	end.

-spec get_upstream_actors(State :: client_instance()) -> [pid()].
get_upstream_actors(State) ->
	State#client_state.config#client_conf.upstream_actors.

%%%% General API %%%%
-spec msg_to_master_conf(Msg :: term()) ->
	{ok, MasterConf :: master_conf()} |
	{error, Reason :: string()}.
msg_to_master_conf(Msg) ->
	case Msg of
		#{	number_of_partitions      := NumPartitions,
	        batch_size                := BatchSize,
	        window_size               := WindowSize,
	        number_of_windows         := NumberOfWindows,
	        window_size_amplification := WindowSizeAmplification
        } when
        	is_integer(NumPartitions) andalso (NumPartitions > 0),
        	is_integer(BatchSize) andalso (BatchSize > 0),
        	is_integer(NumberOfWindows) andalso (NumberOfWindows > 0),
        	is_integer(WindowSizeAmplification) andalso (WindowSizeAmplification > 0) ->
        	case duration:from_string(WindowSize) of
        		{ok, WS} ->
        			{ok, #master_conf{
        				num_partitions = NumPartitions,
						batch_size = BatchSize,
						window_size = WS,
						num_windows = NumberOfWindows,
						amplification = WindowSizeAmplification
        			}};
        		{error, Reason} ->
        			{error, io_lib:fwrite("Could not convert WindowSize to duration. Error was:~p. Input was:~p.~n", [Reason, WindowSize])}
        	end;
        #{	number_of_partitions      := _NumPartitions,
	        batch_size                := _BatchSize,
	        window_size               := _WindowSize,
	        number_of_windows         := _NumberOfWindows,
	        window_size_amplification := _WindowSizeAmplification
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
	process_flag(trap_exit, true),
	ConfInstance = Instance#master_state{config = Conf},
	% ok = logger:set_primary_config(level, debug), % should be set globally
	?LOG_INFO("Setting up StreamingWindows master:~p.~n", [Conf]),
	Pids = lists:seq(0, get_num_partitions(ConfInstance)-1),
	Sources = lists:map(fun(Pid) -> spawn_link(source_fun(Pid)) end, Pids),
	NewInstance = ConfInstance#master_state{sources = Sources},
	ClientConf = #client_conf{
		batch_size 		= get_batch_size(ConfInstance),
		window_size 	= get_window_size(ConfInstance),
		amplification 	= get_amplification(ConfInstance),
		upstream_actors = Sources},
	{ok, NewInstance, ClientConf}.

-spec master_prepare_iteration(Instance :: master_instance(), ClientData :: [client_data()]) ->
	{ok, NewInstance :: master_instance()}.
master_prepare_iteration(Instance, ClientData) ->
	?LOG_DEBUG("Preparing StreamingWindows iteration."),
	[Windowers | _Rest] = ClientData,
	Self = self(),
	Sinks = lists:map(fun(Windower) -> 
		spawn_link(sink_fun(Self, get_num_windows(Instance), Windower))
	end, Windowers),
	NewInstance = Instance#master_state{sinks = Sinks},
	{ok, NewInstance}.

-spec master_run_iteration(Instance :: master_instance()) ->
	{ok, NewInstance :: master_instance()}.
master_run_iteration(Instance) ->
	lists:foreach(fun(Sink) -> Sink ! start end, Instance#master_state.sinks),
	ok = bench_helpers:await_all(Instance#master_state.sinks, ok),
	{ok, Instance}.

-spec master_cleanup_iteration(Instance :: master_instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, NewInstance :: master_instance()}.
master_cleanup_iteration(Instance, LastIteration, _ExecTimeMillis) ->
	?LOG_DEBUG("Cleaning up StreamingWindows master."),
	lists:foreach(fun(Source) -> Source ! {reset, self()} end, Instance#master_state.sources),
	ok = bench_helpers:await_exit_all(Instance#master_state.sinks),
	NewInstance = Instance#master_state{sinks = undefined},
	ok = bench_helpers:await_all(Instance#master_state.sources, flushed),
	case LastIteration of
		true ->
			lists:foreach(fun(Source) -> Source ! stop end, Instance#master_state.sources),
			ok = bench_helpers:await_exit_all(Instance#master_state.sources),
			LastInstance = Instance#master_state{sources = undefined},
			?LOG_INFO("StreamingWindows done!"),
			{ok, LastInstance};
		_ ->
			{ok, NewInstance}
	end.

%%%% On Client Instance %%%%%

-spec client_setup(Instance :: client_instance(), Conf :: client_conf()) ->
	{ok, NewInstance :: client_instance(), ClientData :: client_data()}.
client_setup(Instance, Conf) ->
	%ok = logger:set_primary_config(level, debug), % should be set globally
	?LOG_DEBUG("Setting up StreamingWindows (client)."),
	ConfInstance = Instance#client_state{config = Conf},
	process_flag(trap_exit, true),
	Windowers = lists:map(fun(Source) -> 
		spawn_link(windower_fun(get_window_size(ConfInstance), get_batch_size(ConfInstance), get_amplification(ConfInstance), Source))
	end, get_upstream_actors(ConfInstance)),
	NewInstance = ConfInstance#client_state{
		windowers = Windowers
	},
	{ok, NewInstance, Windowers}.

-spec client_prepare_iteration(Instance :: client_instance()) ->
	{ok, NewInstance :: client_instance()}.
client_prepare_iteration(Instance) ->
	?LOG_DEBUG("Preparing StreamingWindows iteration (client)."),
	{ok, Instance}.

-spec client_cleanup_iteration(Instance :: client_instance(), LastIteration :: boolean()) ->
	{ok, NewInstance :: client_instance()}.
client_cleanup_iteration(Instance, LastIteration) ->
	?LOG_DEBUG("Cleaning StreamingWindows client."),
	case LastIteration of
		true ->
			lists:foreach(fun(Windower) -> Windower ! stop end, Instance#client_state.windowers),
			ok = bench_helpers:await_exit_all(Instance#client_state.windowers),
			NewInstance = Instance#client_state{windowers = undefined},
			{ok, NewInstance};
		_ ->
			{ok, Instance}
	end.

%%%% Messages %%%%
-type ready() :: {ready, integer(), pid()}.
-type next() :: next.
-type reset() :: {reset, pid()}.
-type flushed() :: flushed.

-type windower_start() :: {start, pid()}.
-type windower_stop() :: wstop. % differentiate from normal stop!
-type event() :: {event, integer(), integer(), integer()}.
-type flush() :: flush.

-type start() :: start.
-type window() :: {window, integer(), integer(), float()}.

% uncomment to check, but it fucks with syntax highlighting -.-
-type spawn_fun() :: fun(() -> ok).

%%%% Source %%%%
-record(source_state, {
	partition_id = 0 :: integer(),
	random :: rand:state(),
	downstream :: pid() | 'undefined',
	remaining = 0 :: integer(),
	current_ts = 0 :: integer(),
	flushing = false :: boolean(),
	reply_on_flushed :: pid() | 'undefined'}).
-type source_state() :: #source_state{}.

-spec source_fun(Pid :: integer()) -> spawn_fun().
source_fun(Pid) ->
	Rand = rand:seed(exsp,Pid), % fastest and least secure PRNG
	State = #source_state{partition_id = Pid, random = Rand},
	fun() -> source_iter(State) end.

-spec source_iter(State :: source_state()) -> ok.
source_iter(State) ->
	receive
		stop ->
			?LOG_DEBUG("Source stopping"),
			ok;
		Msg ->
			NewState = source_handle(State, Msg),
			source_iter(NewState)
	end.

-spec source_handle(State :: source_state(), Msg :: ready() | next() | reset() | flushed()) -> NewState :: source_state().
source_handle(State, {ready, BatchSize, Downstream}) ->
	case State#source_state.flushing of
		false ->
			?LOG_DEBUG("Got ready"),
			NewState = State#source_state{
				remaining = State#source_state.remaining + BatchSize,
				downstream = Downstream},
			SendState = source_send(NewState),
			SendState;
		_ ->
			State % drop msg
	end;
source_handle(State, next) ->
	case State#source_state.flushing of
		false ->
			SendState = source_send(State),
			SendState;
		_ ->
			State % drop msg
	end;
source_handle(State, {reset, Return}) ->
	case State#source_state.flushing of
		false ->
			?LOG_DEBUG("Got reset"),
			State#source_state.downstream ! flush,
			NewState = State#source_state{
				flushing = true,
				reply_on_flushed = Return,
				current_ts = 0,
				remaining = 0,
				downstream = undefined
			},
			NewState
	end; % on true crash!
source_handle(State, flushed) -> 
	case State#source_state.flushing of
		true ->
			?LOG_DEBUG("Got flushed"),
			State#source_state.reply_on_flushed ! {flushed, self()},
			NewState = State#source_state{
				flushing = false,
				reply_on_flushed = undefined
			},
			NewState
	end.% on false crash!

-spec source_send(State :: source_state()) -> NewState :: source_state().
source_send(State) ->
	if
		State#source_state.remaining > 0 ->
			{RandState, NextLong} = source_next_long(State),
			Msg = {event, State#source_state.current_ts, State#source_state.partition_id, NextLong},
			%?LOG_DEBUG("Sending message ~p to ~p.", [Msg, State#source_state.downstream]),
			State#source_state.downstream ! Msg,
			NewState = RandState#source_state {
				current_ts = State#source_state.current_ts + 1,
				remaining = State#source_state.remaining - 1
			},
			if
				NewState#source_state.remaining > 0 ->
					self() ! next;
				true ->
					?LOG_DEBUG("Waiting for ready...")
			end,
			NewState;
		true -> State
	end.

-define(U64_MAX, 16#ffffffffffffffff).
-define(I64_SHIFT, 16#0fffffffffffffff+1).

% produce a random integer() of equivalent size to the JVM long and rust i64
-spec source_next_long(State :: source_state()) -> {NewState :: source_state(), Long :: integer()}.
source_next_long(State) ->
	{ULong, NewRandState} = rand:uniform_s(?U64_MAX,State#source_state.random),
	Long = ULong - ?I64_SHIFT,
	NewState = State#source_state{random = NewRandState},
	{NewState, Long}.

%%%% Windower %%%%
-record(windower_state, {
	window_size :: duration:duration(),
	batch_size :: integer(),
	amplification :: integer(),
	upstream :: pid(),
	window_size_ms :: integer(),
	ready_mark :: integer(),
	downstream :: pid() | 'undefined',
	current_window = [] :: [integer()],
	window_start_ts = 0 :: integer(),
	received_since_ready = 0 :: integer(),
	running = false :: boolean()}).
-type windower_state() :: #windower_state{}.

-spec windower_fun(WindowSize :: duration:duration(), BatchSize :: integer(), Amplification :: integer(), Upstream :: pid()) -> spawn_fun().
windower_fun(WindowSize, BatchSize, Amplification, Upstream) ->
	WindowSizeMs = duration:to_millis(WindowSize),
	ReadyMark = BatchSize div 2,
	State = #windower_state{
		window_size = WindowSize,
		batch_size = BatchSize,
		amplification = Amplification,
		upstream = Upstream,
		window_size_ms = WindowSizeMs,
		ready_mark = ReadyMark
	},
	fun() -> windower_iter(State) end. 

-spec windower_iter(State :: windower_state()) -> ok.
windower_iter(State) ->
	receive
		stop ->
			?LOG_DEBUG("Windower stopping"),
			ok;
		Msg ->
			NewState = windower_handle(State, Msg),
			windower_iter(NewState)
	end.

-spec windower_handle(State :: windower_state(), Msg :: windower_start() | windower_stop() | event() | flush()) -> State :: windower_state().
windower_handle(State, {start, Downstream}) ->
	case State#windower_state.running of
		false ->
			?LOG_DEBUG("Got start"),
			NewState = State#windower_state{
				downstream = Downstream,
				running = true,
				received_since_ready = 0
			},
			NewState#windower_state.upstream ! {ready, NewState#windower_state.batch_size, self()},
			NewState
	end; % crash on true
windower_handle(State, wstop) ->
	case State#windower_state.running of
		true ->
			?LOG_DEBUG("Got stop"),
			NewState = State#windower_state{
				downstream = undefined,
				running = false,
				window_start_ts = 0,
				current_window = []
			},
			NewState
	end; % crash on false
windower_handle(State, {event, Ts, Pid, Value}) ->
	case State#windower_state.running of
		true ->
			%?LOG_DEBUG("Got event ts=~B.", [Ts]),
			NewValues = lists:duplicate(State#windower_state.amplification, Value),
			WindowEnd = windower_get_window_end(State),
			NewState = if 
				Ts < WindowEnd ->
					State#windower_state{
						current_window = lists:append(NewValues, State#windower_state.current_window)
					};
				true ->
					TriggeredState = windower_trigger_window(State, Pid),
					TriggeredState#windower_state{
						current_window = NewValues,
						window_start_ts = Ts
					}
			end,
			windower_manage_ready(NewState);
		_ ->
			?LOG_DEBUG("Dropping message (event), since not running"),
			State
	end;
windower_handle(State, flush) ->
	?LOG_DEBUG("Got flush"),
	State#windower_state.upstream ! flushed,
	State.

-spec windower_get_window_end(State :: windower_state()) -> integer().
windower_get_window_end(State) ->
	State#windower_state.window_start_ts + State#windower_state.window_size_ms.

-spec windower_trigger_window(State :: windower_state(), Pid :: integer()) -> NewState :: windower_state().
windower_trigger_window(State, Pid) ->
	?LOG_DEBUG("Triggering window with <don't calc> messages."),
	case State#windower_state.current_window of
		[] ->
			?LOG_WARNING("Windows should not be empty in the benchmark!"),
			State;
		Window ->
			{ok, Median} = statistics:median(Window),
			Msg = {window, State#windower_state.window_start_ts, Pid, Median},
			State#windower_state.downstream ! Msg,
			State#windower_state{current_window = []}
	end.

-spec windower_manage_ready(State :: windower_state()) -> NewState :: windower_state().
windower_manage_ready(State) ->
	NewReceivedSinceReady = State#windower_state.received_since_ready + 1,
	if
		NewReceivedSinceReady > State#windower_state.ready_mark ->
			?LOG_DEBUG("Sending ready"),
			State#windower_state.upstream ! {ready, NewReceivedSinceReady, self()},
			State#windower_state{received_since_ready = 0};
		true ->
			State#windower_state{received_since_ready = NewReceivedSinceReady}
	end.

%%%% Sink %%%%
-record(sink_state, {
	return :: pid(),
	num_windows :: integer(),
	upstream :: pid(),
	window_count = 0 :: integer()}).
-type sink_state() :: #sink_state{}.


-spec sink_fun(Return :: pid(), NumWindows :: integer(), Upstream :: pid()) -> spawn_fun().
sink_fun(Return, NumWindows, Upstream) ->
	State = #sink_state{
		return = Return,
		num_windows = NumWindows,
		upstream = Upstream
	},
	fun() -> sink_iter(State) end.

-spec sink_iter(State :: sink_state()) -> ok.
sink_iter(State) ->
	receive
		Msg ->
			case sink_handle(State, Msg) of
				{state, NewState} ->
					sink_iter(NewState);
				stop ->
					?LOG_DEBUG("Sink stopping"),
					ok
			end
	end.

-spec sink_handle(State :: sink_state(), Msg :: start() | window()) -> {state, NewState :: sink_state()} | stop.
sink_handle(State, start) ->
	?LOG_DEBUG("Got start"),
	State#sink_state.upstream ! {start, self()},
	{state, State};
sink_handle(State, {window, _Ts, _Pid, Median}) ->
	?LOG_DEBUG("Got window with median=~w.", [Median]),
	NewState = State#sink_state{window_count = State#sink_state.window_count + 1},
	if
		NewState#sink_state.window_count == NewState#sink_state.num_windows ->
			NewState#sink_state.return ! {ok, self()},
			NewState#sink_state.upstream ! wstop, % not stop!
			?LOG_DEBUG("Sink Done!"),
			stop;
		true ->
			?LOG_DEBUG("Got ~b/~b windows.", [NewState#sink_state.window_count, NewState#sink_state.num_windows]),
			{state, NewState}
	end.
