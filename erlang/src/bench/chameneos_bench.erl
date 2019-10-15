-module(chameneos_bench).

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
	num_chameneos = -1 :: integer(),
	num_meetings = -1 :: integer()}).

-type conf() :: #conf{}.

-record(state, {
	conf = #conf{} :: conf(), 
	mall :: pid() | 'undefined',
	chameneos = [] :: [pid()]}).

-type instance() :: #state{}.

-spec get_num_chameneos(Instance :: instance()) -> integer().
get_num_chameneos(Instance) ->
	Instance#state.conf#conf.num_chameneos.

-spec get_num_meetings(Instance :: instance()) -> integer().
get_num_meetings(Instance) ->
	Instance#state.conf#conf.num_meetings.

-spec new_instance() -> instance().
new_instance() -> 
	#state{}.

-spec msg_to_conf(Msg :: term()) -> {ok, conf()} | {error, string()}.
msg_to_conf(Msg) ->
	case Msg of
		#{	number_of_chameneos := NCham, 
			number_of_meetings := NMeet
		} when 
			is_integer(NCham) andalso (NCham > 0),
			is_integer(NMeet) andalso (NMeet > 0) ->
			{ok, #conf{num_chameneos = NCham, num_meetings = NMeet}};
		#{	number_of_chameneos := NCham, 
			number_of_meetings := NMeet
		} ->
			{error, io_lib:fwrite("Invalid config parameters: ~p or ~p", [NCham, NMeet])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p", [Msg])}
	end.


-spec setup(Instance :: instance(), Conf :: conf()) -> {ok, instance()}.
setup(Instance, Conf) ->
	NewInstance = Instance#state{conf = Conf},
	process_flag(trap_exit, true),
	{ok, NewInstance}.

-spec prepare_iteration(Instance :: instance()) -> {ok, instance()}.
prepare_iteration(Instance) -> 
	Self = self(),
	Mall = spawn_link(fun() -> mall(get_num_meetings(Instance), get_num_chameneos(Instance), Self) end),
	Range = lists:seq(1, get_num_chameneos(Instance)),
	ChameneoFun = fun(Index) -> 
		InitialColour = chameneos_colour:for_id(Index),
		spawn_link(fun () -> chameneo(Mall, InitialColour) end)
	end,
	Chameneos = lists:map(ChameneoFun, Range),
	NewInstance = Instance#state{mall = Mall, chameneos = Chameneos},
	{ok, NewInstance}.

-spec run_iteration(Instance :: instance()) -> {ok, instance()}.
run_iteration(Instance) ->
	lists:foreach(fun(Chameneo) -> Chameneo ! start end, Instance#state.chameneos),
	receive
		ok ->
			{ok, Instance}
		% may get some exit messages already, which shouldn't fail the test
		% X -> 
		% 	io:fwrite("Got unexpected message during iteration: ~p!~n",[X]),
		% 	throw(X)
	end.

-spec cleanup_iteration(Instance :: instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, instance()}.
cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
	ok = bench_helpers:await_exit_all([Instance#state.mall | Instance#state.chameneos]),
	NewInstance = Instance#state{mall = undefined, chameneos = []},
	{ok, NewInstance}.


%%%%%% Messages %%%%%%
-record(meeting_count, {count :: integer()}).
-record(meet, {colour :: chameneos_colour:chameneos_colour(), chameneo :: pid()}).
-type mall_msg() :: #meeting_count{} | #meet{}.
-record(change, {colour :: chameneos_colour:chameneos_colour()}).
-type chameneo_msg() :: start | exit | #meet{} | #change{}.

%%%%%% Mall %%%%%%
-record(mall_state, {
	num_meetings :: integer(),
	num_chameneos :: integer(),
	waiting_chameneo :: pid() | 'undefined',
	sum_meetings = 0 :: integer(),
	meetings_count = 0 :: integer(),
	num_faded = 0 :: integer()
	}).
-type mall_instance() :: #mall_state{}.

-spec mall(NumMeetings :: integer(), NumChameneos :: integer(), Return :: pid()) -> ok.
mall(NumMeetings, NumChameneos, Return) ->
	Instance = #mall_state{num_meetings = NumMeetings, num_chameneos = NumChameneos},
	ok = run_mall(Instance),
	Return ! ok.

-spec run_mall(Instance :: mall_instance()) -> ok.
run_mall(Instance) ->
	Res = receive
		Msg when is_record(Msg, meeting_count) ->
			handle_mall_msg(Instance, Msg);
		Msg when is_record(Msg, meet) ->
			handle_mall_msg(Instance, Msg);
		X -> 
			io:fwrite("Mall got unexpected message: ~p!~n",[X]),
			throw(X)
	end,
	case Res of
		{ok, NewInstance} ->
			run_mall(NewInstance);
		done ->
			ok
	end.

-spec handle_mall_msg(Instance :: mall_instance(), Msg :: mall_msg()) -> {ok, NewInstance :: mall_instance()} | done.
handle_mall_msg(Instance, #meeting_count{count = Count}) ->
	NewFaded = Instance#mall_state.num_faded + 1,
	NewMeetings = Instance#mall_state.sum_meetings + Count,
	if
		NewFaded == Instance#mall_state.num_chameneos ->
			done;
		true ->
			{ok, Instance#mall_state{num_faded = NewFaded, sum_meetings = NewMeetings}}
	end;
handle_mall_msg(Instance, Msg) when is_record(Msg, meet) ->
	#meet{chameneo = Chameneo} = Msg,
	if 
		Instance#mall_state.meetings_count < Instance#mall_state.num_meetings ->
			case Instance#mall_state.waiting_chameneo of
				Other when is_pid(Other) ->
					NewMeetings = Instance#mall_state.meetings_count + 1,
					Other ! Msg,
					{ok, Instance#mall_state{meetings_count = NewMeetings, waiting_chameneo = undefined}};
				undefined ->
					{ok, Instance#mall_state{waiting_chameneo = Chameneo}}
			end;
		true -> 
			Chameneo ! exit,
			{ok, Instance}
	end.

%%%%%% Chameneo %%%%%%
-record(chameneo_state, {
	mall :: pid(),
	colour :: chameneos_colour:chameneos_colour(),
	meetings = 0 :: integer()
	}).
-type chameneo_instance() :: #chameneo_state{}.

-spec chameneo(Mall :: pid(), InitialColour :: chameneos_colour:chameneos_colour()) -> ok.
chameneo(Mall, InitialColour) ->
	Instance = #chameneo_state{mall = Mall, colour = InitialColour},
	ok = run_chameneo(Instance).

-spec run_chameneo(Instance :: chameneo_instance()) -> ok.
run_chameneo(Instance) ->
	Res = receive
		start ->
			handle_chameneo_msg(Instance, start);
		exit ->
			handle_chameneo_msg(Instance, exit);
		Msg when is_record(Msg, meet) ->
			handle_chameneo_msg(Instance, Msg);
		Msg when is_record(Msg, change) ->
			handle_chameneo_msg(Instance, Msg);
		X -> 
			io:fwrite("Chameneo got unexpected message: ~p!~n",[X]),
			throw(X)
	end,
	case Res of
		{ok, NewInstance} ->
			run_chameneo(NewInstance);
		done ->
			ok
	end.

-spec handle_chameneo_msg(Instance :: chameneo_instance(), Msg :: chameneo_msg()) -> {ok, NewInstance :: chameneo_instance()} | done.
handle_chameneo_msg(Instance, start) ->
	Instance#chameneo_state.mall ! #meet{colour = Instance#chameneo_state.colour, chameneo = self()},
	{ok, Instance};
handle_chameneo_msg(Instance, #meet{colour = OtherColour, chameneo = Chameneo}) ->
	NewColour = chameneos_colour:complement(Instance#chameneo_state.colour, OtherColour),
	NewMeetings = Instance#chameneo_state.meetings + 1,
	Chameneo ! #change{colour = NewColour},
	NewInstance = Instance#chameneo_state{colour = NewColour, meetings = NewMeetings},
	NewInstance#chameneo_state.mall ! #meet{colour = NewInstance#chameneo_state.colour, chameneo = self()},
	{ok, NewInstance};
handle_chameneo_msg(Instance, #change{colour = NewColour}) ->
	NewMeetings = Instance#chameneo_state.meetings + 1,
	NewInstance = Instance#chameneo_state{colour = NewColour, meetings = NewMeetings},
	NewInstance#chameneo_state.mall ! #meet{colour = NewInstance#chameneo_state.colour, chameneo = self()},
	{ok, NewInstance};
handle_chameneo_msg(Instance, exit) ->
	% no point in setting colour to faded, since we are throwing away the state anyway
	Instance#chameneo_state.mall ! #meeting_count{count = Instance#chameneo_state.meetings},
	done.

%%%% TESTS %%%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

chameneos_test() ->
	ChamR = #{
		number_of_chameneos => 10,
		number_of_meetings => 10
	},
	{ok, Result} = benchmarks_server:await_benchmark_result(?MODULE, ChamR, "Chameneos"),
	true = test_result:is_success(Result) orelse test_result:is_rse_failure(Result).

-endif.
