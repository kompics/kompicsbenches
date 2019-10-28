-module(benchmark_runner).

-export([
	min_runs/0,
	max_runs/0,
	rse_target/0,
	start/1,
	run/2
	]).

-define(MIN_RUNS, 30).
-define(MAX_RUNS, 100).
-define(RSE_TARGET, 0.1). % 10% RSE

-spec min_runs() -> integer().
min_runs() ->
	?MIN_RUNS.

-spec max_runs() -> integer().
max_runs() ->
	?MAX_RUNS.

-spec rse_target() -> float().
rse_target() ->
	?RSE_TARGET.

-spec start(RunnerAddr :: socket_addr:ip()) -> ok.
start(RunnerAddr) ->
	RunnerPort = socket_addr:port(RunnerAddr),
	{ok, _Server} = grpc:start_server(
		runner_server,
		tcp,
		RunnerPort,
		#{'BenchmarkRunner' => #{handler => benchmarks_server}}),
	io:fwrite("Benchmark server is running on TCP port=~b.~n",[RunnerPort]),
	ok.

-spec run(Benchmark :: module(), Msg :: term()) ->
	{ok, [float()]} |
	{error, string()}.
run(Benchmark, Msg) ->
	case (catch run_caught(Benchmark, Msg)) of
		{ok, Data} ->
			{ok, Data};
		{error, Reason} ->
			io:format(user, "Error during run:~p~n", [Reason]),
			{error, Reason};
		Other ->
			io:format(user, "Error during run:~p~n", [Other]),
			{error, io_lib:fwrite("Error during run:~p~n", [Other])}
	end.

-spec run_caught(Benchmark :: module(), Msg :: term()) ->
	{ok, [float()]} |
	{error, string()}.
run_caught(Benchmark, Msg) ->
	case Benchmark:msg_to_conf(Msg) of
		{ok, Conf} ->
			Instance = Benchmark:new_instance(),
			{ok, SetupInstance} = Benchmark:setup(Instance, Conf),
			{ok, PreparedFirstInstance} = Benchmark:prepare_iteration(SetupInstance),
			{Time, RunFirstInstance} = measure(Benchmark,PreparedFirstInstance),
			{ok, CountedInstance, CountedResults} = run_counted(Benchmark, RunFirstInstance, ?MIN_RUNS - 1, [Time]),
			{ResType,FinalInstance,ResArg} = run_rse(Benchmark, CountedInstance, ?MIN_RUNS, CountedResults),
			{ok, _} = Benchmark:cleanup_iteration(FinalInstance, true, Time), % yeah, it's the wrong time...but whatever
			{ResType, ResArg};
		{error, Reason} ->
			{error, Reason}
	end.


-spec run_counted(Benchmark :: module(), Instance, Count :: integer(), Res :: [float()]) ->
	{ok, Instance, [float()]}.
run_counted(_Benchmark, Instance, Count, Res) when Count =< 0 ->
	{ok, Instance, Res};
run_counted(Benchmark, Instance, Count, Res) ->
	[LastTime|_] = Res,
	{ok, CleanedInstance} = Benchmark:cleanup_iteration(Instance, false, LastTime),
	{ok, PreparedInstance} = Benchmark:prepare_iteration(CleanedInstance),
	{Time, RunInstance} = measure(Benchmark,PreparedInstance),
	run_counted(Benchmark, RunInstance, Count - 1, [Time|Res]).

-spec run_rse(Benchmark :: module(), Instance, Count :: integer(), Res :: [float()]) ->
	{ok, Instance, [float()]} |
	{error, Instance, string()}.
run_rse(_Benchmark, Instance, Count, Res) when Count >= ?MAX_RUNS ->
	RSE = statistics:rse(Res),
	if
		RSE > ?RSE_TARGET ->
			{error, Instance, io_lib:fwrite("RSE target of ~f% was not met by value ~f% after ~b runs!~n",[?RSE_TARGET*100.0, RSE*100.0, Count])};
		true ->
			{ok, Instance, Res}
	end;
run_rse(Benchmark, Instance, Count, Res) ->
	case statistics:rse(Res) of
		RSE when RSE =< ?RSE_TARGET ->
			{ok, Instance, Res};
		_ ->
			[LastTime|_] = Res,
			{ok, CleanedInstance} = Benchmark:cleanup_iteration(Instance, false, LastTime),
			{ok, PreparedInstance} = Benchmark:prepare_iteration(CleanedInstance),
			{Time, RunInstance} = measure(Benchmark,PreparedInstance),
			run_rse(Benchmark, RunInstance, Count + 1, [Time|Res])
	end.

-spec measure(Benchmark :: module(), Instance) -> {float(), Instance}.
measure(Benchmark,Instance) ->
	{Time, {ok, RunInstance}} = timer:tc(Benchmark,run_iteration,[Instance]),
	TimeMillis = Time / 1000.0,
	{TimeMillis, RunInstance}.
