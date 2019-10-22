<<<<<<< HEAD
-module(bench_helpers).

-export([
	await_exit/1,
	await_exit_all/1,
	await_all/2,
	benchmark_type/1
	]).

-spec await_exit(Pid :: pid()) ->
	{ok, any()} |
	{error, any()}.
await_exit(Pid) ->
	receive
		{'EXIT', Pid, Reason} ->
			{ok, Reason};
		X ->
			{error, {expected_exit_but_got, X}}
	end.

-spec await_exit_all(Pids :: [pid()]) ->
	ok |
	{error, any()}.
await_exit_all([]) ->
	ok;
await_exit_all(Pids) ->
	receive
		{'EXIT', Pid, _Reason} ->
			NewPids = lists:delete(Pid, Pids),
			if
				length(NewPids) == length(Pids) ->
					{error, io_lib:fwrite("Process ~p exited, but was not found in ~p.~n", [Pid, Pids])};
				true ->
					await_exit_all(NewPids)
			end;
		X ->
			{error, {expected_exit_but_got, X}}
	end.

-spec await_all(Pids :: [pid()], Token :: term()) ->
	ok |
	{error, term()}.
await_all([], _Token) ->
	ok;
await_all(Pids, Token) ->
	receive
		{Token, Pid} ->
			NewPids = lists:delete(Pid, Pids),
			if
				length(NewPids) == length(Pids) ->
					{error, io_lib:fwrite("Process ~p sent token, but was not found in ~p.~n", [Pid, Pids])};
				true ->
					await_all(NewPids, Token)
			end
	end.

-spec benchmark_type(Module :: module()) ->
	local |
	distributed |
	none.
benchmark_type(Module) ->
	Attrs = Module:module_info(attributes),
	case lists:search(
		fun(Entry) -> 
			case Entry of
				{behaviour, _Behaviours} -> 
					true;
				_ -> false
			end
		end, Attrs) of 
		{value, {behaviour, [local_benchmark]}} ->
			local;
		{value, {behaviour, [distributed_benchmark]}} ->
			distributed;
		_ ->
			none
	end.
=======
-module(bench_helpers).

-export([
	isolate/1,
	await_exit/1,
	await_exit_all/1,
	await_all/2,
	benchmark_type/1
	]).

-spec isolate(Fun :: fun(() -> T)) -> T.
isolate(Fun) ->
	Self = self(),
	_Pid = spawn_link(fun() -> 
		Res = Fun(),
		Self ! {result, Res}
	end),
	receive
		{result, Res} ->
			Res
	end.

-spec await_exit(Pid :: pid()) ->
	{ok, any()} |
	{error, any()}.
await_exit(Pid) ->
	receive
		{'EXIT', Pid, Reason} ->
			{ok, Reason};
		X ->
			{error, {expected_exit_but_got, X}}
	end.

-spec await_exit_all(Pids :: [pid()]) ->
	ok |
	{error, any()}.
await_exit_all([]) ->
	ok;
await_exit_all(Pids) ->
	receive
		{'EXIT', Pid, _Reason} ->
			NewPids = lists:delete(Pid, Pids),
			if
				length(NewPids) == length(Pids) ->
					%io:format(user, "Process ~p exited, but was not found in ~p.~n", [Pid, Pids]),
					{error, io_lib:fwrite("Process ~p exited, but was not found in ~p.~n", [Pid, Pids])};
				true ->
					await_exit_all(NewPids)
			end;
		X ->
			%io:format(user, "Expected 'EXIT', but got: ~p.~n", [X]),
			{error, {expected_exit_but_got, X}}
	end.

-spec await_all(Pids :: [pid()], Token :: term()) ->
	ok |
	{error, term()}.
await_all([], _Token) ->
	ok;
await_all(Pids, Token) ->
	receive
		{Token, Pid} ->
			NewPids = lists:delete(Pid, Pids),
			if
				length(NewPids) == length(Pids) ->
					{error, io_lib:fwrite("Process ~p sent token, but was not found in ~p.~n", [Pid, Pids])};
				true ->
					await_all(NewPids, Token)
			end
	end.

-spec benchmark_type(Module :: module()) ->
	local |
	distributed |
	none.
benchmark_type(Module) ->
	Attrs = Module:module_info(attributes),
	case lists:search(
		fun(Entry) -> 
			case Entry of
				{behaviour, _Behaviours} -> 
					true;
				_ -> false
			end
		end, Attrs) of 
		{value, {behaviour, [local_benchmark]}} ->
			local;
		{value, {behaviour, [distributed_benchmark]}} ->
			distributed;
		_ ->
			none
	end.
>>>>>>> c92c44604e519dd0b6cc96f7ef122b9ca6b9cde1
