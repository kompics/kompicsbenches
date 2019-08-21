-module(bench_helpers).

-export([
	await_exit/1,
	await_exit_all/1,
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
			{error, X}
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
			{error, X}
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
