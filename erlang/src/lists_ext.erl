-module(lists_ext).

-export([fail_fast_foldl/3]).

-spec fail_fast_foldl(Fun :: fun((Acc, Value) -> {ok, Acc} | fail), Acc, List :: [Value]) ->
	{ok, Acc} | {fail, Value}.
fail_fast_foldl(Fun, Acc0, List) ->
	rec_fail_fast_foldl(Fun, Acc0, List).

-spec rec_fail_fast_foldl(Fun :: fun((Acc, Value) -> {ok, Acc} | fail), Acc, List :: [Value])->
	{ok, Acc} | {fail, Value}.
rec_fail_fast_foldl(_Fun, Acc, []) ->
	{ok, Acc};
rec_fail_fast_foldl(Fun, Acc, [Head | Rest]) ->
	case Fun(Acc, Head) of
		{ok, NewAcc} ->
			rec_fail_fast_foldl(Fun, NewAcc, Rest);
		fail ->
			{fail, Head}
	end.

