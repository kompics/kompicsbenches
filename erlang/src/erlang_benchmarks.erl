-module(erlang_benchmarks).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, Args) ->
	io:fwrite("test~n",[]),
	erlang:display(Args),
	erlang:display(application:get_env(erlang_benchmarks, testarg)),
	erlang_sup:start_link().

stop(_State) ->
	ok.
