-module(erlang_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, Args) ->
	erlang:display(Args),
	erlang_sup:start_link().

stop(_State) ->
	ok.
