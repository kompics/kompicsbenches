-module(erlang_benchmarks).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	io:fwrite("Passed args raw:~n",[]),
	%erlang:display(Args),
	erlang:display(application:get_env(erlang_benchmarks, runner)),
	erlang:display(application:get_env(erlang_benchmarks, master)),
	erlang:display(application:get_env(erlang_benchmarks, client)),
	erlang:display(application:get_env(erlang_benchmarks, clients)),
	{ok, RunnerAddrStr} = application:get_env(erlang_benchmarks, runner),
	{ok, MasterAddrStr} = application:get_env(erlang_benchmarks, master),
	{ok, ClientAddrStr} = application:get_env(erlang_benchmarks, client),
	{ok, NumClients} = application:get_env(erlang_benchmarks, clients),
	true = is_integer(NumClients),
	io:fwrite("Passed args:~s ~s ~s ~b ~n",[RunnerAddrStr, MasterAddrStr, ClientAddrStr, NumClients]),
	case {socket_addr:parse(RunnerAddrStr), socket_addr:parse(MasterAddrStr), socket_addr:parse(ClientAddrStr)} of
		{{ok, RunnerAddr}, empty, empty} ->
			% local mode
			io:fwrite("Starting in local mode with runner=~s~n", [socket_addr:to_string(RunnerAddr)]),
			ok = benchmark_runner:start(RunnerAddr),
			ok;
		{empty, {ok, MasterAddr}, {ok, ClientAddr}} ->
			% client mode
			io:fwrite("Starting in client mode with master=~s and client=~s~n", [socket_addr:to_string(MasterAddr), socket_addr:to_string(ClientAddr)]),
			MasterNodeStr = io_lib:fwrite("erlang~b@~s",[socket_addr:port(MasterAddr), socket_addr:addr(MasterAddr)]),
			%io:fwrite("About to ping ~s.~n", [MasterNodeStr]),
			MasterNode = list_to_atom(lists:flatten(MasterNodeStr)),
			ok = connect_to_master(MasterNode, 5),
			{ok, _ClientPid} = benchmark_client:start_link(),
			ok;
		{{ok, RunnerAddr}, {ok, MasterAddr}, empty} ->
			% master mode
			io:fwrite("Starting in master mode with runner=~s and master=~s and ~b clients~n", [socket_addr:to_string(RunnerAddr), socket_addr:to_string(MasterAddr), NumClients]),
			{ok, _MasterPid} = benchmark_master:start_link(#{wait_for => NumClients}),
			ok = benchmark_master:start_runner(RunnerAddr),
			ok;
		_ ->
			io:fwrite("Invalid starting mode!~n")
	end,
	erlang_sup:start_link().

stop(_State) ->
	ok.

-spec connect_to_master(MasterNode :: node(), Attempt :: integer()) -> ok | failed.
connect_to_master(_MasterNode, 0) ->
	failed;
connect_to_master(MasterNode, Attempt) ->
	io:fwrite("Trying to connect to master..."),
	case net_adm:ping(MasterNode) of
		pong ->
			io:fwrite("Succeeded in connecting to master!~n"),
			ok;
		pang ->
			io:fwrite("Failed connect to master!~n"),
			ok = timer:sleep(1000),
			connect_to_master(MasterNode, Attempt - 1)
	end.
