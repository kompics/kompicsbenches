-module(gen_statem_ext).
%%% -----------------------------------------------------------------
%%% Port the multi_call from gen_server to gen_statem.
%%% -----------------------------------------------------------------
-export([reply_multi/1, reply_multi/2, multi_call/2, multi_call/3, multi_call/4]).
-export([middleman/6]).

-type from() :: {To :: pid(), Tag :: reference()}.

-export_type([from/0]).

%%% -----------------------------------------------------------------
%%% Reply to a multi_call, which requires a slightly different format than normal gen_statem replies.
%%% -----------------------------------------------------------------
-spec reply_multi({reply, From :: from(), Reply :: term()}) -> ok.
reply_multi({reply, From, Reply}) ->
	reply_multi(From, Reply).

-spec reply_multi(From :: from(), Reply :: term()) -> ok.
reply_multi(From, Reply) ->
	{To, Tag} = From,
	Msg = {Tag, Reply},
	%io:fwrite("reply_multi to ~w with ~p.~n",[To, Msg]),
	try To ! Msg of
		_ ->
		    ok
    catch
		_:_ -> ok
    end.

%%% -----------------------------------------------------------------
%%% Make a call to servers at several nodes.
%%% Returns: {[Replies],[BadNodes]}
%%% A Timeout can be given
%%% 
%%% A middleman process is used in case late answers arrives after
%%% the timeout. If they would be allowed to glog the callers message
%%% queue, it would probably become confused. Late answers will 
%%% now arrive to the terminated middleman and so be discarded.
%%% -----------------------------------------------------------------
-spec multi_call(Name :: atom(), Req :: term()) -> 
	{Replies :: [term()], BadNodes :: [node()]}.
multi_call(Name, Req) when is_atom(Name) ->
    do_multi_call([node() | nodes()], Name, Req, infinity).

-spec multi_call(Nodes :: [node()], Name :: atom(), Req :: term()) -> 
	{Replies :: [term()], BadNodes :: [node()]}.
multi_call(Nodes, Name, Req) when is_list(Nodes), is_atom(Name) ->
    do_multi_call(Nodes, Name, Req, infinity).

-spec multi_call(Nodes :: [node()], Name :: atom(), Req :: term(), Timeout :: integer() | infinity) -> 
	{Replies :: [term()], BadNodes :: [node()]}.
multi_call(Nodes, Name, Req, infinity) ->
    do_multi_call(Nodes, Name, Req, infinity);
multi_call(Nodes, Name, Req, Timeout) when is_list(Nodes), is_atom(Name), is_integer(Timeout), Timeout >= 0 ->
    do_multi_call(Nodes, Name, Req, Timeout).

%%%
%%% Internal
%%%

do_multi_call(Nodes, Name, Req, infinity) ->
    Tag = make_ref(),
    Monitors = send_nodes(Nodes, Name, Tag, Req),
    rec_nodes(Tag, Monitors, Name, undefined);
do_multi_call(Nodes, Name, Req, Timeout) ->
    Tag = make_ref(),
    Caller = self(),
    Receiver = spawn(?MODULE, middleman, [Nodes, Name, Tag, Req, Caller, Timeout]),
    %Receiver = spawn(fun() -> middleman(Nodes, Name, Tag, Req, Caller, Timeout) end),
    Mref = erlang:monitor(process, Receiver),
    Receiver ! {self(),Tag},
    receive
		{'DOWN',Mref,_,_,{Receiver,Tag,Result}} ->
			%io:fwrite("multi_call middleman finished: ~p.~n",[Result]),
		    Result;
		{'DOWN',Mref,_,_,Reason} ->
		    %% The middleman code failed. Or someone did 
		    %% exit(_, kill) on the middleman process => Reason==killed
		    %io:fwrite("multi_call middleman failed: ~p.~n",[Reason]),
		    exit(Reason)
    end.

-spec middleman(Nodes :: [node()], Name :: atom(), Tag :: reference(), Req :: term(), Caller :: pid(), Timeout :: integer() | infinity) -> 
	no_return().
middleman(Nodes, Name, Tag, Req, Caller, Timeout) ->
	%% Middleman process. Should be unsensitive to regular
	%% exit signals. The sychronization is needed in case
	%% the receiver would exit before the caller started
	%% the monitor.
	process_flag(trap_exit, true),
	Mref = erlang:monitor(process, Caller),
	receive
		{Caller,Tag} ->
			Monitors = send_nodes(Nodes, Name, Tag, Req),
			TimerId = erlang:start_timer(Timeout, self(), ok),
			Result = rec_nodes(Tag, Monitors, Name, TimerId),
			%io:fwrite("middleman is done: ~p.~n",[Result]),
			exit({self(),Tag,Result});
		{'DOWN',Mref,_,_,_} ->
			%% Caller died before sending us the go-ahead.
			%% Give up silently.
			%io:fwrite("middleman caller died!~n"),
			exit(normal)
	end.

send_nodes(Nodes, Name, Tag, Req) ->
    send_nodes(Nodes, Name, Tag, Req, []).

send_nodes([Node|Tail], Name, Tag, Req, Monitors) when is_atom(Node) ->
    Monitor = start_monitor(Node, Name),
    %% Handle non-existing names in rec_nodes.
    catch {Name, Node} ! {'$gen_call', {self(), {Tag, Node}}, Req},
    send_nodes(Tail, Name, Tag, Req, [Monitor | Monitors]);
send_nodes([_Node|Tail], Name, Tag, Req, Monitors) ->
    %% Skip non-atom Node
    send_nodes(Tail, Name, Tag, Req, Monitors);
send_nodes([], _Name, _Tag, _Req, Monitors) -> 
    Monitors.

%% Against old nodes:
%% If no reply has been delivered within 2 secs. (per node) check that
%% the server really exists and wait for ever for the answer.
%%
%% Against contemporary nodes:
%% Wait for reply, server 'DOWN', or timeout from TimerId.

rec_nodes(Tag, Nodes, Name, TimerId) -> 
    rec_nodes(Tag, Nodes, Name, [], [], 2000, TimerId).

rec_nodes(Tag, [{N,R}|Tail], Name, Badnodes, Replies, Time, TimerId ) ->
    receive
	{'DOWN', R, _, _, _} ->
	    rec_nodes(Tag, Tail, Name, [N|Badnodes], Replies, Time, TimerId);
	{{Tag, N}, Reply} ->  %% Tag is bound !!!
	    unmonitor(R), 
	    rec_nodes(Tag, Tail, Name, Badnodes, 
		      [{N,Reply}|Replies], Time, TimerId);
	{timeout, TimerId, _} ->	
	    unmonitor(R),
	    %% Collect all replies that already have arrived
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies)
    end;
rec_nodes(Tag, [N|Tail], Name, Badnodes, Replies, Time, TimerId) ->
    %% R6 node
    receive
	{nodedown, N} ->
	    monitor_node(N, false),
	    rec_nodes(Tag, Tail, Name, [N|Badnodes], Replies, 2000, TimerId);
	{{Tag, N}, Reply} ->  %% Tag is bound !!!
	    receive {nodedown, N} -> ok after 0 -> ok end,
	    monitor_node(N, false),
	    rec_nodes(Tag, Tail, Name, Badnodes,
		      [{N,Reply}|Replies], 2000, TimerId);
	{timeout, TimerId, _} ->	
	    receive {nodedown, N} -> ok after 0 -> ok end,
	    monitor_node(N, false),
	    %% Collect all replies that already have arrived
	    rec_nodes_rest(Tag, Tail, Name, [N | Badnodes], Replies)
    after Time ->
	    case rpc:call(N, erlang, whereis, [Name]) of
		Pid when is_pid(Pid) -> % It exists try again.
		    rec_nodes(Tag, [N|Tail], Name, Badnodes,
			      Replies, infinity, TimerId);
		_ -> % badnode
		    receive {nodedown, N} -> ok after 0 -> ok end,
		    monitor_node(N, false),
		    rec_nodes(Tag, Tail, Name, [N|Badnodes],
			      Replies, 2000, TimerId)
	    end
    end;
rec_nodes(_, [], _, Badnodes, Replies, _, TimerId) ->
    case catch erlang:cancel_timer(TimerId) of
	false ->  % It has already sent it's message
	    receive
		{timeout, TimerId, _} -> ok
	    after 0 ->
		    ok
	    end;
	_ -> % Timer was cancelled, or TimerId was 'undefined'
	    ok
    end,
    {Replies, Badnodes}.

%% Collect all replies that already have arrived
rec_nodes_rest(Tag, [{N,R}|Tail], Name, Badnodes, Replies) ->
    receive
	{'DOWN', R, _, _, _} ->
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies);
	{{Tag, N}, Reply} -> %% Tag is bound !!!
	    unmonitor(R),
	    rec_nodes_rest(Tag, Tail, Name, Badnodes, [{N,Reply}|Replies])
    after 0 ->
	    unmonitor(R),
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies)
    end;
rec_nodes_rest(Tag, [N|Tail], Name, Badnodes, Replies) ->
    %% R6 node
    receive
	{nodedown, N} ->
	    monitor_node(N, false),
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies);
	{{Tag, N}, Reply} ->  %% Tag is bound !!!
	    receive {nodedown, N} -> ok after 0 -> ok end,
	    monitor_node(N, false),
	    rec_nodes_rest(Tag, Tail, Name, Badnodes, [{N,Reply}|Replies])
    after 0 ->
	    receive {nodedown, N} -> ok after 0 -> ok end,
	    monitor_node(N, false),
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies)
    end;
rec_nodes_rest(_Tag, [], _Name, Badnodes, Replies) ->
    {Replies, Badnodes}.


%%% ---------------------------------------------------
%%% Monitor functions
%%% ---------------------------------------------------

start_monitor(Node, Name) when is_atom(Node), is_atom(Name) ->
    if node() =:= nonode@nohost, Node =/= nonode@nohost ->
	    Ref = make_ref(),
	    self() ! {'DOWN', Ref, process, {Name, Node}, noconnection},
	    {Node, Ref};
       true ->
	    case catch erlang:monitor(process, {Name, Node}) of
		{'EXIT', _} ->
		    %% Remote node is R6
		    monitor_node(Node, true),
		    Node;
		Ref when is_reference(Ref) ->
		    {Node, Ref}
	    end
    end.

%% Cancels a monitor started with Ref=erlang:monitor(_, _).
unmonitor(Ref) when is_reference(Ref) ->
    erlang:demonitor(Ref),
    receive
	{'DOWN', Ref, _, _, _} ->
	    true
    after 0 ->
	    true
    end.
