-module(all_pairs_shortest_path_bench).

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
	num_nodes = -1 :: integer(),
	block_size = -1 :: integer()}).

-type conf() :: #conf{}.

%% Type aliases for convenience
-type inf_float() :: graphs:inf_float().
-type graph() :: graphs:graph(inf_float()).
-type block() :: graphs:block(inf_float()).
-type blocked_graph() :: graphs:blocked_graph(inf_float()).

-record(state, {
	conf = #conf{} :: conf(), 
	manager :: pid() | 'undefined',
	graph :: graph() | 'undefined'}).

-type instance() :: #state{}.

-spec get_num_nodes(Instance :: instance()) -> integer().
get_num_nodes(Instance) ->
	Instance#state.conf#conf.num_nodes.

-spec get_block_size(Instance :: instance()) -> integer().
get_block_size(Instance) ->
	Instance#state.conf#conf.block_size.

-spec new_instance() -> instance().
new_instance() -> 
	#state{}.

-spec msg_to_conf(Msg :: term()) -> {ok, conf()} | {error, string()}.
msg_to_conf(Msg) ->
	case Msg of
		#{	number_of_nodes := NNodes, 
			block_size := BSize
		} when 
			is_integer(NNodes) andalso (NNodes > 0),
			is_integer(BSize) andalso (BSize > 0) ->
			{ok, #conf{num_nodes = NNodes, block_size = BSize}};
		#{	number_of_nodes := NNodes, 
			block_size := BSize
		} ->
			{error, io_lib:fwrite("Invalid config parameters: ~p or ~p", [NNodes, BSize])};
		_ ->
			{error, io_lib:fwrite("Invalid config message:~p", [Msg])}
	end.


-spec setup(Instance :: instance(), Conf :: conf()) -> {ok, instance()}.
setup(Instance, Conf) ->
	ConfInstance = Instance#state{conf = Conf},
	process_flag(trap_exit, true),
	Manager = spawn_link(fun() -> manager(get_block_size(ConfInstance)) end),
	Graph = graphs:generate_graph(get_num_nodes(ConfInstance)),
	NewInstance = ConfInstance#state{manager = Manager, graph = Graph},
	{ok, NewInstance}.

-spec prepare_iteration(Instance :: instance()) -> {ok, instance()}.
prepare_iteration(Instance) -> 
	{ok, Instance}.

-spec run_iteration(Instance :: instance()) -> {ok, instance()}.
run_iteration(Instance) ->
	Instance#state.manager ! new_compute_fw(Instance#state.graph, self()),
	receive
		ok ->
			{ok, Instance};
		X -> 
			io:fwrite("Got unexpected message during iteration: ~p!~n",[X]),
			throw(X)
	end.

-spec cleanup_iteration(Instance :: instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, instance()}.
cleanup_iteration(Instance, LastIteration, _ExecTimeMillis) ->
	case LastIteration of
		true ->
			Instance#state.manager ! stop,
			{ok, _} = bench_helpers:await_exit(Instance#state.manager),
			NewInstance = Instance#state{manager = undefined, graph = undefined},
			{ok, NewInstance};
		false ->
			{ok, Instance}
	end.


%%%%%% Messages %%%%%%
-record(compute_fw, {
		graph :: graph(),
		return :: pid()
	}).
-spec new_compute_fw(Graph :: graph(), Return :: pid()) -> #compute_fw{}.
new_compute_fw(Graph, Return) ->
	#compute_fw{graph = Graph, return = Return}.

-record(block_result, {block :: block()}).
-spec new_block_result(Block :: block()) -> #block_result{}.
new_block_result(Block) ->
	#block_result{block = Block}.

-type manager_msg() :: #compute_fw{} | #block_result{} | stop.

-record(neighbours, {nodes :: [pid()]}).
-spec new_neighbours(Nodes :: [pid()]) -> #neighbours{}.
new_neighbours(Nodes) ->
	#neighbours{nodes = Nodes}.

-record(phase_result, {k :: integer(), data :: block()}).
-spec new_phase_result(K :: integer(), Data :: block()) -> #phase_result{}.
new_phase_result(K, Data) ->
	#phase_result{k = K, data = Data}.

-type block_msg() :: #neighbours{} | #phase_result{}.

%%%%%% Manager %%%%%%
-record(manager_state, {
	block_size :: integer(),
	return :: pid() | 'undefined',
	assembly :: blocked_graph() | 'undefined',
	missing_blocks = -1 :: integer()
	}).
-type manager_instance() :: #manager_state{}.

-spec manager(BlockSize :: integer()) -> ok.
manager(BlockSize) ->
	Instance = #manager_state{block_size = BlockSize},
	ok = run_manager(Instance).

-spec run_manager(Instance :: manager_instance()) -> ok.
run_manager(Instance) ->
	Res = receive
		Msg when is_record(Msg, compute_fw) ->
			handle_manager_msg(Instance, Msg);
		Msg when is_record(Msg, block_result) ->
			handle_manager_msg(Instance, Msg);
		stop ->
			handle_manager_msg(Instance, stop);
		X -> 
			io:fwrite("Manager got unexpected message: ~p!~n",[X]),
			throw(X)
	end,
	case Res of
		{ok, NewInstance} ->
			run_manager(NewInstance);
		done ->
			ok
	end.

-spec handle_manager_msg(Instance :: manager_instance(), Msg :: manager_msg()) -> {ok, NewInstance :: manager_instance()} | done.
handle_manager_msg(Instance, #compute_fw{graph = Graph, return = Return}) ->
	DistInstance = distribute_graph(Instance, Graph),
	NewInstance = DistInstance#manager_state{return = Return},
	{ok, NewInstance};
handle_manager_msg(Instance, #block_result{block = Block}) ->
	Pos = graphs:block_position(Block),
	% io:format(user, "Updating assembly when missing_blocks=~b.~n", [Instance#manager_state.missing_blocks]),
	NewAssembly = graphs:blocked_set(Pos, Block, Instance#manager_state.assembly),
	NewMissingBlocks = Instance#manager_state.missing_blocks - 1,
	if 
		NewMissingBlocks == 0 ->
			_Result = graphs:assemble_from_blocks(NewAssembly),
			Instance#manager_state.return ! ok,
			% cleanup
			NewInstance = Instance#manager_state{
				return = undefined,
				assembly = undefined,
				missing_blocks = -1
			},
			{ok, NewInstance};
		true ->
			NewInstance = Instance#manager_state{assembly = NewAssembly, missing_blocks = NewMissingBlocks},
			{ok, NewInstance}
	end;
handle_manager_msg(_Instance, stop) ->
	done.

-spec distribute_graph(Instance :: manager_instance(), Graph :: graph()) -> NewInstance :: manager_instance().
distribute_graph(Instance, Graph) ->
	BlockSize = Instance#manager_state.block_size,
	Blocks = graphs:break_into_blocks(BlockSize, Graph),
	%io:format(user, "Blocks: ~p~n", [Blocks]),
	NumNodes = graphs:num_nodes(Graph),
	NumBlocks = graphs:blocked_size(Blocks),
	Self = self(),
	BlockActors = graphs:blocked_map(fun(Block) -> 
		spawn_link(fun() -> block_actor(Block, NumNodes, Self) end)
	end, Blocks),
	%io:format(user, "BlockActors: ~p~n", [BlockActors]),
	ok = lists:foreach(fun({BI, BJ}) -> 
		Neighbours = lists:foldl(fun(RCIndex, Neigh) -> 
			RNeigh = if 
				RCIndex == BI ->	
					Neigh;
				true ->
					[gb_trees:get({RCIndex, BJ}, BlockActors) | Neigh]
			end,
			CNeigh = if 
				RCIndex == BJ ->	
					RNeigh;
				true ->
					[gb_trees:get({BI, RCIndex}, BlockActors) | RNeigh]
				end,
			CNeigh
		end,[],lists:seq(0, NumBlocks - 1)),
		BlockActor = gb_trees:get({BI, BJ}, BlockActors),
		BlockActor ! new_neighbours(Neighbours)
	end, graphs:tabulate(NumBlocks)),
	NewInstance = Instance#manager_state{assembly = Blocks, missing_blocks = NumBlocks * NumBlocks},
	NewInstance.

%%%%%% Block Actor %%%%%%
-record(block_actor_state, {
	num_nodes :: integer(),
	manager :: pid(),
	neighbours = [] :: [pid()],
	num_neighbours = 0 :: integer(),
	k = -1 :: integer(),
	neighbour_data = gb_trees:empty() :: gb_trees:tree(integer(), block()),
	next_neighbour_data = gb_trees:empty() :: gb_trees:tree(integer(), block()),
	current_data :: block()
	}).
-type block_actor_instance() :: #block_actor_state{}.

-spec block_actor(InitialBlock :: block(), NumNodes :: integer(), Manager :: pid()) -> ok.
block_actor(InitialBlock, NumNodes, Manager) ->
	Instance = #block_actor_state{num_nodes = NumNodes, manager = Manager, current_data = InitialBlock},
	ok = block_actor_wait_for_neighbours(Instance).

-spec block_actor_wait_for_neighbours(Instance :: block_actor_instance()) -> ok.
block_actor_wait_for_neighbours(Instance) ->
	receive
		#neighbours{nodes = Nodes} ->
			NumNeighbours = length(Nodes),
			NewInstance = Instance#block_actor_state{neighbours = Nodes, num_neighbours = NumNeighbours},
			notify_neighbours(NewInstance),
			block_actor_run_phases(NewInstance)
	end.

-spec block_actor_run_phases(Instance :: block_actor_instance()) -> ok.
block_actor_run_phases(Instance) ->
	receive
		#phase_result{k = OtherK, data = Data} when OtherK == Instance#block_actor_state.k ->
			NewData = gb_trees:insert(graphs:get_block_id(Data), Data, Instance#block_actor_state.neighbour_data),
			DataSize = gb_trees:size(NewData),
			NewInstance = Instance#block_actor_state{neighbour_data = NewData},
			if
				DataSize == Instance#block_actor_state.num_neighbours ->
					case block_actor_update(NewInstance) of
						{ok, UpdatedInstance} ->
							block_actor_run_phases(UpdatedInstance);
						done ->
							ok
					end;
				true ->
					block_actor_run_phases(NewInstance)
			end;
		#phase_result{k = OtherK, data = Data} when OtherK == (Instance#block_actor_state.k + 1) ->
			NewData = gb_trees:insert(graphs:get_block_id(Data), Data, Instance#block_actor_state.next_neighbour_data),
			NewInstance = Instance#block_actor_state{next_neighbour_data = NewData},
			block_actor_run_phases(NewInstance);
		X ->
			io:fwrite("BlockActor got unexpected message: ~p!~n",[X]),
			throw(X)
	end.

-spec block_actor_update(Instance :: block_actor_instance()) -> {ok, NewInstance :: block_actor_instance()} | done.
block_actor_update(Instance) ->
	K = Instance#block_actor_state.k + 1,
	NeighbourFun = fun(BlockId) ->
			gb_trees:get(BlockId, Instance#block_actor_state.neighbour_data)
	end,
	NewData = graphs:compute_floyd_warshall_inner(NeighbourFun, K, Instance#block_actor_state.current_data),
	NotifyInstance = Instance#block_actor_state{k = K, current_data = NewData},
	notify_neighbours(NotifyInstance),
	case K == (NotifyInstance#block_actor_state.num_nodes - 1) of
		true ->
			NotifyInstance#block_actor_state.manager ! new_block_result(NewData),
			done;
		false ->
			NewInstance = NotifyInstance#block_actor_state{
				neighbour_data = NotifyInstance#block_actor_state.next_neighbour_data,
				next_neighbour_data = gb_trees:empty()
			},
			DataSize = gb_trees:size(NewInstance#block_actor_state.neighbour_data),
			if
				DataSize == NewInstance#block_actor_state.num_neighbours ->
					block_actor_update(NewInstance);
				true ->
					{ok, NewInstance}
			end
	end.

-spec notify_neighbours(Instance :: block_actor_instance()) -> ok.
notify_neighbours(Instance) ->
	Msg = new_phase_result(Instance#block_actor_state.k, Instance#block_actor_state.current_data),
	lists:foreach(fun(Neighbour) -> 
		Neighbour ! Msg
	end, Instance#block_actor_state.neighbours).

%%%% TESTS %%%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

apsp_test() ->
	APSPR = #{
		number_of_nodes => 12,
		block_size => 4
	},
	{ok, Result} = benchmarks_server:await_benchmark_result(?MODULE, APSPR, "AllPairsShortestPath"),
	true = test_result:is_success(Result) orelse test_result:is_rse_failure(Result).

-endif.
