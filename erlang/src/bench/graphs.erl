-module(graphs).

-define(WEIGHT_FACTOR, 10.0).
-define(WEIGHT_CUTOFF, 5.0).

-export([
	tabulate/1,
	generate_graph/1,
	num_nodes/1,
	update_all/2,
	get/2,
	set/3,
	compute_floyd_warshall/1,
	get_block/3,
	break_into_blocks/2,
	is_equal/2,
	blocked_get/2,
	blocked_set/3,
	blocked_map/2,
	blocked_size/1,
	assemble_from_blocks/1,
	compute_floyd_warshall_inner/3,
	block_position/1,
	get_block_id/1,
	pretty_print/1
	]).


-type position() :: {integer(), integer()}.
-type inf_float() :: float() | 'infinity'.


-record(graph, {
	data :: gb_trees:tree(position(), any()),
	num_nodes :: integer()
	}).

-opaque graph(Data) :: #graph{data :: gb_trees:tree(position(), Data)}.

-record(block, {
	block_id :: integer(),
	block_size :: integer(),
	num_blocks_per_dim :: integer(),
	row_offset :: integer(),
	col_offset :: integer(),
	graph :: #graph{}
	}).

-opaque block(Data) :: #block{graph :: graph(Data)}.

-record(blocked_graph, {
	blocks_per_dim :: integer(),
	blocks :: gb_trees:tree(position(), block(any()))
	}).

-opaque blocked_graph(Data) :: #blocked_graph { blocks :: gb_trees:tree(position(), block(Data)) }.

-type neighbour_fun(Data) :: fun((integer()) -> block(Data)).

-export_type([position/0, inf_float/0, graph/1, block/1, blocked_graph/1, neighbour_fun/1]).

-spec if_plus(L :: inf_float(), R :: inf_float()) -> inf_float().
if_plus(infinity, _) ->
	infinity;
if_plus(_, infinity) ->
	infinity;
if_plus(L, R) ->
	L + R.

-spec if_diff(L :: inf_float(), R :: inf_float()) -> inf_float().
if_diff(infinity, infinity) ->
	0.0;
if_diff(infinity, _) ->
	infinity;
if_diff(_, infinity) ->
	infinity;
if_diff(L, R) ->
	abs(L - R).

-spec if_gt(L :: inf_float(), R :: inf_float()) -> boolean().
if_gt(infinity, _) ->
	true;
if_gt(_, infinity) ->
	false;
if_gt(L, R) ->
	L > R.

-spec tabulate(N :: integer()) -> [position()].
tabulate(N) ->
	[{I-1, J-1} || I <- lists:seq(1, N), J <- lists:seq(1, N)]. % coordinates with zero indexing

-spec num_nodes(Graph :: graph(any())) -> integer().
num_nodes(#graph{num_nodes = NumNodes}) ->
	NumNodes.

-spec generate_graph(NumberOfNodes :: integer()) -> graph(inf_float()).
generate_graph(NumberOfNodes) -> 
	Rand = rand:seed(exsp,NumberOfNodes), % fastest and least secure PRNG
	Positions = tabulate(NumberOfNodes),
	{Weights, _LastRandState} = lists:mapfoldl(
		fun({I, J}, RandState) -> 
			if 
				I == J ->
					{{{I, I}, 0.0}, RandState};
				true ->
					{WeightBase, NewRandState} = rand:uniform_real_s(RandState),
					WeightRaw = WeightBase * ?WEIGHT_FACTOR,
					if
						WeightRaw > ?WEIGHT_CUTOFF ->
							{{{I, J}, infinity}, NewRandState};
						true ->
							{{{I, J}, WeightRaw}, NewRandState}
					end
			end
		end,
		Rand, 
		Positions),
	Data = lists:foldl(fun({Pos, Weight},Tree) -> gb_trees:insert(Pos, Weight, Tree) end, gb_trees:empty(),Weights),
	#graph{data = Data, num_nodes = NumberOfNodes}.

-spec update_all(fun((position(), CurGraph :: graph(T)) -> graph(T)), Graph :: graph(T)) -> graph(T).
update_all(Fun, InitialGraph) ->
	Positions = tabulate(InitialGraph#graph.num_nodes),
	lists:foldl(Fun, InitialGraph, Positions).

-spec get(Pos :: position(), Graph :: graph(T) | block(T)) -> T.
get(Pos, #graph{data = Data}) ->
	gb_trees:get(Pos, Data);
get(Pos, #block{graph = #graph{data = Data}}) ->
	gb_trees:get(Pos, Data).

-spec set(Pos :: position(), Value :: T, Graph :: graph(T)) -> graph(T)
	; (Pos :: position(), Value :: T, Graph :: block(T)) -> block(T).
set(Pos, Value, Graph) when is_record(Graph, graph) ->
	#graph{data=Data} = Graph,
	NewData = gb_trees:update(Pos, Value, Data),
	Graph#graph{data = NewData};
set(Pos, Value, Block) when is_record(Block, block) ->
	#block{graph = #graph{data = Data}} = Block,
	NewData = gb_trees:update(Pos, Value, Data),
	Block#block{graph = Block#block.graph#graph{data = NewData}}.

-spec compute_floyd_warshall(Graph :: graph(inf_float())) -> graph(inf_float()).
compute_floyd_warshall(InitialGraph) ->
	lists:foldl(fun(K, Graph) -> 
		update_all(fun({I, J}, CurGraph) ->
			NewValue = if_plus(get({I, K}, CurGraph), get({K, J}, CurGraph)),
			OldValue = get({I, J}, CurGraph),
			case if_gt(OldValue, NewValue) of
				true ->
					set({I, J}, NewValue, CurGraph);
				false ->
					CurGraph
			end
		end, Graph)
	end, InitialGraph, lists:seq(0, InitialGraph#graph.num_nodes - 1)).

-spec get_block(BlockId :: integer(), BlockSize :: integer(), Graph :: graph(T)) -> block(T).
get_block(BlockId, BlockSize, Graph) ->
	NumBlocksPerDim = Graph#graph.num_nodes div BlockSize,
	GlobalStartRow = (BlockId div NumBlocksPerDim) * BlockSize,
	GlobalStartCol = (BlockId rem NumBlocksPerDim) * BlockSize,
	BlockWeights = lists:map(fun({I, J}) -> 
		Weight = get({I + GlobalStartRow, J + GlobalStartCol}, Graph),
		{{I, J}, Weight}
	end, tabulate(BlockSize)),
	Data = lists:foldl(fun({Pos, Weight},Tree) -> gb_trees:insert(Pos, Weight, Tree) end, gb_trees:empty(),BlockWeights),
	SubGraph = #graph{num_nodes = BlockSize, data = Data},
	#block{block_id = BlockId, block_size = BlockSize, num_blocks_per_dim = NumBlocksPerDim, row_offset = GlobalStartRow, col_offset = GlobalStartCol, graph = SubGraph}.

-spec break_into_blocks(BlockSize :: integer(), Graph :: graph(T)) -> blocked_graph(T).
break_into_blocks(BlockSize, Graph) ->
	0 = Graph#graph.num_nodes rem BlockSize, % only allow evenly blocked graphs
	NumBlocksPerDim = Graph#graph.num_nodes div BlockSize,
	Blocks = lists:map(fun({I, J}) -> 
		BlockId = (I * NumBlocksPerDim) + J,
		{{I, J}, get_block(BlockId, BlockSize, Graph)}
	end, tabulate(NumBlocksPerDim)),
	Data = lists:foldl(fun({Pos, Block},Tree) -> gb_trees:insert(Pos, Block, Tree) end, gb_trees:empty(),Blocks),
	#blocked_graph{blocks_per_dim = NumBlocksPerDim, blocks = Data}.

-spec is_equal(G1 :: graph(T), G2 :: graph(T)) -> boolean().
is_equal(G1, G2) ->
	Size = G1#graph.num_nodes,
	case Size == G2#graph.num_nodes of
		true ->
			lists:foldl(fun(Pos, Acc) -> 
				%Acc andalso (get(Pos, G1) == get(Pos, G2))
				case Acc of
					true -> 
						V1 = get(Pos, G1),
						V2 = get(Pos, G2),
						Res = (V1 == V2),
						case Res of
							true -> 
								true;
							false ->
								io:fwrite("Values ~w and ~w, don't match up with delta=~w", [V1, V2, if_diff(V1, V2)]),
								false
						end;
					false -> 
						false
				end
			end, true, tabulate(Size));
		false -> 
			io:fwrite("Dimensions ~b and ~b don't match!~n", [G1#graph.num_nodes, G2#graph.num_nodes]),
			false
	end.


-spec blocked_get(Pos :: position(), Blocks :: blocked_graph(T)) -> block(T).
blocked_get(Pos, Blocks) ->
	gb_trees:get(Pos, Blocks#blocked_graph.blocks).

-spec blocked_set(Pos :: position(), Block :: block(T), Blocks :: blocked_graph(T)) -> blocked_graph(T).
blocked_set(Pos, Block, Blocks) when is_record(Blocks, blocked_graph) ->
	NewBlocks = gb_trees:update(Pos, Block, Blocks#blocked_graph.blocks),
	Blocks#blocked_graph{blocks = NewBlocks};
blocked_set(_Pos, _Block, Blocks) ->
	io:format(user, "Expected a blocked_graph, but got: ~p~n.", [Blocks]),
	throw(bullshit).

-spec blocked_map(Fun :: fun((block(T)) -> Out), Blocks :: blocked_graph(T)) -> gb_trees:tree(position(), Out).
blocked_map(Fun, Blocks) ->
	gb_trees:map(fun(_Pos, Block) -> Fun(Block) end, Blocks#blocked_graph.blocks).

-spec blocked_size(Blocks :: blocked_graph(any())) -> integer().
blocked_size(#blocked_graph{blocks_per_dim = Size}) ->
	Size.

-spec assemble_from_blocks(Blocks :: blocked_graph(T)) -> graph(T).
assemble_from_blocks(Blocks) -> 
	ZeroBlock = blocked_get({0, 0}, Blocks),
	BlockSize = ZeroBlock#block.block_size,
	TotalSize = BlockSize * Blocks#blocked_graph.blocks_per_dim,
	DataList = lists:map(fun({I, J}) -> 
		BlockRow = I div BlockSize,
		BlockCol = J div BlockSize,
		LocalRow = I rem BlockSize,
		LocalCol = J rem BlockSize,
		Block = blocked_get({BlockRow, BlockCol},Blocks),
		Value = get({LocalRow, LocalCol},Block),
		{{I, J}, Value}
	end,tabulate(TotalSize)),
	Data = lists:foldl(fun({Pos, Weight},Tree) -> gb_trees:insert(Pos, Weight, Tree) end, gb_trees:empty(),DataList),
	#graph{data = Data, num_nodes = TotalSize}.

-spec element_at(Row :: integer(), Col :: integer(), Neighbours :: neighbour_fun(T), Block :: block(T), CurGraph :: graph(T)) -> T.
element_at(Row, Col, Neighbours, Block, CurGraph) -> 
	BlockSize = Block#block.block_size,
	DestBlockId = ((Row div BlockSize) * Block#block.num_blocks_per_dim) + (Col div BlockSize),
	LocalRow = Row rem BlockSize,
	LocalCol = Col rem BlockSize,
	if
		DestBlockId == Block#block.block_id ->
			get({LocalRow, LocalCol}, CurGraph);
		true ->
			BlockData = Neighbours(DestBlockId),
			get({LocalRow, LocalCol}, BlockData)
	end.

-spec compute_floyd_warshall_inner(Neighbours :: neighbour_fun(inf_float()), K :: integer(), Block :: block(inf_float())) -> block(inf_float()).
compute_floyd_warshall_inner(Neighbours, K, Block) ->
	NewGraph = update_all(fun({I, J}, Graph) ->
		GlobalI = Block#block.row_offset + I,
		GlobalJ = Block#block.col_offset + J,
		NewValue = if_plus(
			element_at(GlobalI, K, Neighbours, Block, Graph),
			element_at(K, GlobalJ, Neighbours, Block, Graph)),
		OldValue = get({I, J}, Graph),
		case if_gt(OldValue, NewValue) of
			true ->
				set({I, J}, NewValue, Graph);
			false ->
				Graph
		end
	end, Block#block.graph),
	Block#block{graph = NewGraph}.

-spec block_position(Block :: block(any())) -> position().
block_position(Block) ->
	BlockId = Block#block.block_id,
	NumBlocksPerDim = Block#block.num_blocks_per_dim,
	I = BlockId div NumBlocksPerDim,
	J = BlockId rem NumBlocksPerDim,
	{I, J}.

-spec get_block_id(Block :: block(any())) -> integer().
get_block_id(#block{block_id = BlockId}) ->
	BlockId. 

-spec pretty_print(Graph :: graph(inf_float()) | block(inf_float())) -> string().
pretty_print(Graph) when is_record(Graph, graph) ->
	Res = lists:map(fun(RowIndex) ->
		Row = lists:map(fun(ColIndex) -> 
			case get({RowIndex, ColIndex}, Graph) of
				infinity ->
					"+∞"; % " +∞  ";
				Num ->
					io_lib:fwrite("~9.1.0f", [Num])
			end
		end, lists:seq(0, Graph#graph.num_nodes - 1)),
		lists:join(" ", Row)
	end, lists:seq(0, Graph#graph.num_nodes - 1)),
	lists:flatten(lists:join("\n", Res));
pretty_print(Block) when is_record(Block, block) ->
	Res = lists:map(fun(RowIndex) ->
		Row = lists:map(fun(ColIndex) -> 
			case get({RowIndex, ColIndex}, Block) of
				infinity ->
					" +∞  ";
				Num ->
					io_lib:fwrite("~5.1.0f", Num)
			end
		end, lists:seq(0, Block#block.block_size - 1)),
		lists:join(" ", Row)
	end, lists:seq(0, Block#block.block_size - 1)),
	lists:flatten(lists:join("\n", Res)).

%%%% TESTS %%%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

graph_generation_test() ->
	G = generate_graph(5),
	true = is_equal(G, G).

graph_blocking_roundtrip_test() ->
	G = generate_graph(10),
	Blocks = break_into_blocks(5, G),
	_Ignore = lists:map(fun(Pos) -> 
		Pos = block_position(blocked_get(Pos, Blocks)),
		ok
	end, tabulate(Blocks#blocked_graph.blocks_per_dim)),
	G2 = assemble_from_blocks(Blocks),
	true = is_equal(G, G2).

graph_block_floyd_warshall_test() ->
	G = generate_graph(10),
	io:fwrite("Orig:~n~ts~n", [pretty_print(G)]),
	Blocks = break_into_blocks(5, G),
	SPBlocks = lists:foldl(fun(K, CurBlocks) ->
		LookupFun = fun(BlockId) ->
			I = BlockId div CurBlocks#blocked_graph.blocks_per_dim,
			J = BlockId rem CurBlocks#blocked_graph.blocks_per_dim,
			blocked_get({I, J}, CurBlocks)
		end,
		NewBlocks = gb_trees:map(fun(_Pos, Block) -> 
			compute_floyd_warshall_inner(LookupFun, K, Block)
		end, CurBlocks#blocked_graph.blocks),
		CurBlocks#blocked_graph{blocks = NewBlocks}
	end, Blocks, lists:seq(0, 9)),
	GBlocked = assemble_from_blocks(SPBlocks),
	G2 = compute_floyd_warshall(G),
	io:fwrite("Blocked:~n~ts~n", [pretty_print(GBlocked)]),
	io:fwrite("G2:~n~ts~n", [pretty_print(G2)]),
	true = is_equal(GBlocked, G2).

-endif.

