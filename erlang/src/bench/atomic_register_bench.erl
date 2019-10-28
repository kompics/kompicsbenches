-module(atomic_register_bench).

-behaviour(distributed_benchmark).

%% API
-import(lists, [sublist/2]).
-export([
  msg_to_master_conf/1,
  new_master/0,
  new_client/0,
  master_setup/3,
  master_prepare_iteration/2,
  master_run_iteration/1,
  master_cleanup_iteration/3,
  client_setup/2,
  client_prepare_iteration/1,
  client_cleanup_iteration/2
]).

-record(master_conf, {
  read_workload = 0.0 :: float(),
  write_workload = 0.0 :: float(),
  partition_size = 0 :: integer(),
  number_of_keys = 0 :: integer()}).
-type master_conf() :: #master_conf{}.
-record(client_conf, {
  read_workload = 0.0 :: float(),
  write_workload = 0.0 :: float()}).
-type client_conf() :: #client_conf{}.
-type client_data() :: pid().

-record(master_state, {
  config = #master_conf{} :: master_conf(),
  atomic_register :: pid() | 'undefined',
  active_nodes :: [pid()] | 'undefined',
  init_id :: integer()}).

-type master_instance() :: #master_state{}.

-record(client_state, {
  config = #client_conf{} :: client_conf(),
  atomic_register :: pid() | 'undefined'}).

-type client_instance() :: #client_state{}.

-spec get_read_workload(Caller:: atom(), State:: master_instance() | client_instance()) -> float().
get_read_workload(master, State) ->
  State#master_state.config#master_conf.read_workload;
get_read_workload(client, State) ->
  State#client_state.config#client_conf.read_workload.

-spec get_write_workload(Caller:: 'master' | 'client', State:: master_instance() | client_instance()) -> float().
get_write_workload(master, State) ->
  State#master_state.config#master_conf.write_workload;
get_write_workload(client, State) ->
  State#client_state.config#client_conf.write_workload.

-spec get_num_keys(State:: master_instance()) -> integer().
get_num_keys(State) ->
  State#master_state.config#master_conf.number_of_keys.

-spec get_partition_size(State:: master_instance()) -> integer().
get_partition_size(State) ->
  State#master_state.config#master_conf.partition_size.

-spec msg_to_master_conf(Msg :: term()) ->
  {ok, MasterConf :: master_conf()} |
  {error, Reason :: string()}.
msg_to_master_conf(Msg) ->
  case Msg of
    #{read_workload := ReadWorkload,
      write_workload := WriteWorkload,
      partition_size := PartitionSize,
      number_of_keys := NumberOfKeys
    } when
      is_float(ReadWorkload) andalso (ReadWorkload >= 0),
      is_float(WriteWorkload) andalso (WriteWorkload >= 0),
      is_integer(PartitionSize) andalso (PartitionSize > 0),
      is_integer(NumberOfKeys) andalso (NumberOfKeys > 0) ->
      {ok, #master_conf{read_workload = ReadWorkload, write_workload = WriteWorkload, partition_size = PartitionSize, number_of_keys = NumberOfKeys}};
    #{read_workload := _ReadWorkload,
      write_workload := _WriteWorkload,
      partition_size := _PartitionSize,
      number_of_keys := _NumberOfKeys
    } ->
      {error, io_lib:fwrite("Invalid config parameters:~p.~n", [Msg])};
    _ ->
      {error, io_lib:fwrite("Invalid config message:~p.~n", [Msg])}
  end.

-spec new_master() -> MasterInstance :: master_instance().
new_master() ->
  #master_state{init_id = 0}.

-spec new_client() -> ClientInstance :: client_instance().
new_client() ->
  #client_state{}.

-record(atomicreg_state, {
  read_workload :: float(),
  write_workload :: float(),
  n = 0 :: integer(),
  nodes :: [pid()] | 'undefined',
  rank = -1 :: integer(),
  min_key = -1 :: integer(),
  max_key = -1 :: integer(),
  run_id = -1 :: integer() | 'undefined',
  master :: pid() | 'undefined',
  read_count :: integer() | 'undefined',
  write_count :: integer() | 'undefined',
  register_state = maps:new() :: map(),
  register_readlist = maps:new() :: map()
}).

-type atomicreg_instance() :: #atomicreg_state{}.

%%%% On Master Instance %%%%%

-spec master_setup(Instance :: master_instance(), Conf :: master_conf(), Meta :: distributed_benchmark:deployment_metadata()) ->
  {ok, Newinstance :: master_instance(), ClientConf :: client_conf()} |
  {error, Reason :: string()}.
master_setup(Instance, Conf, Meta) ->
  io:fwrite("Setting up Atomic Register(Master)"),
  NumClients = distributed_benchmark:meta_num_clients(Meta),
  %logger:set_primary_config(level, error), % should be set globally
  PartitionSize = Conf#master_conf.partition_size,
  case NumClients of
    N when N < PartitionSize - 1 ->
      logger:error("Not enough clients, partitionsize=~w, clients=~w", [PartitionSize, N]),
      {error, "Not enough clients"};
    _ ->
      NewInstance = Instance#master_state{config = Conf},
      process_flag(trap_exit, true),
      ClientConf = #client_conf{read_workload = get_read_workload(master, NewInstance), write_workload = get_write_workload(master, NewInstance) },
      {ok, NewInstance, ClientConf}
  end.

-spec master_prepare_iteration(Instance :: master_instance(), ClientData :: [client_data()]) ->
  {ok, NewInstance :: master_instance()}.
master_prepare_iteration(Instance, ClientData) ->
  io:fwrite("Preparing iteration"),
  State = #atomicreg_state{read_workload = get_read_workload(master, Instance), write_workload = get_write_workload(master, Instance)},
  AtomicRegister = spawn_link(fun() -> atomic_register(State) end),
  ActiveNodes = [AtomicRegister | sublist(ClientData, NumClients = get_partition_size(Instance) - 1)],
  NumKeys = get_num_keys(Instance),
  send_init(ActiveNodes, 0, InitId = Instance#master_state.init_id, ActiveNodes, 0, NumKeys - 1),
  wait_for_init_acks(NumClients + 1, InitId),
  NewInstance = Instance#master_state{atomic_register = AtomicRegister, active_nodes = ActiveNodes, init_id = InitId + 1},
  io:fwrite("Preparation completed"),
  {ok, NewInstance}.

master_run_iteration(Instance) ->
  io:fwrite("Running iteration!"),
  lists:foreach(fun(Node) -> Node ! run end, Instance#master_state.active_nodes),
  wait_for_done(get_partition_size(Instance)),
  {ok, Instance}.

-spec master_cleanup_iteration(Instance :: master_instance(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
  {ok, NewInstance :: master_instance()}.
master_cleanup_iteration(Instance, _LastIteration, _ExecTimeMillis) ->
  io:fwrite("Cleaning up Atomic Register(Master) side.~n"),
  AtomicRegister = Instance#master_state.atomic_register,
  AtomicRegister ! stop,
  {ok, _} = bench_helpers:await_exit(AtomicRegister),
  NewInstance = Instance#master_state{atomic_register = undefined, active_nodes = undefined},
  {ok, NewInstance}.

-spec send_init(NodesRemaining :: [pid()], Rank :: integer(), InitId :: integer(), Nodes :: [pid()], MinKey :: integer(), MaxKey :: integer()) -> ok.
send_init([], _, _, _, _, _) -> ok;
send_init([H|T], Rank, InitId, Nodes, MinKey, MaxKey) ->
  H ! {init, Rank, InitId, Nodes, self(), MinKey, MaxKey},
  send_init(T, Rank + 1, InitId, Nodes, MinKey, MaxKey).

wait_for_init_acks(0, _) -> ok;
wait_for_init_acks(Remaining, InitId) when Remaining > 0 ->
  receive
    {init_ack, InitId} ->
      wait_for_init_acks(Remaining - 1, InitId);
    X ->
      io:fwrite("Got unexpected message during preparation: ~p!~n",[X]),
      throw(X)
  end.

wait_for_done(0) -> ok;
wait_for_done(Remaining) when Remaining > 0 ->
  receive
    done -> wait_for_done(Remaining - 1);
    X ->
      io:fwrite("Got unexpected message during iteration: ~p!~n",[X]),
      throw(X)
  end.

%%%% On Client Instance %%%%%
-spec client_setup(Instance :: client_instance(), Conf :: client_conf()) ->
  {ok, NewInstance :: client_instance(), ClientData :: client_data()}.
client_setup(Instance, Conf) ->
  io:fwrite("Atomic Register(Client) Setup"),
  case logger:set_primary_config(level, all) of
    ok -> ok;
    {error, _} -> io:fwrite("Failed to set logger level~n")
  end,
  ConfInstance = Instance#client_state{config = Conf},
  process_flag(trap_exit, true),
  State = #atomicreg_state{read_workload = get_read_workload(client, ConfInstance), write_workload = get_write_workload(client, ConfInstance)},
  AtomicRegister = spawn_link(fun() -> atomic_register(State) end),
  NewInstance = Instance#client_state{atomic_register = AtomicRegister},
  {ok, NewInstance, AtomicRegister}.

-spec client_prepare_iteration(Instance :: client_instance()) ->
  {ok, NewInstance :: client_instance()}.
client_prepare_iteration(Instance) ->
  io:fwrite("Preparing Atomic Register iteration.~n"),
  {ok, Instance}.

-spec client_cleanup_iteration(Instance :: client_instance(), LastIteration :: boolean()) ->
  {ok, NewInstance :: client_instance()}.
client_cleanup_iteration(Instance, LastIteration) ->
  io:fwrite("Cleaning up Atomic Register(Client) side.~n"),
  case LastIteration of
    true ->
      AtomicRegister = Instance#client_state.atomic_register,
      AtomicRegister ! stop,
      {ok, _} = bench_helpers:await_exit(AtomicRegister),
      NewInstance = Instance#client_state{atomic_register = undefined},
      {ok, NewInstance};
    _ ->
      {ok, Instance}
  end.

%%%%%% Atomic Register %%%%%%

-record(register_state, {
  ts = 0 :: integer(),
  wr = 0 :: integer(),
  value = 0 :: integer(),
  acks = 0 :: integer(),
  readval = 0 :: integer(),
  writeval = 0 :: integer(),
  rid = 0 :: integer(),
  reading = false :: boolean(),
  first_received_ts = -1 :: integer(),
  skip_impose = true :: boolean()
}).

-spec atomic_register(State :: atomicreg_instance()) -> ok.
atomic_register(State) ->
  CurrentRunId = State#atomicreg_state.run_id,
  receive
    {init, Rank, InitId, Nodes, Master, MinKey, MaxKey} ->
      NewState = State#atomicreg_state{rank = Rank, run_id = InitId, nodes = Nodes, n = length(Nodes), master = Master, min_key = MinKey, max_key = MaxKey, register_readlist = maps:new(), register_state = maps:new()},
      Master ! {init_ack, InitId},
      atomic_register(NewState);

    run ->
      NewState = invoke_operations(State),
      atomic_register(NewState);

    {read, Sender, RunId, Key, Rid} when RunId == CurrentRunId ->
      CurrentRegister = maps:get(Key, State#atomicreg_state.register_state, #register_state{}),
      Sender ! {value, self(), CurrentRunId, Key, Rid, CurrentRegister#register_state.ts, CurrentRegister#register_state.wr, CurrentRegister#register_state.value},
      atomic_register(State);

    {read, _, RunId, _, _} when RunId /= CurrentRunId -> atomic_register(State);

    {value, Sender, RunId, Key, Rid, Ts, Wr, Value} when RunId == CurrentRunId ->
      CurrentRegister = maps:get(Key, State#atomicreg_state.register_state, #register_state{}),
      if
        Rid == CurrentRegister#register_state.rid ->
          ReadList = maps:get(Key, State#atomicreg_state.register_readlist, maps:new()),
          UpdatedCurrentRegister =
            case CurrentRegister#register_state.reading of
              true ->
                  if
                    map_size(ReadList) == 0 ->
                      CurrentRegister#register_state{first_received_ts = Ts, readval = Value};
                    (CurrentRegister#register_state.skip_impose) andalso (CurrentRegister#register_state.first_received_ts /= Ts) ->
                      CurrentRegister#register_state{skip_impose = false};
                    true ->
                      CurrentRegister
                  end;
              false -> CurrentRegister
            end,
          NewReadList = maps:put(Sender, {Ts, Wr, Value}, ReadList),
          if
            map_size(NewReadList) > (State#atomicreg_state.n / 2) ->
              if
                (UpdatedCurrentRegister#register_state.reading) andalso (UpdatedCurrentRegister#register_state.skip_impose) ->
                  ReadVal = UpdatedCurrentRegister#register_state.readval,
                  NewRegisterState = UpdatedCurrentRegister#register_state{value = ReadVal},
                  NewRegisterStateMap = maps:put(Key, NewRegisterState, State#atomicreg_state.register_state),
                  NewReadListMap = maps:put(Key, maps:new(), State#atomicreg_state.register_readlist),
                  NewState = read_response(State#atomicreg_state{register_state = NewRegisterStateMap, register_readlist = NewReadListMap}, Key, ReadVal),
                  atomic_register(NewState);
                true ->
                  {MaxTs, RR, ReadVal} = lists:max(maps:values(NewReadList)),
                  NewRegisterState = CurrentRegister#register_state{value = ReadVal},
                  NewRegisterStateMap = maps:put(Key, NewRegisterState, State#atomicreg_state.register_state),
                  NewReadListMap = maps:put(Key, maps:new(), State#atomicreg_state.register_readlist),
                  case NewRegisterState#register_state.reading of
                    true ->
                      bcast(State#atomicreg_state.nodes, {write, self(), CurrentRunId, Key, Rid, MaxTs, RR, ReadVal});
                    false ->
                      bcast(State#atomicreg_state.nodes, {write, self(), CurrentRunId, Key, Rid, MaxTs+1, State#atomicreg_state.rank, NewRegisterState#register_state.writeval})
                  end,
                  atomic_register(State#atomicreg_state{register_state = NewRegisterStateMap, register_readlist = NewReadListMap})
              end;
            true -> % haven't got majority yet
              NewReadListMap = maps:put(Key, NewReadList, State#atomicreg_state.register_readlist),
              NewRegisterStateMap = maps:put(Key, UpdatedCurrentRegister, State#atomicreg_state.register_state),
              atomic_register(State#atomicreg_state{register_state = NewRegisterStateMap, register_readlist = NewReadListMap})
          end;
        true -> atomic_register(State)  % don't care about msg with less rid
      end;

    {value, _, RunId, _, _, _, _, _} when RunId /= CurrentRunId -> atomic_register(State);

    {write, Sender, RunId, Key, Rid, Ts, Wr, Value} ->
      NewState =
        case RunId of
          CurrentRunId ->
            RegisterStateMap = State#atomicreg_state.register_state,
            CurrentRegister = maps:get(Key, RegisterStateMap, #register_state{}),
              if
                {Ts, Wr} > {CurrentRegister#register_state.ts, CurrentRegister#register_state.wr} ->
                  NewRegisterState = CurrentRegister#register_state{ts = Ts, wr = Wr, value = Value},
                  State#atomicreg_state{register_state = maps:put(Key, NewRegisterState, RegisterStateMap)};
                true -> State
              end;
          _ -> State
        end,
      Sender ! {ack, RunId, Key, Rid},
      atomic_register(NewState);

    {ack, RunId, Key, Rid} when RunId == CurrentRunId ->
      RegisterStateMap = State#atomicreg_state.register_state,
      CurrentRegister = maps:get(Key, RegisterStateMap, #register_state{}),
      if
        Rid == CurrentRegister#register_state.rid ->
          Acks = CurrentRegister#register_state.acks + 1,
          if
            Acks > State#atomicreg_state.n / 2 ->
              NewRegisterState = CurrentRegister#register_state{acks = 0},
              UpdatedState = State#atomicreg_state{register_state = maps:put(Key, NewRegisterState, RegisterStateMap)},
              NewState =
                if
                  NewRegisterState#register_state.reading -> read_response(UpdatedState, Key, NewRegisterState#register_state.readval);
                  true -> write_response(UpdatedState, Key)
                end,
              atomic_register(NewState);
            true ->
              NewRegisterState = CurrentRegister#register_state{acks = Acks},
              NewState = State#atomicreg_state{register_state = maps:put(Key, NewRegisterState, RegisterStateMap)},
              atomic_register(NewState)
          end;
        true -> atomic_register(State)  % don't care about msg with less rid
      end;

    {ack, RunId, _, _} when RunId /= CurrentRunId -> atomic_register(State);

    stop -> ok;

    X ->
      io:fwrite("Atomic Register got unexpected message: ~p!~n",[X]),
      throw(X) % don't accept weird stuff,
  end.

-spec bcast(Nodes :: list(), Msg :: any()) -> ok.
bcast(Nodes, Msg) -> lists:foreach(fun(Node) -> Node ! Msg end, Nodes).

-spec invoke_read(Key :: integer(), StopKey :: integer(), State :: atomicreg_instance()) -> atomicreg_instance().
invoke_read(Key, StopKey, State) when Key == StopKey -> State;
invoke_read(Key, StopKey, State) when Key < StopKey ->
  RegisterStateMap = State#atomicreg_state.register_state,
  OldRegisterState = maps:get(Key, RegisterStateMap, #register_state{}),
  NewRid = OldRegisterState#register_state.rid + 1,
  NewRegisterState = OldRegisterState#register_state{reading = true, rid = NewRid},
  NewRegisterStateMap = maps:put(Key, NewRegisterState, RegisterStateMap),
  NewReadList = maps:put(Key, maps:new(), State#atomicreg_state.register_readlist),
  NewState = State#atomicreg_state{register_state = NewRegisterStateMap, register_readlist = NewReadList},
  Msg = {read, self(), NewState#atomicreg_state.run_id, Key, NewRid},
  bcast(NewState#atomicreg_state.nodes, Msg),
  invoke_read(Key + 1, StopKey, NewState).

-spec invoke_write(Key :: integer(), StopKey :: integer(), State :: atomicreg_instance()) -> atomicreg_instance().
invoke_write(Key, StopKey, State) when Key == StopKey -> State;
invoke_write(Key, StopKey, State) when Key < StopKey ->
  RegisterStateMap = State#atomicreg_state.register_state,
  OldRegisterState = maps:get(Key, RegisterStateMap, #register_state{}),
  NewRid = OldRegisterState#register_state.rid + 1,
  NewRegisterState = OldRegisterState#register_state{reading = false, rid = NewRid, writeval = State#atomicreg_state.rank},
  NewRegisterStateMap = maps:put(Key, NewRegisterState, RegisterStateMap),
  NewReadList = maps:put(Key, maps:new(), State#atomicreg_state.register_readlist),
  NewState = State#atomicreg_state{register_state = NewRegisterStateMap, register_readlist = NewReadList},
  Msg = {read, self(), NewState#atomicreg_state.run_id, Key, NewRid},
  bcast(NewState#atomicreg_state.nodes, Msg),
  invoke_write(Key + 1, StopKey, NewState).

-spec invoke_operations(State :: atomicreg_instance()) -> atomicreg_instance().
invoke_operations(State) ->
  SelfRank = State#atomicreg_state.rank,
  MinKey = State#atomicreg_state.min_key,
  MaxKey = State#atomicreg_state.max_key,
  NumKeys = MaxKey - MinKey + 1,
  NumReads = trunc(State#atomicreg_state.read_workload * NumKeys),
  NumWrites = trunc(State#atomicreg_state.write_workload * NumKeys),
  NewState = State#atomicreg_state{read_count = NumReads, write_count = NumWrites},
  logger:info("Invoking operations #reads=~w, #writes=~w, selfRank=~w", [NumReads, NumWrites, SelfRank]),
  case SelfRank rem 2 of
    0 ->
      UpdatedState = invoke_read(MinKey, MinKey + NumReads, NewState),
      invoke_write(MinKey + NumReads, MinKey + NumReads + NumWrites, UpdatedState);
    _ ->
      UpdatedState = invoke_write(MinKey, MinKey + NumWrites, NewState),
      invoke_read(MinKey + NumWrites, MinKey + NumReads + NumWrites, UpdatedState)
  end.

-spec read_response(State :: atomicreg_instance(), Key :: integer(), Value :: integer()) -> atomicreg_instance().
read_response(State, _Key, _Value) ->
  ReadCount = State#atomicreg_state.read_count - 1,
  WriteCount = State#atomicreg_state.write_count,
  NewState = State#atomicreg_state{read_count = ReadCount},
  if
    (ReadCount == 0) andalso (WriteCount == 0) ->
      logger:info("DONE!!!"),
      State#atomicreg_state.master ! done,
      NewState;
    true ->
      NewState
  end.

-spec write_response(State :: atomicreg_instance(), Key :: integer()) -> atomicreg_instance().
write_response(State, _Key) ->
  WriteCount = State#atomicreg_state.write_count - 1,
  ReadCount = State#atomicreg_state.read_count,
  NewState = State#atomicreg_state{write_count = WriteCount},
  if
    (WriteCount == 0) andalso (ReadCount == 0) ->
      logger:info("DONE!!!"),
      State#atomicreg_state.master ! done,
      NewState;
    true ->
      NewState
  end.
