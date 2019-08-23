-module(distributed_benchmark).

-callback msg_to_master_conf(Msg :: term()) ->
	{ok, MasterConf :: term()} |
	{error, Reason :: string()}.

-callback new_master() ->
	MasterInstance :: term().

-callback new_client() ->
	ClientInstance :: term().

%%%% On Master Instance %%%%%

-callback master_setup(MasterInstance :: term(), MasterConf :: term()) -> 
	{ok, NewMasterInstance :: term(), ClientConf :: term()}.

-callback master_prepare_iteration(MasterInstance :: term(), ClientData :: [term()]) ->
	{ok, NewMasterInstance :: term()}.

-callback master_run_iteration(MasterInstance :: term()) ->
	{ok, NewMasterInstance :: term()}.

-callback master_cleanup_iteration(MasterInstance :: term(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, NewMasterInstance :: term()}.

%%%% On Client Instance %%%%%

-callback client_setup(ClientInstance :: term(), ClientConf :: term()) ->
	{ok, NewClientInstance :: term(), ClientData :: term()}.

-callback client_prepare_iteration(ClientInstance :: term()) ->
	{ok, NewClientInstance :: term()}.

-callback client_cleanup_iteration(ClientInstance :: term(), LastIteration :: boolean()) ->
	{ok, NewClientInstance :: term()}.
