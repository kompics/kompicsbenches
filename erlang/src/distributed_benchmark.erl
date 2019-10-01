-module(distributed_benchmark).

-record(deployment_metadata, {num_clients = 0 :: integer()}).
-opaque deployment_metadata() :: #deployment_metadata{}.
-export_type([deployment_metadata/0]).

-export([meta_create/1, meta_num_clients/1]).

%%%% General API %%%%%

-callback msg_to_master_conf(Msg :: term()) ->
	{ok, MasterConf :: term()} |
	{error, Reason :: string()}.

-callback new_master() ->
	MasterInstance :: term().

-callback new_client() ->
	ClientInstance :: term().

%%%% On Master Instance %%%%%

-callback master_setup(MasterInstance :: term(), MasterConf :: term(), Meta :: deployment_metadata()) -> 
	{ok, NewMasterInstance :: term(), ClientConf :: term()} |
	{error, Reason :: string()}.

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

%%%% Metadata Functions %%%%%

-spec meta_create(NumClients :: integer()) -> deployment_metadata().
meta_create(NumClients) ->
	#deployment_metadata{num_clients = NumClients}.

-spec meta_num_clients(Meta :: deployment_metadata()) -> integer().
meta_num_clients(Meta) ->
	Meta#deployment_metadata.num_clients.
