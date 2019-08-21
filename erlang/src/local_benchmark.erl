-module(local_benchmark).

-callback new_instance() -> 
	Instance :: term().

-callback msg_to_conf(Msg :: term()) ->
	{ok, Conf :: term()} |
	{error, Reason :: string()}.

-callback setup(Instance :: term(), Conf :: term()) ->
	{ok, NewInstance :: term()}.

-callback prepare_iteration(Instance :: term()) ->
	{ok, NewInstance :: term()}.

-callback run_iteration(Instance :: term()) ->
	{ok, NewInstance :: term()}.

-callback cleanup_iteration(Instance :: term(), LastIteration :: boolean(), ExecTimeMillis :: float()) ->
	{ok, NewInstance :: term()}.
