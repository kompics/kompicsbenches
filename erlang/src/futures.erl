-module(futures).

-record(future, { tag :: reference(), process :: pid() }).
-opaque future() :: #future{}.

-record(promise, { tag :: reference(), on_complete :: fun((reference(), any()) -> ok)}).
-opaque promise() :: #promise{}.

-export_type([future/0,promise/0]).

-export([promise_pair/0, promise_complete/1, complete/2, await/1, pipe/2]).
-spec promise_pair() -> {future(), promise()}.
promise_pair() ->
	Tag = make_ref(),
	Pid = spawn(fun() -> rendevouz_process(Tag, none) end),
	{#future{tag = Tag, process = Pid}, #promise{tag = Tag, on_complete = fun(T,X) -> send_to_future(Pid, T, X) end}}.

-spec promise_complete(OnComplete :: fun((reference(), any()) -> ok)) -> promise().
promise_complete(OnComplete) ->
	Tag = make_ref(),
	#promise{tag = Tag, on_complete = OnComplete}.

-spec send_to_future(Pid :: pid(), Tag :: reference(), Value :: any()) -> ok.
send_to_future(Pid, Tag, Value) ->
	Pid ! {future_value, Tag, Value},
	ok.

-spec complete(Promise :: promise(), Value :: any()) -> ok.
complete(#promise{tag = Tag, on_complete = Fun}, Value) ->
	Fun(Tag, Value),
	ok.

-spec await(Future :: future()) -> {ok, Value :: any()}.
await(#future{tag = Tag, process = Pid}) ->
	Pid ! {future_notify, Tag, self()},
	receive
		{future_get, Tag, Value} ->
			{ok, Value}
	end.

-spec pipe(Future :: future(), To :: pid()) -> ok.
pipe(#future{tag = Tag, process = Pid}, To) ->
	Pid ! {future_notify, Tag, To},
	ok.

rendevouz_process(Tag, none) ->
	receive
		{future_value, Tag, Value} ->
			rendevouz_process(Tag, Value)
	end;
rendevouz_process(Tag, Value) ->
	receive
		{future_notify, Tag, From} ->
			From ! {future_get, Tag, Value}
	end.
