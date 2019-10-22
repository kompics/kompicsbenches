-module(duration).

-export([
	from_string/1,
  	to_millis/1,
  	zero/0]).

-record(duration, {
	number = 0 :: integer(),
	unit = second :: erlang:time_unit()
	}).
-opaque duration() :: #duration{}.

-export_type([duration/0]).

-spec zero() -> duration().
zero() ->
	#duration{}.

-spec to_millis(Duration :: duration()) -> WholeMillis :: integer().
to_millis(Duration) ->
	erlang:convert_time_unit(Duration#duration.number, Duration#duration.unit, millisecond).
-spec from_string(Str :: string()) -> {ok, duration()} | {error, Reason :: any()}.
from_string(Str) ->
	Prepared = string:lowercase(string:trim(Str)),
	{Num, RawUnit} = string:take(Prepared, lists:seq($0,$9)),
	Number = list_to_integer(Num),
	TrimmedUnit = string:trim(RawUnit),
	case string_to_unit(TrimmedUnit) of
		{ok, Unit} ->
			{ok, #duration{number = Number, unit = Unit}};
		Err ->
		 Err
	end.

-spec string_to_unit(Str :: string()) -> {ok, erlang:time_unit()} | {error, Reason :: any()}.
string_to_unit(Str) ->
	case Str of 
		"s" ->  
			{ok, second};
		"sec" ->  
			{ok, second};
		"second" ->  
			{ok, second};
		"seconds" ->  
			{ok, second};
		"ms" ->  
			{ok, millisecond};
		"millis" ->  
			{ok, millisecond};
		"millisecond" ->  
			{ok, millisecond};
		"milliseconds" ->  
			{ok, millisecond};
		"us" ->  
			{ok, microsecond};
		"µs" ->  
			{ok, microsecond};
		"micros" ->  
			{ok, microsecond};
		"microsecond" ->  
			{ok, microsecond};
		"microseconds" ->  
			{ok, microsecond};
		"ns" ->  
			{ok, nanosecond};
		"nanos" ->  
			{ok, nanosecond};
		"nanosecond" ->  
			{ok, nanosecond};
		"nanoseconds" -> 
			{ok, nanosecond};
		X ->
			{error, io_lib:fwrite("Unknown time unit: ~p.", [X])}
	end.
