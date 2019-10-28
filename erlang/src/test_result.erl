<<<<<<< HEAD
-module(test_result).

-export([
	not_implemented/0,
	success/2,
	failure/1,
  from_result/1
	]).

-spec not_implemented() -> benchmarks:'TestResult'().
not_implemented() ->
  #{sealed_value => {not_implemented, #{}}}.

-spec success(integer(), [float()]) -> benchmarks:'TestResult'().
success(NumberOfRuns, RunResults) ->
  #{sealed_value => {success, #{
    number_of_runs => NumberOfRuns,
    run_results => RunResults
  }}}.

-spec failure(string()) -> benchmarks:'TestResult'().
failure(Reason) ->
  #{sealed_value => {failure, #{
    reason => Reason
  }}}.

-spec from_result(Result :: {ok, [float()]} | {error, string()}) -> benchmarks:'TestResult'().
from_result(Result) ->
  case Result of
    {ok, Data} ->
      success(length(Data), Data);
    {error, Msg} ->
      failure(Msg)
  end.
=======
-module(test_result).

-export([
	not_implemented/0,
	success/2,
  is_success/1,
	failure/1,
  is_failure/1,
  is_rse_failure/1,
  from_result/1
	]).

-spec not_implemented() -> benchmarks:'TestResult'().
not_implemented() ->
  #{sealed_value => {not_implemented, #{}}}.

-spec success(integer(), [float()]) -> benchmarks:'TestResult'().
success(NumberOfRuns, RunResults) ->
  #{sealed_value => {success, #{
    number_of_runs => NumberOfRuns,
    run_results => RunResults
  }}}.

-spec is_success(Result :: benchmarks:'TestResult'()) -> boolean().
is_success(#{sealed_value := {success, _}}) ->
  true;
is_success(_) ->
  false.

-spec failure(string()) -> benchmarks:'TestResult'().
failure(Reason) ->
  #{sealed_value => {failure, #{
    reason => Reason
  }}}.

-spec is_failure(Result :: benchmarks:'TestResult'()) -> boolean().
is_failure(#{sealed_value := {failure, _}}) ->
  true;
is_failure(_) ->
  false.

-spec is_rse_failure(Result :: benchmarks:'TestResult'()) -> boolean().
is_rse_failure(#{sealed_value := {failure, #{ reason := Reason }}}) ->
  case string:find(Reason, "RSE") of
    nomatch ->
      false;
    _ ->
      true
  end;
is_rse_failure(_) ->
  false.

-spec from_result(Result :: {ok, [float()]} | {error, string()}) -> benchmarks:'TestResult'().
from_result(Result) ->
  case Result of
    {ok, Data} ->
      success(length(Data), Data);
    {error, Msg} ->
      failure(Msg)
  end.
>>>>>>> c92c44604e519dd0b6cc96f7ef122b9ca6b9cde1
