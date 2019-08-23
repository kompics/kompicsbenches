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
