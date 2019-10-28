-module(statistics).

-export([
	rse/1,
	median/1
	]).

-record(stats, {
	sample_size :: integer(),
	sample_mean :: float(),
	sample_variance :: float(),
	sample_standard_deviation :: float(),
	standard_error_of_the_mean :: float(),
	relative_error_of_the_mean :: float()
	}).

-opaque stats() :: #stats{}.

-export_type([stats/0]).

-spec rse(Data :: [float()]) -> float().
rse(Data) ->
	{ok, Stats} = calculate_stats(Data),
	Stats#stats.relative_error_of_the_mean.
	
-spec calculate_stats(Data :: [float()]) -> {ok, stats()} | empty.
calculate_stats([]) ->
	empty;
calculate_stats(Data) ->
	SampleSize = length(Data),
	SampleMean = lists:sum(Data) / SampleSize,
	SampleVariance = lists:foldl(fun(Sample, Acc) ->
		Err = Sample - SampleMean,
		Acc + (Err * Err)
	end, 0.0, Data),
	SampleStandardDeviation = math:sqrt(SampleVariance),
	StandardErrorOfTheMean = SampleStandardDeviation / math:sqrt(SampleSize),
	RelativeErrorOfTheMean = StandardErrorOfTheMean / SampleMean,
	Stats = #stats{
		sample_size = SampleSize,
		sample_mean = SampleMean,
		sample_variance = SampleVariance,
		sample_standard_deviation = SampleStandardDeviation,
		standard_error_of_the_mean = StandardErrorOfTheMean,
		relative_error_of_the_mean = RelativeErrorOfTheMean
	},
	{ok, Stats}.

-spec median(List :: [integer()] | [float()]) -> {ok, Median :: float()} | empty.
median([]) -> 
	empty;
median(Unsorted) ->
    Sorted = lists:sort(Unsorted),
    Length = length(Sorted),
    case Length rem 2 of
    	0 ->
    		LowerMiddle = Length div 2, % nth is 1 indexed
    		UpperMiddle = LowerMiddle + 1,
    		LowerValue = lists:nth(LowerMiddle, Sorted),
    		UpperValue = lists:nth(UpperMiddle, Sorted),
    		Median = float(LowerValue + UpperValue)/2,
    		{ok, Median};
    	1 ->
    		Middle = Length div 2 + 1, % nth is 1 indexed
    		Median = lists:nth(Middle, Sorted),
    		{ok, float(Median)}

    end.
