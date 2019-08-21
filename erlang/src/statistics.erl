-module(statistics).

-export([
	rse/1
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

% lazy val sampleSize = results.size.toDouble;
%   lazy val sampleMean = results.sum / sampleSize;
%   lazy val sampleVariance = results.foldLeft(0.0) { (acc, sample) =>
%     val err = sample - sampleMean;
%     acc + (err * err)
%   } / (sampleSize - 1.0);
%   lazy val sampleStandardDeviation = Math.sqrt(sampleVariance);
%   lazy val standardErrorOfTheMean = sampleStandardDeviation / Math.sqrt(sampleSize);
%   lazy val relativeErrorOfTheMean = standardErrorOfTheMean / sampleMean;
%   lazy val symmetricConfidenceInterval95: (Double, Double) = {
%     val cidist = 1.96 * standardErrorOfTheMean;
%     (sampleMean - cidist, sampleMean + cidist)
%   }
