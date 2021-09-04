#!/usr/bin/env python
import sys
import argparse
import os

from scipy.stats import t
from numpy import average, std
from math import sqrt

parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
#parser.add_argument('-t', nargs='?', default='./', help='Output directory')

args = parser.parse_args()
print("Plotting with args:",args)
data_files = [f for f in os.listdir(args.s) if f.endswith('.data')]
print("MEAN,SSD,SEM,RSE,CI95LO,CI95UP")
for filename in data_files :
    data = []
    for line in open(args.s + "/" + filename, 'r'):
        #print(line)
        data.append(float(line))

    mean = average(data)
    # evaluate sample variance by setting delta degrees of freedom (ddof) to
    # 1. The degree used in calculations is N - ddof
    stddev = std(data, ddof=1)
    sem = stddev/sqrt(len(data))
    rse = sem/mean
    # Get the endpoints of the range that contains 95% of the distribution
    t_bounds = t.interval(0.95, len(data) - 1)
    # sum mean to the confidence interval
    ci = [mean + critval * stddev / sqrt(len(data)) for critval in t_bounds]
    print "%s" % filename
    print "%f,%f,%f,%f,%f,%f" % (mean, stddev, sem, rse, ci[0], ci[1])
