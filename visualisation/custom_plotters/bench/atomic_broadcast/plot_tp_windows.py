import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import sys
import argparse
import os
import numpy as np

parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('-t', nargs='?', help='Output directory')
parser.add_argument('-w', required=True, type=int, help='Window duration (s)')

args = parser.parse_args()
print("Plotting with args:",args)

f = open(args.s, 'r')
all_ts = []
all_tp = []
for line in f:
	ts_throughput = line.split(" ")
	start_ts = int(ts_throughput[0].split(",")[0])
	for csv in ts_throughput:
		ts_tp = csv.split(",")
		ts = (int(ts_tp[0]) - start_ts) / 1000000000	# convert from ns to s
		tp = int(ts_tp[1]) / args.w
		all_ts.append(ts)
		all_tp.append(tp)

plt.plot(all_ts, all_tp, marker='o')
plt.xticks(np.arange(min(all_ts), max(all_ts) + args.w, args.w))
if args.t is not None:
    target_dir = args.t
else:
    target_dir = "./"
plt.savefig(target_dir + "/test.png")