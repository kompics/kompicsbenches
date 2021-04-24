import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import sys
import argparse
import os
import numpy as np
import datetime

def format_time(seconds):
    """Formats a timedelta duration to [N days] %M:%S format"""
    secs_in_a_min = 60

    minutes, seconds = divmod(seconds, secs_in_a_min)

    time_fmt = "{:d}:{:02d}".format(minutes, seconds)
    return time_fmt

parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('-t', nargs='?', help='Output directory')
parser.add_argument('-w', required=True, type=int, help='Window duration (s)')

args = parser.parse_args()
print("Plotting with args:",args)

f = open(args.s, 'r')
all_tp = []
for line in f:
	ts_throughput = line.split(" ")
	start_ts = int(ts_throughput[0].split(",")[0])
	for window_idx, csv in enumerate(ts_throughput):
		ts_tp = csv.split(",")
		#ts = (int(ts_tp[0]) - start_ts) / 1000000000	# convert from ns to s
		tp = int(ts_tp[1]) / args.w
		#all_ts.append(ts)
		if len(all_tp) <= window_idx:
			all_tp.append([])
		all_tp[window_idx].append(tp)

all_ts = []
all_avg_tp = []
all_min_tp = []
all_max_tp = []
for i in range(len(all_tp)):
	all_ts.append(format_time(seconds=(i+1) * args.w))
	avg_tp = sum(all_tp[i])/len(all_tp[i])
	min_tp = min(all_tp[i])
	max_tp = max(all_tp[i])

	all_avg_tp.append(avg_tp)
	all_min_tp.append(min_tp)
	all_max_tp.append(max_tp)


plt.plot(all_ts, all_avg_tp, marker='o')
plt.plot(all_ts, all_min_tp, marker='o')
plt.plot(all_ts, all_max_tp, marker='o')
plt.gcf().autofmt_xdate()
#plt.xticks(0, len(all_avg_tp) + args.w, args.w)
if args.t is not None:
    target_dir = args.t
else:
    target_dir = "./"
plt.savefig(target_dir + "/test.png")
