import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import sys
import argparse
import os
import numpy as np
import datetime
import scipy.stats as st
from pathlib import Path
from matplotlib.ticker import (MultipleLocator,
                               FormatStrFormatter,
                               AutoMinorLocator)

def get_label(filename):
    csv = filename.split(",")
    algorithm = csv[0]
    if algorithm == "paxos":
        algorithm = "Omni Paxos"
    else:
        algorithm = "Raft"
    reconfig = csv[len(csv)-1].split(".")[0]
    if reconfig == "none":
        label = algorithm
    else:
        label = "{}, {}".format(algorithm, reconfig).replace("-", " ")
    return label

def get_file_str(filename):
    csv = filename.split(",")
    print(csv)
    algorithm = csv[0]
    if algorithm == "paxos":
        algorithm = "omnipaxos"
    else:
        algorithm = "raft"
    reconfig = csv[len(csv)-1].split(".")[0]
    if reconfig == "none":
        label = algorithm
    else:
        label = "{}-{}".format(algorithm, reconfig)
    return label

def format_time(seconds, _):
    """Formats a timedelta duration to [N days] %M:%S format"""
    secs_in_a_min = 60
    #print(seconds)
    minutes, seconds = divmod(int(seconds), secs_in_a_min)
    #print("minutes: {}, seconds: {}".format(minutes, seconds))
    time_fmt = "{:d}:{:02d}".format(minutes, seconds)
    return time_fmt

parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('-t', nargs='?', help='Output directory')
parser.add_argument('-w', required=True, type=int, help='Window duration (s)')
parser.set_defaults(feature=True)

args = parser.parse_args()
print("Plotting with args:",args)

fig, ax = plt.subplots()

f = open(args.s, 'r')
for line in f:
	num_decided_per_window = line.split(",")
	filtered_decided = list(filter(lambda x: x.isdigit(), num_decided_per_window))
	all_ts = list(range(args.w, (len(filtered_decided) + 1) * args.w, args.w))
	all_tp = list(map(lambda num_decided: int(num_decided) / args.w, filtered_decided))

	plt.plot(all_ts, all_tp, marker='.')

max_ts = max(all_ts)
x_axis = np.arange(0, max_ts+3*args.w, 3*args.w)

plt.ylabel("Throughput (ops/s)")
plt.xlabel("Time")
plt.xticks(x_axis)
ax.xaxis.set_major_formatter(format_time)

plt.ylim(bottom=0)
plt.gcf().autofmt_xdate()

fig.set_size_inches(12, 6)

split = args.s.split("/")
exp_str = split[len(split)-3]
exp_str_split = exp_str.split("-")
num_nodes = exp_str_split[0]
num_cp = exp_str_split[1]
reconfig = exp_str_split[len(exp_str_split) - 1]
title = "All runs {} nodes, {} concurrent proposals, {}".format(num_nodes, num_cp, get_label(split[len(split)-1]))
if reconfig != "off":
	title += ", {} reconfiguration".format(reconfig)
plt.title(title)

if args.t is not None:
    target_dir = args.t + "/"
else:
    target_dir = "./"
Path(target_dir).mkdir(parents=True, exist_ok=True)
plt.savefig(target_dir + "all-runs-{}-{}.png".format(get_file_str(split[len(split)-1]), exp_str), dpi = 600)
