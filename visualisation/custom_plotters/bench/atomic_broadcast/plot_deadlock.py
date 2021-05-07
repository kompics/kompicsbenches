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

colors = {
  "Omni-Paxos 1 min": "dodgerblue",
  "Omni-Paxos 2 min": "blue",
  "Omni-Paxos 4 min": "midnightblue",
  "Raft 1 min": "limegreen",
  "Raft 2 min": "orange",
  "Raft 4 min": "crimson",
}

def get_label_and_color(filename, dirname):
	csv = filename.split(",")
	dir_split = dirname.split("-")
	algorithm = csv[0]
	if algorithm == "paxos":
		algorithm = "Omni-Paxos"
	else:
		algorithm = "Raft"
	reconfig = csv[len(csv)-1].split(".")[0]
	if reconfig == "none":
		label = algorithm
	else:
		label = "{} {}".format(algorithm, reconfig.replace("-", " "))

	minutes = dir_split[1]
	label = label + " {} min".format(minutes)
	color = colors[label]
	return (label, color)

def format_time(seconds, _):
    """Formats a timedelta duration to [N days] %M:%S format"""
    secs_in_a_min = 60

    minutes, seconds = divmod(seconds, secs_in_a_min)

    time_fmt = "{:d}:{:02d}".format(minutes, seconds)
    return time_fmt

parser = argparse.ArgumentParser()

#parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('-t', nargs='?', help='Output directory')
parser.add_argument('-w', required=True, type=int, help='Window duration (s)')
parser.add_argument('--no-ci', dest='ci', action='store_false')
parser.set_defaults(feature=True)

args = parser.parse_args()
print("Plotting with args:",args)

fig, ax = plt.subplots()

SIZE = 20
plt.rc('axes', labelsize=SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SIZE)    # fontsize of the tick labels

max_ts = 0

directories = ["deadlock-1-min", "deadlock-2-min", "deadlock-4-min"]

for d in directories:
	full_dir = "/mnt/d/kompicsbenches/google-cloud/deadlock/{}".format(d)
	data_files = [f for f in os.listdir(full_dir) if f.endswith('.data')]
	for filename in data_files :
		f = open(full_dir + "/" + filename, 'r')
		print("Reading", filename, "...")
		all_tp = []
		for line in f:
			num_decided_per_window = line.split(",")
			for window_idx, num_decided in enumerate(num_decided_per_window):
				if num_decided.isdigit():
					tp = int(num_decided) / args.w
					if len(all_tp) <= window_idx:
						all_tp.append([])
					all_tp[window_idx].append(tp)

		all_ts = []
		all_avg_tp = []
		all_ci95_lo = []
		all_ci95_hi = []

		all_tp_filtered = list(filter(lambda x: len(x) == 10, all_tp)) 
		for (window_idx, all_tp_per_window) in enumerate(all_tp_filtered):
			#if window_idx < 3:
				#continue
			ts = (window_idx+1) * args.w
			if ts > max_ts:
				max_ts = ts
			all_ts.append(ts)
			#all_ts.append(format_time(seconds=(i+1) * args.w))
			avg_tp = sum(all_tp_per_window)/len(all_tp_per_window)
			all_avg_tp.append(avg_tp)
			if args.ci:
				if sum(all_tp_per_window) > 0:
					(ci95_lo, ci95_hi) = st.t.interval(alpha=0.95, df=len(all_tp_per_window)-1, loc=np.mean(np.array(all_tp_per_window)), scale=st.sem(np.array(all_tp_per_window))) 
					if ci95_lo < 0:
						ci95_lo = 0
					#print((ci95_lo, ci95_hi))
					all_ci95_lo.append(ci95_lo)
					all_ci95_hi.append(ci95_hi)
				else:
					all_ci95_lo.append(all_tp_per_window[0])
					all_ci95_hi.append(all_tp_per_window[0])

		(label, color) = get_label_and_color(filename, d)
		if "Paxos" in label:
			linestyle = 'dashed'
		else:
			linestyle = 'solid'
		ax.plot(all_ts, np.array(all_avg_tp), marker=".", label=label, color=color, linestyle = linestyle)
		#ax.plot(all_ts, np.array(all_ci95_lo))
		#ax.plot(all_ts, np.array(all_ci95_hi))
		if args.ci:
			ax.fill_between(all_ts, all_ci95_lo, all_ci95_hi, alpha=0.2, color = color)
		#ax.plot(all_ts, all_min_tp, marker='o')
		#ax.plot(all_ts, all_max_tp, marker='o')

ax.legend(loc = "lower right")
x_axis = np.arange(0, max_ts+4*args.w, 4*args.w)
#plt.axvline(x=20, label='Network Partition')

handles, labels = plt.gca().get_legend_handles_labels()
for h in handles:
	print(h)
order = [1,3,5,0,2,4]
plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order])

plt.ylabel("Throughput (ops/s)")
plt.xlabel("Time")
plt.xticks(x_axis)
ax.xaxis.set_major_formatter(format_time)

plt.ylim(bottom=0)
plt.gcf().autofmt_xdate()

fig.set_size_inches(12, 6)

#split = args.s.split("/")
#exp_str = split[len(split)-3]
#exp_str_split = exp_str.split("-")
#num_nodes = exp_str_split[0]
#num_cp = exp_str_split[1]
exp_str = "deadlock"
title = "Deadlock scenario"
plt.title(title)

if args.t is not None:
    target_dir = args.t + "/deadlock/{}-{}/".format(num_nodes, num_cp)
else:
    target_dir = "./"
if args.ci == False:
	exp_str = exp_str + "-no-ci"
Path(target_dir).mkdir(parents=True, exist_ok=True)
plt.savefig(target_dir + "{}.pdf".format(exp_str), dpi = 1000)
