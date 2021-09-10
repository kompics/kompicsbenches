import numpy as np
import matplotlib.pyplot as plt
import util
import argparse
import os


parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('--title', dest='show_title', action='store_true')
parser.set_defaults(feature=True)

args = parser.parse_args()

def to_tp(t):
    return num_proposals/(t/1000)

def plot_series(exp, files):
    for filename in files:
        file_path = "{}/{}/{}".format(args.s, exp, filename)
        f = open(file_path, 'r')
        print("Reading", file_path, "...")
        s = filename.split("-")
        algo = s[0]
        if algo == "omni":
            algo = "Omni-Paxos"
        else:
            algo = "Raft"
        cp = int(s[1].split(".")[0])
        cp_str = cp
        if cp >= 1000:
         cp_str = "{}k".format(int(cp/1000))
        label = "{}\n{}".format(algo, cp_str) 
        all_t = []
        for line in f:
            all_t.append(float(line))
        all_tp = list(map(to_tp, all_t))
        if exp == "cpu":
            cpu_series[label] = all_tp
        else:
            wan_series[label] = all_tp

num_proposals = 5 * 1000000

SIZE = 20
plt.rc('axes', labelsize=SIZE) 
plt.rc('xtick', labelsize=SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SIZE)    # fontsize of the tick labels

FILENAME = "cpu-wan-box"

cpu_files = [f for f in os.listdir(args.s + "/" + "cpu") if f.endswith('.data')]
wan_files = [f for f in os.listdir(args.s + "/" + "wan") if f.endswith('.data')]

cpu_series = {}
wan_series = {}

fig, ax = plt.subplots(nrows=2, ncols=1, sharex=True)
plot_series("cpu", cpu_files)
plot_series("wan", wan_files)

#fig.set_size_inches(32, 3)
fig.set_size_inches(16, 6)
ax[0].set_yticks([0, 50000, 100000, 150000])
ax[0].boxplot(cpu_series.values())
#ax[0].set_xticklabels(cpu_series.keys())
ax[0].yaxis.set_major_formatter(util.format_k)
ax[0].set_ylabel("Throughput (ops/s)")

ax[1].set_yticks([0, 50000, 100000, 150000])
ax[1].boxplot(wan_series.values())
#ax[1].set_xticklabels(wan_series.keys())
ax[1].yaxis.set_major_formatter(util.format_k)
if args.show_title:
    ax[0].set_title("CPU", fontsize=SIZE)
    ax[1].set_title("WAN", fontsize=SIZE)

plt.savefig("{}.pdf".format(FILENAME), dpi = 600, bbox_inches='tight')