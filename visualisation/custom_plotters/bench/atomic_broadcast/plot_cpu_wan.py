import numpy as np
import matplotlib.pyplot as plt
import util
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--ssd', dest='ssd', action='store_true')
parser.add_argument('--pos', dest='pos', action='store_true')
#parser.set_defaults(feature=True)

args = parser.parse_args()

def to_tp(t):
    return num_proposals/(t/1000)

def ci_to_err_tp(ci):
    (lo, hi) = ci
    (lo_tp, hi_tp) = (to_tp(hi), to_tp(lo))
    err = (hi_tp-lo_tp)/2
    return err

def plot_series(idx, s):
    for (label, data, err) in s:
        color = util.colors[label]
        split = label.split(",")
        linestyle = util.linestyles[split[0]]
        if len(split) > 1:
            marker = util.markers[split[1]]
        else:
            marker = "."
        ax[idx].errorbar(x_axis, data, label=label, color=color, marker=marker, linestyle=linestyle, yerr=err, capsize=8)

num_proposals = 5 * 1000000

SIZE = 20
plt.rc('axes', labelsize=SIZE) 
plt.rc('xtick', labelsize=SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SIZE)    # fontsize of the tick labels

FILENAME = "cpu-wan"

# wan
wan_omni = [44511.534449, 32488.908057, 34328.186441]
wan_raft = [631830.748108, 100105.734023, 36649.930293]
wan_omni_ci = [(44306.817798,44716.251101), (32400.297279,32577.518836), (33979.349731,34677.023152)]
wan_raft_ci = [(-45537.857279,1309199.353494), (37952.279589,162259.188457), (35868.363244,37431.497342)]
wan_omni_ssd = [286.174146, 123.869328, 487.640097]
wan_raft_ssd = [946896.017568, 86884.538217, 1092.555398]

# weak-cpu
cpu_omni = [52263.383862, 34477.434701, 36762.414132]
cpu_raft = [73984.820682, 60661.052559, 52868.311547]
cpu_omni_ci = [(51916.253015,52610.514708), (34102.238273,34852.631128), (36278.576758,37246.251506)]
cpu_raft_ci = [(40275.257500,107694.383864), (36694.625127,84627.479991), (35503.668577,70232.954517)]
cpu_omni_ssd = [485.255463, 524.488440, 676.358010]
cpu_raft_ssd = [47122.720004, 33502.755383, 24274.097063]

x_axis = [1, 2, 3]
y_axis = np.arange(-40000, 165000, 40000)
num_cp = ["500", "5k", "50k"]

wan_series = [
    ("Raft", list(map(to_tp, wan_raft)), list(map(ci_to_err_tp, wan_raft_ci))),
    #("Raft, n=5", list(map(to_tp, raft5)), list(map(ci_to_err_tp, raft5_ci))),
    ("Omni-Paxos", list(map(to_tp, wan_omni)), list(map(ci_to_err_tp, wan_omni_ci))),
    #("Omni-Paxos, n=5", list(map(to_tp, omni5)), list(map(ci_to_err_tp, omni5_ci)))
]

cpu_series = [
    ("Raft", list(map(to_tp, cpu_raft)), list(map(ci_to_err_tp, cpu_raft_ci))),
    #("Raft, n=5", list(map(to_tp, raft5)), list(map(ci_to_err_tp, raft5_ci))),
    ("Omni-Paxos", list(map(to_tp, cpu_omni)), list(map(ci_to_err_tp, cpu_omni_ci))),
    #("Omni-Paxos, n=5", list(map(to_tp, omni5)), list(map(ci_to_err_tp, omni5_ci)))
]

all_series = [cpu_series, wan_series]

fig, ax = plt.subplots(nrows=1, ncols=2, sharex=True, sharey=True)
for (idx, series) in enumerate(all_series):
    plot_series(idx, series)

if args.pos:
    ax[1].set_ylim(bottom=0)
    y_axis = np.arange(0, 165000, 40000)
ax[0].yaxis.set_major_formatter(util.format_k)
ax[0].set_title("CPU", fontsize=SIZE)
ax[1].set_title("WAN", fontsize=SIZE)
ax[0].set_ylabel("Throughput (ops/s)")
#ax.legend(loc = "lower center", fontsize=18, ncol=2)

fig.set_size_inches(15, 3)
fig.text(0.5, -0.08, 'Number of concurrent proposals', ha='center', va='center', fontsize=20)
handles, labels = ax[0].get_legend_handles_labels()
#fig.legend(handles, labels, loc='upper center', ncol=2, fontsize=20)
#ax[0].legend(handles, labels ,loc='upper center', 
             #bbox_to_anchor=(1.2, 1.2), fancybox=False, shadow=False, ncol=2)

fig.legend(handles, labels, loc="upper center", fontsize=18, ncol=2)
fig.subplots_adjust(top=0.67)

plt.xticks(x_axis, num_cp)
plt.yticks(y_axis)
#ax.set_ylim(top=160000)
#ax.yaxis.set_major_formatter(util.format_k)
suffix = "ci"
if args.ssd == True:
    suffix = "ssd"
plt.savefig("{}-{}.pdf".format(FILENAME, suffix), dpi = 600, bbox_inches='tight')