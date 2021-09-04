import numpy as np
import matplotlib.pyplot as plt
import util
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--hide-y', dest='hide_y', action='store_true')
parser.add_argument('--pos-only', dest='pos_only', action='store_true')
#parser.set_defaults(feature=True)

args = parser.parse_args()
def to_tp(t):
    return num_proposals/(t/1000)

def ci_to_err_tp(ci):
    (lo, hi) = ci
    (lo_tp, hi_tp) = (to_tp(hi), to_tp(lo))
    err = (hi_tp-lo_tp)/2
    return err


num_proposals = 5 * 1000000

SIZE = 20
plt.rc('axes', labelsize=SIZE) 
plt.rc('xtick', labelsize=SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SIZE)    # fontsize of the tick labels

FILENAME = "cpu"

# weak-cpu
omni3 = [52263.383862, 34477.434701, 36762.414132]
raft3 = [73984.820682, 60661.052559, 52868.311547]
omni3_ci = [(51916.253015,52610.514708), (34102.238273,34852.631128), (170268.826939,181783.742712)]
raft3_ci = [(40275.257500,107694.383864), (36694.625127,84627.479991), (34102.238273,34852.631128)]

# wan
#omni3 = [44511.534449, 32488.908057, 34328.186441]
#raft3 = [631830.748108, 100105.734023, 36649.930293]
#omni3_ci = [(44306.817798,44716.251101), (32400.297279,32577.518836), (33979.349731,34677.023152)]
#raft3_ci = [(-45537.857279,1309199.353494), (37952.279589,162259.188457), (35868.363244,37431.497342)]

x_axis = [1, 2, 3]
y_axis = np.arange(0, 165000, 40000)
num_cp = ["500", "5k", "50k"]

all_series = [
    ("Raft", list(map(to_tp, raft3)), list(map(ci_to_err_tp, raft3_ci))),
    #("Raft, n=5", list(map(to_tp, raft5)), list(map(ci_to_err_tp, raft5_ci))),
    ("Omni-Paxos", list(map(to_tp, omni3)), list(map(ci_to_err_tp, omni3_ci))),
    #("Omni-Paxos, n=5", list(map(to_tp, omni5)), list(map(ci_to_err_tp, omni5_ci)))
]


fig, ax = plt.subplots()
for (label, data, err) in all_series:
    color = util.colors[label]
    split = label.split(",")
    linestyle = util.linestyles[split[0]]
    if len(split) > 1:
        marker = util.markers[split[1]]
    else:
        marker = "."
    plt.errorbar(x_axis, data, label=label, color=color, marker=marker, linestyle=linestyle, yerr=err, capsize=8)

ax.yaxis.set_major_formatter(util.format_k)
ax.legend(loc = "lower center", fontsize=19.5, ncol=2)

if args.hide_y == True:
    ax.get_legend().remove()
    #ax.yaxis.set_major_formatter(lambda a, b : "")
    plt.gca().axes.yaxis.set_ticklabels([])
else:
    plt.ylabel("Throughput (ops/s)")

if args.pos_only == True:
    ax.set_ylim(bottom=0)

fig.set_size_inches(6, 5)
ax.set_xlabel('Number of concurrent proposals')
plt.xticks(x_axis, num_cp)
plt.yticks(y_axis)
#ax.set_ylim(top=0000)
#ax.yaxis.set_major_formatter(util.format_k)

plt.savefig("{}.pdf".format(FILENAME), dpi = 600, bbox_inches='tight')