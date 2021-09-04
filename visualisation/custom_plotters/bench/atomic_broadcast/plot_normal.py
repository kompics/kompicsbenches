import numpy as np
import matplotlib.pyplot as plt
import util

FILENAME = "normal"
num_proposals = 20 * 1000000

def to_tp(t):
    return num_proposals/(t/1000)

def ci_to_err_tp(ci):
    (lo, hi) = ci
    (lo_tp, hi_tp) = (to_tp(hi), to_tp(lo))
    err = (hi_tp-lo_tp)/2
    return err


SIZE = 20
plt.rc('axes', labelsize=SIZE) 
plt.rc('xtick', labelsize=SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SIZE)    # fontsize of the tick labels

omni3 = [205369.870502, 151845.072763, 176026.284826]
omni5 = [188783.921863, 151473.061979, 172812.258542]
raft3 = [202910.487669, 152194.463236, 171368.814179]
raft5 = [213335.412579, 151205.056044, 172256.930474]

omni3_ci = [(200655.311545,210084.429458), (148622.887022,155067.258505), (170268.826939,181783.742712)]
omni5_ci =[(187998.109969,189569.733758), (147830.808101,155115.315857), (167962.814977,177661.702106)]
raft3_ci = [(194744.236397,211076.738940), (147495.951944,156892.974529), (166193.274187,176544.354172)]
raft5_ci = [(203751.300990,222919.524169), (147028.232293,155381.879795), (166357.588526,178156.272421)]
x_axis = [1, 2, 3]
num_cp = ["500", "5k", "50k"]

all_series = [
    ("Raft, n=3", list(map(to_tp, raft3)), list(map(ci_to_err_tp, raft3_ci))),
    ("Raft, n=5", list(map(to_tp, raft5)), list(map(ci_to_err_tp, raft5_ci))),
    ("Omni-Paxos, n=3", list(map(to_tp, omni3)), list(map(ci_to_err_tp, omni3_ci))),
    ("Omni-Paxos, n=5", list(map(to_tp, omni5)), list(map(ci_to_err_tp, omni5_ci)))
]


fig, ax = plt.subplots()
for (label, data, err) in all_series:
    color = util.colors[label]
    split = label.split(",")
    linestyle = util.linestyles[split[0]]
    marker = util.markers[split[1]]
    plt.errorbar(x_axis, data, label=label, color=color, marker=marker, linestyle=linestyle, yerr=err, capsize=8)

ax.yaxis.set_major_formatter(util.format_k)
ax.legend(loc = "lower right", fontsize=20, ncol=2)

fig.set_size_inches(15, 3.5)
ax.set_ylabel('Throughput (ops/s)')
ax.set_xlabel('Number of concurrent proposals')
plt.xticks(x_axis, num_cp)
plt.yticks([90000, 110000, 130000])
#ax.yaxis.set_major_formatter(util.format_k)

plt.savefig("{}.pdf".format(FILENAME), dpi = 600, bbox_inches='tight')