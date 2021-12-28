import numpy as np
import matplotlib.pyplot as plt
import util

num_proposals = 5 * 1000000
def ms_to_tp(t):
    return num_proposals/(t/1000)

FILENAME = "chained"
TITLE = 'Chained scenario'
raft = [50533.435782,52911.101637,60091.845166,65547.680951,54038.709471999995,74749.261094,90319.950333,60373.700495,60754.824064,69841.72998399999]
raft_pv = [50377.20796,47050.350992,46920.739193999994,45494.902544,46634.340917999994,46939.709022999996,51820.560351,50758.268513999996,51059.424573,48731.035656]
paxos = [50505.792305999996,50304.649215,50369.734166999995,49491.659029999995,49579.429575999995,49768.561979,49150.848915999995,49201.637051,49719.578301,49750.827285]

raft_s = list(map(ms_to_tp, raft))
raft_pv_s = list(map(ms_to_tp, raft_pv))
paxos_s = list(map(ms_to_tp, paxos))

my_dict = {'Raft': raft_s, 'Omni-Paxos': paxos_s, 'Raft PV+CQ': raft_pv_s}

y_axis = np.arange(0, 130000, 20000)

MEDIUM_SIZE = 18
SIZE = 20
plt.rc('axes', labelsize=SIZE) 
plt.rc('xtick', labelsize=SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SIZE)    # fontsize of the tick labels

fig, ax = plt.subplots()
#fig.set_size_inches(10.3, 6)
fig.set_size_inches(8, 5.67)
#ax.set_title(TITLE, fontsize=MEDIUM_SIZE)
ax.boxplot(my_dict.values())
#ax.boxplot(my_dict.values(), positions=[1, 1.6, 2.2])
ax.set_xticklabels(my_dict.keys())
#ax.set_ylabel('Throughput (ops/s)')
ax.set_yticks(y_axis)
ax.yaxis.set_major_formatter(util.format_k)

plt.savefig("{}.pdf".format(FILENAME), dpi = 600, bbox_inches='tight')