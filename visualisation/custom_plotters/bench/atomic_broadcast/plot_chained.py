import numpy as np
import matplotlib.pyplot as plt
import util

def ms_to_s(n):
    return n/1000


FILENAME = "chained"
TITLE = 'Chained scenario'
raft = [72992.598572, 49096.596413, 52026.085097999996, 57850.760213999994, 48789.793243, 52498.125788, 49160.668132, 72751.197831, 61090.073779, 51998.231606999994]
raft_pv = [43329.604495,43388.161511,43767.704653,42241.795067,42119.735282,43769.912786,44565.963586,43569.776734,43557.760668999996,42316.443001]
paxos = [50655.495052, 50384.418675, 49658.661265999996, 50432.528299, 49910.884253,50599.022819, 51244.531276999995, 50479.078968999995, 50542.043344, 51361.560896999996]

raft_s = list(map(ms_to_s, raft))
raft_pv_s = list(map(ms_to_s, raft_pv))
paxos_s = list(map(ms_to_s, paxos))

my_dict = {'Raft': raft_s, 'Omni-Paxos': paxos_s, 'Raft PV+CQ': raft_pv_s}

y_axis = np.arange(40, 75, 5)

MEDIUM_SIZE = 18
SIZE = 20
plt.rc('axes', labelsize=SIZE) 
plt.rc('xtick', labelsize=SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SIZE)    # fontsize of the tick labels
plt.yticks(y_axis)

fig, ax = plt.subplots()
fig.set_size_inches(10.3, 6)
#ax.set_title(TITLE, fontsize=MEDIUM_SIZE)
ax.boxplot(my_dict.values())
#ax.boxplot(my_dict.values(), positions=[1, 1.6, 2.2])
ax.set_xticklabels(my_dict.keys())
ax.set_ylabel('Execution time (s)')
ax.set_yticks(y_axis)
#ax.yaxis.set_major_formatter(util.format_k)

plt.savefig("{}.pdf".format(FILENAME), dpi = 600, bbox_inches='tight')