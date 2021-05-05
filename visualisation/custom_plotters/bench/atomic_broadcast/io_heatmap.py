import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import palettable
import colorcet as cc

#cmap = "bone_r"
#cmap = "PuRd"
#cmap = cc.m_linear_worb_100_25_c53_r
cmap = palettable.colorbrewer.diverging.RdGy_11_r.mpl_colormap
#cmap = cc.m_blues

linecolor = "gray"


sns.set()
leader_sent = [19.96,19.82,0.598,50.08,0,0,0,0,315.23,23.78,0,0,0.0012,0,0.0008,6.64,19.38,19.5,19.64]
leader_received = [3.14,3.14,0.099,4.81,0,0,0,0,0.00064,16.2,3.25,3.23,0,0,0.000832,1.06,3.12,3.13,3.14]
continued_sent = [0.784,0.786,0.0243,1.2,0,0,0,0,0.00512,3.24,0,0,0,0,0.00064,0.265,0.779,0.783,0.781]
continued_received = [4.99,4.95,0.15,1.26,0,0,0,0,0.006,3.4,0,0,0.00016,0,0.000672,1.66,4.83,4.88,4.91]
new_sent = [0,0,0,0,0,0.0014,0,0,0,0,0,0,0.0008,3.23,0,0,0.00032,0.264,0.779]
new_received = [0,0,0,15,0,0,0,0,105.07,3.4,0,0,0.00016,0,0.00067,1.66,4.85,4.87,4.91]


#data = [leader_sent, leader_received, continued_sent, continued_received, new_sent, new_received]

leader = [leader_sent, leader_received]
continued = [continued_sent, continued_received]
new = [new_sent, new_received]
print("ls: {}, lr: {}, cs: {}, cr: {}, ns: {}, nr: {}".format(len(leader_sent), len(leader_received), len(continued_sent), len(continued_received), len(new_sent), len(new_received)))


fig,(ax1,ax2,ax3) = plt.subplots(3,1, sharex=True, sharey=True)
#cbar_ax = fig.add_axes([.91, .3, .03, .4])
cbar_ax = fig.add_axes([.93, .3, .03, .4])
g1 = sns.heatmap(leader, cmap=cmap, linecolor=linecolor, linewidths=1, square=True,ax=ax1, cbar_ax = cbar_ax, yticklabels=["sent", "received"], vmin=0, vmax=350, cbar_kws={'label': 'I/O (MB)'})
g2 = sns.heatmap(continued, cmap=cmap, linecolor=linecolor, linewidths=1, square=True,ax=ax2, cbar=False, yticklabels=["sent", "received"], vmin=0, vmax=350)
g3 = sns.heatmap(new, cmap=cmap, linecolor=linecolor, linewidths=1, square=True,ax=ax3, cbar=False, yticklabels=["sent", "received"], vmin=0, vmax=350)
ax1.set_title("Leader")
ax2.set_title("Continued Follower")
ax3.set_title("New Follower")

#fig.tight_layout(rect=[0, 0, .9, 1])
fig.set_size_inches(12, 6)
#ax.hlines([2, 4, 6], *ax.get_xlim())
plt.savefig("test.png")