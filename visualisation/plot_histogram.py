# uncomment for use on WSL  

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np
import sys
import argparse
import os

parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('-n', nargs='?', default=-1, help='Number of lines to read per file')
parser.add_argument('-t', nargs='?', default='./', help='Output directory')

legends = []
all_plots = []

args = parser.parse_args()
print("Plotting with args:",args)
n = int(args.n)
data_files = [f for f in os.listdir(args.s) if f.endswith('.data')]
for filename in data_files :
    count = 0
    data = []
    print("Reading", filename, "...")
    for line in open(args.s + filename, 'r'):
        count += 1
        #print(line)
        data.append(float(line))
        if count == n:
            break
    all_plots.append(data)
    if "paxos" in filename:
        legends.append("paxos")
    else:
        legends.append("raft")
    
print("Plotting",len(all_plots),"series")
kwargs = dict(alpha=0.75, bins=100, log=True)

for data in all_plots:
    plt.hist(data, **kwargs)
        
plt.title('Latency')
#plt.xlabel('Proposal id')
plt.xlabel('Latency (micro s)')
plt.legend(legends, loc='upper right')
plt.savefig(args.t + 'histogram.png', dpi = 600)
plt.show()
