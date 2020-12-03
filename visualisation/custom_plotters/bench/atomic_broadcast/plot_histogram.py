import matplotlib
matplotlib.use('Agg')

import plotly.graph_objects as go
import matplotlib.pyplot as plt
import numpy as np
import sys
import argparse
import os

parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('-n', nargs='?', default=-1, help='Number of lines to read per file')
parser.add_argument('-t', nargs='?', default='./', help='Output directory')
parser.add_argument('-sample', nargs='?', default=1, help='Sample every X datapoint')
parser.add_argument('-keep', nargs='+', default=[0, sys.maxint], type=int, help='Interval of datapoints that must be kept (i.e. not sampled)')
#parser.add_argument('-trim', nargs = '?', default=0, help='Remove the X first and X last datapoints')

all_plots = []

args = parser.parse_args()
print("Plotting with args:",args)
n = int(args.n)
sample = int(args.sample)
(keep_min, keep_max) = tuple(args.keep)
data_files = [f for f in os.listdir(args.s) if f.endswith('.data')]
for filename in data_files :
    count = 0
    x = []
    print("Reading", filename, "...")
    for line in open(args.s + "/" + filename, 'r'):
        count += 1
        #print(line)
        if count % sample == 0 or (count >= keep_min and count <= keep_max):
            try:
                x.append(float(line)/1000)  # ms
            except:
                pass
        if count == n:
            break

    if "paxos" in filename:
    	legend = "paxos"
    else:
        if "leader" in filename:
            legend = "raft-replace-leader"
        elif "follower" in filename:
            legend = "raft-replace-follower"
        else:
            legend = "raft"
    all_plots.append((legend, x))
#kwargs = dict(alpha=0.75, bins=100, log=True)

fig = go.Figure()
fig.update_yaxes(type="log")
for (legend, x) in all_plots:
    data = go.Histogram(x = x, name = legend, bingroup=1, nbinsx = 100)
    fig.add_trace(data)

fig.update_layout(
    title="Histogram of latencies",
    xaxis_title="Latency (ms)",
    #legend_title="nodes: {}, concurrent_proposals: {}".format(nodes, cp)
)

       
fig.write_html(args.t + '/histogram.html', auto_open=False)
