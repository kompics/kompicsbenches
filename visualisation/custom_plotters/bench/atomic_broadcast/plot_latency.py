import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np
import sys
import argparse
import os
import plotly.graph_objects as go


parser = argparse.ArgumentParser()

parser.add_argument('-s', required=True, help='Directory of raw results')
parser.add_argument('-n', nargs='?', default=-1, help='Number of lines to read per file')
parser.add_argument('-t', nargs='?', help='Output directory')
parser.add_argument('-sample', nargs='?', default=1, help='Sample every X datapoint')
parser.add_argument('-keep', nargs='+', default=[0, sys.maxint], type=int, help='Interval of datapoints that must be kept')
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
    y = []
    print("Reading", filename, "...")
    for line in open(args.s + "/" + filename, 'r'):
        count += 1
        #print(line)
        if count % sample == 0 or (count >= keep_min and count <= keep_max):
            try:
                x.append(count)
                y.append(float(line)/1000)  # ms
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
    all_plots.append((legend, x, y))

    
print("Plotting",len(all_plots),"series")
fig = go.Figure()
for (legend, x, y) in all_plots:
    data = go.Scattergl(x = x, y = y, mode = 'markers', name = legend, hovertemplate ='(%{x:.d}, %{y:.d})')
    fig.add_trace(data)

dir_name = os.path.basename(os.path.normpath(args.s))
if "-" in dir_name:
    (nodes, cp) = tuple(dir_name.split("-"))
    fig.update_layout(
        title="Latency of every {}th proposal, fully-sampled interval: [{}, {}]".format(sample, keep_min, keep_max),
        xaxis_title="Proposal id",
        yaxis_title="Latency (ms)",
        legend_title="nodes: {}, concurrent_proposals: {}".format(nodes, cp)
    )
else:
    fig.update_layout(
        title="Latency of every proposal",
        xaxis_title="Proposal id",
        yaxis_title="Latency (ms)",
        #legend_title="3 nodes"
    )

if args.t is not None:
    target_dir = args.t
else:
    target_dir = args.s
fig.write_html(target_dir + '/latency.html', auto_open=False)
