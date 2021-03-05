import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np
import sys
import argparse
import os
import plotly.graph_objects as go


parser = argparse.ArgumentParser()

parser.add_argument('-s', nargs='+', help='timestamp and latency directory')
parser.add_argument('-n', nargs='?', default=-1, help='Number of lines to read per file')
parser.add_argument('-t', nargs='?', help='Output directory')
parser.add_argument('-sample', nargs='?', default=1, help='Sample every X datapoint')
parser.add_argument('-keep', nargs='+', default=[0, sys.maxint], type=int, help='Interval of datapoints that must be kept')

colors = {
  "paxos": "royalblue",
  "raft": "red",
  "raft-replace-leader": "forestgreen",
  "raft-replace-follower": "red"
}

all_leader_changes = []

all_timestamps = {} # legend -> timestamp
all_latencies = {}  # legend -> latency

args = parser.parse_args()
print("Plotting with args:",args)
n = int(args.n)
sample = int(args.sample)
(keep_min, keep_max) = tuple(args.keep)
(ts_dir, latency_dir) = tuple(args.s) 

# read timestamp files
ts_files = [f for f in os.listdir(ts_dir) if f.endswith('.data')]
for filename in ts_files :
    count = 0
    timestamps = []
    leader_changes = []
    f = open(ts_dir + "/" + filename, 'r')
    print("Reading timestamp file", filename, "...")
    # read leader_changes
    first_line = f.readline();
    pid_ts_str = first_line.split(" ")
    for csv_str in pid_ts_str:    
        pid_ts = csv_str.split(",")
        try:            
            pid = int(pid_ts[0])
            ts = float(pid_ts[1])/1000  # ms
            leader_changes.append((pid, ts))
        except:
            pass
    # read timestamps
    for line in f:
        count += 1
        #print(line)
        if count % sample == 0 or (count >= keep_min and count <= keep_max):
            try:
                timestamps.append(float(line)/1000) # ms
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
    all_timestamps[legend] = timestamps
    all_leader_changes.append((legend, leader_changes))

# read latency files
latency_files = [f for f in os.listdir(latency_dir) if f.endswith('.data')]
for filename in latency_files :
    count = 0
    latencies = []
    f = open(latency_dir + "/" + filename, 'r')
    print("Reading latency file", filename, "...")
    # read timestamps
    for line in f:
        count += 1
        #print(line)
        if count % sample == 0 or (count >= keep_min and count <= keep_max):
            try:
                latencies.append(float(line)/1000) # ms
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
    all_latencies[legend] = latencies

initial_leaders = []
fig = go.Figure()
for (algorithm, leader_changes) in all_leader_changes:
	for (pid, ts) in leader_changes:
		if ts == 0:
			initial_leaders.append((algorithm, pid))
		else:
			fig.add_vline(x=ts, line_dash="dash", line_color=colors[algorithm], opacity=0.7, annotation_text="{} changed leader: {}".format(algorithm, pid), annotation_position="top left", annotation_textangle=90)

fig.add_vline(x=0, line_dash="dash", line_color="black", opacity=0.25, annotation_text="Initial leaders: {}".format(initial_leaders), annotation_position="top left", annotation_textangle=90)

for (legend, timestamps) in all_timestamps.iteritems():
    data = go.Scattergl(x = timestamps, y = all_latencies[legend], mode = 'markers', name = legend, hovertemplate ='(%{x:.d}, %{y:.d})', marker = dict(color = colors[legend]))
    fig.add_trace(data)

dir_name = os.path.basename(os.path.normpath(ts_dir))
if "-" in dir_name:
    (nodes, cp) = tuple(dir_name.split("-"))
    fig.update_layout(
        title="Timestamp and Latency of every {}th response since start of benchmark, fully-sampled interval: [{}, {}]".format(sample, keep_min, keep_max),
        xaxis_title="Timestamp (ms)",
        yaxis_title="Latency (ms)",
        legend_title="nodes: {}, concurrent_proposals: {}".format(nodes, cp),
    )
else:
    fig.update_layout(
        title="Timestamp of every response since start of benchmark",
        xaxis_title="Timestamp (ms)",
        yaxis_title="Latency (ms)",
        #legend_title="3 nodes"
    )
     
if args.t is not None:
    target_dir = args.t
else:
    target_dir = ts_dir

if not os.path.exists(target_dir):
    os.makedirs(target_dir)
fig.write_html(target_dir + '/ts_latency.html', auto_open=False)
