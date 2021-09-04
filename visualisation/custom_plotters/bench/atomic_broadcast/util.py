def format_k(y, _):
	if y > 1000 or y < -1000:
		return "{}k".format(int(y/1000))
	else:
		return int(y)

def format_time(seconds, _):
    """Formats a timedelta duration to [N days] %M:%S format"""
    secs_in_a_min = 60

    minutes, seconds = divmod(seconds, secs_in_a_min)

    time_fmt = "{:d}:{:02d}".format(minutes, seconds)
    return time_fmt

colors = {
  "Omni-Paxos": "dodgerblue",
  "Omni-Paxos replace follower": "dodgerblue",
  "Omni-Paxos replace leader": "midnightblue",
  "Omni-Paxos 1 min": "dodgerblue",
  "Omni-Paxos 2 min": "blue",
  "Omni-Paxos 4 min": "midnightblue",
  "Omni-Paxos, n=3": "dodgerblue",
  "Omni-Paxos, n=5": "midnightblue",

  "Raft": "orange",
  "Raft replace follower": "orange",
  "Raft replace leader": "crimson",
  "Raft PV+CQ": "crimson",
  "Raft PV+CQ 1 min": "limegreen",
  "Raft PV+CQ 2 min": "orange",
  "Raft PV+CQ 4 min": "crimson",
  "Raft, n=3": "orange",
  "Raft, n=5": "crimson"
}

linestyles = {
  "Omni-Paxos": "solid",
  "Raft": "dashdot" ,
}

markers = {
  # deadlock plots
  "1 min": ".",
  "2 min": "v",
  "4 min": "s",
  "PV+CQ 1 min": ".",
  "PV+CQ 2 min": "v",
  "PV+CQ 4 min": "s",
  # reconfig plots
  "replace follower": ".",
  "replace leader": "v",
  # periodic plots
  "Omni-Paxos": ".",
  "Raft": "v" ,
  "Raft PV+CQ": "^",
  # normal plots
  " n=3": ".",
  " n=5": "v"
}