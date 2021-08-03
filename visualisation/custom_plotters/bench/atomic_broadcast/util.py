def format_k(y, _):
	if y > 1000:
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

  "Raft": "orange",
  "Raft replace follower": "orange",
  "Raft replace leader": "crimson",
  "Raft PV+CQ": "crimson",
  "Raft 1 min": "limegreen",
  "Raft 2 min": "orange",
  "Raft 4 min": "crimson",
}