package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.sics.kompics.KompicsEvent;

import java.util.List;
import java.util.Set;

public class INIT implements KompicsEvent {
    public List<NetAddress> nodes;
    public int id;
    public int rank;
    public long min, max;
    public INIT(int rank, int id, List<NetAddress> nodes, long min, long max) {this.rank = rank; this.id = id; this.nodes = nodes; this.min = min; this.max = max;}
}
