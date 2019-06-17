package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.sics.kompics.KompicsEvent;

import java.util.Set;

public class INIT implements KompicsEvent {
    public Set<NetAddress> nodes;
    public int id;
    public INIT(int id, Set<NetAddress> nodes) { this.id = id; this.nodes = nodes; }
}
