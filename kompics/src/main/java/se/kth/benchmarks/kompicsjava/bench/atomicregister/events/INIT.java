package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.sics.kompics.KompicsEvent;

import java.util.Set;

public class INIT implements KompicsEvent {
    public Set<NetAddress> nodes;
    public INIT(Set<NetAddress> nodes) { this.nodes = nodes; }
}
