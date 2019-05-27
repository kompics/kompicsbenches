package se.kth.benchmarks.kompicsjava.broadcast;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.sics.kompics.KompicsEvent;

import java.util.Set;

public class BEBRequest implements KompicsEvent {
    public final KompicsEvent payload;
    public final Set<NetAddress> nodes;
    public final NetAddress src;

    public BEBRequest( NetAddress src, Set<NetAddress> nodes, KompicsEvent payload) {
        this.payload = payload;
        this.nodes = nodes;
        this.src = src;
    }
}
