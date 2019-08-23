package se.kth.benchmarks.kompicsjava.broadcast;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.sics.kompics.KompicsEvent;

import java.util.List;
import java.util.Set;

public class BEBRequest implements KompicsEvent {
    public final KompicsEvent payload;
    public final List<NetAddress> nodes;

    public BEBRequest(List<NetAddress> nodes, KompicsEvent payload) {
        this.payload = payload;
        this.nodes = nodes;
    }
}
