package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class ACK implements KompicsEvent {
    public int rid;
    public ACK(int rid){ this.rid = rid; }
}
