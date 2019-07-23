package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class INIT_ACK implements KompicsEvent {
    public final int init_id;

    public INIT_ACK(int init_id){ this.init_id = init_id; }
}
