package se.kth.benchmarks.kompicsjava.partitioningcomponent.events;

import se.sics.kompics.KompicsEvent;

public class InitAck implements KompicsEvent {
    public final int init_id;

    public InitAck(int init_id){ this.init_id = init_id; }
}
