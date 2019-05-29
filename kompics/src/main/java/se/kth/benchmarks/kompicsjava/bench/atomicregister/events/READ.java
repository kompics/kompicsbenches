package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class READ implements KompicsEvent {
    public int rid;
    public READ(int rid){ this.rid = rid; }
}
