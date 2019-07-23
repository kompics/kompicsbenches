package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class READ implements KompicsEvent {
    public long key;
    public int rid;
    public READ(long key, int rid){ this.key = key; this.rid = rid; }
}
