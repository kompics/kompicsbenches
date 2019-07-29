package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class READ implements KompicsEvent {
    public long key;
    public int rid;
    public int run_id;
    public READ(int run_id, long key, int rid){ this.run_id = run_id; this.key = key; this.rid = rid; }
}
