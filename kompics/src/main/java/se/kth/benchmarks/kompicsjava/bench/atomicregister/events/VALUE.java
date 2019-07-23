package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class VALUE implements KompicsEvent {
    public long key;
    public int rid, ts, wr, value;

    public VALUE(long key, int rid, int ts, int wr, int value){
        this.key = key;
        this.rid = rid;
        this.ts = ts;
        this.wr = wr;
        this.value = value;
    }
}
