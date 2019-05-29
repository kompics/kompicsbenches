package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class WRITE implements KompicsEvent {
    public int rid, ts, wr, value;

    public WRITE(int rid, int ts, int wr, int value){
        this.rid = rid;
        this.ts = ts;
        this.wr = wr;
        this.value = value;
    }
}
