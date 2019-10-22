package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class Value implements KompicsEvent {
    public long key;
    public int run_id, rid, ts, wr, value;

    public Value(int run_id, long key, int rid, int ts, int wr, int value){
        this.run_id = run_id;
        this.key = key;
        this.rid = rid;
        this.ts = ts;
        this.wr = wr;
        this.value = value;
    }
}
