package se.kth.benchmarks.kompicsjava.bench.throughputpingpong;

import se.sics.kompics.KompicsEvent;

public class Ping implements KompicsEvent {
    public final long index;
    
    public Ping(long index) {
        this.index = index;
    }
}
