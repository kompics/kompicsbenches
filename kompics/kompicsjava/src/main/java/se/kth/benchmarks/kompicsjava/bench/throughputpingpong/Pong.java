package se.kth.benchmarks.kompicsjava.bench.throughputpingpong;

import se.sics.kompics.KompicsEvent;

public class Pong implements KompicsEvent {
    public final long index;
    
    public Pong(long index) {
        this.index = index;
    }
}
