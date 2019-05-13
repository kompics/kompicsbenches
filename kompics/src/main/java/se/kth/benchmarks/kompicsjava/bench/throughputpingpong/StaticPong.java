package se.kth.benchmarks.kompicsjava.bench.throughputpingpong;

import se.sics.kompics.KompicsEvent;

public class StaticPong implements KompicsEvent {
    public static final StaticPong event = new StaticPong();
}
