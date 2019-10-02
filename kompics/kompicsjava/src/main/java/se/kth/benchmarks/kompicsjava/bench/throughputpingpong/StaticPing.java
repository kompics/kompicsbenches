package se.kth.benchmarks.kompicsjava.bench.throughputpingpong;

import se.sics.kompics.KompicsEvent;

public class StaticPing implements KompicsEvent {
    public static final StaticPing event = new StaticPing();
}
