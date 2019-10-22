package se.kth.benchmarks.kompicsjava.bench.netpingpong;

import se.sics.kompics.KompicsEvent;

public class Ping implements KompicsEvent {
    public static final Ping event = new Ping();
}
