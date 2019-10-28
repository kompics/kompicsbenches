package se.kth.benchmarks.kompicsjava.bench.pingpong;

import se.sics.kompics.KompicsEvent;

public class Ping implements KompicsEvent {
    public static final Ping event = new Ping();
}
