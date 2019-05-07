package se.kth.benchmarks.kompicsjava.bench.pingpong;

import se.sics.kompics.KompicsEvent;

public class Pong implements KompicsEvent {
    public static final Pong event = new Pong();
}
