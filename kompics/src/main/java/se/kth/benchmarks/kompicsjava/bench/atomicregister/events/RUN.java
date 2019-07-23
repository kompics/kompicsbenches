package se.kth.benchmarks.kompicsjava.bench.atomicregister.events;

import se.sics.kompics.KompicsEvent;

public class RUN implements KompicsEvent {
    public static final RUN event = new RUN();
}
