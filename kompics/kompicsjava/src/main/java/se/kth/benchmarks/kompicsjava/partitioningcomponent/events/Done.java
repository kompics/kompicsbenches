package se.kth.benchmarks.kompicsjava.partitioningcomponent.events;

import se.sics.kompics.KompicsEvent;

public class Done implements KompicsEvent {
    public static final Done event = new Done();
}
