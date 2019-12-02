package se.kth.benchmarks.kompicsjava.partitioningcomponent.events;

import se.kth.benchmarks.test.KVTestUtil;
import se.sics.kompics.KompicsEvent;

import java.util.List;

public class TestDone implements KompicsEvent {
    public final List<KVTestUtil.KVTimestamp> timestamps;

    public TestDone(List<KVTestUtil.KVTimestamp> timestamps){ this.timestamps = timestamps; }
}
