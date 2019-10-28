package se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong;

import java.util.Objects;

import se.sics.kompics.KompicsEvent;

public class StaticPing implements KompicsEvent {
    public static StaticPing event(int id) {
        return new StaticPing(id); // TODO do this with addresses instead
    }
    
    public final int id;
    
    private StaticPing(int id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof StaticPing)) {
            return false;
        }
        StaticPing other = (StaticPing) obj;
        return id == other.id;
    }
}
