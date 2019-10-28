package se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong;

import java.util.Objects;

import se.sics.kompics.KompicsEvent;

public class StaticPong implements KompicsEvent {
    public static StaticPong event(int id) {
        return new StaticPong(id); // TODO do this with addresses instead
    }
    
    public final int id;
    
    private StaticPong(int id) {
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
        if (!(obj instanceof StaticPong)) {
            return false;
        }
        StaticPong other = (StaticPong) obj;
        return id == other.id;
    }
}
