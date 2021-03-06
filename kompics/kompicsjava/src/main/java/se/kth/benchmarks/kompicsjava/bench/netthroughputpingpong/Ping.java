package se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong;

import java.util.Objects;

import se.sics.kompics.KompicsEvent;

public class Ping implements KompicsEvent {

    public final long index;
    public final int id;

    public Ping(long index, int id) {
        this.index = index;
        this.id = id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, index);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Ping)) {
            return false;
        }
        Ping other = (Ping) obj;
        return id == other.id && index == other.index;
    }
}
