package se.kth.benchmarks.kompicsjava.bench.fibonacci;

import se.sics.kompics.KompicsEvent;
import java.util.UUID;

public class Messages {
    public static class FibRequest implements KompicsEvent {
        public final int n;
        public final UUID id;

        public FibRequest(int n, UUID id) {
            this.n = n;
            this.id = id;
        }
    }

    public static class FibResponse implements KompicsEvent {
        public final long value;

        public FibResponse(long value) {
            this.value = value;
        }
    }
}
