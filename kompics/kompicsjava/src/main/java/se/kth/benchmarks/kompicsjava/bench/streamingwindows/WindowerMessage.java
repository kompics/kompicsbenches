package se.kth.benchmarks.kompicsjava.bench.streamingwindows;

import java.util.Objects;
import se.sics.kompics.KompicsEvent;

public abstract class WindowerMessage {
    public static class Start implements KompicsEvent {

        public final int partitionId;

        public Start(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Start)) {
                return false;
            }
            Start other = (Start) obj;
            return this.partitionId == other.partitionId;
        }
    }

    public static class Stop implements KompicsEvent {

        public final int partitionId;

        public Stop(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Stop)) {
                return false;
            }
            Stop other = (Stop) obj;
            return this.partitionId == other.partitionId;
        }
    }

    public static class Event implements KompicsEvent {

        public final long ts;
        public final int partitionId;
        public final long value;

        public Event(long ts, int partitionId, long value) {
            this.ts = ts;
            this.partitionId = partitionId;
            this.value = value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ts, partitionId, value);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Event)) {
                return false;
            }
            Event other = (Event) obj;
            return this.ts == other.ts && this.partitionId == other.partitionId && this.value == other.value;
        }
    }

    public static class Flush implements KompicsEvent {

        public final int partitionId;

        public Flush(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Flush)) {
                return false;
            }
            Flush other = (Flush) obj;
            return this.partitionId == other.partitionId;
        }
    }

    public static class Ready implements KompicsEvent {

        public final int partitionId;
        public final long numMessages;

        public Ready(int partitionId, long numMessages) {
            this.partitionId = partitionId;
            this.numMessages = numMessages;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId, numMessages);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Ready)) {
                return false;
            }
            Ready other = (Ready) obj;
            return this.partitionId == other.partitionId && this.numMessages == other.numMessages;
        }
    }

    public static class WindowAggregate implements KompicsEvent {

        public final long ts;
        public final int partitionId;
        public final double value;

        public WindowAggregate(long ts, int partitionId, double value) {
            this.ts = ts;
            this.partitionId = partitionId;
            this.value = value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ts, partitionId, value);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof WindowAggregate)) {
                return false;
            }
            WindowAggregate other = (WindowAggregate) obj;
            return this.ts == other.ts && this.partitionId == other.partitionId && this.value == other.value;
        }
    }

    public static class Flushed implements KompicsEvent {

        public final int partitionId;

        public Flushed(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Flushed)) {
                return false;
            }
            Flushed other = (Flushed) obj;
            return this.partitionId == other.partitionId;
        }
    }
}
