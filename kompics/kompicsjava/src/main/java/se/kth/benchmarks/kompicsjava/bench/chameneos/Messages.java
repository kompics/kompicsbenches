package se.kth.benchmarks.kompicsjava.bench.chameneos;

import se.sics.kompics.KompicsEvent;
import java.util.UUID;

public class Messages {
    public static class MeetingCount implements KompicsEvent {
        public final long count;

        public MeetingCount(long count) {
            this.count = count;
        }
    }

    public static class MeetMe implements KompicsEvent {
        public final ChameneosColour colour;
        public final UUID chameneo;

        public MeetMe(ChameneosColour colour, UUID chameneo) {
            this.colour = colour;
            this.chameneo = chameneo;
        }
    }

    public static class MeetUp implements KompicsEvent {
        public final ChameneosColour colour;
        public final UUID chameneo;
        public final UUID target;

        public MeetUp(ChameneosColour colour, UUID chameneo, UUID target) {
            this.colour = colour;
            this.chameneo = chameneo;
            this.target = target;
        }
    }

    public static class Change implements KompicsEvent {
        public final ChameneosColour colour;
        public final UUID target;

        public Change(ChameneosColour colour, UUID target) {
            this.colour = colour;
            this.target = target;
        }
    }

    public static class Exit implements KompicsEvent {
        public static final Exit EVENT = new Exit();
    }
}
