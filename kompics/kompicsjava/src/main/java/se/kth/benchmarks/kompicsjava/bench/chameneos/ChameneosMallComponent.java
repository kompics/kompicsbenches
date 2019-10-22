package se.kth.benchmarks.kompicsjava.bench.chameneos;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Fault;
import se.sics.kompics.Positive;
import se.sics.kompics.Negative;
import se.sics.kompics.Kill;
import se.sics.kompics.Start;
import se.sics.kompics.Component;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import java.util.UUID;
import se.kth.benchmarks.kompicsjava.bench.chameneos.Messages.*;

public class ChameneosMallComponent extends ComponentDefinition {
    final Negative<MallPort> mallPort = provides(MallPort.class);

    final long numMeetings;
    final int numChameneos;
    final CountDownLatch latch;

    public ChameneosMallComponent(Init init) {
        this.numMeetings = init.numMeetings;
        this.numChameneos = init.numChameneos;
        this.latch = init.latch;
        // subscriptions
        subscribe(countHandler, mallPort);
        subscribe(meetHandler, mallPort);
    }

    private Optional<UUID> waitingChameneo = Optional.empty();
    private long sumMeetings = 0l;
    private long meetingsCount = 0l;
    private int numFaded = 0;
    private boolean sentExit = false;

    final Handler<MeetingCount> countHandler = new Handler<MeetingCount>() {
        @Override
        public void handle(MeetingCount event) {
            logger.debug("Got meeting count={}", event.count);
            numFaded++;
            sumMeetings += event.count;
            if (numFaded == numChameneos) {
                latch.countDown();
                logger.info("Done!");
            }
        }
    };

    final Handler<MeetMe> meetHandler = new Handler<MeetMe>() {
        @Override
        public void handle(MeetMe event) {
            logger.debug("Got MeetMe from {}", event.chameneo);
            if (meetingsCount < numMeetings) {
                if (waitingChameneo.isPresent()) {
                    UUID other = waitingChameneo.get();
                    meetingsCount++;
                    trigger(new MeetUp(event.colour, other, event.chameneo), mallPort);
                    waitingChameneo = Optional.empty();
                } else {
                    waitingChameneo = Optional.of(event.chameneo);
                }
            } else if (!sentExit) {
                trigger(Exit.EVENT, mallPort);
                sentExit = true;// only send once
            } // else just drop, since we already sent exit to everyone
        }
    };

    public static class Init extends se.sics.kompics.Init<ChameneosMallComponent> {
        public final long numMeetings;
        public final int numChameneos;
        public final CountDownLatch latch;

        public Init(long numMeetings, int numChameneos, CountDownLatch latch) {
            this.numMeetings = numMeetings;
            this.numChameneos = numChameneos;
            this.latch = latch;
        }
    }
}
