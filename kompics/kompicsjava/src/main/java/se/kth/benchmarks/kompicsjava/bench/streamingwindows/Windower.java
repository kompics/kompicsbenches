package se.kth.benchmarks.kompicsjava.bench.streamingwindows;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.KompicsEvent;
import se.kth.benchmarks.kompics.ConfigKeys;
import se.kth.benchmarks.kompicsjava.bench.streamingwindows.WindowerMessage.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;

public class Windower extends ComponentDefinition {
    private final Positive<Network> net = requires(Network.class);

    final int partitionId;
    final long windowSizeMS;
    final long batchSize;
    final long amplification;
    final NetAddress upstream;
    final NetAddress selfAddr;

    final long readyMark;

    private Optional<NetAddress> downstream = Optional.empty();

    private ArrayList<Long> currentWindow = new ArrayList<>();
    private long windowStartTS = 0L;

    private long receivedSinceReady = 0L;
    private boolean running = false;
    private boolean waitingOnFlushed = false;

    public Windower(Init init) {
        this.partitionId = init.partitionId;
        this.windowSizeMS = init.windowSize.toMillis();
        this.batchSize = init.batchSize;
        this.amplification = init.amplification;
        this.upstream = init.upstream;
        this.selfAddr = config().getValue(ConfigKeys.SELF_ADDR_KEY, NetAddress.class);

        this.readyMark = batchSize / 2;
        // subscriptions
        subscribe(startHandler, net);
        subscribe(stopHandler, net);
        subscribe(eventHandler, net);
        subscribe(flushHandler, net);
    }

    private ClassMatchedHandler<WindowerMessage.Start, NetMessage> startHandler = new ClassMatchedHandler<WindowerMessage.Start, NetMessage>() {

        @Override
        public void handle(WindowerMessage.Start content, NetMessage context) {
            if (content.partitionId == partitionId) {
                if (!running) {
                    logger.debug("Got Start");
                    downstream = Optional.of(context.getSource());
                    running = true;
                    receivedSinceReady = 0l;
                    sendUpstream(new Ready(partitionId, batchSize));
                } else {
                    throw new RuntimeException("Got Start while running!");
                }
            }
        }
    };
    private ClassMatchedHandler<Stop, NetMessage> stopHandler = new ClassMatchedHandler<Stop, NetMessage>() {

        @Override
        public void handle(Stop content, NetMessage context) {
            if (content.partitionId == partitionId) {
                if (running) {
                    logger.debug("Got Stop");
                    running = false;
                    currentWindow.clear();
                    downstream = Optional.empty();
                    windowStartTS = 0l;
                    if (waitingOnFlushed) {
                        sendUpstream(new Flushed(partitionId));
                        waitingOnFlushed = false;
                    }
                } else {
                    throw new RuntimeException("Got Stop while not running!");
                }
            }
        }
    };

    private ClassMatchedHandler<Event, NetMessage> eventHandler = new ClassMatchedHandler<Event, NetMessage>() {

        @Override
        public void handle(Event content, NetMessage context) {
            if (content.partitionId == partitionId) {
                if (running) {
                    if (content.ts >= (windowStartTS + windowSizeMS)) {
                        triggerWindow();
                        windowStartTS = content.ts;
                    }
                    for (long _i = 0l; _i < amplification; _i++) {
                        currentWindow.add(content.value);
                    }
                    manageReady();
                } else {
                    logger.trace("Dropping message Event, since not running!");
                }
            }
        }
    };

    private ClassMatchedHandler<Flush, NetMessage> flushHandler = new ClassMatchedHandler<Flush, NetMessage>() {

        @Override
        public void handle(Flush content, NetMessage context) {
            if (content.partitionId == partitionId) {
                if (!running) { // delay flushing response until not running
                    sendUpstream(new Flushed(partitionId));
                } else {
                    waitingOnFlushed = true;
                }
            }
        }
    };

    private void triggerWindow() {
        logger.debug("Triggering window with {} messages", currentWindow.size());
        if (currentWindow.isEmpty()) {
            logger.warn("Windows should not be empty in the benchmark!");
            return;
        }
        java.util.Collections.sort(currentWindow);
        int len = currentWindow.size();
        double median;
        if (len % 2 == 0) { // is even
            int upperMiddle = len / 2;
            int lowerMiddle = upperMiddle - 1;
            median = ((double) (currentWindow.get(lowerMiddle) + currentWindow.get(upperMiddle))) / 2.0;
        } else { // is odd
            int middle = len / 2;
            median = (double) currentWindow.get(middle);
        }
        currentWindow.clear();
        sendDownstream(new WindowAggregate(windowStartTS, partitionId, median));
    }

    private void sendUpstream(KompicsEvent event) {
        NetMessage mgs = NetMessage.viaTCP(selfAddr, upstream, event);
        trigger(mgs, net);
    }

    private void sendDownstream(KompicsEvent event) {
        NetMessage mgs = NetMessage.viaTCP(selfAddr, downstream.get(), event);
        trigger(mgs, net);
    }

    private void manageReady() {
        receivedSinceReady++;
        if (receivedSinceReady > readyMark) {
            logger.debug("Sending Ready");
            sendUpstream(new Ready(partitionId, receivedSinceReady));
            receivedSinceReady = 0l;
        }
    }

    public static class Init extends se.sics.kompics.Init<Windower> {
        public final int partitionId;
        public final Duration windowSize;
        public final long batchSize;
        public final long amplification;
        public final NetAddress upstream;

        public Init(int partitionId, Duration windowSize, long batchSize, long amplification, NetAddress upstream) {
            this.partitionId = partitionId;
            this.windowSize = windowSize;
            this.batchSize = batchSize;
            this.amplification = amplification;
            this.upstream = upstream;
        }
    }
}
