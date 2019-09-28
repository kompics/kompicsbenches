package se.kth.benchmarks.kompicsjava.bench.streamingwindows;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.kth.benchmarks.kompics.ConfigKeys;
import se.kth.benchmarks.kompicsjava.bench.streamingwindows.WindowerMessage.*;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;

public class StreamSource extends ComponentDefinition {
    private final Positive<Network> net = requires(Network.class);

    private final int partitionId;
    private final Random random;
    private final NetAddress selfAddr;

    private Optional<NetAddress> downstream = Optional.empty();
    private long remaining = 0;
    private long currentTS = 0l;
    private boolean flushing = false;
    private Optional<CompletableFuture<Void>> replyOnFlushed = Optional.empty();

    public StreamSource(Init init) {
        this.partitionId = init.partitionId;
        this.random = new Random(partitionId);
        this.selfAddr = config().getValue(ConfigKeys.SELF_ADDR_KEY, NetAddress.class);

        // subscriptions
        subscribe(readyHandler, net);
        subscribe(flushedHandler, net);
        subscribe(resetHandler, loopback);
        subscribe(nextHandler, loopback);
    }

    private ClassMatchedHandler<Ready, NetMessage> readyHandler = new ClassMatchedHandler<Ready, NetMessage>() {

        @Override
        public void handle(Ready content, NetMessage context) {
            if (content.partitionId == partitionId) {
                if (!flushing) {
                    logger.debug("Got Ready");
                    remaining += content.numMessages;
                    downstream = Optional.of(context.getSource());
                    send();
                } // else drop
            }
        }
    };

    private ClassMatchedHandler<Flushed, NetMessage> flushedHandler = new ClassMatchedHandler<Flushed, NetMessage>() {

        @Override
        public void handle(Flushed content, NetMessage context) {
            if (content.partitionId == partitionId) {
                if (flushing) {
                    logger.debug("Got Flushed");
                    replyOnFlushed.get().complete(null); // null as Void instance
                    replyOnFlushed = Optional.empty();
                    flushing = false;
                } else {
                    throw new RuntimeException("Received Flushed while not flushing!");
                }
            }
        }
    };

    private Handler<Reset> resetHandler = new Handler<Reset>() {

        @Override
        public void handle(Reset event) {
            logger.debug("Got Reset");
            flushing = true;
            replyOnFlushed = Optional.of(event.replyTo);
            sendDownstream(new Flush(partitionId));
            currentTS = 0l;
            remaining = 0l;
            downstream = Optional.empty();
        }

    };

    private Handler<Next> nextHandler = new Handler<Next>() {

        @Override
        public void handle(Next _event) {
            if (!flushing) {
                send();
            } // else drop
        }

    };

    private void sendDownstream(KompicsEvent event) {
        NetMessage mgs = NetMessage.viaTCP(selfAddr, downstream.get(), event);
        trigger(mgs, net);
    }

    private void send() {
        if (remaining > 0l) {
            sendDownstream(new Event(currentTS, partitionId, random.nextLong()));
            currentTS++;
            remaining--;
            if (remaining > 0l) {
                trigger(NEXT, onSelf);
            } else {
                logger.debug("Waiting for Ready...");
            }
        }
    }

    public CompletableFuture<Void> reset() {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        trigger(new Reset(promise), onSelf);
        return promise;
    }

    static final Next NEXT = new Next();

    static class Next implements KompicsEvent {
    }

    static class Reset implements KompicsEvent {
        final CompletableFuture<Void> replyTo;

        Reset(CompletableFuture<Void> replyTo) {
            this.replyTo = replyTo;
        }
    }

    public static class Init extends se.sics.kompics.Init<StreamSource> {
        public final int partitionId;

        public Init(int partitionId) {
            this.partitionId = partitionId;
        }
    }
}
