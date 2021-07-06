package se.kth.benchmarks.kompicsjava.bench.sizedthroughput;

import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.kth.benchmarks.kompics.ConfigKeys;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;

public class SizedThroughputSource extends ComponentDefinition {
    private final Positive<Network> net = requires(Network.class);

    private final int id;
    private CountDownLatch latch;
    private final NetAddress selfAddr;
    private NetAddress sink;
    private int messageSize;
    private int batchSize;
    private int batchCount;
    private int ackedBatches = 0;
    private int sentBatches = 0;
    private SizedThroughputMessage msg;

    public SizedThroughputSource(Init init) {
        this.selfAddr = config().getValue(ConfigKeys.SELF_ADDR_KEY, NetAddress.class);
        this.id = init.id;
        this.messageSize = init.messageSize;
        this.batchSize = init.batchSize;
        this.batchCount = init.batchCount;
        this.sink = init.sink;
        this.latch = init.latch;
        this.msg = new SizedThroughputMessage(messageSize, id);

        subscribe(ackHandler, net);
        subscribe(startHandler, control);
    };

    private void send() {
        sentBatches++;
        for (int i = 0; i < batchSize; i++) {
            trigger(NetMessage.viaTCP(selfAddr, sink, msg), net);
        }
    };

    private Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            // Start two batches
            send();
            send();
        }
    };

    private ClassMatchedHandler<SizedThroughputSink.Ack, NetMessage> ackHandler
            = new ClassMatchedHandler<SizedThroughputSink.Ack, NetMessage>() {
        @Override
        public void handle(SizedThroughputSink.Ack content, NetMessage context) {
            if (content.id == id) {
                ackedBatches++;
                if (sentBatches < batchCount) {
                    // Keep sending
                    send();
                } else if (ackedBatches == batchCount) {
                    // Done
                    latch.countDown();
                }
            }
        }
    };

    public static class Init extends se.sics.kompics.Init<SizedThroughputSource> {
        final int messageSize;
        final int batchSize;
        final int batchCount;
        final int id;
        final NetAddress sink;
        final CountDownLatch latch;

        public Init(int messageSize, int batchSize, int batchCount, int id, NetAddress sink, CountDownLatch latch) {
            this.messageSize = messageSize;
            this.batchSize = batchSize;
            this.batchCount = batchCount;
            this.id = id;
            this.sink = sink;
            this.latch = latch;
        }
    }
}
