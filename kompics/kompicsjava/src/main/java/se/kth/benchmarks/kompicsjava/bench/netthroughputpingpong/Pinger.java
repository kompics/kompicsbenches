package se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong;

import java.util.concurrent.CountDownLatch;

import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.kth.benchmarks.kompics.ConfigKeys;
import se.kth.benchmarks.kompicsjava.net.*;

public class Pinger extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<Pinger> {
        public final int id;
        public final CountDownLatch latch;
        public final long count;
        public final long pipeline;
        public final NetAddress ponger;

        public Init(int id, CountDownLatch latch, long count, long pipeline, NetAddress ponger) {
            this.id = id;
            this.latch = latch;
            this.count = count;
            this.pipeline = pipeline;
            this.ponger = ponger;
        }
    }

    private final int id;
    private final CountDownLatch latch;
    private final long count;
    private final long pipeline;

    private final NetAddress ponger;
    private final NetAddress selfAddr;

    private long sentCount = 0;
    private long recvCount = 0;

    /*
     * Ports
     */
    private Positive<Network> net = requires(Network.class);

    public Pinger(Init init) {
        id = init.id;
        latch = init.latch;
        count = init.count;
        pipeline = init.pipeline;
        ponger = init.ponger;
        selfAddr = config().getValue(ConfigKeys.SELF_ADDR_KEY, NetAddress.class);

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(pongHandler, net);
    }

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            assert (selfAddr != null);
            long pipelined = 0;
            while (pipelined < pipeline && sentCount < count) {
                trigger(NetMessage.viaTCP(selfAddr, ponger, new Ping(sentCount, id)), net);
                pipelined++;
                sentCount++;
            }

        }

    };

    private ClassMatchedHandler<Pong, NetMessage> pongHandler = new ClassMatchedHandler<Pong, NetMessage>() {

        @Override
        public void handle(Pong content, NetMessage context) {
            if (content.id == id) {
                recvCount++;
                if (recvCount < count) {
                    if (sentCount < count) {
                        trigger(NetMessage.viaTCP(selfAddr, ponger, new Ping(sentCount, id)), net);
                        sentCount++;
                    }
                } else {
                    latch.countDown();
                }
            }
        }

    };
}
