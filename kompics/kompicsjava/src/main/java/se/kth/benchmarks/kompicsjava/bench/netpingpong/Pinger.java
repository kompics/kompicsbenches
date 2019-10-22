package se.kth.benchmarks.kompicsjava.bench.netpingpong;

import java.util.concurrent.CountDownLatch;

import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.kth.benchmarks.kompicsjava.net.*;
import se.kth.benchmarks.kompicsscala.KompicsSystemProvider;

public class Pinger extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<Pinger> {
        public final CountDownLatch latch;
        public final long count;
        public final NetAddress ponger;

        public Init(CountDownLatch latch, long count, NetAddress ponger) {
            this.latch = latch;
            this.count = count;
            this.ponger = ponger;
        }
    }

    private final CountDownLatch latch;
    private final long count;
    private final NetAddress ponger;
    private final NetAddress selfAddr;

    private long countDown;

    /*
     * Ports
     */
    private Positive<Network> net = requires(Network.class);

    public Pinger(Init init) {
        latch = init.latch;
        count = init.count;
        ponger = init.ponger;
        countDown = count;
        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(pongHandler, net);
    }

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            assert (selfAddr != null);
            trigger(NetMessage.viaTCP(selfAddr, ponger, Ping.event), net);
        }

    };

    private ClassMatchedHandler<Pong, NetMessage> pongHandler = new ClassMatchedHandler<Pong, NetMessage>() {

        @Override
        public void handle(Pong content, NetMessage context) {
            if (countDown > 0) {
                countDown--;
                trigger(NetMessage.viaTCP(selfAddr, ponger, Ping.event), net);
            } else {
                latch.countDown();
            }
        }

    };
}