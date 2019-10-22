package se.kth.benchmarks.kompicsjava.bench.pingpong;

import java.util.concurrent.CountDownLatch;

import se.sics.kompics.*;

public class Pinger extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<Pinger> {
        public final CountDownLatch latch;
        public final long count;

        public Init(CountDownLatch latch, long count) {
            this.latch = latch;
            this.count = count;
        }
    }

    private final CountDownLatch latch;
    private final long count;

    private long countDown;

    public Pinger(Init init) {
        latch = init.latch;
        count = init.count;
        countDown = count;

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(pongHandler, ppp);
    }

    private Positive<PingPongPort> ppp = requires(PingPongPort.class);

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            trigger(Ping.event, ppp);
        }

    };

    private Handler<Pong> pongHandler = new Handler<Pong>() {

        @Override
        public void handle(Pong event) {
            if (countDown > 0) {
                countDown--;
                trigger(Ping.event, ppp);
            } else {
                latch.countDown();
            }
        }

    };
}