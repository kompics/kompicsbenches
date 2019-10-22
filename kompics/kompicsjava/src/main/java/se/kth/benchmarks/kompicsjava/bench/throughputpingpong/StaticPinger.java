package se.kth.benchmarks.kompicsjava.bench.throughputpingpong;

import java.util.concurrent.CountDownLatch;

import se.sics.kompics.*;

public class StaticPinger extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<StaticPinger> {
        public final CountDownLatch latch;
        public final long count;
        public final long pipeline;

        public Init(CountDownLatch latch, long count, long pipeline) {
            this.latch = latch;
            this.count = count;
            this.pipeline = pipeline;
        }
    }

    private final CountDownLatch latch;
    private final long count;
    private final long pipeline;

    private long sentCount = 0;
    private long recvCount = 0;

    public StaticPinger(Init init) {
        latch = init.latch;
        count = init.count;
        pipeline = init.pipeline;

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(pongHandler, ppp);
    }

    private Positive<PingPongPort> ppp = requires(PingPongPort.class);

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            long pipelined = 0;
            while (pipelined < pipeline && sentCount < count) {
                trigger(StaticPing.event, ppp);
                pipelined++;
                sentCount++;
            }
        }

    };

    private Handler<StaticPong> pongHandler = new Handler<StaticPong>() {

        @Override
        public void handle(StaticPong event) {
            recvCount++;
            if (recvCount < count) {
                if (sentCount < count) {
                    trigger(StaticPing.event, ppp);
                    sentCount++;
                }
            } else {
                latch.countDown();
            }
        }

    };
}