package se.kth.benchmarks.kompicsjava.bench.throughputpingpong;

import se.sics.kompics.*;

public class StaticPonger extends ComponentDefinition {

    public StaticPonger() {
        // Subscriptions
        subscribe(pingHandler, ppp);
    }

    private Negative<PingPongPort> ppp = provides(PingPongPort.class);

    private Handler<StaticPing> pingHandler = new Handler<StaticPing>() {

        @Override
        public void handle(StaticPing event) {
            trigger(StaticPong.event, ppp);
        }

    };
}