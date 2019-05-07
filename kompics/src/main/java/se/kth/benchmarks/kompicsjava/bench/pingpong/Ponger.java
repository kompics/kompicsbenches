package se.kth.benchmarks.kompicsjava.bench.pingpong;

import se.sics.kompics.*;

public class Ponger extends ComponentDefinition {

    public Ponger() {
        // Subscriptions
        subscribe(pingHandler, ppp);
    }

    private Negative<PingPongPort> ppp = provides(PingPongPort.class);

    private Handler<Ping> pingHandler = new Handler<Ping>() {

        @Override
        public void handle(Ping event) {
            trigger(Pong.event, ppp);
        }

    };
}