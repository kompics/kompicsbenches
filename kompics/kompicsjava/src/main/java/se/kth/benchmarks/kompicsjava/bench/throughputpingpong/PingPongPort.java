package se.kth.benchmarks.kompicsjava.bench.throughputpingpong;

import se.sics.kompics.PortType;

public class PingPongPort extends PortType {{
    request(StaticPing.class);
    request(Ping.class);
    indication(StaticPong.class);
    indication(Pong.class);
}}
