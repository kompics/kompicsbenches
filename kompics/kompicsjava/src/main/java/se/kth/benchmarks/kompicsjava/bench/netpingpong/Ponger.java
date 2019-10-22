package se.kth.benchmarks.kompicsjava.bench.netpingpong;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;
import se.kth.benchmarks.kompicsscala.KompicsSystemProvider;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;

public class Ponger extends ComponentDefinition {

    /*
     * Ports
     */
    private Positive<Network> net = requires(Network.class);

    private final NetAddress selfAddr;

    public Ponger() {

        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);

        // Subscriptions
        subscribe(pingHandler, net);
    }

    private ClassMatchedHandler<Ping, NetMessage> pingHandler = new ClassMatchedHandler<Ping, NetMessage>() {

        @Override
        public void handle(Ping content, NetMessage context) {
            trigger(context.reply(selfAddr, Pong.event), net);
        }

    };
}