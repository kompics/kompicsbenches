package se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;
import se.kth.benchmarks.kompicsscala.KompicsSystemProvider;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;

public class Ponger extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<Ponger> {
        public final int id;

        public Init(int id) {
            this.id = id;
        }
    }

    /*
     * Ports
     */
    private Positive<Network> net = requires(Network.class);

    private final int id;
    private final NetAddress selfAddr;

    public Ponger(Init init) {
        id = init.id;
        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);

        // Subscriptions
        subscribe(pingHandler, net);
    }

    private ClassMatchedHandler<Ping, NetMessage> pingHandler = new ClassMatchedHandler<Ping, NetMessage>() {

        @Override
        public void handle(Ping content, NetMessage context) {
            if (content.id == id) {
                trigger(context.reply(selfAddr, new Pong(content.index, id)), net);
            }
        }

    };
}