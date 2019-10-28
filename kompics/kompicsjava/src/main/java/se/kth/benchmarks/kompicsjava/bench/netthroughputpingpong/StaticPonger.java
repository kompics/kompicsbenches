package se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;
import se.kth.benchmarks.kompicsscala.KompicsSystemProvider;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;

public class StaticPonger extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<StaticPonger> {
        public final int id;

        public Init(int id) {
            this.id = id;
        }
    }

    /*
     * Ports
     */
    private Positive<Network> net = requires(Network.class);

    private final NetAddress selfAddr;
    private final int id;

    public StaticPonger(Init init) {
        id = init.id;

        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);

        // Subscriptions
        subscribe(pingHandler, net);
    }

    private ClassMatchedHandler<StaticPing, NetMessage> pingHandler = new ClassMatchedHandler<StaticPing, NetMessage>() {

        @Override
        public void handle(StaticPing content, NetMessage context) {
            if (content.id == id) {
                trigger(context.reply(selfAddr, StaticPong.event(id)), net);
            }
        }

    };
}