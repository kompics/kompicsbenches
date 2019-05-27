package se.kth.benchmarks.kompicsjava.broadcast;

import se.kth.benchmarks.kompicsjava.bench.netpingpong.Ping;
import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;

public class BEBComp extends ComponentDefinition {
    private final NetAddress selfAddr;
    private final Positive<Network> net = requires(Network.class);
    private final Negative<BestEffortBroadcast> beb = provides(BestEffortBroadcast.class);

    public BEBComp(NetAddress selfAddr){
        this.selfAddr = selfAddr;
        subscribe(deliverHandler, net);
        subscribe(requestHandler, beb);
    }

    private final Handler<BEBRequest> requestHandler = new Handler<BEBRequest>() {
        @Override
        public void handle(BEBRequest event) {
            for (NetAddress addr: event.nodes) {
                trigger(NetMessage.viaTCP(selfAddr, addr, Ping.event), net);
            }
        }
    };

    private final Handler<NetMessage> deliverHandler = new Handler<NetMessage>() {
        @Override
        public void handle(NetMessage msg) {
            trigger(new BEBDeliver(msg.extractValue(), msg.getSource()), beb);
        }
    };
}
