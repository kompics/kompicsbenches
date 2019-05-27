package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import se.kth.benchmarks.kompicsjava.broadcast.BEBDeliver;
import se.kth.benchmarks.kompicsjava.broadcast.BEBRequest;
import se.kth.benchmarks.kompicsjava.broadcast.BestEffortBroadcast;
import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;

import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ReadImposeWriteConsultMajority extends ComponentDefinition {
    public static class Init extends se.sics.kompics.Init<ReadImposeWriteConsultMajority> {
        private final Collection<NetAddress> nodes;
        private final NetAddress selfAddress;
        private final int selfRank;

        public Init(int selfRank, NetAddress selfAddress, Collection<NetAddress> nodes) {
            this.selfRank = selfRank;
            this.selfAddress = selfAddress;
            this.nodes = nodes;
        }
    }

    private Negative<AtomicRegister> atomicRegister = provides(AtomicRegister.class);
    private Positive<BestEffortBroadcast> beb = requires(BestEffortBroadcast.class);
    private Positive<Network> net = requires(Network.class);

    private int selfRank;
    private final NetAddress selfAddr;
    private final Set<NetAddress> nodes;
    private final int N;
    private int ts, wr;
    private int value;
    private int acks;
    private int rid;
    private Boolean reading;
    private int readval;
    private int writeval;
    private HashMap<NetAddress, Tuple> readList;


    public ReadImposeWriteConsultMajority(Init init) {
//        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);
        selfRank = init.selfRank;
        selfAddr = init.selfAddress;
        nodes = new HashSet<>(init.nodes);
        N = nodes.size();
        subscribe(startHandler, control);
        subscribe(readRequestHandler, beb);
        subscribe(writeRequestHandler, beb);
        subscribe(ARReadHandler, atomicRegister);
        subscribe(ARWriteHandler, atomicRegister);
        subscribe(readResponseHandler, net);
        subscribe(ackHandler, net);
    }

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            acks = 0;
            rid = 0;
            reading = false;
            readList = new HashMap<>();
        }

    };

    private Handler<AR_Read_Request> ARReadHandler = new Handler<AR_Read_Request>() {

        @Override
        public void handle(AR_Read_Request event) {
            rid++;
            acks = 0;
            readList.clear();
            reading = true;
            trigger(new BEBRequest(selfAddr, nodes, new READ(rid)), beb);
        }

    };

    private Handler<AR_Write_Request> ARWriteHandler = new Handler<AR_Write_Request>() {

        @Override
        public void handle(AR_Write_Request event) {
            rid++;
            writeval = event.value;
            acks = 0;
            readList.clear();
            reading = true;
            trigger(new BEBRequest(selfAddr, nodes, new READ(rid)), beb);
        }

    };

    private ClassMatchedHandler<READ, BEBDeliver> readRequestHandler = new ClassMatchedHandler<READ, BEBDeliver>() {
        @Override
        public void handle(READ read, BEBDeliver bebDeliver) {
            trigger(NetMessage.viaTCP(selfAddr, bebDeliver.src, new VALUE(read.rid, ts, wr, value)), net);
        }
    };

    private ClassMatchedHandler<WRITE, BEBDeliver> writeRequestHandler = new ClassMatchedHandler<WRITE, BEBDeliver>() {
        @Override
        public void handle(WRITE write, BEBDeliver bebDeliver) {
            if (write.ts > ts && write.wr > wr){
                ts = write.ts;
                wr = write.wr;
                value = write.value;
            }
            trigger(NetMessage.viaTCP(selfAddr, bebDeliver.src, new ACK(write.rid)), net);
        }
    };

    private ClassMatchedHandler<VALUE, NetMessage> readResponseHandler = new ClassMatchedHandler<VALUE, NetMessage>() {
        @Override
        public void handle(VALUE v, NetMessage msg) {
            if (v.rid == rid){
                readList.put(msg.getSource(), new Tuple(v.ts, v.wr, v.value));
                if (readList.size() > N/2){
                    Tuple maxtuple = highest(readList.values());
                    int maxts = maxtuple.ts;
                    int rr = maxtuple.wr;
                    readval = maxtuple.value;
                    int bcastval;
                    readList.clear();
                    if (reading){
                        bcastval = readval;
                    } else {
                        rr = selfRank;
                        maxts++;
                        bcastval = writeval;
                    }
                    trigger(new BEBRequest(selfAddr, nodes, new WRITE(rid, maxts, rr, bcastval)), beb);
                }
            }
        }
    };

    private ClassMatchedHandler<ACK, NetMessage> ackHandler = new ClassMatchedHandler<ACK, NetMessage>() {
        @Override
        public void handle(ACK v, NetMessage msg) {
            if (v.rid == rid){
                acks++;
                if (acks > N/2){
                    acks = 0;
                    if (reading){
                        reading = false;
                        trigger(new AR_Read_Response(readval), atomicRegister);
                    } else {
                        trigger(new AR_Write_Response(), atomicRegister);
                    }
                }
            }
        }
    };


    private Tuple highest(Collection<Tuple> readListValues){
        Tuple maxtuple = readListValues.iterator().next();
        for (Tuple t : readListValues){
            if (t.ts > maxtuple.ts){
                maxtuple = t;
            }
        }
        return maxtuple;
    }

    private class Tuple{
        int ts;     // seq nr
        int wr;     // process
        int value;

        Tuple(int ts, int wr, int value){
            this.ts = ts;
            this.wr = wr;
            this.value = value;
        }

//        @Override
//        public int compareTo(Tuple t) {
//            if (this.ts > t.ts && this.wr > t.wr){
//                return 1;
//            }
//            else if (this.ts < t.ts && this.wr < t.wr){
//                return -1;
//            }
//            else return 0;
//        }
    }

    private class READ implements KompicsEvent {
        int rid;
        READ(int rid){ this.rid = rid; }
    }

    private class ACK implements KompicsEvent {
        int rid;
        ACK(int rid){ this.rid = rid; }
    }

    private class VALUE implements KompicsEvent{
        int rid, ts, wr, value;

        VALUE(int rid, int ts, int wr, int value){
            this.rid = rid;
            this.ts = ts;
            this.wr = wr;
            this.value = value;
        }
    }

    private class WRITE implements KompicsEvent{
        int rid, ts, wr, value;

        WRITE(int rid, int ts, int wr, int value){
            this.rid = rid;
            this.ts = ts;
            this.wr = wr;
            this.value = value;
        }
    }
}
