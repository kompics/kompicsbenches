package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.*;
import se.kth.benchmarks.kompicsjava.broadcast.BEBDeliver;
import se.kth.benchmarks.kompicsjava.broadcast.BEBRequest;
import se.kth.benchmarks.kompicsjava.broadcast.BestEffortBroadcast;
import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;

import se.kth.benchmarks.kompicsscala.KompicsSystemProvider;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class AtomicRegister extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<AtomicRegister> {
        private final Set<NetAddress> nodes;
        private final long num_read;
        private final long num_write;
        private final CountDownLatch latch;

        public Init(CountDownLatch latch, Set<NetAddress> nodes, long num_read, long num_write) {
            this.latch = latch;
            this.nodes = nodes;
            this.num_read = num_read;
            this.num_write = num_write;
        }
    }

    private Positive<BestEffortBroadcast> beb = requires(BestEffortBroadcast.class);
    private Positive<Network> net = requires(Network.class);

    private int selfRank;
    private final NetAddress selfAddr;
    private Set<NetAddress> nodes;
    private int N;
    private int ts, wr;
    private int value;
    private int acks;
    private int rid;
    private Boolean reading;
    private int readval;
    private int writeval;
    private HashMap<NetAddress, Tuple> readList;

    // benchmark variables
    private CountDownLatch latch;
    private long read_count;
    private long write_count;
    private int init_ack_count;
    private Random random;

    private static final int INIT_ACK = -1;
    private static final int MAX_WRITEVAL = 100;


    public AtomicRegister(Init init) {
        random = new Random();
        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);
        selfRank = selfAddr.hashCode();
        read_count = init.num_read;
        write_count = init.num_write;
        if (init.nodes != null){
            nodes = new HashSet<>(init.nodes);
            nodes.add(selfAddr);
            N = nodes.size();
        }
        subscribe(startHandler, control);
        subscribe(readRequestHandler, beb);
        subscribe(writeRequestHandler, beb);
        subscribe(readResponseHandler, net);
        subscribe(ackHandler, net);
    }

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            assert(selfAddr != null);
            acks = 0;
            rid = 0;
            reading = false;
            readList = new HashMap<>();
            if (N > 0) {
                init_ack_count = N;
                trigger(new BEBRequest(nodes, new INIT(nodes)), beb);
            }
        }

    };

    private void invokeRead(){
        rid++;
        acks = 0;
        readList.clear();
        reading = true;
        trigger(new BEBRequest(nodes, new READ(rid)), beb);
    }

    private void invokeWrite(int value){
        rid++;
        writeval = value;
        acks = 0;
        readList.clear();
        reading = true;
        trigger(new BEBRequest(nodes, new READ(rid)), beb);
    }

    private void responseRead(int read_value){
        read_count--;
        if (read_count == 0 && write_count == 0){
            latch.countDown();
        }
        else invokeWrite(random.nextInt(MAX_WRITEVAL));
    }

    private void responseWrite(){
        write_count--;
        if (read_count == 0&& write_count == 0){
            latch.countDown();
        }
        else invokeRead();

    }

    private ClassMatchedHandler<INIT, BEBDeliver> initHandler = new ClassMatchedHandler<INIT, BEBDeliver>() {
        @Override
        public void handle(INIT init, BEBDeliver bebDeliver) {
            trigger(NetMessage.viaTCP(selfAddr, bebDeliver.src, new ACK(INIT_ACK)), net);
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
                    trigger(new BEBRequest(nodes, new WRITE(rid, maxts, rr, bcastval)), beb);
                }
            }
        }
    };

    private ClassMatchedHandler<ACK, NetMessage> ackHandler = new ClassMatchedHandler<ACK, NetMessage>() {
        @Override
        public void handle(ACK a, NetMessage msg) {
            if (a.rid == INIT_ACK){
                init_ack_count--;
                if (init_ack_count == 0){
                    logger.debug("Got init ack from everybody! Starting experiment");
                    invokeWrite(random.nextInt(MAX_WRITEVAL));
                }
            }
            else if (a.rid == rid){
                acks++;
                if (acks > N/2){
                    acks = 0;
                    if (reading){
                        reading = false;
                        responseRead(readval);
                    } else {
                        responseWrite();
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
    }

}
